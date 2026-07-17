package relayserver_test

// Full end-to-end test: consumer registers, provider connects via WebSocket,
// consumer POSTs /ollama/api/chat, relay streams NDJSON chunks from the provider
// back to the consumer, credits are settled, and no consumer identity leaks to
// the provider side.  Run with: go test -race ./internal/relayserver/

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/handoff-org/handoff-relay/internal/ledger"
	"github.com/handoff-org/handoff-relay/internal/protocol"
	"github.com/handoff-org/handoff-relay/internal/registry"
	"github.com/handoff-org/handoff-relay/internal/relayserver"
)

func TestE2E_StreamingChatWithSettlement(t *testing.T) {
	// NDJSON lines the simulated provider will send back (Ollama wire format).
	// The done frame carries eval_count=7, so the relay should settle 7 tokens.
	ollamaLines := []string{
		`{"model":"llama3","done":false,"response":"Hello"}`,
		`{"model":"llama3","done":false,"response":" world"}`,
		`{"model":"llama3","done":true,"eval_count":7}`,
	}

	// ── 1. Start the relay server ──────────────────────────────────────────────
	l, err := ledger.Open(filepath.Join(t.TempDir(), "e2e.sqlite"))
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	reg := registry.New()
	srv := relayserver.New(reg, l)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// ── 2. Register a consumer ─────────────────────────────────────────────────
	regResp, err := http.Post(ts.URL+"/register", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /register: %v", err)
	}
	var regBody map[string]any
	if err := json.NewDecoder(regResp.Body).Decode(&regBody); err != nil {
		t.Fatalf("decode /register: %v", err)
	}
	regResp.Body.Close()
	consumerToken, _ := regBody["token"].(string)
	if consumerToken == "" {
		t.Fatal("consumer token is empty")
	}

	// ── 3. Connect a provider via WebSocket ────────────────────────────────────
	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws/provider"
	provConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial /ws/provider: %v", err)
	}

	provToken := "e2e-provider-token-abc123"
	if err := provConn.WriteJSON(protocol.ProviderRegister{
		Type:    "register",
		Token:   provToken,
		Models:  []string{"llama3"},
		GPUType: "test-gpu",
	}); err != nil {
		t.Fatalf("provider: write register: %v", err)
	}
	var ack protocol.ProviderAck
	if err := provConn.ReadJSON(&ack); err != nil {
		t.Fatalf("provider: read ack: %v", err)
	}
	if ack.Type != "ack" {
		t.Fatalf("provider: ack.Type = %q, want ack", ack.Type)
	}

	// ── 4. Provider goroutine: read one job, stream chunks, exit ───────────────
	var provWg sync.WaitGroup
	provWg.Add(1)
	go func() {
		defer provWg.Done()
		defer provConn.Close()

		var jobMsg struct {
			Type  string         `json:"type"`
			JobID string         `json:"jobId"`
			Body  map[string]any `json:"body"`
		}
		if err := provConn.ReadJSON(&jobMsg); err != nil {
			t.Errorf("provider: read job: %v", err)
			return
		}
		if jobMsg.Type != "job" {
			t.Errorf("provider: msg type = %q, want job", jobMsg.Type)
			return
		}

		// Verify the relay stripped consumer identity fields before forwarding.
		if _, leaked := jobMsg.Body["user"]; leaked {
			t.Errorf("provider: body contains 'user' field — consumer identity leaked")
		}
		if _, leaked := jobMsg.Body["session_id"]; leaked {
			t.Errorf("provider: body contains 'session_id' field — identity leaked")
		}
		// stream must be forced to true by SanitizeBody
		if jobMsg.Body["stream"] != true {
			t.Errorf("provider: body['stream'] = %v, want true", jobMsg.Body["stream"])
		}

		// Stream NDJSON chunks back to the relay.
		for i, line := range ollamaLines {
			done := i == len(ollamaLines)-1
			if err := provConn.WriteJSON(protocol.JobChunk{
				Type:  "chunk",
				JobID: jobMsg.JobID,
				Data:  line,
				Done:  done,
			}); err != nil {
				t.Errorf("provider: write chunk %d: %v", i, err)
				return
			}
		}
	}()

	// ── 5. POST /ollama/api/chat as the consumer ───────────────────────────────
	// Include "user" to confirm it is stripped before reaching the provider.
	chatReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/ollama/api/chat",
		strings.NewReader(`{"model":"llama3","messages":[],"user":"alice"}`))
	chatReq.Header.Set("Authorization", "Bearer "+consumerToken)
	chatReq.Header.Set("Content-Type", "application/json")
	chatResp, err := http.DefaultClient.Do(chatReq)
	if err != nil {
		t.Fatalf("POST /ollama/api/chat: %v", err)
	}
	defer chatResp.Body.Close()
	if chatResp.StatusCode != http.StatusOK {
		t.Fatalf("chat status = %d, want 200", chatResp.StatusCode)
	}
	// Job ID header should be present.
	if chatResp.Header.Get("X-Handoff-Job-Id") == "" {
		t.Error("response missing X-Handoff-Job-Id header")
	}

	// ── 6. Consume the streamed NDJSON response ────────────────────────────────
	scanner := bufio.NewScanner(chatResp.Body)
	var received []string
	for scanner.Scan() {
		if line := scanner.Text(); line != "" {
			received = append(received, line)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("reading chat response: %v", err)
	}

	// ── 7. Verify chunk order and content ─────────────────────────────────────
	if len(received) != len(ollamaLines) {
		t.Fatalf("received %d chunks, want %d", len(received), len(ollamaLines))
	}
	for i, want := range ollamaLines {
		if received[i] != want {
			t.Errorf("chunk[%d] = %q, want %q", i, received[i], want)
		}
	}

	// ── 8. Wait for provider goroutine to clean up ─────────────────────────────
	provWg.Wait()

	// ── 9. Verify settlement: consumer debited, provider credited ──────────────
	// Relay calls Settle(jobID, consumerHash, providerHash, evalCount=7) after the
	// channel drains. The HTTP response body EOF happens after that return, so by
	// the time we read chatResp.Body to completion the settle is committed.

	check := func(token string, wantBalance, wantEarned, wantSpent int64, who string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/credits", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s GET /credits: %v", who, err)
		}
		defer resp.Body.Close()
		var c map[string]int64
		if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
			t.Fatalf("%s decode credits: %v", who, err)
		}
		if c["balance"] != wantBalance {
			t.Errorf("%s balance = %d, want %d", who, c["balance"], wantBalance)
		}
		if c["earned"] != wantEarned {
			t.Errorf("%s earned = %d, want %d", who, c["earned"], wantEarned)
		}
		if c["spent"] != wantSpent {
			t.Errorf("%s spent = %d, want %d", who, c["spent"], wantSpent)
		}
	}

	const bonus = int64(ledger.SIGNUP_BONUS)
	const settled = int64(7) // eval_count from the done frame
	check(consumerToken, bonus-settled, bonus, settled, "consumer")
	check(provToken, bonus+settled, bonus+settled, 0, "provider")
}

// TestE2E_ProviderDisconnectMidStream verifies that when a provider closes its
// WebSocket connection while a job is in flight, the relay closes the consumer's
// response channel and the HTTP handler returns (no goroutine leak).
func TestE2E_ProviderDisconnectMidStream(t *testing.T) {
	l, err := ledger.Open(filepath.Join(t.TempDir(), "disco.sqlite"))
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	reg := registry.New()
	srv := relayserver.New(reg, l)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Register consumer.
	regResp, _ := http.Post(ts.URL+"/register", "application/json", nil)
	var regBody map[string]any
	_ = json.NewDecoder(regResp.Body).Decode(&regBody)
	regResp.Body.Close()
	consumerToken, _ := regBody["token"].(string)

	// Connect provider.
	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws/provider"
	provConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = provConn.WriteJSON(protocol.ProviderRegister{
		Type: "register", Token: "disco-token", Models: []string{"llama3"},
	})
	var ack protocol.ProviderAck
	_ = provConn.ReadJSON(&ack)

	// Provider: accept the job, send one partial chunk, then disconnect.
	var provWg sync.WaitGroup
	provWg.Add(1)
	go func() {
		defer provWg.Done()
		var jobMsg struct {
			Type  string `json:"type"`
			JobID string `json:"jobId"`
		}
		if err := provConn.ReadJSON(&jobMsg); err != nil || jobMsg.Type != "job" {
			provConn.Close()
			return
		}
		// Send one chunk, then abruptly close — simulates a crash.
		_ = provConn.WriteJSON(protocol.JobChunk{
			Type:  "chunk",
			JobID: jobMsg.JobID,
			Data:  `{"done":false,"response":"partial"}`,
			Done:  false,
		})
		provConn.Close()
	}()

	// Consumer makes request and reads the response.
	chatReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/ollama/api/chat",
		strings.NewReader(`{"model":"llama3","messages":[]}`))
	chatReq.Header.Set("Authorization", "Bearer "+consumerToken)
	chatReq.Header.Set("Content-Type", "application/json")
	chatResp, err := http.DefaultClient.Do(chatReq)
	if err != nil {
		t.Fatalf("POST chat: %v", err)
	}
	defer chatResp.Body.Close()
	if chatResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", chatResp.StatusCode)
	}

	// Drain the body — the relay should close it once the provider disconnects.
	scanner := bufio.NewScanner(chatResp.Body)
	for scanner.Scan() {
	}
	// No assertion on content; we just verify the consumer receives EOF cleanly
	// (no deadlock, no stuck goroutine).

	provWg.Wait()
}
