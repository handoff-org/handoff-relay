package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── ExtractEvalCount ──────────────────────────────────────────────────────────

func TestExtractEvalCount_DoneFrame(t *testing.T) {
	line := []byte(`{"done":true,"eval_count":1234}`)
	if got := ExtractEvalCount(line); got != 1234 {
		t.Errorf("got %d, want 1234", got)
	}
}

func TestExtractEvalCount_NotDone(t *testing.T) {
	line := []byte(`{"done":false,"eval_count":99}`)
	if got := ExtractEvalCount(line); got != 0 {
		t.Errorf("got %d, want 0 (not a done frame)", got)
	}
}

func TestExtractEvalCount_InvalidJSON(t *testing.T) {
	if got := ExtractEvalCount([]byte("not json")); got != 0 {
		t.Errorf("got %d, want 0 for invalid JSON", got)
	}
}

func TestExtractEvalCount_MissingEvalCount(t *testing.T) {
	line := []byte(`{"done":true}`)
	if got := ExtractEvalCount(line); got != 0 {
		t.Errorf("got %d, want 0 when eval_count absent", got)
	}
}

// ── SanitizeBody ─────────────────────────────────────────────────────────────

func TestSanitizeBody_RemovesBlockedFields(t *testing.T) {
	raw := map[string]any{
		"model":      "llama3",
		"messages":   []any{"hi"},
		"user":       "alice",
		"session_id": "sess-123",
		"client_id":  "cli-abc",
	}
	clean := SanitizeBody(raw)

	for _, blocked := range []string{"user", "session_id", "client_id"} {
		if _, ok := clean[blocked]; ok {
			t.Errorf("SanitizeBody should have removed %q", blocked)
		}
	}
	if clean["model"] != "llama3" {
		t.Error("SanitizeBody should preserve model")
	}
}

func TestSanitizeBody_SetsStream(t *testing.T) {
	clean := SanitizeBody(map[string]any{"model": "llama3"})
	if clean["stream"] != true {
		t.Errorf("SanitizeBody should force stream=true, got %v", clean["stream"])
	}
}

func TestSanitizeBody_CaseInsensitiveBlock(t *testing.T) {
	raw := map[string]any{"User": "bob", "model": "x"}
	clean := SanitizeBody(raw)
	if _, ok := clean["User"]; ok {
		t.Error("SanitizeBody should block 'User' (case-insensitive)")
	}
}

func TestSanitizeBody_PreservesMessages(t *testing.T) {
	msgs := []any{map[string]any{"role": "user", "content": "hello"}}
	raw := map[string]any{"model": "llama3", "messages": msgs}
	clean := SanitizeBody(raw)

	b, _ := json.Marshal(clean["messages"])
	if string(b) == "null" {
		t.Error("SanitizeBody should preserve messages field")
	}
}

// ── ProxyToOllama ─────────────────────────────────────────────────────────────

func drainChan(ch <-chan []byte) [][]byte {
	var out [][]byte
	for line := range ch {
		out = append(out, line)
	}
	return out
}

// TestProxyToOllama_LargeNDJSONLine verifies that the 4 MB scanner buffer
// allows lines well above the 64 KB default bufio.Scanner limit.
func TestProxyToOllama_LargeNDJSONLine(t *testing.T) {
	// Build a line with a 128 KB response field — far above the 64 KB default.
	bigPayload := strings.Repeat("x", 128*1024)
	chunk1 := fmt.Sprintf(`{"done":false,"response":%q}`, bigPayload)
	chunk2 := `{"done":true,"eval_count":42}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		fmt.Fprintln(w, chunk1)
		fmt.Fprintln(w, chunk2)
	}))
	defer ts.Close()

	ch := make(chan []byte, 10)
	var chunks [][]byte
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		chunks = drainChan(ch)
	}()

	evalCount, err := ProxyToOllama(context.Background(), ts.URL, map[string]any{"model": "llama3"}, ch)
	close(ch)
	<-collectorDone

	if err != nil {
		t.Fatalf("ProxyToOllama error: %v", err)
	}
	if evalCount != 42 {
		t.Errorf("evalCount = %d, want 42", evalCount)
	}
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(chunks))
	}
	// First chunk must survive intact (not truncated at 64 KB).
	if len(chunks[0]) < 128*1024 {
		t.Errorf("first chunk len = %d, want >= 128 KB (large line test)", len(chunks[0]))
	}
}

// TestProxyToOllama_UpstreamErrorBodyForwarded verifies that when Ollama
// returns an HTTP error, the error body is forwarded as a raw line and
// ProxyToOllama itself does not return an error (passthrough behaviour).
func TestProxyToOllama_UpstreamErrorBodyForwarded(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, `{"error":"model not found"}`)
	}))
	defer ts.Close()

	ch := make(chan []byte, 10)
	var chunks [][]byte
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		chunks = drainChan(ch)
	}()

	evalCount, err := ProxyToOllama(context.Background(), ts.URL, map[string]any{"model": "bad"}, ch)
	close(ch)
	<-collectorDone

	// ProxyToOllama does not check HTTP status; it forwards body lines as-is.
	if err != nil {
		t.Fatalf("ProxyToOllama returned unexpected error: %v", err)
	}
	if evalCount != 0 {
		t.Errorf("evalCount = %d, want 0 for error response", evalCount)
	}
	if len(chunks) != 1 {
		t.Fatalf("got %d chunks, want 1 (the error body line)", len(chunks))
	}
	if !strings.Contains(string(chunks[0]), "error") {
		t.Errorf("forwarded chunk = %q, want it to contain the error body", chunks[0])
	}
}

// TestProxyToOllama_ConnectionRefused verifies that a dial failure returns an error.
func TestProxyToOllama_ConnectionRefused(t *testing.T) {
	// Listen then immediately close to get a port that is known to refuse connections.
	ch := make(chan []byte, 4)
	_, err := ProxyToOllama(context.Background(), "http://127.0.0.1:1", map[string]any{"model": "x"}, ch)
	close(ch)
	if err == nil {
		t.Error("ProxyToOllama should return an error when the upstream refuses the connection")
	}
}
