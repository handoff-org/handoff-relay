package relayserver_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/handoff-org/handoff-relay/internal/ledger"
	"github.com/handoff-org/handoff-relay/internal/registry"
	"github.com/handoff-org/handoff-relay/internal/relayserver"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	l, err := ledger.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatalf("ledger.Open: %v", err)
	}
	reg := registry.New()
	srv := relayserver.New(reg, l)
	return httptest.NewServer(srv.Handler())
}

// ── /health ───────────────────────────────────────────────────────────────────

func TestHealth(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// ── /register ────────────────────────────────────────────────────────────────

func TestRegister_IssuesTokenAndBalance(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/register", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := body["token"]; !ok {
		t.Error("response missing 'token'")
	}
	if _, ok := body["balance"]; !ok {
		t.Error("response missing 'balance'")
	}
}

func TestRegister_MethodNotAllowed(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/register")
	if err != nil {
		t.Fatalf("GET /register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

// ── /credits ─────────────────────────────────────────────────────────────────

func TestCredits_NoAuthReturnsUnauthorized(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/credits")
	if err != nil {
		t.Fatalf("GET /credits: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestCredits_AuthenticatedReturnsBalance(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	// Register to get a real token.
	regResp, err := http.Post(ts.URL+"/register", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /register: %v", err)
	}
	defer regResp.Body.Close()
	var regBody map[string]any
	if err := json.NewDecoder(regResp.Body).Decode(&regBody); err != nil {
		t.Fatalf("decode register: %v", err)
	}
	token, _ := regBody["token"].(string)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/credits", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /credits: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var credits map[string]int64
	if err := json.NewDecoder(resp.Body).Decode(&credits); err != nil {
		t.Fatalf("decode credits: %v", err)
	}
	if credits["balance"] <= 0 {
		t.Errorf("balance = %d, want > 0 (signup bonus)", credits["balance"])
	}
}

// ── /ollama/api/chat ──────────────────────────────────────────────────────────

func TestChat_NoAuthReturnsUnauthorized(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/ollama/api/chat", "application/json",
		strings.NewReader(`{"model":"llama3","messages":[]}`))
	if err != nil {
		t.Fatalf("POST /ollama/api/chat: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestChat_MethodNotAllowed(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/ollama/api/chat", nil)
	req.Header.Set("Authorization", "Bearer token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /ollama/api/chat: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestChat_NoProviderReturnsServiceUnavailable(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	// Register to get a token with balance.
	regResp, _ := http.Post(ts.URL+"/register", "application/json", nil)
	var regBody map[string]any
	_ = json.NewDecoder(regResp.Body).Decode(&regBody)
	regResp.Body.Close()
	token, _ := regBody["token"].(string)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/ollama/api/chat",
		strings.NewReader(`{"model":"llama3","messages":[]}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /ollama/api/chat: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 (no provider)", resp.StatusCode)
	}
}

func TestChat_InvalidJSONReturns400(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/ollama/api/chat",
		strings.NewReader(`not json`))
	req.Header.Set("Authorization", "Bearer sometoken")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /ollama/api/chat: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// ── /rating ───────────────────────────────────────────────────────────────────

func TestRating_MethodNotAllowed(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/rating")
	if err != nil {
		t.Fatalf("GET /rating: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestRating_NoAuthReturnsUnauthorized(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/rating", "application/json",
		strings.NewReader(`{"jobId":"x","rating":4}`))
	if err != nil {
		t.Fatalf("POST /rating: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestRating_InvalidRatingReturns400(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	// Auth check passes; rating validation happens next.
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/rating",
		strings.NewReader(`{"jobId":"x","rating":9}`))
	req.Header.Set("Authorization", "Bearer sometoken")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /rating: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (rating 9 out of range)", resp.StatusCode)
	}
}

func TestRating_UnknownJobReturnsNotFound(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/rating",
		strings.NewReader(`{"jobId":"no-such-job","rating":4}`))
	req.Header.Set("Authorization", "Bearer sometoken")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /rating: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (unknown job)", resp.StatusCode)
	}
}
