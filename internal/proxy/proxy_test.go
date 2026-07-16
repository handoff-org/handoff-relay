package proxy

import (
	"encoding/json"
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
