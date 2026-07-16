// Package proxy handles the WebSocket-to-HTTP proxy leg: forwarding an Ollama
// /api/chat request to a connected provider and streaming the response back.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ExtractEvalCount pulls eval_count from the final Ollama done object.
func ExtractEvalCount(line []byte) int64 {
	var obj struct {
		Done      bool  `json:"done"`
		EvalCount int64 `json:"eval_count"`
	}
	if err := json.Unmarshal(line, &obj); err != nil {
		return 0
	}
	if obj.Done {
		return obj.EvalCount
	}
	return 0
}

// SanitizeBody strips consumer-identifying fields from an /api/chat request
// and returns a clean copy safe to forward to a provider.
func SanitizeBody(raw map[string]any) map[string]any {
	clean := make(map[string]any, len(raw))
	blocked := map[string]bool{
		"user": true, "session_id": true, "client_id": true,
	}
	for k, v := range raw {
		if !blocked[strings.ToLower(k)] {
			clean[k] = v
		}
	}
	// Always stream so the relay can pipe chunks back.
	clean["stream"] = true
	return clean
}

// ProxyToOllama forwards body directly to a local Ollama server and streams
// the NDJSON response, writing lines to ch. Used by the provider daemon.
func ProxyToOllama(ctx context.Context, ollamaURL string, body map[string]any, ch chan<- []byte) (int64, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ollamaURL+"/api/chat", bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(io.LimitReader(resp.Body, 64<<20))
	var evalCount int64
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		select {
		case ch <- append([]byte{}, line...):
		case <-ctx.Done():
			return evalCount, ctx.Err()
		}
		if n := ExtractEvalCount(line); n > 0 {
			evalCount = n
		}
	}
	return evalCount, scanner.Err()
}
