// Package relayserver provides the shared relay HTTP server used by both the
// standalone relay binary (cmd/relay) and the embedded-relay mode of the
// provider daemon (cmd/serve --embedded-relay).
package relayserver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/handoff-org/handoff-relay/internal/ledger"
	"github.com/handoff-org/handoff-relay/internal/protocol"
	"github.com/handoff-org/handoff-relay/internal/proxy"
	"github.com/handoff-org/handoff-relay/internal/registry"
)

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

// Server is the relay HTTP server. Create one with New, then call ListenAndServe.
type Server struct {
	reg         *registry.Registry
	ledger      *ledger.Ledger
	jobMu       sync.RWMutex
	jobChannels map[string]chan []byte
}

// New creates a relay server backed by the given registry and ledger.
func New(reg *registry.Registry, l *ledger.Ledger) *Server {
	return &Server{
		reg:         reg,
		ledger:      l,
		jobChannels: make(map[string]chan []byte),
	}
}

func tokenHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", h)
}

func (s *Server) handleProviderWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("provider upgrade: %v", err)
		return
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	var reg protocol.ProviderRegister
	if err := conn.ReadJSON(&reg); err != nil || reg.Type != "register" {
		log.Printf("provider: bad registration: %v", err)
		return
	}
	hash := tokenHash(reg.Token)

	suspended, err := s.ledger.SuspendCheck(hash)
	if err != nil {
		log.Printf("provider %s: suspend check error: %v", hash[:8], err)
	}
	if suspended {
		_ = conn.WriteJSON(map[string]string{"type": "error", "reason": "suspended"})
		return
	}

	balance, _, _, _ := s.ledger.Balance(hash)
	_ = conn.WriteJSON(protocol.ProviderAck{Type: "ack", Balance: balance})

	p := &registry.Provider{
		ID:      hash,
		Models:  reg.Models,
		GPUType: reg.GPUType,
		Conn:    conn,
	}
	s.reg.Register(p)
	defer s.reg.Unregister(hash)
	log.Printf("provider %s connected  models=%v  gpu=%s", hash[:8], reg.Models, reg.GPUType)

	for {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("provider %s disconnected: %v", hash[:8], err)
			return
		}
		var base struct {
			Type  string `json:"type"`
			JobID string `json:"jobId"`
		}
		if err := json.Unmarshal(msg, &base); err != nil {
			continue
		}
		switch base.Type {
		case "heartbeat":
		case "chunk":
			var chunk protocol.JobChunk
			if err := json.Unmarshal(msg, &chunk); err != nil {
				continue
			}
			s.jobMu.RLock()
			ch := s.jobChannels[chunk.JobID]
			s.jobMu.RUnlock()
			if ch == nil {
				continue
			}
			if chunk.Done {
				// Forward the final frame (contains eval_count) before closing.
				if chunk.Data != "" {
					ch <- []byte(chunk.Data)
				}
				close(ch)
				s.jobMu.Lock()
				delete(s.jobChannels, chunk.JobID)
				s.jobMu.Unlock()
			} else {
				ch <- []byte(chunk.Data)
			}
		case "reject":
			s.jobMu.RLock()
			ch := s.jobChannels[base.JobID]
			s.jobMu.RUnlock()
			if ch != nil {
				close(ch)
				s.jobMu.Lock()
				delete(s.jobChannels, base.JobID)
				s.jobMu.Unlock()
			}
		}
	}
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	consumerHash := tokenHash(strings.TrimPrefix(auth, "Bearer "))

	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	model, _ := body["model"].(string)
	if model == "" {
		http.Error(w, "model required", http.StatusBadRequest)
		return
	}

	provider := s.reg.Pick(model)
	if provider == nil {
		http.Error(w, fmt.Sprintf("no provider available for model %q", model), http.StatusServiceUnavailable)
		return
	}

	balance, _, _, err := s.ledger.Balance(consumerHash)
	if err != nil || balance <= 0 {
		http.Error(w, "insufficient credits", http.StatusPaymentRequired)
		return
	}

	jobID := uuid.New().String()
	ch := make(chan []byte, 128)
	s.jobMu.Lock()
	s.jobChannels[jobID] = ch
	s.jobMu.Unlock()

	cleanBody := proxy.SanitizeBody(body)
	if err := provider.Send(protocol.JobRequest{
		Type:  "job",
		JobID: jobID,
		Body:  cleanBody,
	}); err != nil {
		http.Error(w, "failed to forward job", http.StatusBadGateway)
		s.jobMu.Lock()
		delete(s.jobChannels, jobID)
		s.jobMu.Unlock()
		return
	}

	flusher, canFlush := w.(http.Flusher)
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Handoff-Job-Id", jobID)

	var evalCount int64
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-ch:
			if !ok {
				if evalCount > 0 {
					_ = s.ledger.Settle(jobID, consumerHash, provider.ID, evalCount)
				}
				return
			}
			_, _ = w.Write(line)
			_, _ = w.Write([]byte("\n"))
			if canFlush {
				flusher.Flush()
			}
			if n := proxy.ExtractEvalCount(line); n > 0 {
				evalCount = n
			}
		}
	}
}

func (s *Server) handleCredits(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	hash := tokenHash(strings.TrimPrefix(auth, "Bearer "))
	balance, earned, spent, err := s.ledger.Balance(hash)
	if err != nil {
		http.Error(w, "ledger error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int64{
		"balance": balance,
		"earned":  earned,
		"spent":   spent,
	})
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	token := uuid.New().String()
	hash := tokenHash(token)
	balance, _, _, err := s.ledger.Balance(hash)
	if err != nil {
		http.Error(w, "ledger error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token":   token,
		"balance": balance,
	})
}

func (s *Server) handleRating(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	callerHash := tokenHash(strings.TrimPrefix(auth, "Bearer "))

	var req struct {
		JobID  string `json:"jobId"`
		Rating int    `json:"rating"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Rating < 1 || req.Rating > 5 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Verify the caller is the consumer of this job (prevents rating-stuffing by providers).
	consumer, err := s.ledger.JobConsumer(req.JobID)
	if err != nil {
		http.Error(w, "ledger error", http.StatusInternalServerError)
		return
	}
	if consumer == "" {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	if consumer != callerHash {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	_ = s.ledger.Rate(req.JobID, req.Rating)
	w.WriteHeader(http.StatusNoContent)
}

// Handler returns the HTTP mux for this relay server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/provider", s.handleProviderWS)
	mux.HandleFunc("/ollama/api/chat", s.handleChat)
	mux.HandleFunc("/register", s.handleRegister)
	mux.HandleFunc("/credits", s.handleCredits)
	mux.HandleFunc("/rating", s.handleRating)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// ListenAndServe binds to addr, signals ready (if non-nil), then serves until
// ctx is cancelled. Blocks until the server has shut down.
func (s *Server) ListenAndServe(ctx context.Context, addr string, ready chan<- struct{}) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if ready != nil {
		close(ready)
	}
	httpSrv := &http.Server{
		Handler:      s.Handler(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 10 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()
	log.Printf("relay listening on %s", addr)
	if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
