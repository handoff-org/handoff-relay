// Package registry maintains the in-memory map of connected provider WebSocket
// connections, keyed by model name. Thread-safe via a RWMutex.
package registry

import (
	"sync"

	"github.com/gorilla/websocket"
)


// Provider represents a connected provider node.
type Provider struct {
	ID      string          // token SHA-256 hex (never the raw token)
	Models  []string        // advertised Ollama model tags
	GPUType string          // e.g. "Apple M4 32GB"
	Conn    *websocket.Conn // live WebSocket connection to the daemon
	mu      sync.Mutex      // guards concurrent writes on this connection
}

// Send serializes msg as JSON and writes it to the provider's WebSocket.
func (p *Provider) Send(msg any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Conn.WriteJSON(msg)
}

// Registry maps model tags to a pool of available providers.
type Registry struct {
	mu      sync.RWMutex
	byModel map[string][]*Provider // model → []Provider
	byID    map[string]*Provider   // providerID → Provider
	rr      map[string]uint64      // per-model round-robin counter
}

func New() *Registry {
	return &Registry{
		byModel: make(map[string][]*Provider),
		byID:    make(map[string]*Provider),
		rr:      make(map[string]uint64),
	}
}

// Register adds a provider to the registry.
func (r *Registry) Register(p *Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[p.ID] = p
	for _, m := range p.Models {
		r.byModel[m] = append(r.byModel[m], p)
	}
}

// Unregister removes a provider when it disconnects.
func (r *Registry) Unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.byID[id]
	if !ok {
		return
	}
	delete(r.byID, id)
	for _, m := range p.Models {
		pool := r.byModel[m]
		for i, pp := range pool {
			if pp.ID == id {
				r.byModel[m] = append(pool[:i], pool[i+1:]...)
				break
			}
		}
		if len(r.byModel[m]) == 0 {
			delete(r.byModel, m)
		}
	}
}

// Pick returns an available provider for the given model using round-robin, or nil if none.
func (r *Registry) Pick(model string) *Provider {
	r.mu.Lock()
	defer r.mu.Unlock()
	pool := r.byModel[model]
	if len(pool) == 0 {
		return nil
	}
	idx := r.rr[model] % uint64(len(pool))
	r.rr[model]++
	return pool[idx]
}

// Count returns the number of providers serving a given model.
func (r *Registry) Count(model string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byModel[model])
}
