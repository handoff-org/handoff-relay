// Relay server — the "Uber dispatch" for the handoff peer GPU network.
//
// Providers connect on /ws/provider and register their available models.
// Consumers hit /ollama/api/chat (Ollama-compatible REST); the relay selects a
// provider, forwards the sanitized request, and streams the response back.
// Credits are tracked in a local SQLite ledger; identity is stripped before
// any request reaches a provider.
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/handoff-org/handoff-relay/internal/ledger"
	"github.com/handoff-org/handoff-relay/internal/registry"
	"github.com/handoff-org/handoff-relay/internal/relayserver"
)

func main() {
	addr   := flag.String("addr", ":8765", "listen address")
	dbPath := flag.String("db", "ledger.sqlite", "path to SQLite ledger file")
	flag.Parse()

	l, err := ledger.Open(*dbPath)
	if err != nil {
		log.Fatalf("open ledger: %v", err)
	}

	srv := relayserver.New(registry.New(), l)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("relay shutting down…")
	if err := srv.ListenAndServe(ctx, *addr, nil); err != nil {
		log.Fatalf("relay: %v", err)
	}
}
