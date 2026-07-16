# handoff-relay

Relay server and provider daemon for the [Handoff](https://github.com/handoff-org) peer GPU network.

## Overview

Two binaries:

- **`relay`** — standalone relay server. Brokers inference jobs between consumers and GPU providers over WebSocket.
- **`serve`** (`handoff-serve`) — provider daemon. Runs on a machine with a local Ollama server, registers available models, and proxies jobs when the GPU is idle.

`handoff-serve --embedded-relay` starts both in a single process — useful for local development or self-hosted setups where you don't need a separate relay machine.

## Build

```bash
go build -o relay ./cmd/relay
go build -o serve ./cmd/serve
```

Requires Go 1.22+ and CGO (for SQLite).

## Usage

### Relay server

```bash
./relay --addr :8765 --db ledger.sqlite
```

### Provider daemon

```bash
# Connect to a remote relay
./serve --token <your-token> --relay wss://relay.handoff.sh

# Self-hosted: relay + provider in one process
./serve --token <your-token> --relay ws://localhost:8765 --embedded-relay
```

### Service install (macOS)

Installs as a launchd agent that starts automatically on login:

```bash
./serve --token <your-token> --install-service
./serve --uninstall-service
```

Logs go to `/tmp/handoff-serve.log`.

## API

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check |
| `POST /register` | Issue a new token + starting balance |
| `GET /credits` | Balance for the authenticated token |
| `POST /ollama/api/chat` | Submit an inference job (streams NDJSON) |
| `WS /ws/provider` | Provider connection |

Authentication: `Authorization: Bearer <token>` on consumer endpoints.

## Deploy

See [`scripts/deploy.sh`](scripts/deploy.sh) for a Docker-based setup and [`scripts/nginx.conf`](scripts/nginx.conf) for a TLS + WebSocket reverse proxy config.

```bash
docker build -t handoff-relay .
docker run -d -p 127.0.0.1:8765:8765 -v relay-data:/data \
  handoff-relay --addr :8765 --db /data/ledger.sqlite
```

## Credits

Jobs are priced in tokens consumed (`eval_count` from Ollama's done frame). The relay settles credits between provider and consumer after each job completes.
