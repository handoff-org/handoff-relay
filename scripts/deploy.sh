#!/usr/bin/env bash
# Deploy the handoff relay server.
# Run from the repo root on the VPS:  ./scripts/deploy.sh
set -euo pipefail

IMAGE="handoff-relay"
CONTAINER="handoff-relay"
DATA_DIR="${DATA_DIR:-/data/handoff}"
PORT="${PORT:-8765}"

echo "→ building image"
docker build -t "$IMAGE" .

echo "→ stopping old container (if any)"
docker rm -f "$CONTAINER" 2>/dev/null || true

echo "→ creating data dir $DATA_DIR"
mkdir -p "$DATA_DIR"

echo "→ starting container"
docker run -d \
  --restart=always \
  --name "$CONTAINER" \
  -p "127.0.0.1:${PORT}:8765" \
  -v "${DATA_DIR}:/data" \
  "$IMAGE"

echo "✓ relay running on 127.0.0.1:${PORT} (proxied via nginx)"
echo "  Logs: docker logs -f $CONTAINER"
