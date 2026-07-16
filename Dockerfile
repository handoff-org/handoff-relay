# Stage 1: build
FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /bin/handoff-relay ./cmd/relay/

# Stage 2: minimal runtime (alpine keeps libc for cgo/sqlite3)
FROM alpine:3.19

RUN apk add --no-cache ca-certificates sqlite-libs

COPY --from=builder /bin/handoff-relay /usr/local/bin/handoff-relay

EXPOSE 8765

ENTRYPOINT ["handoff-relay"]
CMD ["--addr", ":8765", "--db", "/data/ledger.sqlite"]
