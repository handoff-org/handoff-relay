// Provider daemon — runs on a machine with a local Ollama server and an idle GPU.
// It connects to the relay, registers available models, and proxies inference
// jobs to the local Ollama instance when the GPU is not in active use.
//
// Usage:
//
//	handoff-serve --token <relay-token> [--relay wss://relay.handoff.sh] [--ollama http://localhost:11434]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/gorilla/websocket"
	"github.com/handoff-org/handoff-relay/internal/ledger"
	"github.com/handoff-org/handoff-relay/internal/protocol"
	"github.com/handoff-org/handoff-relay/internal/proxy"
	"github.com/handoff-org/handoff-relay/internal/registry"
	"github.com/handoff-org/handoff-relay/internal/relayserver"
)

const maxConcurrent = 1 // default: one job at a time to avoid GPU thrashing

// gpuIdleFraction returns the GPU utilization fraction (0.0–1.0) via ioreg
// on macOS. Returns 0.0 (assume idle) on any error or non-macOS platform.
func gpuIdleFraction() float64 {
	// Use class-based query so it works on all Apple Silicon generations.
	// "Device Utilization %" is nested inside PerformanceStatistics dict on the
	// same line, so we search the raw output rather than filtering with -k.
	out, err := exec.Command("ioreg", "-r", "-d", "1", "-c", "AGXAccelerator").Output()
	if err != nil {
		return 0.0
	}
	needle := `"Device Utilization %"=`
	for _, line := range strings.Split(string(out), "\n") {
		idx := strings.Index(line, needle)
		if idx < 0 {
			continue
		}
		rest := line[idx+len(needle):]
		var pct float64
		if _, err := fmt.Sscanf(rest, "%f", &pct); err == nil {
			return pct / 100.0
		}
	}
	return 0.0
}

func isIdle() bool {
	return gpuIdleFraction() < 0.10
}

func availableModels(ollamaURL string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", ollamaURL+"/api/tags", nil)
	if err != nil {
		return nil
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var data struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil
	}
	names := make([]string, 0, len(data.Models))
	for _, m := range data.Models {
		names = append(names, m.Name)
	}
	return names
}

// wsAddrToHTTP converts a WebSocket URL to an HTTP listen address.
// e.g. "ws://localhost:8765" → ":8765"
func wsAddrToHTTP(wsURL string) string {
	u, err := url.Parse(wsURL)
	if err != nil || u.Host == "" {
		return ":8765"
	}
	_, port, err := net.SplitHostPort(u.Host)
	if err != nil || port == "" {
		return ":8765"
	}
	return ":" + port
}

// startEmbeddedRelay launches a relay server inside this process and blocks
// until it is ready to accept connections before returning.
func startEmbeddedRelay(ctx context.Context, wsURL, dbPath string) error {
	httpAddr := wsAddrToHTTP(wsURL)
	l, err := ledger.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open ledger %s: %w", dbPath, err)
	}
	srv := relayserver.New(registry.New(), l)
	ready := make(chan struct{})
	go func() {
		if err := srv.ListenAndServe(ctx, httpAddr, ready); err != nil {
			log.Printf("embedded relay stopped: %v", err)
		}
	}()
	<-ready
	log.Printf("embedded relay ready on %s", httpAddr)
	return nil
}

const plistLabel = "sh.handoff.serve"

var plistTmpl = template.Must(template.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key><string>{{.Label}}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{{.BinPath}}</string>
		<string>--token</string><string>{{.Token}}</string>
		<string>--relay</string><string>{{.RelayAddr}}</string>
	</array>
	<key>RunAtLoad</key><true/>
	<key>KeepAlive</key><true/>
	<key>StandardOutPath</key><string>/tmp/handoff-serve.log</string>
	<key>StandardErrorPath</key><string>/tmp/handoff-serve.log</string>
</dict>
</plist>
`))

func plistPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", plistLabel+".plist"), nil
}

func installService(token, relayAddr string) {
	if token == "" {
		log.Fatal("--token is required for --install-service (or set HANDOFF_PEER_TOKEN)")
	}
	binPath, err := os.Executable()
	if err != nil {
		log.Fatalf("resolve binary path: %v", err)
	}
	path, err := plistPath()
	if err != nil {
		log.Fatalf("home dir: %v", err)
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("write plist: %v", err)
	}
	err = plistTmpl.Execute(f, map[string]string{
		"Label":     plistLabel,
		"BinPath":   binPath,
		"Token":     token,
		"RelayAddr": relayAddr,
	})
	f.Close()
	if err != nil {
		log.Fatalf("render plist: %v", err)
	}
	out, err := exec.Command("launchctl", "load", path).CombinedOutput()
	if err != nil {
		log.Fatalf("launchctl load: %v\n%s", err, out)
	}
	fmt.Printf("✓ handoff-serve installed and started.\n")
	fmt.Printf("  It will run automatically on login.\n")
	fmt.Printf("  Logs: /tmp/handoff-serve.log\n")
	fmt.Printf("  To stop: handoff-serve --uninstall-service\n")
}

func uninstallService() {
	path, err := plistPath()
	if err != nil {
		log.Fatalf("home dir: %v", err)
	}
	// Unload first (ignore errors — service may not be running).
	_ = exec.Command("launchctl", "unload", path).Run()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Fatalf("remove plist: %v", err)
	}
	fmt.Println("✓ handoff-serve service removed.")
}

func main() {
	token        := flag.String("token", os.Getenv("HANDOFF_PEER_TOKEN"), "relay auth token")
	relayAddr    := flag.String("relay", "wss://relay.handoff.sh", "relay WebSocket URL")
	ollamaURL    := flag.String("ollama", "http://localhost:11434", "local Ollama base URL")
	gpuType      := flag.String("gpu", "", "GPU description (auto-detected if empty)")
	installSvc   := flag.Bool("install-service", false, "install as a launchd service (macOS) and exit")
	uninstallSvc := flag.Bool("uninstall-service", false, "uninstall the launchd service and exit")
	embedded     := flag.Bool("embedded-relay", false, "start a local relay before connecting as a provider")
	relayDB      := flag.String("relay-db", "/tmp/handoff-relay.sqlite", "SQLite ledger path for embedded relay")
	flag.Parse()

	if *installSvc {
		installService(*token, *relayAddr)
		return
	}
	if *uninstallSvc {
		uninstallService()
		return
	}

	if *token == "" {
		log.Fatal("--token is required (or set HANDOFF_PEER_TOKEN)")
	}
	if *gpuType == "" {
		*gpuType = detectGPU()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *embedded {
		if err := startEmbeddedRelay(ctx, *relayAddr, *relayDB); err != nil {
			log.Fatalf("embedded relay: %v", err)
		}
	}

	// Semaphore for concurrent job limit.
	sem := make(chan struct{}, maxConcurrent)
	for i := 0; i < maxConcurrent; i++ {
		sem <- struct{}{}
	}

	for {
		if err := runLoop(ctx, *token, *relayAddr, *ollamaURL, *gpuType, sem); err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("relay connection lost: %v — reconnecting in 5s", err)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
			}
		} else {
			break
		}
	}
	log.Println("handoff-serve stopped")
}

func runLoop(ctx context.Context, token, relayAddr, ollamaURL, gpuType string, sem chan struct{}) error {
	models := availableModels(ollamaURL)
	if len(models) == 0 {
		log.Printf("warning: no models found at %s (is Ollama running?)", ollamaURL)
	}

	u, err := url.Parse(relayAddr + "/ws/provider")
	if err != nil {
		return err
	}

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial relay: %w", err)
	}
	defer conn.Close()

	// Register.
	if err := conn.WriteJSON(protocol.ProviderRegister{
		Type:    "register",
		Token:   token,
		Models:  models,
		GPUType: gpuType,
	}); err != nil {
		return fmt.Errorf("register: %w", err)
	}

	// Expect ack.
	var ack protocol.ProviderAck
	if err := conn.ReadJSON(&ack); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	log.Printf("connected to relay  models=%v  balance=%d", models, ack.Balance)

	// Heartbeat ticker.
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	connErr := make(chan error, 1)
	msgCh := make(chan []byte, 16)

	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				connErr <- err
				return
			}
			msgCh <- msg
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-connErr:
			return err
		case <-ticker.C:
			_ = conn.WriteJSON(protocol.ProviderHeartbeat{Type: "heartbeat"})
		case msg := <-msgCh:
			var base struct {
				Type  string         `json:"type"`
				JobID string         `json:"jobId"`
				Body  map[string]any `json:"body"`
			}
			if err := json.Unmarshal(msg, &base); err != nil || base.Type != "job" {
				continue
			}
			if !isIdle() {
				_ = conn.WriteJSON(protocol.JobReject{
					Type:   "reject",
					JobID:  base.JobID,
					Reason: "busy",
				})
				continue
			}
			select {
			case <-sem:
				go func(jobID string, body map[string]any) {
					defer func() { sem <- struct{}{} }()
					handleJob(ctx, conn, jobID, body, ollamaURL)
				}(base.JobID, base.Body)
			default:
				// All slots full.
				_ = conn.WriteJSON(protocol.JobReject{
					Type:   "reject",
					JobID:  base.JobID,
					Reason: "busy",
				})
			}
		}
	}
}

func handleJob(ctx context.Context, conn *websocket.Conn, jobID string, body map[string]any, ollamaURL string) {
	ch := make(chan []byte, 64)
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		evalCount, err := proxy.ProxyToOllama(jobCtx, ollamaURL, body, ch)
		close(ch)
		if err != nil && jobCtx.Err() == nil {
			log.Printf("job %s: ollama error: %v", jobID[:8], err)
		}
		_ = evalCount // relay settles from its own done-event parsing
	}()

	for line := range ch {
		chunk := protocol.JobChunk{
			Type:  "chunk",
			JobID: jobID,
			Data:  string(line),
		}
		// Check if this is the final done message.
		chunk.Done = isDone(line)
		if err := sendChunk(conn, chunk); err != nil {
			log.Printf("job %s: send chunk: %v", jobID[:8], err)
			return
		}
		if chunk.Done {
			return
		}
	}
	// Channel closed without a done frame (e.g. Ollama error) — send a synthetic done.
	_ = sendChunk(conn, protocol.JobChunk{Type: "chunk", JobID: jobID, Data: "", Done: true})
}

func isDone(line []byte) bool {
	var obj struct {
		Done bool `json:"done"`
	}
	_ = json.Unmarshal(line, &obj)
	return obj.Done
}

func sendChunk(conn *websocket.Conn, chunk protocol.JobChunk) error {
	// Use the Provider.Send mutex pattern via a local lock.
	return conn.WriteJSON(chunk)
}

func detectGPU() string {
	out, err := exec.Command("ioreg", "-r", "-d", "1", "-c", "AGXAccelerator", "-k", "model").Output()
	if err != nil || len(out) == 0 {
		return "unknown"
	}
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, `"model"`) {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		model := strings.Trim(strings.TrimSpace(parts[1]), `"`)
		if model != "" {
			return model
		}
	}
	return "unknown"
}
