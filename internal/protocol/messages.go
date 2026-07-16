package protocol

// ProviderRegister is sent by a provider daemon when it connects to the relay.
type ProviderRegister struct {
	Type    string   `json:"type"`    // "register"
	Token   string   `json:"token"`   // relay auth token (hashed before storage)
	Models  []string `json:"models"`  // Ollama model tags this provider has available
	GPUType string   `json:"gpuType"` // e.g. "Apple M4 32GB"
}

// ProviderHeartbeat is sent every 30s to keep the connection alive.
type ProviderHeartbeat struct {
	Type string `json:"type"` // "heartbeat"
}

// JobRequest is forwarded by the relay to a provider for each inference job.
// The consumer's identity has been stripped; only an ephemeral job ID is included.
type JobRequest struct {
	Type    string         `json:"type"`    // "job"
	JobID   string         `json:"jobId"`   // ephemeral UUID, unknown to consumer
	Body    map[string]any `json:"body"`    // sanitized /api/chat request body
}

// JobChunk is a single NDJSON token chunk streamed back from the provider.
type JobChunk struct {
	Type  string `json:"type"`  // "chunk"
	JobID string `json:"jobId"`
	Data  string `json:"data"`  // raw NDJSON line from Ollama
	Done  bool   `json:"done"`  // true on the final chunk
}

// JobReject is sent by the provider when it cannot accept a job (e.g. GPU busy).
type JobReject struct {
	Type   string `json:"type"`   // "reject"
	JobID  string `json:"jobId"`
	Reason string `json:"reason"` // e.g. "busy"
}

// ProviderAck is sent by the relay to confirm registration.
type ProviderAck struct {
	Type    string `json:"type"`    // "ack"
	Balance int64  `json:"balance"` // token credits on this account
}
