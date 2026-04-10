package reporting

import "time"

type WorkerIdentity struct {
	WorkerID    string `json:"worker_id"`
	Name        string `json:"name"`
	Source      string `json:"source"`
	GitSHA      string `json:"git_sha"`
	ProfileName string `json:"profile_name"`
	AppName     string `json:"app_name,omitempty"`
	MachineID   string `json:"machine_id,omitempty"`
	Region      string `json:"region,omitempty"`
}

type HeartbeatPayload struct {
	WorkerIdentity
	SentAt                  time.Time `json:"sent_at"`
	UptimeSeconds           float64   `json:"uptime_seconds,omitempty"`
	DBSizeBytes             int64     `json:"db_size_bytes,omitempty"`
	WALSizeBytes            int64     `json:"wal_size_bytes,omitempty"`
	DBTXID                  uint64    `json:"db_txid,omitempty"`
	DBStatus                string    `json:"db_status,omitempty"`
	LastSyncAgeSeconds      float64   `json:"last_sync_age_seconds,omitempty"`
	LitestreamUptimeSeconds float64   `json:"litestream_uptime_seconds,omitempty"`
}

type VerificationPayload struct {
	WorkerIdentity
	StartedAt               time.Time `json:"started_at"`
	CompletedAt             time.Time `json:"completed_at"`
	CheckType               string    `json:"check_type"`
	Status                  string    `json:"status"`
	Passed                  bool      `json:"passed"`
	Summary                 string    `json:"summary,omitempty"`
	ErrorMessage            string    `json:"error_message,omitempty"`
	DurationMS              int       `json:"duration_ms,omitempty"`
	DBSizeBytes             int64     `json:"db_size_bytes,omitempty"`
	WALSizeBytes            int64     `json:"wal_size_bytes,omitempty"`
	DBTXID                  uint64    `json:"db_txid,omitempty"`
	DBStatus                string    `json:"db_status,omitempty"`
	LastSyncAgeSeconds      float64   `json:"last_sync_age_seconds,omitempty"`
	LitestreamUptimeSeconds float64   `json:"litestream_uptime_seconds,omitempty"`
}
