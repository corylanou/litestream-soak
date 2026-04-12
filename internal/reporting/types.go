package reporting

import (
	"strings"
	"time"
)

const LegacyRuntimeTelemetryError = "worker telemetry did not include snapshot health metadata; litestream runtime fields may be stale"

type WorkerIdentity struct {
	WorkerID      string `json:"worker_id"`
	Name          string `json:"name"`
	Source        string `json:"source"`
	GitSHA        string `json:"git_sha"`
	ProfileName   string `json:"profile_name"`
	ProfileConfig string `json:"profile_config,omitempty"`
	AppName       string `json:"app_name,omitempty"`
	MachineID     string `json:"machine_id,omitempty"`
	Region        string `json:"region,omitempty"`
}

type RuntimePayload struct {
	UptimeSeconds             float64   `json:"uptime_seconds,omitempty"`
	DBSizeBytes               int64     `json:"db_size_bytes,omitempty"`
	WALSizeBytes              int64     `json:"wal_size_bytes,omitempty"`
	DBTXID                    uint64    `json:"db_txid,omitempty"`
	DBStatus                  string    `json:"db_status,omitempty"`
	LastSyncAgeSeconds        float64   `json:"last_sync_age_seconds,omitempty"`
	LitestreamUptimeSeconds   float64   `json:"litestream_uptime_seconds,omitempty"`
	SnapshotCollectedAt       time.Time `json:"snapshot_collected_at,omitempty"`
	LitestreamSnapshotHealthy bool      `json:"litestream_snapshot_healthy"`
	LitestreamSnapshotError   string    `json:"litestream_snapshot_error,omitempty"`
}

func (p RuntimePayload) Normalize(observedAt time.Time) RuntimePayload {
	normalized := p
	hadSnapshotMetadata := normalized.hasSnapshotMetadata()
	if !hadSnapshotMetadata && normalized.hasLitestreamRuntimeFields() {
		normalized.LitestreamSnapshotError = LegacyRuntimeTelemetryError
	}
	if normalized.SnapshotCollectedAt.IsZero() && !observedAt.IsZero() {
		normalized.SnapshotCollectedAt = observedAt
	}
	if hadSnapshotMetadata {
		return normalized
	}
	return normalized
}

func (p RuntimePayload) hasSnapshotMetadata() bool {
	return !p.SnapshotCollectedAt.IsZero() || p.LitestreamSnapshotHealthy || strings.TrimSpace(p.LitestreamSnapshotError) != ""
}

func (p RuntimePayload) hasLitestreamRuntimeFields() bool {
	if p.DBTXID > 0 || p.LitestreamUptimeSeconds > 0 {
		return true
	}
	status := strings.TrimSpace(p.DBStatus)
	return status != "" && status != "unknown"
}

type HeartbeatPayload struct {
	WorkerIdentity
	SentAt time.Time `json:"sent_at"`
	RuntimePayload
}

type VerificationPayload struct {
	WorkerIdentity
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	CheckType    string    `json:"check_type"`
	Status       string    `json:"status"`
	Passed       bool      `json:"passed"`
	Summary      string    `json:"summary,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	DurationMS   int       `json:"duration_ms,omitempty"`
	RuntimePayload
}
