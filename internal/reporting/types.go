package reporting

import (
	"strings"
	"time"
)

const LegacyRuntimeTelemetryError = "worker telemetry did not include snapshot health metadata; litestream runtime fields may be stale"

const (
	RuntimeSnapshotStatusMissing   = "missing"
	RuntimeSnapshotStatusHealthy   = "healthy"
	RuntimeSnapshotStatusLegacy    = "legacy"
	RuntimeSnapshotStatusUnhealthy = "unhealthy"
)

type WorkerIdentity struct {
	WorkerID      string `json:"worker_id"`
	Name          string `json:"name"`
	Source        string `json:"source"`
	GitSHA        string `json:"git_sha"`
	LitestreamSHA string `json:"litestream_sha,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	ImageRef      string `json:"image_ref,omitempty"`
	VolumeID      string `json:"volume_id,omitempty"`
	VolumeSizeGB  string `json:"volume_size_gb,omitempty"`
	ProfileName   string `json:"profile_name"`
	ProfileConfig string `json:"profile_config,omitempty"`
	ProfileHash   string `json:"profile_hash,omitempty"`
	AppName       string `json:"app_name,omitempty"`
	MachineID     string `json:"machine_id,omitempty"`
	Region        string `json:"region,omitempty"`
}

type RuntimePayload struct {
	UptimeSeconds             float64   `json:"uptime_seconds,omitempty"`
	DataDiskTotalBytes        uint64    `json:"data_disk_total_bytes,omitempty"`
	DataDiskUsedBytes         uint64    `json:"data_disk_used_bytes,omitempty"`
	DataDiskFreeBytes         uint64    `json:"data_disk_free_bytes,omitempty"`
	DataDiskAvailableBytes    uint64    `json:"data_disk_available_bytes,omitempty"`
	DataDiskUsedPercent       float64   `json:"data_disk_used_percent,omitempty"`
	DBSizeBytes               int64     `json:"db_size_bytes,omitempty"`
	WALSizeBytes              int64     `json:"wal_size_bytes,omitempty"`
	LitestreamDirSizeBytes    int64     `json:"litestream_dir_size_bytes,omitempty"`
	LitestreamLTXSizeBytes    int64     `json:"litestream_ltx_size_bytes,omitempty"`
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

func SnapshotStatus(payload *RuntimePayload) string {
	switch {
	case payload == nil:
		return RuntimeSnapshotStatusMissing
	case payload.LitestreamSnapshotHealthy:
		return RuntimeSnapshotStatusHealthy
	case payload.LitestreamSnapshotError == LegacyRuntimeTelemetryError:
		return RuntimeSnapshotStatusLegacy
	case payload.hasSnapshotMetadata() || payload.hasLitestreamRuntimeFields():
		return RuntimeSnapshotStatusUnhealthy
	default:
		return RuntimeSnapshotStatusMissing
	}
}

type HeartbeatPayload struct {
	WorkerIdentity
	SentAt time.Time `json:"sent_at"`
	RuntimePayload
}

type VerificationPayload struct {
	WorkerIdentity
	StartedAt    time.Time             `json:"started_at"`
	CompletedAt  time.Time             `json:"completed_at"`
	CheckType    string                `json:"check_type"`
	Status       string                `json:"status"`
	Passed       bool                  `json:"passed"`
	Summary      string                `json:"summary,omitempty"`
	ErrorMessage string                `json:"error_message,omitempty"`
	DurationMS   int                   `json:"duration_ms,omitempty"`
	Steps        []VerificationStep    `json:"steps,omitempty"`
	FailureDebug *FailureDebugSnapshot `json:"failure_debug,omitempty"`
	RuntimePayload
}

type WorkerEventPayload struct {
	WorkerIdentity
	EventType    string                `json:"event_type"`
	Message      string                `json:"message,omitempty"`
	SentAt       time.Time             `json:"sent_at"`
	FailureDebug *FailureDebugSnapshot `json:"failure_debug,omitempty"`
	RuntimePayload
}

type VerificationStep struct {
	Name        string    `json:"name"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMS  int       `json:"duration_ms"`
	Status      string    `json:"status"`
	Error       string    `json:"error,omitempty"`
}

type FailureDebugSnapshot struct {
	CapturedAt          time.Time                    `json:"captured_at"`
	Reason              string                       `json:"reason,omitempty"`
	Run                 WorkerIdentity               `json:"run"`
	ProcessTable        []ProcessSnapshot            `json:"process_table,omitempty"`
	FDCounts            []ProcessFDCounts            `json:"fd_counts,omitempty"`
	SocketSummary       SocketSummary                `json:"socket_summary,omitempty"`
	Disk                DiskSnapshot                 `json:"disk,omitempty"`
	Cgroup              CgroupSnapshot               `json:"cgroup,omitempty"`
	LitestreamExit      *ProcessExitSnapshot         `json:"litestream_exit,omitempty"`
	VerificationSteps   []VerificationStep           `json:"verification_steps,omitempty"`
	ObjectStoragePrefix *ObjectStoragePrefixSnapshot `json:"object_storage_prefix,omitempty"`
	CommandOutputs      []CommandOutput              `json:"command_outputs,omitempty"`
	LitestreamLogTail   []string                     `json:"litestream_log_tail,omitempty"`
	LoadLogTail         []string                     `json:"load_log_tail,omitempty"`
}

type ProcessSnapshot struct {
	PID     int    `json:"pid"`
	PPID    int    `json:"ppid,omitempty"`
	State   string `json:"state,omitempty"`
	Command string `json:"command,omitempty"`
	Args    string `json:"args,omitempty"`
}

type ProcessFDCounts struct {
	PID     int            `json:"pid"`
	Command string         `json:"command,omitempty"`
	Total   int            `json:"total"`
	ByType  map[string]int `json:"by_type,omitempty"`
	Samples []string       `json:"samples,omitempty"`
	Error   string         `json:"error,omitempty"`
}

type SocketSummary struct {
	Path        string   `json:"path"`
	LineCount   int      `json:"line_count"`
	SampleLines []string `json:"sample_lines,omitempty"`
	Error       string   `json:"error,omitempty"`
}

type DiskSnapshot struct {
	DataDir      string            `json:"data_dir"`
	DataFiles    map[string]string `json:"data_files,omitempty"`
	DataUsage    string            `json:"data_usage,omitempty"`
	LargestPaths []string          `json:"largest_paths,omitempty"`
	Error        string            `json:"error,omitempty"`
}

type CgroupSnapshot struct {
	MemoryCurrent string            `json:"memory_current,omitempty"`
	MemoryMax     string            `json:"memory_max,omitempty"`
	CPUStat       map[string]string `json:"cpu_stat,omitempty"`
	PidsCurrent   string            `json:"pids_current,omitempty"`
	PidsMax       string            `json:"pids_max,omitempty"`
	Error         string            `json:"error,omitempty"`
}

type ProcessExitSnapshot struct {
	Process  string    `json:"process"`
	ExitedAt time.Time `json:"exited_at"`
	Error    string    `json:"error,omitempty"`
	ExitCode *int      `json:"exit_code,omitempty"`
	Signal   string    `json:"signal,omitempty"`
}

type ObjectStoragePrefixSnapshot struct {
	URL              string         `json:"url"`
	ObjectCount      int            `json:"object_count,omitempty"`
	TotalBytes       int64          `json:"total_bytes,omitempty"`
	LevelCounts      map[string]int `json:"level_counts,omitempty"`
	LatestObjects    []string       `json:"latest_objects,omitempty"`
	CommandTruncated bool           `json:"command_truncated,omitempty"`
	Error            string         `json:"error,omitempty"`
}

type CommandOutput struct {
	Name      string `json:"name"`
	Output    string `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}
