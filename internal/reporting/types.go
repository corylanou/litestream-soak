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

const (
	WorkerEventDiskFullNoProgress     = "platform_disk_full_no_progress"
	WorkerEventDiskFullRecovered      = "platform_disk_full_recovered"
	WorkerEventDiskFullRecoveryFailed = "platform_disk_full_recovery_failed"
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
	UptimeSeconds                   float64   `json:"uptime_seconds,omitempty"`
	DataDiskTotalBytes              uint64    `json:"data_disk_total_bytes,omitempty"`
	DataDiskUsedBytes               uint64    `json:"data_disk_used_bytes,omitempty"`
	DataDiskFreeBytes               uint64    `json:"data_disk_free_bytes,omitempty"`
	DataDiskAvailableBytes          uint64    `json:"data_disk_available_bytes,omitempty"`
	DataDiskUsedPercent             float64   `json:"data_disk_used_percent,omitempty"`
	DBSizeBytes                     int64     `json:"db_size_bytes,omitempty"`
	WALSizeBytes                    int64     `json:"wal_size_bytes,omitempty"`
	DBCount                         int       `json:"db_count,omitempty"`
	DBTotalSizeBytes                int64     `json:"db_total_size_bytes,omitempty"`
	WALTotalSizeBytes               int64     `json:"wal_total_size_bytes,omitempty"`
	LitestreamDirSizeBytes          int64     `json:"litestream_dir_size_bytes,omitempty"`
	LitestreamLTXSizeBytes          int64     `json:"litestream_ltx_size_bytes,omitempty"`
	DBTXID                          uint64    `json:"db_txid,omitempty"`
	ReplicatedTXID                  uint64    `json:"replicated_txid,omitempty"`
	DBStatus                        string    `json:"db_status,omitempty"`
	LastSyncAgeSeconds              float64   `json:"last_sync_age_seconds,omitempty"`
	LastSyncAgeP50Seconds           float64   `json:"last_sync_age_p50_seconds,omitempty"`
	LastSyncAgeP95Seconds           float64   `json:"last_sync_age_p95_seconds,omitempty"`
	LastSyncAgeMaxSeconds           float64   `json:"last_sync_age_max_seconds,omitempty"`
	ReplicationLagP95               uint64    `json:"replication_lag_p95,omitempty"`
	ReplicationLagMax               uint64    `json:"replication_lag_max,omitempty"`
	ReplicationLagOverThreshold     int       `json:"replication_lag_over_threshold,omitempty"`
	LitestreamDiskFullMetricPresent bool      `json:"litestream_disk_full_metric_present,omitempty"`
	LitestreamDiskFull              bool      `json:"litestream_disk_full,omitempty"`
	LitestreamRSSBytes              int64     `json:"litestream_rss_bytes,omitempty"`
	LitestreamCPUSecondsTotal       float64   `json:"litestream_cpu_seconds_total,omitempty"`
	LitestreamGoroutines            int       `json:"litestream_goroutines,omitempty"`
	LitestreamFDs                   int       `json:"litestream_fds,omitempty"`
	WorkerRSSBytes                  int64     `json:"worker_rss_bytes,omitempty"`
	WorkerFDs                       int       `json:"worker_fds,omitempty"`
	DiskPressureNoProgress          bool      `json:"disk_pressure_no_progress,omitempty"`
	DiskPressureNoProgressSeconds   float64   `json:"disk_pressure_no_progress_seconds,omitempty"`
	DiskFullSignalObserved          bool      `json:"disk_full_signal_observed,omitempty"`
	DiskFullSignalMessage           string    `json:"disk_full_signal_message,omitempty"`
	DiskFullRecoveryAttempted       bool      `json:"disk_full_recovery_attempted,omitempty"`
	DiskFullRecoveryFreedBytes      int64     `json:"disk_full_recovery_freed_bytes,omitempty"`
	DiskFullRecovered               bool      `json:"disk_full_recovered,omitempty"`
	DiskFullRecoverySeconds         float64   `json:"disk_full_recovery_seconds,omitempty"`
	DiskFullRecoveryWithoutRestart  bool      `json:"disk_full_recovery_without_restart,omitempty"`
	LitestreamUptimeSeconds         float64   `json:"litestream_uptime_seconds,omitempty"`
	SnapshotCollectedAt             time.Time `json:"snapshot_collected_at,omitempty"`
	LitestreamSnapshotHealthy       bool      `json:"litestream_snapshot_healthy"`
	LitestreamSnapshotError         string    `json:"litestream_snapshot_error,omitempty"`
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
	return normalized
}

func (p RuntimePayload) hasSnapshotMetadata() bool {
	return !p.SnapshotCollectedAt.IsZero() || p.LitestreamSnapshotHealthy || strings.TrimSpace(p.LitestreamSnapshotError) != ""
}

func (p RuntimePayload) hasLitestreamRuntimeFields() bool {
	if p.DBTXID > 0 || p.ReplicatedTXID > 0 || p.DBCount > 0 || p.LitestreamUptimeSeconds > 0 {
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
	StartedAt             time.Time              `json:"started_at"`
	CompletedAt           time.Time              `json:"completed_at"`
	CheckType             string                 `json:"check_type"`
	Status                string                 `json:"status"`
	Passed                bool                   `json:"passed"`
	Summary               string                 `json:"summary,omitempty"`
	ErrorMessage          string                 `json:"error_message,omitempty"`
	DurationMS            int                    `json:"duration_ms,omitempty"`
	Steps                 []VerificationStep     `json:"steps,omitempty"`
	ReplicaLevels         *ReplicaLevelReport    `json:"replica_levels,omitempty"`
	FailureClassification *FailureClassification `json:"failure_classification,omitempty"`
	FailureDebug          *FailureDebugSnapshot  `json:"failure_debug,omitempty"`
	RuntimePayload
}

type WorkerEventPayload struct {
	WorkerIdentity
	EventType          string                `json:"event_type"`
	Message            string                `json:"message,omitempty"`
	SentAt             time.Time             `json:"sent_at"`
	ActiveVerification *ActiveVerification   `json:"active_verification,omitempty"`
	FailureDebug       *FailureDebugSnapshot `json:"failure_debug,omitempty"`
	RuntimePayload
}

type ActiveVerification struct {
	StartedAt  time.Time `json:"started_at"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
	CheckType  string    `json:"check_type,omitempty"`
	Status     string    `json:"status,omitempty"`
	AgeSeconds float64   `json:"age_seconds,omitempty"`
	Stale      bool      `json:"stale,omitempty"`
}

type VerificationStep struct {
	Name            string     `json:"name"`
	StartedAt       time.Time  `json:"started_at"`
	CompletedAt     time.Time  `json:"completed_at"`
	DurationMS      int        `json:"duration_ms"`
	Status          string     `json:"status"`
	Error           string     `json:"error,omitempty"`
	Command         []string   `json:"command,omitempty"`
	DeadlineAt      *time.Time `json:"deadline_at,omitempty"`
	ExitCode        *int       `json:"exit_code,omitempty"`
	Signal          string     `json:"signal,omitempty"`
	ContextCanceled bool       `json:"context_canceled,omitempty"`
	ContextError    string     `json:"context_error,omitempty"`
	OutputTail      string     `json:"output_tail,omitempty"`
}

type ReplicaLevelReport struct {
	CapturedAt        time.Time             `json:"captured_at"`
	Command           []string              `json:"command,omitempty"`
	TargetTXID        string                `json:"target_txid,omitempty"`
	TargetTXIDDecimal uint64                `json:"target_txid_decimal,omitempty"`
	Levels            []ReplicaLevelSummary `json:"levels,omitempty"`
	OutputTail        string                `json:"output_tail,omitempty"`
	Error             string                `json:"error,omitempty"`
	Truncated         bool                  `json:"truncated,omitempty"`
}

type ReplicaLevelSummary struct {
	Level          int    `json:"level"`
	LevelName      string `json:"level_name"`
	ObjectCount    int    `json:"object_count"`
	TotalBytes     int64  `json:"total_bytes,omitempty"`
	MinTXID        string `json:"min_txid,omitempty"`
	MinTXIDDecimal uint64 `json:"min_txid_decimal,omitempty"`
	MaxTXID        string `json:"max_txid,omitempty"`
	MaxTXIDDecimal uint64 `json:"max_txid_decimal,omitempty"`
}

type FailureDebugSnapshot struct {
	CapturedAt                        time.Time                    `json:"captured_at"`
	Reason                            string                       `json:"reason,omitempty"`
	Run                               WorkerIdentity               `json:"run"`
	FailureClassification             *FailureClassification       `json:"failure_classification,omitempty"`
	SyncStatusBeforeSync              *LitestreamSyncStatus        `json:"sync_status_before_sync,omitempty"`
	SyncStatusAfterSyncFailure        *LitestreamSyncStatus        `json:"sync_status_after_sync_failure,omitempty"`
	LitestreamGoroutinesOnSyncFailure *LitestreamGoroutineSnapshot `json:"litestream_goroutines_on_sync_failure,omitempty"`
	ProcessTable                      []ProcessSnapshot            `json:"process_table,omitempty"`
	FDCounts                          []ProcessFDCounts            `json:"fd_counts,omitempty"`
	SocketSummary                     SocketSummary                `json:"socket_summary,omitempty"`
	Disk                              DiskSnapshot                 `json:"disk,omitempty"`
	Cgroup                            CgroupSnapshot               `json:"cgroup,omitempty"`
	LitestreamExit                    *ProcessExitSnapshot         `json:"litestream_exit,omitempty"`
	VerificationSteps                 []VerificationStep           `json:"verification_steps,omitempty"`
	RestorePlan                       *RestorePlanSnapshot         `json:"restore_plan,omitempty"`
	ObjectStoragePrefix               *ObjectStoragePrefixSnapshot `json:"object_storage_prefix,omitempty"`
	CommandOutputs                    []CommandOutput              `json:"command_outputs,omitempty"`
	LitestreamLogTail                 []string                     `json:"litestream_log_tail,omitempty"`
	LoadLogTail                       []string                     `json:"load_log_tail,omitempty"`
}

type RestorePlanSnapshot struct {
	CapturedAt        time.Time          `json:"captured_at"`
	Command           []string           `json:"command,omitempty"`
	TargetTXID        string             `json:"target_txid,omitempty"`
	TargetTXIDDecimal uint64             `json:"target_txid_decimal,omitempty"`
	CandidateCount    int                `json:"candidate_count,omitempty"`
	Entries           []RestorePlanEntry `json:"entries,omitempty"`
	Complete          bool               `json:"complete"`
	OutputTail        string             `json:"output_tail,omitempty"`
	Error             string             `json:"error,omitempty"`
	Truncated         bool               `json:"truncated,omitempty"`
}

type RestorePlanEntry struct {
	Level      int    `json:"level"`
	LevelName  string `json:"level_name,omitempty"`
	MinTXID    string `json:"min_txid,omitempty"`
	MaxTXID    string `json:"max_txid,omitempty"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	ObjectPath string `json:"object_path,omitempty"`
}

type LitestreamSyncStatus struct {
	CapturedAt            time.Time      `json:"captured_at"`
	DurationMS            int            `json:"duration_ms,omitempty"`
	StatusCode            int            `json:"status_code,omitempty"`
	Active                *bool          `json:"active,omitempty"`
	Operation             string         `json:"operation,omitempty"`
	Phase                 string         `json:"phase,omitempty"`
	ElapsedSeconds        *float64       `json:"elapsed_seconds,omitempty"`
	ExecutorWaiterCount   *int           `json:"executor_waiter_count,omitempty"`
	ExecutorWaitStartedAt string         `json:"executor_wait_started_at,omitempty"`
	ExecutorWaitSeconds   *float64       `json:"executor_wait_seconds,omitempty"`
	Raw                   map[string]any `json:"raw,omitempty"`
	Output                string         `json:"output,omitempty"`
	Error                 string         `json:"error,omitempty"`
	Truncated             bool           `json:"truncated,omitempty"`
}

type LitestreamGoroutineSnapshot struct {
	CapturedAt time.Time `json:"captured_at"`
	DurationMS int       `json:"duration_ms,omitempty"`
	StatusCode int       `json:"status_code,omitempty"`
	Output     string    `json:"output,omitempty"`
	Error      string    `json:"error,omitempty"`
	Truncated  bool      `json:"truncated,omitempty"`
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
	URL              string                       `json:"url"`
	ObjectCount      int                          `json:"object_count,omitempty"`
	TotalBytes       int64                        `json:"total_bytes,omitempty"`
	LevelCounts      map[string]int               `json:"level_counts,omitempty"`
	LatestObjects    []string                     `json:"latest_objects,omitempty"`
	LevelListings    []ObjectStorageLevelSnapshot `json:"level_listings,omitempty"`
	CommandTruncated bool                         `json:"command_truncated,omitempty"`
	Error            string                       `json:"error,omitempty"`
}

type ObjectStorageLevelSnapshot struct {
	Level             string   `json:"level"`
	URL               string   `json:"url"`
	DurationMS        int      `json:"duration_ms,omitempty"`
	ObjectCount       int      `json:"object_count,omitempty"`
	ObjectCountCapped bool     `json:"object_count_capped,omitempty"`
	PageCount         int      `json:"page_count,omitempty"`
	TotalBytes        int64    `json:"total_bytes,omitempty"`
	LatestObjects     []string `json:"latest_objects,omitempty"`
	TimedOut          bool     `json:"timed_out,omitempty"`
	Truncated         bool     `json:"truncated,omitempty"`
	Error             string   `json:"error,omitempty"`
}

type FailureClassification struct {
	Stage       string              `json:"stage,omitempty"`
	Signature   string              `json:"signature,omitempty"`
	ObjectStore *ObjectStoreFailure `json:"object_store,omitempty"`
	Restore     *RestoreFailure     `json:"restore,omitempty"`
}

type ObjectStoreFailure struct {
	Operation      string `json:"operation,omitempty"`
	HTTPStatus     int    `json:"http_status,omitempty"`
	APICode        string `json:"api_code,omitempty"`
	RequestID      string `json:"request_id,omitempty"`
	Bucket         string `json:"bucket,omitempty"`
	Prefix         string `json:"prefix,omitempty"`
	RedactedPrefix string `json:"redacted_prefix,omitempty"`
	Phase          string `json:"phase,omitempty"`
}

type RestoreFailure struct {
	Phase string `json:"phase,omitempty"`
}

type CommandOutput struct {
	Name      string `json:"name"`
	Output    string `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}
