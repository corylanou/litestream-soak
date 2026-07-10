package worker

import (
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/workload"
)

type Config struct {
	WorkerID      string
	WorkerName    string
	GitSHA        string
	LitestreamSHA string
	RunID         string
	ImageRef      string
	VolumeID      string
	VolumeSizeGB  string
	Source        string // "main" or "pr"
	AppName       string
	MachineID     string
	Region        string

	// Paths
	DataDir    string
	DBPath     string
	ConfigPath string
	SocketPath string

	// Profile
	ProfileName string
	WriteRate   int
	Pattern     string
	PayloadSize int
	ReadRatio   float64
	Workers     int
	InitialSize string

	NumDatabases            int
	ActivePercent           float64
	ActiveRotateInterval    time.Duration
	ActiveSetSeed           int64
	ConfigMode              string
	VerifySampleSize        int
	VerifyChangedLimit      int
	ReplicationLagThreshold uint64

	// Load duration — set very high for continuous operation.
	LoadDuration time.Duration

	// Verification
	VerifyInterval           time.Duration
	MonitorInterval          time.Duration
	VerifyType               string // quick, integrity, checksum, full
	VerifySyncDegradedAfter  time.Duration
	VerifySyncTimeout        time.Duration
	DiskFullNoProgressWindow time.Duration
	DiskFullRecoveryReserve  int64
	DiskFullRecoveryTimeout  time.Duration

	// Replica config
	ReplicaType string // "file" or "s3"
	ReplicaPath string // for file:// replicas (local directory)

	// S3/Tigris config (only used when ReplicaType == "s3")
	S3Bucket                                  string
	S3Endpoint                                string
	S3AccessKey                               string
	S3SecretKey                               string
	S3Path                                    string
	S3PartSize                                string
	S3Concurrency                             int
	S3FaultProxyEnabled                       bool
	S3FaultProxyEndpoint                      string
	S3FaultProxyMode                          string
	S3FaultProxyTargetEndpoint                string
	S3FaultProxyListenAddr                    string
	S3FaultProxyMinContentLength              int64
	S3FaultProxyResetAfterBytes               int64
	S3FaultProxyFailFirstAttempts             int
	S3FaultProxyMaxFailures                   int
	S3FaultProxySourceLevel                   string
	S3FaultProxyRequireObservedSourceGet      bool
	S3FaultProxyRequireObservedSourceRangeGet bool
	ReplicaLevelReporting                     bool

	// Litestream config
	SnapshotInterval         time.Duration
	SyncInterval             time.Duration
	L1CompactionInterval     time.Duration
	L2CompactionInterval     time.Duration
	L3CompactionInterval     time.Duration
	L0Retention              time.Duration
	L0RetentionCheckInterval time.Duration
	TruncatePageN            *int

	// Pinned reader: long-lived read transactions that pin the WAL
	PinnedReaderHold  time.Duration
	PinnedReaderPause time.Duration

	// Replay
	LoadMode       string  // "synthetic", "replay", "both"
	ReplayDataset  string  // "taxi", "gharchive", "orders"
	ReplayDataPath string  // path to dataset file
	ReplayDataURL  string  // remote dataset URL downloaded on demand
	ReplaySpeed    float64 // speed multiplier (1.0 = real-time)
	ReplayLoop     bool

	// Metrics
	MetricsAddr string

	// Control plane
	ControlBaseURL string
}

func DefaultConfig() Config {
	return Config{
		WorkerID:   "worker-1",
		WorkerName: "worker-1",
		Source:     "main",

		DataDir:    "/data",
		DBPath:     "/data/test.db",
		ConfigPath: "/data/litestream.yml",
		SocketPath: "/data/litestream.sock",

		ProfileName: "low-volume",
		WriteRate:   10,
		Pattern:     "constant",
		PayloadSize: 1024,
		ReadRatio:   0.2,
		Workers:     1,
		InitialSize: "5MB",

		ActivePercent:           2,
		ActiveRotateInterval:    30 * time.Minute,
		ActiveSetSeed:           1,
		ConfigMode:              "list",
		VerifySampleSize:        5,
		VerifyChangedLimit:      100,
		ReplicationLagThreshold: 0,

		LoadDuration: 87600 * time.Hour, // ~10 years

		LoadMode:    "synthetic",
		ReplaySpeed: 10.0,
		ReplayLoop:  true,

		VerifyInterval:           30 * time.Minute,
		MonitorInterval:          15 * time.Second,
		VerifyType:               "integrity",
		VerifySyncDegradedAfter:  5 * time.Minute,
		VerifySyncTimeout:        15 * time.Minute,
		DiskFullNoProgressWindow: 10 * time.Minute,
		DiskFullRecoveryTimeout:  10 * time.Minute,

		ReplicaType: "file",
		ReplicaPath: "/data/replicas",

		S3Endpoint: "https://fly.storage.tigris.dev",
		S3Path:     "soak",

		S3FaultProxyListenAddr:        "127.0.0.1:19000",
		S3FaultProxyMode:              "uploadpart-reset",
		S3FaultProxyMinContentLength:  8 * 1024 * 1024,
		S3FaultProxyResetAfterBytes:   2 * 1024 * 1024,
		S3FaultProxyFailFirstAttempts: 2,
		S3FaultProxySourceLevel:       "0001",

		SnapshotInterval: 10 * time.Minute,
		SyncInterval:     1 * time.Second,

		MetricsAddr: ":9091",
	}
}

func applyS3FaultProfileBase(c *Config) {
	c.WriteRate = 750
	c.Pattern = "wave"
	c.PayloadSize = 32768
	c.ReadRatio = 0.1
	c.Workers = 8
	c.InitialSize = "256MB"
	c.S3PartSize = "8MB"
	c.S3Concurrency = 2
	c.S3FaultProxyEnabled = true
	c.ReplicaLevelReporting = true
}

func applyCompactionSourceStreamDropProfile(c *Config) {
	applyS3FaultProfileBase(c)
	c.S3FaultProxyMode = "source-get-reset"
	c.S3FaultProxyMinContentLength = 1
	c.S3FaultProxyResetAfterBytes = 64 * 1024
	c.S3FaultProxyFailFirstAttempts = 2
	c.S3FaultProxySourceLevel = "0001"
	c.S3FaultProxyRequireObservedSourceGet = true
	c.S3FaultProxyRequireObservedSourceRangeGet = true
}

func applyUploadPartRetryQuotaProfile(c *Config) {
	applyS3FaultProfileBase(c)
	c.S3Concurrency = 1
	c.S3FaultProxyMode = "uploadpart-reset"
	c.S3FaultProxyMinContentLength = 8 * 1024 * 1024
	c.S3FaultProxyResetAfterBytes = 2 * 1024 * 1024
	c.S3FaultProxyFailFirstAttempts = 3
	c.S3FaultProxyMaxFailures = 51
}

func applyProviderHTTP408Profile(c *Config) {
	applyS3FaultProfileBase(c)
	c.S3Concurrency = 1
	c.S3FaultProxyMode = "provider-http-408"
	c.S3FaultProxyMinContentLength = 0
	c.S3FaultProxyResetAfterBytes = 1
	c.S3FaultProxyFailFirstAttempts = 1
	c.S3FaultProxyMaxFailures = 1
}

func applyManyDBProfileBase(c *Config, numDatabases, workers int, configMode string) {
	c.LoadMode = "many-db"
	c.WriteRate = 20
	c.Pattern = "constant"
	c.PayloadSize = 512
	c.Workers = workers
	c.NumDatabases = numDatabases
	c.ActivePercent = 2
	c.ConfigMode = configMode
	c.VerifySampleSize = 5
	// The S3 observe proxy is opt-in until it re-signs forwarded requests:
	// it currently forwards litestream's Host header, which Tigris resolves
	// as bucket "127.0.0.1" (NoSuchBucket) — see issue #146.
	c.S3FaultProxyFailFirstAttempts = 0
}

func applyProviderRequestCanceledProfile(c *Config) {
	applyS3FaultProfileBase(c)
	c.S3Concurrency = 1
	c.S3FaultProxyMode = "provider-request-canceled"
	c.S3FaultProxyMinContentLength = 0
	c.S3FaultProxyResetAfterBytes = 1
	c.S3FaultProxyFailFirstAttempts = 1
	c.S3FaultProxyMaxFailures = 1
}

func ConfigFromEnv() (Config, error) {
	c := DefaultConfig()

	if v := os.Getenv("WORKER_ID"); v != "" {
		c.WorkerID = v
	}
	if v := os.Getenv("WORKER_NAME"); v != "" {
		c.WorkerName = v
	}
	if v := os.Getenv("GIT_SHA"); v != "" {
		c.GitSHA = v
	}
	c.LitestreamSHA = resolveLitestreamSHA()
	if v := os.Getenv("SOAK_RUN_ID"); v != "" {
		c.RunID = v
	}
	if v := os.Getenv("SOAK_IMAGE_REF"); v != "" {
		c.ImageRef = v
	}
	if v := os.Getenv("SOAK_VOLUME_ID"); v != "" {
		c.VolumeID = v
	}
	if v := os.Getenv("SOAK_VOLUME_SIZE_GB"); v != "" {
		c.VolumeSizeGB = v
	}
	if v := os.Getenv("SOURCE"); v != "" {
		c.Source = v
	}
	if v := os.Getenv("FLY_APP_NAME"); v != "" {
		c.AppName = v
	}
	if v := os.Getenv("FLY_MACHINE_ID"); v != "" {
		c.MachineID = v
	}
	if v := os.Getenv("FLY_REGION"); v != "" {
		c.Region = v
	}
	if v := os.Getenv("DATA_DIR"); v != "" {
		c.DataDir = v
		c.DBPath = v + "/test.db"
		c.ConfigPath = v + "/litestream.yml"
		c.SocketPath = v + "/litestream.sock"
	}

	if v := os.Getenv("PROFILE"); v != "" {
		c.ProfileName = v
		switch v {
		case "low-volume":
			c.WriteRate = 10
			c.Pattern = "constant"
			c.PayloadSize = 1024
			c.Workers = 1
			c.InitialSize = "5MB"
		case "high-volume":
			c.WriteRate = 500
			c.Pattern = "wave"
			c.PayloadSize = 4096
			c.Workers = 8
			c.InitialSize = "50MB"
		case "compaction-source-stream-drop":
			applyCompactionSourceStreamDropProfile(&c)
		case "uploadpart-retry-quota", "s3-flap":
			applyUploadPartRetryQuotaProfile(&c)
		case "provider-408-requestcanceled":
			applyProviderHTTP408Profile(&c)
		case "provider-http-408":
			applyProviderHTTP408Profile(&c)
		case "provider-request-canceled":
			applyProviderRequestCanceledProfile(&c)
		case "burst-volume":
			c.WriteRate = 1000
			c.Pattern = "burst"
			c.PayloadSize = 2048
			c.Workers = 4
			c.InitialSize = "20MB"
		case "read-heavy":
			c.WriteRate = 80
			c.Pattern = "constant"
			c.PayloadSize = 512
			c.ReadRatio = 0.95
			c.Workers = 6
			c.InitialSize = "10MB"
		case "constrained-disk":
			c.WriteRate = 40
			c.Pattern = "constant"
			c.PayloadSize = 4096
			c.ReadRatio = 0.2
			c.Workers = 2
			c.InitialSize = "420MB"
			c.MonitorInterval = time.Second
			c.VerifyInterval = 5 * time.Minute
			c.VerifyType = "integrity"
			c.VerifySyncDegradedAfter = time.Minute
			c.VerifySyncTimeout = 3 * time.Minute
			c.DiskFullNoProgressWindow = 7 * time.Second
			c.DiskFullRecoveryReserve = 300 * 1024 * 1024
			c.DiskFullRecoveryTimeout = 5 * time.Minute
			c.SnapshotInterval = 2 * time.Minute
			c.SyncInterval = time.Second
		case "many-dbs-100-list":
			applyManyDBProfileBase(&c, 100, 2, "list")
		case "many-dbs-100-dir":
			applyManyDBProfileBase(&c, 100, 2, "dir")
		case "many-dbs-500-list":
			applyManyDBProfileBase(&c, 500, 3, "list")
		case "many-dbs-500-dir":
			applyManyDBProfileBase(&c, 500, 3, "dir")
		case "many-dbs-500-dir-lowfreq":
			// Values must stay in sync with manyDB500FleetWorkers in internal/orchestrator/fleet.go.
			applyManyDBProfileBase(&c, 500, 3, "dir")
			c.SnapshotInterval = time.Hour
			c.L1CompactionInterval = 5 * time.Minute
			c.L2CompactionInterval = 30 * time.Minute
			c.L3CompactionInterval = 6 * time.Hour
			c.L0Retention = time.Hour
			c.L0RetentionCheckInterval = 2 * time.Minute
		case "many-dbs-1000-dir":
			applyManyDBProfileBase(&c, 1000, 4, "dir")
		}
	}

	if v := os.Getenv("WRITE_RATE"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &c.WriteRate); err != nil {
			return c, fmt.Errorf("invalid WRITE_RATE: %w", err)
		}
	}
	if v := os.Getenv("PATTERN"); v != "" {
		c.Pattern = v
	}
	if v := os.Getenv("PAYLOAD_SIZE"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &c.PayloadSize); err != nil {
			return c, fmt.Errorf("invalid PAYLOAD_SIZE: %w", err)
		}
	}
	if v := os.Getenv("READ_RATIO"); v != "" {
		if _, err := fmt.Sscanf(v, "%f", &c.ReadRatio); err != nil {
			return c, fmt.Errorf("invalid READ_RATIO: %w", err)
		}
	}
	if v := os.Getenv("LOAD_WORKERS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &c.Workers); err != nil {
			return c, fmt.Errorf("invalid LOAD_WORKERS: %w", err)
		}
	}
	if v := os.Getenv("INITIAL_SIZE"); v != "" {
		c.InitialSize = v
	}
	if v := os.Getenv("NUM_DATABASES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid NUM_DATABASES: %w", err)
		}
		c.NumDatabases = n
		if c.NumDatabases < 0 {
			return c, fmt.Errorf("invalid NUM_DATABASES: must be non-negative")
		}
	}
	if v := os.Getenv("ACTIVE_PERCENT"); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return c, fmt.Errorf("invalid ACTIVE_PERCENT: %w", err)
		}
		c.ActivePercent = n
		if math.IsNaN(c.ActivePercent) || c.ActivePercent < 0 || c.ActivePercent > 100 {
			return c, fmt.Errorf("invalid ACTIVE_PERCENT: must be between 0 and 100")
		}
	}
	if v := os.Getenv("ACTIVE_ROTATE_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid ACTIVE_ROTATE_INTERVAL: %w", err)
		}
		if d <= 0 {
			return c, fmt.Errorf("invalid ACTIVE_ROTATE_INTERVAL: must be positive")
		}
		c.ActiveRotateInterval = d
	}
	if v := strings.TrimSpace(os.Getenv("ACTIVE_SET_SEED")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return c, fmt.Errorf("invalid ACTIVE_SET_SEED: %w", err)
		}
		c.ActiveSetSeed = n
	}
	if v := strings.TrimSpace(os.Getenv("CONFIG_MODE")); v != "" {
		c.ConfigMode = v
	}
	if v := os.Getenv("VERIFY_SAMPLE_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid VERIFY_SAMPLE_SIZE: %w", err)
		}
		c.VerifySampleSize = n
		if c.VerifySampleSize <= 0 {
			return c, fmt.Errorf("invalid VERIFY_SAMPLE_SIZE: must be positive")
		}
	}
	if v := os.Getenv("VERIFY_CHANGED_LIMIT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid VERIFY_CHANGED_LIMIT: %w", err)
		}
		if n <= 0 {
			return c, fmt.Errorf("invalid VERIFY_CHANGED_LIMIT: must be positive")
		}
		c.VerifyChangedLimit = n
	}
	if v := os.Getenv("REPLICATION_LAG_THRESHOLD"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return c, fmt.Errorf("invalid REPLICATION_LAG_THRESHOLD: %w", err)
		}
		c.ReplicationLagThreshold = n
	}

	if v := os.Getenv("VERIFY_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid VERIFY_INTERVAL: %w", err)
		}
		c.VerifyInterval = d
	}
	if v := os.Getenv("MONITOR_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid MONITOR_INTERVAL: %w", err)
		}
		if d <= 0 {
			return c, fmt.Errorf("invalid MONITOR_INTERVAL: must be positive")
		}
		c.MonitorInterval = d
	}
	if v := os.Getenv("VERIFY_TYPE"); v != "" {
		c.VerifyType = v
	}
	if v := os.Getenv("VERIFY_SYNC_DEGRADED_AFTER"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid VERIFY_SYNC_DEGRADED_AFTER: %w", err)
		}
		c.VerifySyncDegradedAfter = d
	}
	if v := os.Getenv("VERIFY_SYNC_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid VERIFY_SYNC_TIMEOUT: %w", err)
		}
		c.VerifySyncTimeout = d
	}
	if v := os.Getenv("DISK_FULL_NO_PROGRESS_WINDOW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid DISK_FULL_NO_PROGRESS_WINDOW: %w", err)
		}
		c.DiskFullNoProgressWindow = d
	}
	if v := os.Getenv("DISK_FULL_RECOVERY_RESERVE_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return c, fmt.Errorf("invalid DISK_FULL_RECOVERY_RESERVE_BYTES: %w", err)
		}
		if n < 0 {
			return c, fmt.Errorf("invalid DISK_FULL_RECOVERY_RESERVE_BYTES: must be non-negative")
		}
		c.DiskFullRecoveryReserve = n
	}
	if v := os.Getenv("DISK_FULL_RECOVERY_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid DISK_FULL_RECOVERY_TIMEOUT: %w", err)
		}
		c.DiskFullRecoveryTimeout = d
	}

	if v := os.Getenv("REPLICA_TYPE"); v != "" {
		c.ReplicaType = v
	}
	if v := os.Getenv("REPLICA_PATH"); v != "" {
		c.ReplicaPath = v
	}
	if v := os.Getenv("S3_BUCKET"); v != "" {
		c.S3Bucket = v
	}
	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		c.S3Endpoint = v
	}
	if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
		c.S3AccessKey = v
	}
	if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
		c.S3SecretKey = v
	}
	if v := os.Getenv("S3_PATH"); v != "" {
		c.S3Path = v
	}
	if v := strings.TrimSpace(os.Getenv("LITESTREAM_S3_PART_SIZE")); v != "" {
		c.S3PartSize = v
	}
	if v := os.Getenv("LITESTREAM_S3_CONCURRENCY"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &c.S3Concurrency); err != nil {
			return c, fmt.Errorf("invalid LITESTREAM_S3_CONCURRENCY: %w", err)
		}
		if c.S3Concurrency <= 0 {
			return c, fmt.Errorf("invalid LITESTREAM_S3_CONCURRENCY: must be positive")
		}
	}
	if parseBoolEnv(os.Getenv("S3_FAULT_PROXY_ENABLED")) {
		c.S3FaultProxyEnabled = true
	}
	if parseBoolEnv(os.Getenv("S3_OBSERVE_PROXY_ENABLED")) {
		c.S3FaultProxyEnabled = true
		c.S3FaultProxyMode = "observe"
	}
	if v := strings.TrimSpace(os.Getenv("S3_FAULT_PROXY_TARGET_ENDPOINT")); v != "" {
		c.S3FaultProxyTargetEndpoint = v
	}
	if v := strings.TrimSpace(os.Getenv("S3_FAULT_PROXY_MODE")); v != "" {
		c.S3FaultProxyMode = v
	}
	if v := strings.TrimSpace(os.Getenv("S3_FAULT_PROXY_LISTEN_ADDR")); v != "" {
		c.S3FaultProxyListenAddr = v
	}
	if v := strings.TrimSpace(os.Getenv("S3_FAULT_PROXY_MIN_CONTENT_LENGTH")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return c, fmt.Errorf("invalid S3_FAULT_PROXY_MIN_CONTENT_LENGTH: %w", err)
		}
		if n < 0 {
			return c, fmt.Errorf("invalid S3_FAULT_PROXY_MIN_CONTENT_LENGTH: must be non-negative")
		}
		c.S3FaultProxyMinContentLength = n
	}
	if v := strings.TrimSpace(os.Getenv("S3_FAULT_PROXY_RESET_AFTER_BYTES")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return c, fmt.Errorf("invalid S3_FAULT_PROXY_RESET_AFTER_BYTES: %w", err)
		}
		if n <= 0 {
			return c, fmt.Errorf("invalid S3_FAULT_PROXY_RESET_AFTER_BYTES: must be positive")
		}
		c.S3FaultProxyResetAfterBytes = n
	}
	if v := strings.TrimSpace(os.Getenv("S3_FAULT_PROXY_FAIL_FIRST_ATTEMPTS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid S3_FAULT_PROXY_FAIL_FIRST_ATTEMPTS: %w", err)
		}
		if n < 0 {
			return c, fmt.Errorf("invalid S3_FAULT_PROXY_FAIL_FIRST_ATTEMPTS: must be non-negative")
		}
		c.S3FaultProxyFailFirstAttempts = n
	}
	if v := strings.TrimSpace(os.Getenv("S3_FAULT_PROXY_MAX_FAILURES")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid S3_FAULT_PROXY_MAX_FAILURES: %w", err)
		}
		if n < 0 {
			return c, fmt.Errorf("invalid S3_FAULT_PROXY_MAX_FAILURES: must be non-negative")
		}
		c.S3FaultProxyMaxFailures = n
	}
	if v := strings.TrimSpace(os.Getenv("S3_FAULT_PROXY_SOURCE_LEVEL")); v != "" {
		c.S3FaultProxySourceLevel = v
	}
	if parseBoolEnv(os.Getenv("S3_FAULT_PROXY_REQUIRE_OBSERVED_SOURCE_GET")) {
		c.S3FaultProxyRequireObservedSourceGet = true
	}
	if parseBoolEnv(os.Getenv("S3_FAULT_PROXY_REQUIRE_OBSERVED_SOURCE_RANGE_GET")) {
		c.S3FaultProxyRequireObservedSourceRangeGet = true
	}
	if c.S3FaultProxyEnabled && c.s3FaultProxyObserveMode() {
		c.S3FaultProxyFailFirstAttempts = 0
		c.S3FaultProxyMaxFailures = 0
		c.S3FaultProxyRequireObservedSourceGet = false
		c.S3FaultProxyRequireObservedSourceRangeGet = false
	}
	if parseBoolEnv(os.Getenv("REPLICA_LEVEL_REPORTING")) {
		c.ReplicaLevelReporting = true
	}

	if v := os.Getenv("SNAPSHOT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid SNAPSHOT_INTERVAL: %w", err)
		}
		c.SnapshotInterval = d
	}
	if v := os.Getenv("SYNC_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid SYNC_INTERVAL: %w", err)
		}
		c.SyncInterval = d
	}
	if v := os.Getenv("L1_COMPACTION_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid L1_COMPACTION_INTERVAL: %w", err)
		}
		c.L1CompactionInterval = d
	}
	if v := os.Getenv("L2_COMPACTION_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid L2_COMPACTION_INTERVAL: %w", err)
		}
		c.L2CompactionInterval = d
	}
	if v := os.Getenv("L3_COMPACTION_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid L3_COMPACTION_INTERVAL: %w", err)
		}
		c.L3CompactionInterval = d
	}
	if v := os.Getenv("L0_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid L0_RETENTION: %w", err)
		}
		c.L0Retention = d
	}
	if v := os.Getenv("L0_RETENTION_CHECK_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid L0_RETENTION_CHECK_INTERVAL: %w", err)
		}
		c.L0RetentionCheckInterval = d
	}
	if v := strings.TrimSpace(os.Getenv("TRUNCATE_PAGE_N")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid TRUNCATE_PAGE_N: %w", err)
		}
		if n < 0 {
			return c, fmt.Errorf("invalid TRUNCATE_PAGE_N: must be non-negative")
		}
		c.TruncatePageN = &n
	}
	if v := os.Getenv("PINNED_READER_HOLD"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid PINNED_READER_HOLD: %w", err)
		}
		if d < 0 {
			return c, fmt.Errorf("invalid PINNED_READER_HOLD: must be non-negative")
		}
		c.PinnedReaderHold = d
	}
	if v := os.Getenv("PINNED_READER_PAUSE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid PINNED_READER_PAUSE: %w", err)
		}
		if d < 0 {
			return c, fmt.Errorf("invalid PINNED_READER_PAUSE: must be non-negative")
		}
		c.PinnedReaderPause = d
	}
	levelsSet := 0
	for _, d := range []time.Duration{c.L1CompactionInterval, c.L2CompactionInterval, c.L3CompactionInterval} {
		if d != 0 {
			levelsSet++
		}
	}
	if levelsSet != 0 && levelsSet != 3 {
		return c, fmt.Errorf("L1_COMPACTION_INTERVAL, L2_COMPACTION_INTERVAL, and L3_COMPACTION_INTERVAL must be set together")
	}

	if v := os.Getenv("LOAD_MODE"); v != "" {
		c.LoadMode = v
	}
	if v := os.Getenv("REPLAY_DATASET"); v != "" {
		c.ReplayDataset = v
	}
	if v := os.Getenv("REPLAY_DATA_PATH"); v != "" {
		c.ReplayDataPath = v
	}
	if v := os.Getenv("REPLAY_DATA_URL"); v != "" {
		c.ReplayDataURL = v
	}
	if v := os.Getenv("REPLAY_SPEED"); v != "" {
		if _, err := fmt.Sscanf(v, "%f", &c.ReplaySpeed); err != nil {
			return c, fmt.Errorf("invalid REPLAY_SPEED: %w", err)
		}
	}
	if v := os.Getenv("REPLAY_LOOP"); v == "false" || v == "0" {
		c.ReplayLoop = false
	}

	if v := os.Getenv("METRICS_ADDR"); v != "" {
		c.MetricsAddr = v
	}
	if v := os.Getenv("CONTROL_BASE_URL"); v != "" {
		c.ControlBaseURL = v
	}

	if c.WorkerName == "" {
		c.WorkerName = c.WorkerID
	}
	if c.ManyDBEnabled() {
		switch c.manyDBConfigMode() {
		case "list", "dir":
		default:
			return c, fmt.Errorf("invalid CONFIG_MODE: must be list or dir")
		}
	}

	if c.ReplicaType == "s3" && c.S3Bucket == "" {
		return c, fmt.Errorf("S3_BUCKET is required when REPLICA_TYPE=s3")
	}

	return c, nil
}

func resolveLitestreamSHA() string {
	if v := strings.TrimSpace(os.Getenv("LITESTREAM_SHA")); v != "" {
		return v
	}

	shaFile := strings.TrimSpace(os.Getenv("LITESTREAM_SHA_FILE"))
	if shaFile == "" {
		shaFile = "/opt/soak/litestream.sha"
	}

	body, err := os.ReadFile(shaFile)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(body))
}

func parseBoolEnv(value string) bool {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (c Config) ReplicaURL() string {
	if c.ReplicaType == "file" {
		return "file://" + c.ReplicaPath
	}
	return fmt.Sprintf("s3://%s/%s", c.S3Bucket, c.S3Path)
}

func (c Config) ReplicaURLForDB(dbPath string) string {
	if !c.ManyDBEnabled() || strings.TrimSpace(dbPath) == "" || dbPath == c.DBPath {
		return c.ReplicaURL()
	}
	name := filepath.Base(dbPath)
	if c.ReplicaType == "file" {
		return "file://" + filepath.Join(c.ReplicaPath, name)
	}
	return fmt.Sprintf("s3://%s/%s", c.S3Bucket, path.Join(strings.Trim(c.S3Path, "/"), name))
}

func (c Config) ManyDBEnabled() bool {
	return c.NumDatabases > 0
}

func (c Config) ManyDBDir() string {
	return filepath.Join(c.DataDir, "dbs")
}

func (c Config) ManyDBPaths() []string {
	if c.NumDatabases <= 0 {
		return nil
	}
	paths := make([]string, 0, c.NumDatabases)
	for i := 1; i <= c.NumDatabases; i++ {
		paths = append(paths, filepath.Join(c.ManyDBDir(), fmt.Sprintf("db-%05d.db", i)))
	}
	return paths
}

func (c Config) ManyDBActiveCount() int {
	if c.NumDatabases <= 0 || c.ActivePercent <= 0 {
		return 0
	}
	count := int(math.Ceil(float64(c.NumDatabases) * c.ActivePercent / 100))
	if count < 1 {
		return 1
	}
	if count > c.NumDatabases {
		return c.NumDatabases
	}
	return count
}

func (c Config) ManyDBActivePaths() []string {
	return c.ManyDBActivePathsAt(time.Now())
}

func (c Config) ManyDBActivePathsAt(now time.Time) []string {
	paths := c.ManyDBPaths()
	count := c.ManyDBActiveCount()
	if count <= 0 || len(paths) == 0 {
		return nil
	}
	if count >= len(paths) {
		return append([]string(nil), paths...)
	}

	generation := c.manyDBActiveGeneration(now)
	ranked := make([]manyDBRankedPath, 0, len(paths))
	for i, path := range paths {
		ranked = append(ranked, manyDBRankedPath{
			path: path,
			rank: rankManyDBPath(c.ActiveSetSeed, generation, i),
		})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].rank == ranked[j].rank {
			return ranked[i].path < ranked[j].path
		}
		return ranked[i].rank < ranked[j].rank
	})

	active := make([]string, 0, count)
	for _, entry := range ranked[:count] {
		active = append(active, entry.path)
	}
	return active
}

type manyDBRankedPath struct {
	path string
	rank uint64
}

func rankManyDBPath(seed, generation int64, index int) uint64 {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%d/%d/%d", seed, generation, index)
	return h.Sum64()
}

func (c Config) manyDBActiveGeneration(now time.Time) int64 {
	if now.IsZero() {
		now = time.Now()
	}
	return now.UnixNano() / int64(c.manyDBActiveRotateInterval())
}

func (c Config) manyDBActiveRotateInterval() time.Duration {
	if c.ActiveRotateInterval <= 0 {
		return 30 * time.Minute
	}
	return c.ActiveRotateInterval
}

func (c Config) manyDBConfigMode() string {
	mode := strings.TrimSpace(c.ConfigMode)
	if mode == "" {
		return "list"
	}
	return mode
}

func (c Config) s3FaultProxyObserveMode() bool {
	return normalizeS3FaultProxyMode(c.S3FaultProxyMode) == s3FaultProxyModeObserve
}

func (c Config) manyDBVerifyChangedLimit() int {
	if c.VerifyChangedLimit <= 0 {
		return 100
	}
	if c.NumDatabases > 0 && c.VerifyChangedLimit > c.NumDatabases {
		return c.NumDatabases
	}
	return c.VerifyChangedLimit
}

func (c Config) WorkloadConfig() workload.Config {
	cfg := workload.Config{
		LoadMode:                 c.LoadMode,
		WriteRate:                c.WriteRate,
		Pattern:                  c.Pattern,
		PayloadSize:              c.PayloadSize,
		ReadRatio:                c.ReadRatio,
		Workers:                  c.Workers,
		InitialSize:              c.InitialSize,
		MonitorInterval:          c.MonitorInterval.String(),
		VerifyInterval:           c.VerifyInterval.String(),
		VerifyType:               c.VerifyType,
		VerifySyncDegradedAfter:  c.VerifySyncDegradedAfter.String(),
		VerifySyncTimeout:        c.VerifySyncTimeout.String(),
		DiskFullNoProgressWindow: c.DiskFullNoProgressWindow.String(),
		DiskFullRecoveryReserve:  c.DiskFullRecoveryReserve,
		DiskFullRecoveryTimeout:  c.DiskFullRecoveryTimeout.String(),
		SnapshotInterval:         c.SnapshotInterval.String(),
		SyncInterval:             c.SyncInterval.String(),
		S3PartSize:               c.S3PartSize,
		S3Concurrency:            c.S3Concurrency,
		ReplicaLevelReporting:    c.ReplicaLevelReporting,
		NumDatabases:             c.NumDatabases,
		ActivePercent:            c.ActivePercent,
		ActivePercentSet:         c.ManyDBEnabled(),
		ConfigMode:               c.manyDBConfigMode(),
		VerifySampleSize:         c.VerifySampleSize,
		ReplicationLagThreshold:  c.ReplicationLagThreshold,
		ReplayDataset:            c.ReplayDataset,
		ReplayDataPath:           c.ReplayDataPath,
		ReplayDataURL:            c.ReplayDataURL,
		ReplaySpeed:              c.ReplaySpeed,
		ReplayLoop:               c.ReplayLoop,
	}
	if c.L1CompactionInterval > 0 {
		cfg.L1CompactionInterval = c.L1CompactionInterval.String()
	}
	if c.L2CompactionInterval > 0 {
		cfg.L2CompactionInterval = c.L2CompactionInterval.String()
	}
	if c.L3CompactionInterval > 0 {
		cfg.L3CompactionInterval = c.L3CompactionInterval.String()
	}
	if c.L0Retention > 0 {
		cfg.L0Retention = c.L0Retention.String()
	}
	if c.L0RetentionCheckInterval > 0 {
		cfg.L0RetentionCheckInterval = c.L0RetentionCheckInterval.String()
	}
	if c.TruncatePageN != nil {
		n := *c.TruncatePageN
		cfg.TruncatePageN = &n
	}
	if c.PinnedReaderHold > 0 {
		cfg.PinnedReaderHold = c.PinnedReaderHold.String()
	}
	if c.PinnedReaderPause > 0 {
		cfg.PinnedReaderPause = c.PinnedReaderPause.String()
	}
	if c.ManyDBEnabled() {
		cfg.ActiveRotateInterval = c.manyDBActiveRotateInterval().String()
		cfg.ActiveSetSeed = c.ActiveSetSeed
		cfg.VerifyChangedLimit = c.manyDBVerifyChangedLimit()
	}
	if c.S3FaultProxyEnabled {
		cfg.S3FaultProxyEnabled = true
		cfg.S3FaultProxyMode = c.S3FaultProxyMode
		cfg.S3FaultProxyListenAddr = c.S3FaultProxyListenAddr
		cfg.S3FaultProxyMinContentLength = c.S3FaultProxyMinContentLength
		cfg.S3FaultProxyResetAfterBytes = c.S3FaultProxyResetAfterBytes
		cfg.S3FaultProxyFailFirstAttempts = c.S3FaultProxyFailFirstAttempts
		cfg.S3FaultProxyMaxFailures = c.S3FaultProxyMaxFailures
		cfg.S3FaultProxySourceLevel = c.S3FaultProxySourceLevel
		cfg.S3FaultProxyRequireObservedSourceGet = c.S3FaultProxyRequireObservedSourceGet
		cfg.S3FaultProxyRequireObservedSourceRangeGet = c.S3FaultProxyRequireObservedSourceRangeGet
	}
	return cfg
}

func (c Config) monitorInterval() time.Duration {
	if c.MonitorInterval > 0 {
		return c.MonitorInterval
	}
	return DefaultConfig().MonitorInterval
}

func (c Config) s3CommandEnv(proxyEndpoint string) []string {
	env := os.Environ()
	env = setCommandEnv(env, "AWS_ACCESS_KEY_ID", c.S3AccessKey)
	env = setCommandEnv(env, "AWS_SECRET_ACCESS_KEY", c.S3SecretKey)
	if c.S3FaultProxyEnabled && c.ReplicaType == "s3" && strings.TrimSpace(proxyEndpoint) != "" {
		env = setCommandEnv(env, "HTTP_PROXY", proxyEndpoint)
		env = setCommandEnv(env, "HTTPS_PROXY", proxyEndpoint)
		env = setCommandEnv(env, "NO_PROXY", "127.0.0.1,localhost")
	}
	return env
}
