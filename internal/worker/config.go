package worker

import (
	"fmt"
	"os"
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

	// Load duration — set very high for continuous operation.
	LoadDuration time.Duration

	// Verification
	VerifyInterval          time.Duration
	VerifyType              string // quick, integrity, checksum, full
	VerifySyncDegradedAfter time.Duration
	VerifySyncTimeout       time.Duration

	// Replica config
	ReplicaType string // "file" or "s3"
	ReplicaPath string // for file:// replicas (local directory)

	// S3/Tigris config (only used when ReplicaType == "s3")
	S3Bucket    string
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Path      string

	// Litestream config
	SnapshotInterval time.Duration
	SyncInterval     time.Duration

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

		LoadDuration: 87600 * time.Hour, // ~10 years

		LoadMode:    "synthetic",
		ReplaySpeed: 10.0,
		ReplayLoop:  true,

		VerifyInterval:          30 * time.Minute,
		VerifyType:              "integrity",
		VerifySyncDegradedAfter: 5 * time.Minute,
		VerifySyncTimeout:       15 * time.Minute,

		ReplicaType: "file",
		ReplicaPath: "/data/replicas",

		S3Endpoint: "https://fly.storage.tigris.dev",
		S3Path:     "soak",

		SnapshotInterval: 10 * time.Minute,
		SyncInterval:     1 * time.Second,

		MetricsAddr: ":9091",
	}
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

	if v := os.Getenv("VERIFY_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("invalid VERIFY_INTERVAL: %w", err)
		}
		c.VerifyInterval = d
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

func (c Config) ReplicaURL() string {
	if c.ReplicaType == "file" {
		return "file://" + c.ReplicaPath
	}
	return fmt.Sprintf("s3://%s/%s", c.S3Bucket, c.S3Path)
}

func (c Config) WorkloadConfig() workload.Config {
	return workload.Config{
		LoadMode:         c.LoadMode,
		WriteRate:        c.WriteRate,
		Pattern:          c.Pattern,
		PayloadSize:      c.PayloadSize,
		ReadRatio:        c.ReadRatio,
		Workers:          c.Workers,
		InitialSize:      c.InitialSize,
		VerifyInterval:   c.VerifyInterval.String(),
		VerifyType:       c.VerifyType,
		SnapshotInterval: c.SnapshotInterval.String(),
		SyncInterval:     c.SyncInterval.String(),
		ReplayDataset:    c.ReplayDataset,
		ReplayDataPath:   c.ReplayDataPath,
		ReplayDataURL:    c.ReplayDataURL,
		ReplaySpeed:      c.ReplaySpeed,
		ReplayLoop:       c.ReplayLoop,
	}
}
