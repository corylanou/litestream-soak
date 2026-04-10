package worker

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	WorkerID string
	GitSHA   string
	Source   string // "main" or "pr"

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
	VerifyInterval time.Duration
	VerifyType     string // quick, integrity, checksum, full

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
	LoadMode        string  // "synthetic", "replay", "both"
	ReplayDataset   string  // "taxi", "gharchive"
	ReplayDataPath  string  // path to dataset file
	ReplaySpeed     float64 // speed multiplier (1.0 = real-time)
	ReplayLoop      bool

	// Metrics
	MetricsAddr string
}

func DefaultConfig() Config {
	return Config{
		WorkerID: "worker-1",
		Source:   "main",

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

		VerifyInterval: 30 * time.Minute,
		VerifyType:     "integrity",

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
	if v := os.Getenv("GIT_SHA"); v != "" {
		c.GitSHA = v
	}
	if v := os.Getenv("SOURCE"); v != "" {
		c.Source = v
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

	if c.ReplicaType == "s3" && c.S3Bucket == "" {
		return c, fmt.Errorf("S3_BUCKET is required when REPLICA_TYPE=s3")
	}

	return c, nil
}

func (c Config) ReplicaURL() string {
	if c.ReplicaType == "file" {
		return "file://" + c.ReplicaPath
	}
	return fmt.Sprintf("s3://%s/%s", c.S3Bucket, c.S3Path)
}
