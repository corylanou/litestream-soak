package workload

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Config struct {
	LoadMode         string  `json:"load_mode,omitempty"`
	WriteRate        int     `json:"write_rate,omitempty"`
	Pattern          string  `json:"pattern,omitempty"`
	PayloadSize      int     `json:"payload_size,omitempty"`
	ReadRatio        float64 `json:"read_ratio,omitempty"`
	Workers          int     `json:"workers,omitempty"`
	InitialSize      string  `json:"initial_size,omitempty"`
	VerifyInterval   string  `json:"verify_interval,omitempty"`
	VerifyType       string  `json:"verify_type,omitempty"`
	SnapshotInterval string  `json:"snapshot_interval,omitempty"`
	SyncInterval     string  `json:"sync_interval,omitempty"`
	S3PartSize       string  `json:"s3_part_size,omitempty"`
	S3Concurrency    int     `json:"s3_concurrency,omitempty"`
	ReplayDataset    string  `json:"replay_dataset,omitempty"`
	ReplayDataPath   string  `json:"replay_data_path,omitempty"`
	ReplayDataURL    string  `json:"replay_data_url,omitempty"`
	ReplaySpeed      float64 `json:"replay_speed,omitempty"`
	ReplayLoop       bool    `json:"replay_loop,omitempty"`
	VolumeSizeGB     int     `json:"volume_size_gb,omitempty"`
	MemoryMB         int     `json:"memory_mb,omitempty"`
	CPUs             int     `json:"cpus,omitempty"`
}

func (c Config) JSON() string {
	body, err := json.Marshal(c)
	if err != nil {
		return "{}"
	}
	return string(body)
}

func ParseConfig(raw string) (Config, error) {
	if strings.TrimSpace(raw) == "" {
		return Config{}, nil
	}

	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return Config{}, fmt.Errorf("parse workload config: %w", err)
	}

	return cfg, nil
}

func (c Config) MetricLoadMode() string {
	switch strings.TrimSpace(c.LoadMode) {
	case "", "synthetic":
		return "synthetic"
	default:
		return c.LoadMode
	}
}

func (c Config) MetricReplayDataset() string {
	if strings.TrimSpace(c.ReplayDataset) == "" {
		return "none"
	}
	return c.ReplayDataset
}

func (c Config) MetricPattern() string {
	if strings.TrimSpace(c.Pattern) == "" {
		return "none"
	}
	return c.Pattern
}
