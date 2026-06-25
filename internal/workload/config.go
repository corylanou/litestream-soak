package workload

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Config struct {
	LoadMode                 string  `json:"load_mode,omitempty"`
	WriteRate                int     `json:"write_rate,omitempty"`
	Pattern                  string  `json:"pattern,omitempty"`
	PayloadSize              int     `json:"payload_size,omitempty"`
	ReadRatio                float64 `json:"read_ratio,omitempty"`
	Workers                  int     `json:"workers,omitempty"`
	InitialSize              string  `json:"initial_size,omitempty"`
	VerifyInterval           string  `json:"verify_interval,omitempty"`
	VerifyType               string  `json:"verify_type,omitempty"`
	VerifySyncDegradedAfter  string  `json:"verify_sync_degraded_after,omitempty"`
	VerifySyncTimeout        string  `json:"verify_sync_timeout,omitempty"`
	DiskFullNoProgressWindow string  `json:"disk_full_no_progress_window,omitempty"`
	SnapshotInterval         string  `json:"snapshot_interval,omitempty"`
	SyncInterval             string  `json:"sync_interval,omitempty"`
	S3PartSize               string  `json:"s3_part_size,omitempty"`
	S3Concurrency            int     `json:"s3_concurrency,omitempty"`
	ReplayDataset            string  `json:"replay_dataset,omitempty"`
	ReplayDataPath           string  `json:"replay_data_path,omitempty"`
	ReplayDataURL            string  `json:"replay_data_url,omitempty"`
	ReplaySpeed              float64 `json:"replay_speed,omitempty"`
	ReplayLoop               bool    `json:"replay_loop,omitempty"`
	NumDatabases             int     `json:"num_databases,omitempty"`
	ActivePercent            float64 `json:"active_percent,omitempty"`
	ActivePercentSet         bool    `json:"-"`
	ConfigMode               string  `json:"config_mode,omitempty"`
	VerifySampleSize         int     `json:"verify_sample_size,omitempty"`
	ReplicationLagThreshold  uint64  `json:"replication_lag_threshold,omitempty"`
	VolumeSizeGB             int     `json:"volume_size_gb,omitempty"`
	MemoryMB                 int     `json:"memory_mb,omitempty"`
	CPUs                     int     `json:"cpus,omitempty"`
}

func (c Config) JSON() string {
	body, err := json.Marshal(c)
	if err != nil {
		return "{}"
	}
	return string(body)
}

func (c Config) MarshalJSON() ([]byte, error) {
	type configAlias Config
	body, err := json.Marshal(configAlias(c))
	if err != nil {
		return nil, err
	}
	if !c.ActivePercentSet || c.ActivePercent != 0 {
		return body, nil
	}

	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	fields["active_percent"] = json.RawMessage("0")
	return json.Marshal(fields)
}

func (c *Config) UnmarshalJSON(body []byte) error {
	type configAlias Config
	var parsed configAlias
	if err := json.Unmarshal(body, &parsed); err != nil {
		return err
	}

	*c = Config(parsed)

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return err
	}
	_, c.ActivePercentSet = fields["active_percent"]
	return nil
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
