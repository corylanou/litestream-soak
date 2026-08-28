package worker

import (
	"context"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSetReplicaLevelStats(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WorkerID = "worker-replica-levels"
	cfg.ProfileName = "high-vol-ams"
	cfg.Source = "pr-1479"
	cfg.Region = "ams"
	SetWorkerInfo(cfg)
	base := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source, cfg.Region}

	for _, tt := range []struct {
		name        string
		levels      []reporting.ObjectStorageLevelSnapshot
		wantObjects map[string]float64
		wantBytes   map[string]float64
	}{
		{
			name: "publishes counts and bytes per level",
			levels: []reporting.ObjectStorageLevelSnapshot{
				{Level: "0000", ObjectCount: 1212, TotalBytes: 4096},
				{Level: "0001", ObjectCount: 221, TotalBytes: 8192},
				{Level: "0009", ObjectCount: 80, TotalBytes: 65536},
			},
			wantObjects: map[string]float64{"0000": 1212, "0001": 221, "0009": 80},
			wantBytes:   map[string]float64{"0000": 4096, "0001": 8192, "0009": 65536},
		},
		{
			name: "errored and capped levels keep the previous value",
			levels: []reporting.ObjectStorageLevelSnapshot{
				{Level: "0000", ObjectCount: 0, Error: "command timed out"},
				{Level: "0001", ObjectCount: 5000, TotalBytes: 1, ObjectCountCapped: true},
				{Level: "0009", ObjectCount: 81, TotalBytes: 70000},
			},
			wantObjects: map[string]float64{"0000": 1212, "0001": 221, "0009": 81},
			wantBytes:   map[string]float64{"0000": 4096, "0001": 8192, "0009": 70000},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			SetReplicaLevelStats(tt.levels)
			for level, want := range tt.wantObjects {
				if got := testutil.ToFloat64(replicaLTXObjects.WithLabelValues(append(base, level)...)); got != want {
					t.Fatalf("soak_replica_ltx_objects{level=%s} = %v, want %v", level, got, want)
				}
			}
			for level, want := range tt.wantBytes {
				if got := testutil.ToFloat64(replicaLTXBytes.WithLabelValues(append(base, level)...)); got != want {
					t.Fatalf("soak_replica_ltx_bytes{level=%s} = %v, want %v", level, got, want)
				}
			}
		})
	}
}

func TestReplicaLevelPollerEnabled(t *testing.T) {
	for _, tt := range []struct {
		name     string
		mutate   func(*Config)
		wantRuns bool
	}{
		{"s3 replica with interval", func(c *Config) { c.ReplicaType = "s3"; c.S3Bucket = "b" }, true},
		{"file replica", func(c *Config) { c.ReplicaType = "file"; c.S3Bucket = "b" }, false},
		{"missing bucket", func(c *Config) { c.ReplicaType = "s3"; c.S3Bucket = "" }, false},
		{"disabled interval", func(c *Config) { c.ReplicaType = "s3"; c.S3Bucket = "b"; c.ReplicaLevelPollInterval = 0 }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			p := newReplicaLevelPoller(&cfg)
			calls := 0
			p.list = func(Config) []reporting.ObjectStorageLevelSnapshot { calls++; return nil }
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			p.Run(ctx)
			if (calls > 0) != tt.wantRuns {
				t.Fatalf("list calls = %d, wantRuns %v", calls, tt.wantRuns)
			}
		})
	}
}

func TestReplicaLevelPollerTicks(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "b"
	cfg.ReplicaLevelPollInterval = 10 * time.Millisecond
	p := newReplicaLevelPoller(&cfg)
	calls := make(chan struct{}, 16)
	p.list = func(Config) []reporting.ObjectStorageLevelSnapshot { calls <- struct{}{}; return nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	for i := 0; i < 3; i++ {
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("poller did not list %d times", i+1)
		}
	}
	cancel()
	<-done
}

func TestConfigReplicaLevelPollInterval(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"default", "", 5 * time.Minute, false},
		{"override", "90s", 90 * time.Second, false},
		{"disabled", "0", 0, false},
		{"negative", "-1s", 0, true},
		{"garbage", "soon", 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("REPLICA_LEVEL_POLL_INTERVAL", tt.value)
			cfg, err := ConfigFromEnv()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ReplicaLevelPollInterval != tt.want {
				t.Fatalf("ReplicaLevelPollInterval = %v, want %v", cfg.ReplicaLevelPollInterval, tt.want)
			}
		})
	}
}
