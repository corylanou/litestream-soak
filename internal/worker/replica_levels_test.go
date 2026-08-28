package worker

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestParseReplicaLevels(t *testing.T) {
	for _, tt := range []struct {
		name    string
		output  string
		want    []replicaLevel
		wantErr bool
	}{
		{
			name:   "sorted by level",
			output: "0009 80 65536\n0000 1212 4096\n0001 221 8192\n",
			want: []replicaLevel{
				{Level: "0000", Objects: 1212, Bytes: 4096},
				{Level: "0001", Objects: 221, Bytes: 8192},
				{Level: "0009", Objects: 80, Bytes: 65536},
			},
		},
		{name: "empty prefix", output: "", want: nil},
		{name: "malformed line", output: "0000 12\n", wantErr: true},
		{name: "non-numeric", output: "0000 x 1\n", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseReplicaLevels(tt.output)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("level %d = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// runAwk feeds input through the given awk program, as the poller's shell
// pipeline does, so the aggregation is tested against a real awk.
func runAwk(t *testing.T, program, input string) (string, error) {
	t.Helper()
	cmd := exec.Command("awk", program)
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.Output()
	return string(out), err
}

func TestReplicaLevelAwkAggregatesByLevelDirectory(t *testing.T) {
	// One recursive listing must cover both the single-database layout
	// (prefix/000N/file) and the many-database layout (prefix/<db>/000N/file).
	listing := "" +
		"2026-08-28 18:17 62951 s3://b/soak/w/vol/0000/0000000000000001-0000000000000001.ltx\n" +
		"2026-08-28 18:17 193 s3://b/soak/w/vol/0000/0000000000000002-0000000000000002.ltx\n" +
		"2026-08-28 18:10 300169 s3://b/soak/w/vol/0001/0000000000000001-0000000000000002.ltx\n" +
		"2026-08-28 18:00 979843 s3://b/soak/w/vol/db-042/0009/0000000000000001-0000000000000100.ltx\n" +
		"2026-08-28 18:00 12 s3://b/soak/w/vol/db-042/0000/0000000000000101-0000000000000101.ltx\n" +
		"2026-08-28 18:00 91 s3://b/soak/w/vol/soak-replica-url\n"
	out, err := runAwk(t, replicaLevelAwk, listing)
	if err != nil {
		t.Fatal(err)
	}
	levels, err := parseReplicaLevels(out)
	if err != nil {
		t.Fatal(err)
	}
	want := []replicaLevel{
		{Level: "0000", Objects: 3, Bytes: 62951 + 193 + 12},
		{Level: "0001", Objects: 1, Bytes: 300169},
		{Level: "0009", Objects: 1, Bytes: 979843},
	}
	if len(levels) != len(want) {
		t.Fatalf("got %v, want %v", levels, want)
	}
	for i := range want {
		if levels[i] != want[i] {
			t.Fatalf("level %d = %+v, want %+v", i, levels[i], want[i])
		}
	}
}

func TestSetReplicaLevelStats(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WorkerID = "worker-replica-levels"
	cfg.ProfileName = "high-vol-ams"
	cfg.Source = "pr-1479"
	cfg.Region = "ams"
	SetWorkerInfo(cfg)
	base := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source, cfg.Region}

	SetReplicaLevelStats([]replicaLevel{
		{Level: "0000", Objects: 1212, Bytes: 4096},
		{Level: "0001", Objects: 221, Bytes: 8192},
	})
	SetReplicaLevelListingOK(true)
	for level, want := range map[string]float64{"0000": 1212, "0001": 221} {
		if got := testutil.ToFloat64(replicaLTXObjects.WithLabelValues(append(base, level)...)); got != want {
			t.Fatalf("soak_replica_ltx_objects{level=%s} = %v, want %v", level, got, want)
		}
	}
	if got := testutil.ToFloat64(replicaLTXBytes.WithLabelValues(append(base, "0001")...)); got != 8192 {
		t.Fatalf("soak_replica_ltx_bytes{level=0001} = %v, want 8192", got)
	}
	if got := testutil.ToFloat64(replicaLTXListingOK.WithLabelValues(base...)); got != 1 {
		t.Fatalf("soak_replica_ltx_listing_ok = %v, want 1", got)
	}

	// A failed listing flags staleness but keeps the last good values.
	SetReplicaLevelListingOK(false)
	if got := testutil.ToFloat64(replicaLTXListingOK.WithLabelValues(base...)); got != 0 {
		t.Fatalf("soak_replica_ltx_listing_ok = %v, want 0", got)
	}
	if got := testutil.ToFloat64(replicaLTXObjects.WithLabelValues(append(base, "0000")...)); got != 1212 {
		t.Fatalf("soak_replica_ltx_objects{level=0000} = %v after failure, want 1212", got)
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
			cfg.ReplicaLevelPollInterval = time.Hour
			tt.mutate(&cfg)
			p := newReplicaLevelPoller(&cfg)
			calls := make(chan struct{}, 4)
			ctx, cancel := context.WithCancel(context.Background())
			p.list = func(context.Context, Config) ([]replicaLevel, error) {
				calls <- struct{}{}
				cancel() // stop after the initial poll
				return nil, nil
			}
			done := make(chan struct{})
			go func() { p.Run(ctx); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("poller did not stop")
			}
			cancel()
			if got := len(calls) > 0; got != tt.wantRuns {
				t.Fatalf("polled=%v, want %v", got, tt.wantRuns)
			}
		})
	}
}

func TestReplicaLevelPollerHonorsCancellation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "b"
	cfg.ReplicaLevelPollInterval = 10 * time.Millisecond
	p := newReplicaLevelPoller(&cfg)

	t.Run("pre-cancelled context never lists", func(t *testing.T) {
		calls := 0
		p.list = func(context.Context, Config) ([]replicaLevel, error) { calls++; return nil, nil }
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p.Run(ctx)
		if calls != 0 {
			t.Fatalf("list called %d times on a cancelled context", calls)
		}
	})

	t.Run("in-flight listing observes cancellation and Run returns", func(t *testing.T) {
		started := make(chan struct{}, 1)
		p.list = func(ctx context.Context, _ Config) ([]replicaLevel, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { p.Run(ctx); close(done) }()
		<-started
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after cancellation")
		}
	})

	t.Run("ticks", func(t *testing.T) {
		calls := make(chan struct{}, 16)
		p.list = func(context.Context, Config) ([]replicaLevel, error) { calls <- struct{}{}; return nil, nil }
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
	})
}

func TestListReplicaLevelsRejectsMissingHost(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "b"
	cfg.S3Endpoint = ""
	if _, err := listReplicaLevels(context.Background(), cfg); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
	if _, err := listReplicaLevels(context.Background(), cfg); errors.Is(err, context.Canceled) {
		t.Fatal("unexpected cancellation error")
	}
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
