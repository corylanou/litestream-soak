package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAggregateReplicaListing(t *testing.T) {
	// One recursive listing must cover both the single-database layout
	// (prefix/000N/file) and the many-database layout (prefix/<db>/000N/file),
	// tolerate keys with spaces, and ignore non-object rows.
	listing := "" +
		"2026-08-28 18:17 62951 s3://b/soak/w/vol/0000/0000000000000001-0000000000000001.ltx\n" +
		"2026-08-28 18:17 193 s3://b/soak/w/vol/0000/0000000000000002-0000000000000002.ltx\n" +
		"2026-08-28 18:10 300169 s3://b/soak/w/vol/0001/0000000000000001-0000000000000002.ltx\n" +
		"2026-08-28 18:00 979843 s3://b/soak/w/vol/db 042/0009/0000000000000001-0000000000000100.ltx\n" +
		"2026-08-28 18:00 12 s3://b/soak/w/vol/db 042/0000/0000000000000101-0000000000000101.ltx\n" +
		"2026-08-28 18:00 3000000000 s3://b/soak/w/vol/0002/0000000000000001-0000000000000200.ltx\n" +
		"2026-08-28 18:00 91 s3://b/soak/w/vol/soak-replica-url\n" +
		"                          DIR  s3://b/soak/w/vol/0003/\n" +
		"\n"
	levels, err := aggregateReplicaListing(strings.NewReader(listing))
	if err != nil {
		t.Fatal(err)
	}
	want := []replicaLevel{
		{Level: "0000", Objects: 3, Bytes: 62951 + 193 + 12},
		{Level: "0001", Objects: 1, Bytes: 300169},
		{Level: "0002", Objects: 1, Bytes: 3000000000},
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

func TestReplicaLevelOfKey(t *testing.T) {
	for _, tt := range []struct {
		key   string
		level string
		ok    bool
	}{
		{"s3://b/p/0000/a.ltx", "0000", true},
		{"s3://b/p/db/0009/a.ltx", "0009", true},
		{"s3://b/p/soak-replica-url", "", false},
		{"s3://b/p/00010/a.ltx", "", false},
		{"s3://b/p/00a1/a.ltx", "", false},
		{"a.ltx", "", false},
	} {
		level, ok := replicaLevelOfKey(tt.key)
		if level != tt.level || ok != tt.ok {
			t.Fatalf("replicaLevelOfKey(%q) = %q,%v want %q,%v", tt.key, level, ok, tt.level, tt.ok)
		}
	}
}

func TestReplicaLevelMetrics(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WorkerID = "worker-replica-levels"
	cfg.ProfileName = "high-vol-ams"
	cfg.Source = "pr-1479"
	cfg.Region = "ams"
	SetWorkerInfo(cfg)
	base := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source, cfg.Region}
	objects := func(level string) float64 {
		return testutil.ToFloat64(replicaLTXObjects.WithLabelValues(append(base, level)...))
	}

	p := newReplicaLevelPoller(&cfg)
	var next []replicaLevel
	var nextErr error
	p.list = func(context.Context, Config) ([]replicaLevel, error) { return next, nextErr }

	next = []replicaLevel{{Level: "0000", Objects: 1212, Bytes: 4096}, {Level: "0001", Objects: 221, Bytes: 8192}}
	p.poll(context.Background())
	if got := objects("0000"); got != 1212 {
		t.Fatalf("L0 objects = %v, want 1212", got)
	}
	if got := testutil.ToFloat64(replicaLTXBytes.WithLabelValues(append(base, "0001")...)); got != 8192 {
		t.Fatalf("L1 bytes = %v, want 8192", got)
	}
	if got := testutil.ToFloat64(replicaLTXListingOK.WithLabelValues(base...)); got != 1 {
		t.Fatalf("listing_ok = %v, want 1", got)
	}

	// A failed listing flags staleness but keeps the last good values.
	next, nextErr = nil, context.DeadlineExceeded
	p.poll(context.Background())
	if got := testutil.ToFloat64(replicaLTXListingOK.WithLabelValues(base...)); got != 0 {
		t.Fatalf("listing_ok after failure = %v, want 0", got)
	}
	if got := objects("0000"); got != 1212 {
		t.Fatalf("L0 objects after failure = %v, want 1212 (stale but kept)", got)
	}

	// A level that disappears from a successful listing reads zero.
	next, nextErr = []replicaLevel{{Level: "0001", Objects: 5, Bytes: 10}}, nil
	p.poll(context.Background())
	if got := objects("0000"); got != 0 {
		t.Fatalf("L0 objects after compaction = %v, want 0", got)
	}
	if got := objects("0001"); got != 5 {
		t.Fatalf("L1 objects = %v, want 5", got)
	}
	if got := testutil.ToFloat64(replicaLTXListingOK.WithLabelValues(base...)); got != 1 {
		t.Fatalf("listing_ok = %v, want 1", got)
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
			polled := false
			ctx, cancel := context.WithCancel(context.Background())
			p.list = func(context.Context, Config) ([]replicaLevel, error) {
				polled = true
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
			if polled != tt.wantRuns {
				t.Fatalf("polled=%v, want %v", polled, tt.wantRuns)
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
