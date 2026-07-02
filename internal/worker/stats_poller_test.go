package worker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestDeriveAllocRate(t *testing.T) {
	base := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

	t.Run("first sample sets baseline and returns zero", func(t *testing.T) {
		poller := newStatsPoller(&Config{})
		if got := poller.deriveAllocRate(base, 1000); got != 0 {
			t.Fatalf("rate = %f, want 0", got)
		}
		if got := poller.deriveAllocRate(base.Add(10*time.Second), 2000); got != 100 {
			t.Fatalf("rate after baseline = %f, want 100", got)
		}
	})

	t.Run("steady growth returns delta over elapsed seconds", func(t *testing.T) {
		poller := newStatsPoller(&Config{})
		poller.deriveAllocRate(base, 1000)
		if got := poller.deriveAllocRate(base.Add(4*time.Second), 1500); got != 125 {
			t.Fatalf("rate = %f, want 125", got)
		}
		if got := poller.deriveAllocRate(base.Add(8*time.Second), 1900); got != 100 {
			t.Fatalf("rate = %f, want 100", got)
		}
	})

	t.Run("counter regression resets baseline and returns zero", func(t *testing.T) {
		poller := newStatsPoller(&Config{})
		poller.deriveAllocRate(base, 5000)
		if got := poller.deriveAllocRate(base.Add(5*time.Second), 100); got != 0 {
			t.Fatalf("rate on regression = %f, want 0", got)
		}
		if got := poller.deriveAllocRate(base.Add(10*time.Second), 600); got != 100 {
			t.Fatalf("rate after regression baseline = %f, want 100", got)
		}
	})

	t.Run("zero elapsed returns zero and keeps baseline", func(t *testing.T) {
		poller := newStatsPoller(&Config{})
		poller.deriveAllocRate(base, 1000)
		if got := poller.deriveAllocRate(base, 2000); got != 0 {
			t.Fatalf("rate at zero elapsed = %f, want 0", got)
		}
		if got := poller.deriveAllocRate(base.Add(10*time.Second), 2000); got != 100 {
			t.Fatalf("rate after zero elapsed = %f, want 100", got)
		}
	})

	t.Run("negative elapsed returns zero and keeps baseline", func(t *testing.T) {
		poller := newStatsPoller(&Config{})
		poller.deriveAllocRate(base, 1000)
		if got := poller.deriveAllocRate(base.Add(-time.Second), 2000); got != 0 {
			t.Fatalf("rate at negative elapsed = %f, want 0", got)
		}
		if got := poller.deriveAllocRate(base.Add(10*time.Second), 2000); got != 100 {
			t.Fatalf("rate after negative elapsed = %f, want 100", got)
		}
	})
}

func startStatsPollerIPCServer(t *testing.T, cfg *Config, metricsBody string) {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/txid", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]uint64{"txid": 10})
	})
	mux.HandleFunc("/info", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"uptime_seconds": 120})
	})
	mux.HandleFunc("/list", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"databases":[{"path":"` + cfg.DBPath + `","status":"replicating","txid":10,"replicated_txid":10}]}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(metricsBody))
	})
	mux.HandleFunc("/debug/pprof/goroutine", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("goroutine profile: total 12\n"))
	})
	startTestUnixServer(t, cfg.SocketPath, mux)
}

func newStatsPollerTestConfig(t *testing.T) Config {
	t.Helper()

	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-stats-%d.sock", time.Now().UnixNano()))
	return cfg
}

func statsPollerTestMetricsBody(cfg Config) string {
	return `# HELP litestream_disk_full Whether replication is paused because the local disk is full
# TYPE litestream_disk_full gauge
litestream_disk_full{db="` + cfg.DBPath + `"} 0
# HELP go_memstats_heap_inuse_bytes Number of heap bytes that are in use.
# TYPE go_memstats_heap_inuse_bytes gauge
go_memstats_heap_inuse_bytes 4194304
# HELP go_memstats_stack_inuse_bytes Number of bytes in use by the stack allocator.
# TYPE go_memstats_stack_inuse_bytes gauge
go_memstats_stack_inuse_bytes 1048576
# HELP go_memstats_alloc_bytes_total Total number of bytes allocated, even if freed.
# TYPE go_memstats_alloc_bytes_total counter
go_memstats_alloc_bytes_total 987654321
`
}

func TestCollectLitestreamRuntimeIncludesS3ListRequestsAndMemStats(t *testing.T) {
	cfg := newStatsPollerTestConfig(t)
	startStatsPollerIPCServer(t, &cfg, statsPollerTestMetricsBody(cfg))

	poller := newStatsPoller(&cfg)
	poller.s3ListRequests = func() int64 { return 77 }

	client := poller.ipcClient()
	defer client.CloseIdleConnections()

	snapshot, err := poller.collectLitestreamRuntime(client, time.Now().UTC())
	if err != nil {
		t.Fatalf("collectLitestreamRuntime() error = %v", err)
	}

	if snapshot.S3ListRequestsTotal != 77 {
		t.Fatalf("S3ListRequestsTotal = %d, want 77", snapshot.S3ListRequestsTotal)
	}
	if snapshot.LitestreamHeapInuseBytes != 4194304 {
		t.Fatalf("LitestreamHeapInuseBytes = %d, want 4194304", snapshot.LitestreamHeapInuseBytes)
	}
	if snapshot.LitestreamStackInuseBytes != 1048576 {
		t.Fatalf("LitestreamStackInuseBytes = %d, want 1048576", snapshot.LitestreamStackInuseBytes)
	}
	if snapshot.LitestreamAllocBytesTotal != 987654321 {
		t.Fatalf("LitestreamAllocBytesTotal = %f, want 987654321", snapshot.LitestreamAllocBytesTotal)
	}
	if snapshot.LitestreamAllocRateBytesPerSec != 0 {
		t.Fatalf("LitestreamAllocRateBytesPerSec = %f, want 0 on first sample", snapshot.LitestreamAllocRateBytesPerSec)
	}
}

func TestCollectLitestreamRuntimeNilOrNegativeS3ListRequests(t *testing.T) {
	tests := []struct {
		name string
		fn   func() int64
	}{
		{name: "nil func"},
		{name: "negative value", fn: func() int64 { return -5 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := newStatsPollerTestConfig(t)
			startStatsPollerIPCServer(t, &cfg, statsPollerTestMetricsBody(cfg))

			poller := newStatsPoller(&cfg)
			poller.s3ListRequests = test.fn

			client := poller.ipcClient()
			defer client.CloseIdleConnections()

			snapshot, err := poller.collectLitestreamRuntime(client, time.Now().UTC())
			if err != nil {
				t.Fatalf("collectLitestreamRuntime() error = %v", err)
			}
			if snapshot.S3ListRequestsTotal != 0 {
				t.Fatalf("S3ListRequestsTotal = %d, want 0", snapshot.S3ListRequestsTotal)
			}
		})
	}
}

func TestNewRunnerWiresS3ListRequestsToFaultProxy(t *testing.T) {
	runner := NewRunner(DefaultConfig())

	if runner.s3ListRequests == nil {
		t.Fatal("stats poller s3ListRequests func is not wired")
	}
	if got := runner.s3ListRequests(); got != 0 {
		t.Fatalf("s3ListRequests() = %d with nil proxy, want 0", got)
	}

	runner.s3FaultProxy = &s3FaultProxy{listRequests: 42}
	if got := runner.s3ListRequests(); got != 42 {
		t.Fatalf("s3ListRequests() = %d, want 42", got)
	}
}

func TestCollectLitestreamRuntimeOmitsMemStatsWhenFamiliesAbsent(t *testing.T) {
	cfg := newStatsPollerTestConfig(t)
	startStatsPollerIPCServer(t, &cfg, `# HELP litestream_disk_full Whether replication is paused because the local disk is full
# TYPE litestream_disk_full gauge
litestream_disk_full{db="`+cfg.DBPath+`"} 0
`)

	poller := newStatsPoller(&cfg)

	client := poller.ipcClient()
	defer client.CloseIdleConnections()

	snapshot, err := poller.collectLitestreamRuntime(client, time.Now().UTC())
	if err != nil {
		t.Fatalf("collectLitestreamRuntime() error = %v", err)
	}

	if snapshot.LitestreamHeapInuseBytes != 0 || snapshot.LitestreamStackInuseBytes != 0 {
		t.Fatalf("memstats bytes = %d/%d, want zero", snapshot.LitestreamHeapInuseBytes, snapshot.LitestreamStackInuseBytes)
	}
	if snapshot.LitestreamAllocBytesTotal != 0 || snapshot.LitestreamAllocRateBytesPerSec != 0 {
		t.Fatalf("alloc metrics = %f/%f, want zero", snapshot.LitestreamAllocBytesTotal, snapshot.LitestreamAllocRateBytesPerSec)
	}
}
