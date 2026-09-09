package compare

import (
	"context"
	"fmt"
	"github.com/corylanou/litestream-soak/internal/worker"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAllocationCounterReadsCumulativeBytes(t *testing.T) {
	value, err := allocationTotal([]byte("heap profile: 1: 2 [3: 4]\n# runtime.MemStats\n# TotalAlloc = 123456\n"))
	if err != nil || value != 123456 {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if _, err := allocationTotal([]byte("unsupported")); err == nil {
		t.Fatal("invented allocation total")
	}
}

func TestMonitorMeasuresProgressAndKeepsCollectionFailures(t *testing.T) {
	txid, replicated := uint64(10), uint64(5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/list" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprintf(w, `{"databases":[{"path":"source.db","txid":%d,"replicated_txid":%d}]}`, txid, replicated)
	}))
	defer server.Close()
	m := monitor{client: server.Client(), baseURL: server.URL, path: "source.db", directory: t.TempDir()}
	start := time.Now()
	m.sample(context.Background(), start)
	m.sample(context.Background(), start.Add(time.Second))
	replicated = 10
	m.sample(context.Background(), start.Add(2*time.Second))
	if m.maxLagSeconds != 2 || len(m.samples) != 3 {
		t.Fatalf("lag=%v samples=%d", m.maxLagSeconds, len(m.samples))
	}
	server.Close()
	m.sample(context.Background(), start.Add(3*time.Second))
	if len(m.errors) == 0 {
		t.Fatal("lost IPC collection failure")
	}
}

func TestS3EvidencePreservesFailureWhenRestoreFails(t *testing.T) {
	o := Observation{Error: "restore failed", Metrics: map[string]Measurement{}}
	evidence := worker.ComparisonS3Evidence{Requests: 3, UploadBytes: 4, DownloadBytes: 5, Failures: []string{"PUT returned HTTP 503"}}
	if err := recordS3Evidence(t.TempDir(), evidence, &o); err != nil {
		t.Fatal(err)
	}
	if o.Error != "restore failed" || len(o.Incidents) != 1 || *o.Metrics["object_requests"].Value != 3 {
		t.Fatalf("lost failure: %+v", o)
	}
}

func TestMonitorUsesReadOnlyRealDiagnosticShape(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			posts++
			http.Error(w, "mutation forbidden", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/list" {
			_, _ = fmt.Fprint(w, `{"databases":[{"path":"source.db","status":"replicating","last_sync_at":"2026-09-09T00:00:00Z"}]}`)
			return
		}
		if r.URL.Path == "/debug/sync-status" && r.URL.Query().Get("path") == "source.db" {
			_, _ = fmt.Fprint(w, `{"databases":[{"path":"source.db","active":true,"operation":"db_sync","phase":"sync_complete","txid":9,"wal_size":1024,"last_synced_wal_offset":1024}]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	m := monitor{client: server.Client(), baseURL: server.URL, path: "source.db"}
	_, _, err := m.progress(context.Background())
	if posts != 0 {
		t.Fatalf("sampler made %d mutating requests", posts)
	}
	if err == nil {
		t.Fatal("fabricated replicated TXID absent from pinned binary diagnostic")
	}
	if len(m.lastDiagnostic) == 0 {
		t.Fatal("lost read-only diagnostic evidence")
	}
}

func TestMonitorRetainsAllocationWindowAndSamples(t *testing.T) {
	allocation := uint64(1000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/list":
			_, _ = fmt.Fprint(w, `{"databases":[{"path":"source.db","txid":10,"replicated_txid":10}]}`)
		case "/debug/pprof/heap":
			_, _ = fmt.Fprintf(w, "# TotalAlloc = %d\n", allocation)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	m := monitor{client: server.Client(), baseURL: server.URL, path: "source.db", directory: t.TempDir(), pid: os.Getpid()}
	m.begin(context.Background())
	allocation = 2000
	o := Observation{CompletedOperations: 10, Metrics: map[string]Measurement{}}
	if err := m.finish(context.Background(), &o); err != nil {
		t.Fatal(err)
	}
	if *o.Metrics["allocation_bytes_per_operation"].Value != 100 || o.ProgressSamples != 2 {
		t.Fatalf("observation=%+v", o)
	}
	if _, err := os.Stat(filepath.Join(m.directory, "baseline-heap.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(m.directory, "progress.json")); err != nil {
		t.Fatal(err)
	}
}

func TestReadFailureDoesNotReusePriorSyncTimestamp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"databases":[{"path":"source.db","txid":10,"replicated_txid":10,"last_sync_at":"2026-09-09T00:00:00Z"}]}`)
	}))
	m := monitor{client: server.Client(), baseURL: server.URL, path: "source.db"}
	if _, _, err := m.progress(context.Background()); err != nil {
		t.Fatal(err)
	}
	server.Close()
	if _, _, err := m.progress(context.Background()); err == nil {
		t.Fatal("expected read failure")
	}
	if m.lastSyncAt != nil {
		t.Fatal("reused stale timestamp after IPC failure")
	}
}

func TestPartialProgressSurvivesWorkloadFailure(t *testing.T) {
	m := &monitor{directory: t.TempDir(), samples: []progressSample{{TXID: 12, ReplicatedTXID: 9, ProgressError: "connection closed"}}}
	o := Observation{Error: "workload interrupted"}
	if err := m.preserve(&o); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(m.directory, "progress.json"))
	if err != nil {
		t.Fatal(err)
	}
	if o.ProgressSamples != 1 || !strings.Contains(string(data), "connection closed") {
		t.Fatalf("partial progress lost: %+v %s", o, data)
	}
	if err := m.preserve(&o); err != nil {
		t.Fatalf("deferred persistence duplicated artifact: %v", err)
	}
}
