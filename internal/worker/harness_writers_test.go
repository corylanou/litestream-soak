package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestHarnessWriterTrackerConcurrentWriters(t *testing.T) {
	tracker := &harnessWriterTracker{}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			end := tracker.begin("load_retention", "/data/db")
			_ = tracker.snapshot()
			tracker.setActive("load", "/data/db", i%2 == 0)
			end()
		}()
	}
	wg.Wait()
	for _, writer := range tracker.snapshot() {
		if writer.Name != "load" {
			t.Fatalf("ended writer still active: %+v", writer)
		}
	}
	tracker.setActive("load", "/data/db", false)
	if writers := tracker.snapshot(); len(writers) != 0 {
		t.Fatalf("writers=%+v", writers)
	}
	end := tracker.begin("verifier_checkpoint", "/data/db")
	end()
	end()
	if writers := tracker.snapshot(); len(writers) != 0 {
		t.Fatalf("double end left writers=%+v", writers)
	}
}

func TestSQLiteBusyLogIncidentIncludesActiveHarnessWriters(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), WorkerID: "worker", RunID: "run"}
	runner := NewRunner(cfg)
	runner.harnessWriters = &harnessWriterTracker{}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runner.observeLogIncidents(cancel)
	endRetention := runner.harnessWriters.begin("load_retention", "/data/db")
	runner.harnessWriters.setActive("load", "/data/db", true)
	lines := []string{
		`level=ERROR msg="sync error" db=/data/db error="checkpoint: bump litestream seq: database is locked (5) (SQLITE_BUSY)"`,
		`level=ERROR msg="upload failed" error="connection reset"`,
	}
	for _, line := range lines {
		if _, err := runner.litestreamLog.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	endRetention()
	runner.harnessWriters.setActive("load", "/data/db", false)
	if _, err := runner.litestreamLog.Write([]byte(`level=ERROR msg="sync error" error="database is locked"` + "\n")); err != nil {
		t.Fatal(err)
	}
	var received []reporting.WorkerEventPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event reporting.WorkerEventPayload
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		received = append(received, event)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	runner.reporter = &Reporter{baseURL: server.URL, client: server.Client(), identity: reporting.WorkerIdentity{WorkerID: "worker", RunID: "run"}}
	if err := runner.flushRunEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	if len(received) != 3 {
		t.Fatalf("received=%+v", received)
	}
	slices.SortFunc(received, func(a, b reporting.WorkerEventPayload) int {
		return cmp.Compare(a.Errors, b.Errors)
	})
	busy := received[0]
	if !busy.SQLiteBusy || len(busy.ActiveHarnessWriters) != 2 {
		t.Fatalf("busy event missing writers: %+v", busy)
	}
	names := map[string]bool{}
	for _, writer := range busy.ActiveHarnessWriters {
		if writer.DBPath != "/data/db" || writer.StartedAt.IsZero() || writer.StartedAt.After(time.Now()) {
			t.Fatalf("writer=%+v", writer)
		}
		names[writer.Name] = true
	}
	if !names["load_retention"] || !names["load"] {
		t.Fatalf("writers=%+v", busy.ActiveHarnessWriters)
	}
	if received[1].SQLiteBusy || len(received[1].ActiveHarnessWriters) != 0 {
		t.Fatalf("non-busy event attributed: %+v", received[1])
	}
	if !received[2].SQLiteBusy || len(received[2].ActiveHarnessWriters) != 0 {
		t.Fatalf("idle busy event: %+v", received[2])
	}
}
