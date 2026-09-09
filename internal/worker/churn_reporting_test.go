package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestChurnCountersSurviveRecoveryInReports(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	cfg.RunID = "run-one"
	r := NewRunner(cfg)
	if err := r.recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New("failure")); err != nil {
		t.Fatal(err)
	}
	if err := r.recordChurnAttempt(context.Background(), "claim", 1, time.Millisecond, nil); err != nil {
		t.Fatal(err)
	}
	snapshot := r.currentSnapshot()
	if !snapshot.WorkloadCountersPresent || snapshot.WorkloadAttemptsTotal != 2 || snapshot.WorkloadMutationsTotal != 1 || snapshot.WorkloadErrorsTotal != 1 {
		t.Fatalf("lost cumulative evidence: %+v", snapshot.WorkloadCounters)
	}
}

func TestChurnOutboxReplaysOriginalIdentity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	cfg.RunID = "old-run"
	cfg.WorkerID = "old-worker"
	r := NewRunner(cfg)
	if err := r.recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New("failed before shutdown")); err != nil {
		t.Fatal(err)
	}
	var event reporting.WorkerEventPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := json.NewDecoder(req.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cfg.RunID = "new-run"
	cfg.WorkerID = "new-worker"
	cfg.ControlBaseURL = server.URL
	restarted := NewRunner(cfg)
	restarted.reporter = NewReporter(cfg)
	if err := restarted.flushChurnEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if event.RunID != "old-run" || event.WorkerID != "old-worker" || event.WorkloadErrorsTotal != 1 || event.EventType != "workload_error" {
		t.Fatalf("replay lost original evidence: %+v", event)
	}
	files, err := filepath.Glob(filepath.Join(cfg.DataDir, "churn-outbox", "*.json"))
	if err != nil || len(files) != 0 {
		t.Fatalf("acknowledged outbox=%v err=%v", files, err)
	}
}

func TestChurnFailedDeliveryRemainsPending(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	cfg.ControlBaseURL = server.URL
	cfg.RunID = "retained"
	r := NewRunner(cfg)
	r.reporter = NewReporter(cfg)
	for i := 0; i < 2; i++ {
		if err := r.recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New("busy delivery")); err != nil {
			t.Fatal(err)
		}
	}
	files, err := filepath.Glob(filepath.Join(cfg.DataDir, "churn-outbox", "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("pending files=%v err=%v", files, err)
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var event reporting.WorkerEventPayload
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatal(err)
	}
	if event.WorkloadErrorsTotal != 2 || event.WorkloadAttemptsTotal != 2 || event.WorkloadCounterEpoch == "" {
		t.Fatalf("pending totals lost: %+v", event.WorkloadCounters)
	}
	if NewRunner(cfg).currentSnapshot().WorkloadCounterEpoch == event.WorkloadCounterEpoch {
		t.Fatal("restart reused counter epoch")
	}
}

func TestChurnPersistenceFailureIsFatal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(cfg.DataDir, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(cfg)
	if err := r.recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New("failure")); err == nil {
		t.Fatal("failed durable write hidden")
	}
}

func TestChurnHeartbeatIncludesCounters(t *testing.T) {
	var heartbeat reporting.HeartbeatPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := json.NewDecoder(req.Body).Decode(&heartbeat); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	cfg.ControlBaseURL = server.URL
	cfg.RunID = "heartbeat-run"
	r := NewRunner(cfg)
	r.reporter = NewReporter(cfg)
	if err := r.recordChurnAttempt(context.Background(), "enqueue", 1, time.Millisecond, nil); err != nil {
		t.Fatal(err)
	}
	r.sendHeartbeat(context.Background())
	if heartbeat.RunID != cfg.RunID || heartbeat.WorkloadAttemptsTotal != 1 || heartbeat.WorkloadMutationsTotal != 1 || !heartbeat.WorkloadCountersPresent {
		t.Fatalf("heartbeat lost counters: %+v", heartbeat)
	}
}
