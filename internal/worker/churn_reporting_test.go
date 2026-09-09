package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
	if err := restarted.flushRunEvidence(context.Background()); err != nil {
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
	if err := r.flushRunEvidence(context.Background()); err == nil {
		t.Fatal("failed delivery hidden")
	}
	files, err := filepath.Glob(filepath.Join(cfg.DataDir, "churn-outbox", "*.json"))
	if err != nil || len(files) != 2 {
		t.Fatalf("pending files=%v err=%v", files, err)
	}
	var maxErrors uint64
	for _, path := range files {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var event reporting.WorkerEventPayload
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatal(err)
		}
		maxErrors = max(maxErrors, event.WorkloadErrorsTotal)
		if event.WorkloadCounterEpoch == "" || NewRunner(cfg).currentSnapshot().WorkloadCounterEpoch == event.WorkloadCounterEpoch {
			t.Fatal("missing or reused counter epoch")
		}
	}
	if maxErrors != 2 {
		t.Fatal("pending totals lost")
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

func TestChurnOutboxPreservesEveryFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	r := NewRunner(cfg)
	for _, message := range []string{"first failure", "second failure"} {
		if err := r.recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New(message)); err != nil {
			t.Fatal(err)
		}
	}
	files, err := filepath.Glob(filepath.Join(cfg.DataDir, "churn-outbox", "*.json"))
	if err != nil || len(files) != 2 {
		t.Fatalf("failure details overwritten: files=%v err=%v", files, err)
	}
	ids := map[string]bool{}
	for _, path := range files {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatal(err)
		}
		id, _ := fields["workload_event_id"].(string)
		if id == "" || ids[id] {
			t.Fatalf("missing/duplicate event ID %q", id)
		}
		ids[id] = true
	}
}

func TestChurnBlockedDeliveryDoesNotBlockRecording(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/events") {
			close(entered)
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	cfg.ControlBaseURL = server.URL
	r := NewRunner(cfg)
	if err := r.recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New("failure")); err != nil {
		t.Fatal(err)
	}
	r.reporter = NewReporter(cfg)
	uploadDone := make(chan error, 1)
	go func() { uploadDone <- r.flushRunEvidence(context.Background()) }()
	<-entered
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.recordChurnAttempt(context.Background(), "enqueue", 1, time.Millisecond, nil); err != nil {
			t.Error(err)
		}
		r.sendHeartbeat(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		close(release)
		<-uploadDone
		t.Fatal("HTTP delivery blocked counter update or heartbeat")
	}
	close(release)
	if err := <-uploadDone; err != nil {
		t.Fatal(err)
	}
	if r.currentSnapshot().WorkloadAttemptsTotal != 2 {
		t.Fatal("lost concurrent attempt")
	}
}

func TestChurnRetryPreservesExactEvent(t *testing.T) {
	var received []reporting.WorkerEventPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var event reporting.WorkerEventPayload
		if err := json.NewDecoder(req.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		received = append(received, event)
		if len(received) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	cfg.ControlBaseURL = server.URL
	r := NewRunner(cfg)
	r.reporter = NewReporter(cfg)
	if err := r.recordChurnAttempt(context.Background(), "claim", 0, 2*time.Millisecond, errors.New("original failure")); err != nil {
		t.Fatal(err)
	}
	if err := r.flushRunEvidence(context.Background()); err == nil {
		t.Fatal("expected failed send")
	}
	if err := r.flushRunEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(received) != 2 || received[0].WorkloadEventID == "" || !reflect.DeepEqual(received[0], received[1]) {
		t.Fatalf("retry changed event: %+v", received)
	}
	if received[0].WorkloadOperation != "claim" || received[0].WorkloadErrorKind != "other" || received[0].WorkloadLatencySeconds != .002 {
		t.Fatal("lost error metadata")
	}
}

func TestChurnOutboxCapacityFailsClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	dir := filepath.Join(cfg.DataDir, "churn-outbox")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < evidenceOutboxMaxEvents; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.json", i)), []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	r := NewRunner(cfg)
	if err := r.recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New("overflow")); err == nil || !strings.Contains(err.Error(), "capacity exceeded") {
		t.Fatalf("capacity not enforced: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != evidenceOutboxMaxEvents {
		t.Fatalf("existing evidence changed: %d %v", len(entries), err)
	}
}

func TestChurnUploaderDeliversAndStops(t *testing.T) {
	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK); received <- struct{}{} }))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	cfg.ControlBaseURL = server.URL
	r := NewRunner(cfg)
	r.reporter = NewReporter(cfg)
	stop := r.startEvidenceUploader(context.Background())
	defer stop()
	if err := r.recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New("failure")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("uploader did not deliver")
	}
}

func TestChurnOutboxByteLimitFailsClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.DataDir = t.TempDir()
	dir := filepath.Join(cfg.DataDir, "churn-outbox")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(dir, "pending.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(evidenceOutboxMaxBytes); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := NewRunner(cfg).recordChurnAttempt(context.Background(), "claim", 0, time.Millisecond, errors.New("overflow")); err == nil || !strings.Contains(err.Error(), "capacity exceeded") {
		t.Fatalf("byte capacity not enforced: %v", err)
	}
}
