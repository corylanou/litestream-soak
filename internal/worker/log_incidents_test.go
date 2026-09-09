package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestLogIncidentsSurviveRestartWithOriginalIdentity(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), WorkerID: "worker", RunID: "original"}
	runner := NewRunner(cfg)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runner.observeLogIncidents(cancel)
	log := runner.litestreamLog
	for _, line := range []string{`level=ERROR msg="first failure" token=private`, `level=INFO msg="upload complete"`, `level=WARN msg="second failure recovered later"`} {
		if _, err := log.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	runner = NewRunner(cfg)
	var received []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		received = append(received, event)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	reporter := &Reporter{baseURL: server.URL, client: server.Client(), identity: reporting.WorkerIdentity{WorkerID: "worker", RunID: "replacement"}}
	runner.reporter = reporter
	if err := runner.flushChurnEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	if len(received) != 2 {
		t.Fatalf("received=%v", received)
	}
	for _, event := range received {
		if event["run_id"] != "original" || event["workload_event_id"] == "" {
			t.Fatalf("identity lost: %v", event)
		}
		if strings.Contains(event["message"].(string), "private") {
			t.Fatal("secret not redacted")
		}
	}
	if received[0]["message"] == received[1]["message"] {
		t.Fatal("individual errors coalesced")
	}
	if err := runner.flushChurnEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	if len(received) != 2 {
		t.Fatal("delivered events remained pending")
	}
}

func TestLogOutboxPersistenceFailureStopsRun(t *testing.T) {
	t.Setenv("SOAK_WORKER_TOKEN", "test")
	runner := NewRunner(Config{DataDir: t.TempDir(), ControlBaseURL: "http://127.0.0.1:1", WorkerID: "worker"})
	runner.reporter = NewReporter(runner.cfg)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runner.observeLogIncidents(cancel)
	if err := os.WriteFile(filepath.Join(runner.cfg.DataDir, "churn-outbox"), []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.litestreamLog.Write([]byte("level=ERROR msg=failed\n")); err == nil {
		t.Fatal("failed persistence acknowledged")
	}
	if context.Cause(ctx) == nil || runner.litestreamLog.MaintenanceEvidence().Complete {
		t.Fatal("failed persistence did not stop run and invalidate coverage")
	}
}

func TestLogOutboxCapacityAndNetworkIsolation(t *testing.T) {
	runner := NewRunner(Config{DataDir: t.TempDir()})
	event := reporting.WorkerEventPayload{WorkerIdentity: reporting.WorkerIdentity{WorkerID: "worker"}, Message: "first"}
	if err := runner.persistChurnEvidence(event); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	runner.reporter = &Reporter{baseURL: server.URL, client: server.Client()}
	done := make(chan error, 1)
	go func() {
		done <- runner.flushChurnEvidence(context.Background())
	}()
	<-entered
	event.WorkloadEventID = "two"
	queued := make(chan error, 1)
	go func() { queued <- runner.persistChurnEvidence(event) }()
	select {
	case err := <-queued:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("network delivery blocked local persistence")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	event.Message = strings.Repeat("x", 16*1024*1024)
	if err := runner.persistChurnEvidence(event); err == nil {
		t.Fatal("capacity limit not enforced")
	}
}

func TestLogIncidentRedaction(t *testing.T) {
	for _, line := range []string{`level=ERROR token=private`, `{"level":"ERROR","token":"private"}`, `level=WARN url=https://user:private@example.com/`, `level=ERROR url=https://example.com/?X-Amz-Signature=private`} {
		if got := redactLogIncident(line); strings.Contains(got, "private") {
			t.Fatalf("credential retained: %s", got)
		}
	}
}

func TestFragmentedLogIncidentRetainsFullDiagnostic(t *testing.T) {
	runner := NewRunner(Config{DataDir: t.TempDir(), WorkerID: "worker", RunID: "run"})
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runner.observeLogIncidents(cancel)
	prefix := `level=ERROR msg="` + strings.Repeat("x", 2000)
	if _, err := runner.litestreamLog.Write([]byte(prefix)); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.litestreamLog.Write([]byte(" original diagnostic end\"\n")); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(runner.cfg.DataDir, "churn-outbox"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
	body, err := os.ReadFile(filepath.Join(runner.cfg.DataDir, "churn-outbox", entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var event reporting.WorkerEventPayload
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(event.Message, strings.Repeat("x", 2000)) || !strings.Contains(event.Message, "original diagnostic end") {
		t.Fatal("fragmented diagnostic truncated before persistence")
	}
}

func TestProcessFinalFragmentReachesDurableJournal(t *testing.T) {
	dir := t.TempDir()
	script := "#!/bin/sh\nprintf 'level=INFO msg=retrying-upload\\nlevel=INFO msg=upload-complete\\nlevel=ERROR msg=final-failure'\n"
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	db, err := model.Open(filepath.Join(dir, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	w := model.Worker{ID: "worker", Name: "worker", Source: "main", ProfileConfig: "{}"}
	if err := db.CreateWorker(&w); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event reporting.WorkerEventPayload
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		raw, _ := json.Marshal(event)
		if err := db.RecordEvent(event.WorkerID, event.EventType, event.Message, string(raw)); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	runner := NewRunner(Config{DataDir: dir, WorkerID: "worker", Source: "main", RunID: "original"})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runner.observeLogIncidents(cancel)
	runner.reporter = &Reporter{baseURL: server.URL, client: server.Client()}
	if err := runner.startLitestream(ctx); err != nil {
		t.Fatal(err)
	}
	<-runner.litestreamDoneChan()
	if err := runner.flushChurnEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListEvidenceEvents("main")
	if err != nil || len(events) != 2 {
		t.Fatalf("journal=%+v err=%v", events, err)
	}
	messages := events[0].Message + events[1].Message
	if !strings.Contains(messages, "retrying-upload") || !strings.Contains(messages, "final-failure") {
		t.Fatalf("original diagnostics lost: %s", messages)
	}
}
