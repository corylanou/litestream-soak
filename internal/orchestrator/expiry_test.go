package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

// staleHeartbeatTimeout is negative so that now - (-1h) = now+1h, making any
// heartbeat recorded before "now" fall before the cutoff and be treated as stale.
const staleHeartbeatTimeout = -time.Hour

func TestCleanExpiredWorkersDestroysOnlyExpired(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	ctx := context.Background()

	past := time.Now().Add(-time.Hour).UTC()
	future := time.Now().Add(time.Hour).UTC()

	createTestWorker(t, db, model.Worker{
		ID:            "exp-only-expired",
		Name:          "exp-only-expired",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
		ExpiresAt:     &past,
	})
	createTestWorker(t, db, model.Worker{
		ID:            "exp-only-active",
		Name:          "exp-only-active",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
		ExpiresAt:     &future,
	})
	createTestWorker(t, db, model.Worker{
		ID:            "exp-only-noexpiry",
		Name:          "exp-only-noexpiry",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	mgr := &Manager{db: db}
	mgr.cleanExpiredWorkers(ctx)

	expiredWorker, err := db.GetWorker("exp-only-expired")
	if err != nil {
		t.Fatalf("GetWorker(expired) error = %v", err)
	}
	if expiredWorker.Status != model.WorkerStopped {
		t.Errorf("expired worker status = %q, want %q", expiredWorker.Status, model.WorkerStopped)
	}

	events, err := db.ListWorkerEvents("exp-only-expired", 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents(expired) error = %v", err)
	}
	var destroyedCount int
	for _, e := range events {
		if e.EventType == "worker_destroyed" {
			destroyedCount++
		}
	}
	if destroyedCount != 1 {
		t.Errorf("worker_destroyed event count = %d, want 1", destroyedCount)
	}

	for _, id := range []string{"exp-only-active", "exp-only-noexpiry"} {
		w, err := db.GetWorker(id)
		if err != nil {
			t.Fatalf("GetWorker(%s) error = %v", id, err)
		}
		if w.Status != model.WorkerRunning {
			t.Errorf("worker %s status = %q, want %q", id, w.Status, model.WorkerRunning)
		}
	}
}

func TestCleanExpiredWorkersIdempotent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	ctx := context.Background()

	past := time.Now().Add(-time.Hour).UTC()
	createTestWorker(t, db, model.Worker{
		ID:            "exp-idem-worker",
		Name:          "exp-idem-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
		ExpiresAt:     &past,
	})

	mgr := &Manager{db: db}
	mgr.cleanExpiredWorkers(ctx)
	mgr.cleanExpiredWorkers(ctx)

	events, err := db.ListWorkerEvents("exp-idem-worker", 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents error = %v", err)
	}
	var destroyedCount int
	for _, e := range events {
		if e.EventType == "worker_destroyed" {
			destroyedCount++
		}
	}
	if destroyedCount != 1 {
		t.Errorf("worker_destroyed event count after 2 runs = %d, want 1", destroyedCount)
	}
}

func TestCleanExpiredWorkersAlreadyStoppedExcluded(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	ctx := context.Background()

	past := time.Now().Add(-time.Hour).UTC()
	createTestWorker(t, db, model.Worker{
		ID:            "exp-stopped-worker",
		Name:          "exp-stopped-worker",
		Status:        model.WorkerStopped,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
		ExpiresAt:     &past,
	})

	mgr := &Manager{db: db}
	mgr.cleanExpiredWorkers(ctx)

	events, err := db.ListWorkerEvents("exp-stopped-worker", 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents error = %v", err)
	}
	for _, e := range events {
		if e.EventType == "worker_destroyed" {
			t.Errorf("unexpected worker_destroyed event for already-stopped worker")
		}
	}
}

func TestCheckStaleWorkersMarksDegradedAndRecordsEvent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	ctx := context.Background()

	createTestWorker(t, db, model.Worker{
		ID:            "stale-marks-degraded",
		Name:          "stale-marks-degraded",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerHeartbeat("stale-marks-degraded"); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat error = %v", err)
	}

	mgr := &Manager{db: db}
	mgr.checkStaleWorkers(ctx, staleHeartbeatTimeout)

	w, err := db.GetWorker("stale-marks-degraded")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}
	if w.Status != model.WorkerDegraded {
		t.Errorf("status = %q, want %q", w.Status, model.WorkerDegraded)
	}
	if w.ErrorMessage != "worker missed heartbeat deadline" {
		t.Errorf("error_message = %q, want %q", w.ErrorMessage, "worker missed heartbeat deadline")
	}

	events, err := db.ListWorkerEvents("stale-marks-degraded", 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents error = %v", err)
	}
	var staleCount int
	for _, e := range events {
		if e.EventType == "worker_stale" {
			staleCount++
		}
	}
	if staleCount != 1 {
		t.Errorf("worker_stale event count = %d, want 1", staleCount)
	}
}

func TestCheckStaleWorkersSkipsWorkersWithoutHeartbeat(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	ctx := context.Background()

	createTestWorker(t, db, model.Worker{
		ID:            "stale-no-hb",
		Name:          "stale-no-hb",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	createTestWorker(t, db, model.Worker{
		ID:            "stale-already-degraded",
		Name:          "stale-already-degraded",
		Status:        model.WorkerDegraded,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerHeartbeat("stale-already-degraded"); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat error = %v", err)
	}

	mgr := &Manager{db: db}
	mgr.checkStaleWorkers(ctx, staleHeartbeatTimeout)

	for _, id := range []string{"stale-no-hb", "stale-already-degraded"} {
		events, err := db.ListWorkerEvents(id, 10)
		if err != nil {
			t.Fatalf("ListWorkerEvents(%s) error = %v", id, err)
		}
		for _, e := range events {
			if e.EventType == "worker_stale" {
				t.Errorf("worker %s got unexpected worker_stale event", id)
			}
		}
	}

	w, err := db.GetWorker("stale-no-hb")
	if err != nil {
		t.Fatalf("GetWorker(stale-no-hb) error = %v", err)
	}
	if w.Status != model.WorkerRunning {
		t.Errorf("stale-no-hb status = %q, want running", w.Status)
	}
}

func TestCheckStaleWorkersFreshHeartbeatNotStale(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	ctx := context.Background()

	createTestWorker(t, db, model.Worker{
		ID:            "stale-fresh-hb",
		Name:          "stale-fresh-hb",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerHeartbeat("stale-fresh-hb"); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat error = %v", err)
	}

	mgr := &Manager{db: db}
	mgr.checkStaleWorkers(ctx, time.Hour)

	w, err := db.GetWorker("stale-fresh-hb")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}
	if w.Status != model.WorkerRunning {
		t.Errorf("status = %q, want %q", w.Status, model.WorkerRunning)
	}

	events, err := db.ListWorkerEvents("stale-fresh-hb", 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents error = %v", err)
	}
	for _, e := range events {
		if e.EventType == "worker_stale" {
			t.Errorf("fresh heartbeat within positive timeout got unexpected worker_stale event")
		}
	}
}

func TestCheckStaleWorkersSendsStaleAlert(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	ctx := context.Background()

	alertCh := make(chan alertWebhookPayload, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload alertWebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			alertCh <- payload
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	createTestWorker(t, db, model.Worker{
		ID:            "stale-sends-alert",
		Name:          "stale-sends-alert",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerHeartbeat("stale-sends-alert"); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat error = %v", err)
	}

	alerts := NewAlertDispatcher(db, "http://ctl.example", server.URL, "")
	mgr := &Manager{db: db, alerts: alerts}
	mgr.checkStaleWorkers(ctx, staleHeartbeatTimeout)

	w, err := db.GetWorker("stale-sends-alert")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}
	if w.Status != model.WorkerDegraded {
		t.Errorf("status = %q, want degraded", w.Status)
	}

	events, err := db.ListWorkerEvents("stale-sends-alert", 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents error = %v", err)
	}
	var staleCount int
	for _, e := range events {
		if e.EventType == "worker_stale" {
			staleCount++
		}
	}
	if staleCount != 1 {
		t.Errorf("worker_stale event count = %d, want 1", staleCount)
	}

	select {
	case payload := <-alertCh:
		if payload.Alert.AlertType != "worker_stale" {
			t.Errorf("alert_type = %q, want worker_stale", payload.Alert.AlertType)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for webhook alert")
	}

	dbAlerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	var staleAlertFound bool
	for _, a := range dbAlerts {
		if a.AlertType == "worker_stale" {
			prefix := "worker_stale:stale-sends-alert:"
			if len(a.Fingerprint) > len(prefix) && a.Fingerprint[:len(prefix)] == prefix {
				staleAlertFound = true
			}
		}
	}
	if !staleAlertFound {
		t.Errorf("no worker_stale alert row found with expected fingerprint prefix")
	}
}

func TestRunExpiryLoopStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	mgr := &Manager{db: db}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		mgr.RunExpiryLoop(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunExpiryLoop did not stop after context cancel")
	}
}

func TestRunHeartbeatMonitorStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	mgr := &Manager{db: db}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		mgr.RunHeartbeatMonitor(ctx, time.Minute)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunHeartbeatMonitor did not stop after context cancel")
	}
}
