package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestRecordWindowedEventAt(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	worker := &Worker{
		ID:            "worker-1",
		Name:          "worker-1",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "litestream123",
		ProfileName:   "burst-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	storedWorker, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if storedWorker.LitestreamSHA != "litestream123" {
		t.Fatalf("storedWorker.LitestreamSHA = %q, want litestream123", storedWorker.LitestreamSHA)
	}

	start := time.Date(2026, time.April, 14, 18, 0, 0, 0, time.UTC)
	created, err := db.RecordWindowedEventAt(worker.ID, "platform_disk_full", "Fly log reported disk pressure: database or disk is full", `{"line":1}`, start, 10*time.Minute)
	if err != nil {
		t.Fatalf("first RecordWindowedEventAt() error = %v", err)
	}
	if !created {
		t.Fatalf("first RecordWindowedEventAt() created = false, want true")
	}

	secondAt := start.Add(5 * time.Minute)
	created, err = db.RecordWindowedEventAt(worker.ID, "platform_disk_full", "Fly log reported disk pressure: database or disk is full", `{"line":2}`, secondAt, 10*time.Minute)
	if err != nil {
		t.Fatalf("second RecordWindowedEventAt() error = %v", err)
	}
	if created {
		t.Fatalf("second RecordWindowedEventAt() created = true, want false")
	}

	events, err := db.ListWorkerEvents(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Details != `{"line":2}` {
		t.Fatalf("events[0].Details = %q, want latest details", events[0].Details)
	}
	if !events[0].CreatedAt.Equal(secondAt) {
		t.Fatalf("events[0].CreatedAt = %s, want %s", events[0].CreatedAt, secondAt)
	}

	thirdAt := secondAt.Add(11 * time.Minute)
	created, err = db.RecordWindowedEventAt(worker.ID, "platform_disk_full", "Fly log reported disk pressure: database or disk is full", `{"line":3}`, thirdAt, 10*time.Minute)
	if err != nil {
		t.Fatalf("third RecordWindowedEventAt() error = %v", err)
	}
	if !created {
		t.Fatalf("third RecordWindowedEventAt() created = false, want true")
	}

	events, err = db.ListWorkerEvents(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents() second call error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) after second window = %d, want 2", len(events))
	}
	if !events[0].CreatedAt.Equal(thirdAt) {
		t.Fatalf("newest event CreatedAt = %s, want %s", events[0].CreatedAt, thirdAt)
	}
}

func TestUpsertReadyDeploymentKeepsLitestreamVersionsDistinct(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	for _, litestreamSHA := range []string{"litestream-a", "litestream-b"} {
		if err := db.UpsertReadyDeployment(&Deployment{
			GitSHA:        "soak-sha",
			LitestreamSHA: litestreamSHA,
			ImageRef:      "registry.fly.io/litestream-soak:sha-test",
			Source:        "main",
			Status:        "ready",
		}); err != nil {
			t.Fatalf("UpsertReadyDeployment(%s) error = %v", litestreamSHA, err)
		}
	}

	deployments, err := db.ListDeployments("main", 10)
	if err != nil {
		t.Fatalf("ListDeployments() error = %v", err)
	}
	if len(deployments) != 2 {
		t.Fatalf("len(deployments) = %d, want 2", len(deployments))
	}

	deployment, err := db.GetDeploymentByVersion("main", "soak-sha", "litestream-a")
	if err != nil {
		t.Fatalf("GetDeploymentByVersion() error = %v", err)
	}
	if deployment.LitestreamSHA != "litestream-a" {
		t.Fatalf("deployment.LitestreamSHA = %q, want litestream-a", deployment.LitestreamSHA)
	}
}

func TestCreateWorkerResetsRuntimeStateOnReuse(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	worker := &Worker{
		ID:            "worker-pr-1228-low-vol",
		Name:          "worker-pr-1228-low-vol",
		Status:        WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "old-soak",
		LitestreamSHA: "old-litestream",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker(first) error = %v", err)
	}
	if err := db.UpdateWorkerHeartbeat(worker.ID); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat() error = %v", err)
	}
	if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, reporting.RuntimePayload{}); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
	}

	reused, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker(before reuse) error = %v", err)
	}
	if reused.LastHeartbeatAt == nil {
		t.Fatalf("LastHeartbeatAt before reuse = nil, want value")
	}
	if reused.LastRuntimeAt == nil {
		t.Fatalf("LastRuntimeAt before reuse = nil, want value")
	}
	createdAtBeforeReuse := reused.CreatedAt

	time.Sleep(1100 * time.Millisecond)

	worker.GitSHA = "new-soak"
	worker.LitestreamSHA = "new-litestream"
	worker.Status = WorkerPending
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker(reuse) error = %v", err)
	}

	reused, err = db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker(after reuse) error = %v", err)
	}
	if reused.LastHeartbeatAt != nil {
		t.Fatalf("LastHeartbeatAt after reuse = %v, want nil", reused.LastHeartbeatAt)
	}
	if reused.LastRuntimeAt != nil {
		t.Fatalf("LastRuntimeAt after reuse = %v, want nil", reused.LastRuntimeAt)
	}
	if reused.LastRuntimeJSON != "" {
		t.Fatalf("LastRuntimeJSON after reuse = %q, want empty", reused.LastRuntimeJSON)
	}
	if !reused.CreatedAt.After(createdAtBeforeReuse) {
		t.Fatalf("CreatedAt after reuse = %s, want reset to a newer timestamp", reused.CreatedAt)
	}
}
