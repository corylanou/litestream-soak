package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestUpdateWorkerRuntimeSnapshotHealthyPayloadSetsLastRuntimeAt(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{
		ID:            "worker-healthy",
		Name:          "worker-healthy",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-a",
		LitestreamSHA: "ls-a",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	collectedAt := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	payload := reporting.RuntimePayload{
		LitestreamSnapshotHealthy: true,
		SnapshotCollectedAt:       collectedAt,
	}
	if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, payload); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
	}

	stored, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if stored.LastRuntimeAt == nil {
		t.Fatalf("LastRuntimeAt = nil, want %v", collectedAt)
	}
	if !stored.LastRuntimeAt.Equal(collectedAt) {
		t.Fatalf("LastRuntimeAt = %v, want %v", stored.LastRuntimeAt, collectedAt)
	}
}

func TestUpdateWorkerRuntimeSnapshotUnhealthyPayloadPreservesLastRuntimeAt(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{
		ID:            "worker-unhealthy",
		Name:          "worker-unhealthy",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-a",
		LitestreamSHA: "ls-a",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	unhealthyPayload := reporting.RuntimePayload{
		LitestreamSnapshotHealthy: false,
		LitestreamSnapshotError:   "snapshot failed",
		SnapshotCollectedAt:       time.Now().UTC(),
	}

	t.Run("stays nil when previously nil", func(t *testing.T) {
		if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, unhealthyPayload); err != nil {
			t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
		}
		stored, err := db.GetWorker(worker.ID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.LastRuntimeAt != nil {
			t.Fatalf("LastRuntimeAt = %v, want nil (unhealthy payload must not set timestamp)", stored.LastRuntimeAt)
		}
		if stored.LastRuntimeJSON == "" {
			t.Fatalf("LastRuntimeJSON = empty, want payload JSON (must still be written)")
		}
	})

	t.Run("prior healthy value is preserved", func(t *testing.T) {
		healthyAt := time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC)
		healthyPayload := reporting.RuntimePayload{
			LitestreamSnapshotHealthy: true,
			SnapshotCollectedAt:       healthyAt,
		}
		if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, healthyPayload); err != nil {
			t.Fatalf("UpdateWorkerRuntimeSnapshot(healthy) error = %v", err)
		}

		if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, unhealthyPayload); err != nil {
			t.Fatalf("UpdateWorkerRuntimeSnapshot(unhealthy) error = %v", err)
		}

		stored, err := db.GetWorker(worker.ID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.LastRuntimeAt == nil {
			t.Fatalf("LastRuntimeAt = nil, want %v (prior healthy value must be preserved)", healthyAt)
		}
		if !stored.LastRuntimeAt.Equal(healthyAt) {
			t.Fatalf("LastRuntimeAt = %v, want %v (prior healthy value must be preserved)", stored.LastRuntimeAt, healthyAt)
		}
		if stored.LastRuntimeJSON == "" {
			t.Fatalf("LastRuntimeJSON = empty, want payload JSON (must still be written on unhealthy)")
		}
	})
}
