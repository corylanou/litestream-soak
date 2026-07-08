package model

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPruneBatchedDeletesAcrossBatches(t *testing.T) {
	db := seriesTestDB(t)

	originalBatch := pruneBatchSize
	pruneBatchSize = 3
	t.Cleanup(func() { pruneBatchSize = originalBatch })

	if err := db.CreateWorker(&Worker{ID: "w-prune", Name: "w-prune", Status: WorkerRunning, ProfileName: "low-volume", ProfileConfig: "{}"}); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	old := time.Now().UTC().AddDate(0, 0, -60)
	fresh := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 10; i++ {
		done := old.Add(time.Minute)
		if err := db.RecordVerification(&Verification{
			WorkerID: "w-prune", StartedAt: old.Add(time.Duration(i) * time.Minute), CompletedAt: &done,
			Status: "passed", CheckType: "integrity", Passed: true,
		}); err != nil {
			t.Fatalf("RecordVerification(old %d) error = %v", i, err)
		}
		if err := db.RecordEventAt("w-prune", "test", fmt.Sprintf("old %d", i), "", old); err != nil {
			t.Fatalf("RecordEventAt(old %d) error = %v", i, err)
		}
	}
	doneFresh := fresh.Add(time.Minute)
	if err := db.RecordVerification(&Verification{
		WorkerID: "w-prune", StartedAt: fresh, CompletedAt: &doneFresh,
		Status: "passed", CheckType: "integrity", Passed: true,
	}); err != nil {
		t.Fatalf("RecordVerification(fresh) error = %v", err)
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -30)
	deletedVerifications, err := db.PruneVerificationsBefore(context.Background(), cutoff)
	if err != nil {
		t.Fatalf("PruneVerificationsBefore() error = %v", err)
	}
	if deletedVerifications != 10 {
		t.Fatalf("verifications deleted = %d, want 10", deletedVerifications)
	}
	deletedEvents, err := db.PruneEventsBefore(context.Background(), cutoff)
	if err != nil {
		t.Fatalf("PruneEventsBefore() error = %v", err)
	}
	if deletedEvents != 10 {
		t.Fatalf("events deleted = %d, want 10", deletedEvents)
	}

	remaining, err := db.ListVerifications("w-prune", 50)
	if err != nil {
		t.Fatalf("ListVerifications() error = %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("remaining verifications = %d, want 1", len(remaining))
	}

	checkpoint, err := db.CheckpointWAL()
	if err != nil {
		t.Fatalf("CheckpointWAL() error = %v", err)
	}
	if checkpoint.Busy != 0 {
		t.Fatalf("CheckpointWAL busy = %d, want 0 in a quiet test db", checkpoint.Busy)
	}
}
