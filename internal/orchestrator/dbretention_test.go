package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestPruneDBOnceDropsOnlyOldHistory(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID: "w-ret", Name: "w-ret", Status: model.WorkerRunning,
		Source: "main", ProfileName: "low-volume", ProfileConfig: "{}",
	})

	oldStart := time.Now().UTC().AddDate(0, 0, -45)
	oldDone := oldStart.Add(time.Minute)
	newStart := time.Now().UTC().Add(-time.Hour)
	newDone := newStart.Add(time.Minute)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: "w-ret", StartedAt: oldStart, CompletedAt: &oldDone,
		Status: "passed", CheckType: "integrity", Passed: true,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: "w-ret", StartedAt: newStart, CompletedAt: &newDone,
		Status: "passed", CheckType: "integrity", Passed: true,
	})
	if err := db.RecordEventAt("w-ret", "test", "old event", "", oldStart); err != nil {
		t.Fatalf("RecordEventAt(old) error = %v", err)
	}
	if err := db.RecordEventAt("w-ret", "test", "new event", "", newStart); err != nil {
		t.Fatalf("RecordEventAt(new) error = %v", err)
	}

	mgr := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	mgr.pruneDBOnce(context.Background(), 30)

	stats, err := db.ListVerificationStatsSince("", time.Now().UTC().AddDate(0, 0, -90))
	if err != nil {
		t.Fatalf("ListVerificationStatsSince() error = %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("verifications after prune = %d, want 1", len(stats))
	}
	events, err := db.ListEvents(10)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].Message != "new event" {
		t.Fatalf("events after prune = %+v, want only the new event", events)
	}
}
