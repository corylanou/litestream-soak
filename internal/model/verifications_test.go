package model

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFailedVerificationQueriesIgnoreAborted(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{
		ID:            "worker-aborted-failure-query",
		Name:          "worker-aborted-failure-query",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	failedAt := time.Date(2026, 4, 26, 14, 0, 0, 0, time.UTC)
	failed := &Verification{
		WorkerID:     worker.ID,
		StartedAt:    failedAt.Add(-2 * time.Minute),
		CompletedAt:  &failedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: "checksum mismatch",
	}
	if err := db.RecordVerification(failed); err != nil {
		t.Fatalf("RecordVerification(failed) error = %v", err)
	}

	abortedAt := failedAt.Add(10 * time.Minute)
	aborted := &Verification{
		WorkerID:     worker.ID,
		StartedAt:    abortedAt.Add(-2 * time.Minute),
		CompletedAt:  &abortedAt,
		Status:       "aborted",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: "litestream process stopped during verification",
	}
	if err := db.RecordVerification(aborted); err != nil {
		t.Fatalf("RecordVerification(aborted) error = %v", err)
	}

	latest, err := db.GetLatestFailedVerification(worker.ID)
	if err != nil {
		t.Fatalf("GetLatestFailedVerification() error = %v", err)
	}
	if latest == nil {
		t.Fatal("GetLatestFailedVerification() = nil, want failed verification")
	}
	if latest.ID != failed.ID {
		t.Fatalf("latest failed ID = %d, want %d", latest.ID, failed.ID)
	}

	recent, err := db.ListRecentFailedVerifications(10)
	if err != nil {
		t.Fatalf("ListRecentFailedVerifications() error = %v", err)
	}
	if len(recent) != 1 {
		t.Fatalf("len(recent) = %d, want 1: %+v", len(recent), recent)
	}
	if recent[0].ID != failed.ID {
		t.Fatalf("recent[0].ID = %d, want %d", recent[0].ID, failed.ID)
	}
}
