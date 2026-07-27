package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
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

func TestVerificationFailureClassificationRoundTrip(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{
		ID:            "worker-classification-round-trip",
		Name:          "worker-classification-round-trip",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "many-dbs-100-dir",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	completedAt := time.Date(2026, 7, 26, 1, 29, 13, 0, time.UTC)
	verification := &Verification{
		WorkerID:    worker.ID,
		StartedAt:   completedAt.Add(-time.Minute),
		CompletedAt: &completedAt,
		Status:      "failed",
		CheckType:   "integrity",
		Passed:      false,
		ErrorMessage: `sync database: db sync: stage-write ltx file:
disk full: write header`,
		FailureClassification: &reporting.FailureClassification{
			Stage:     "disk_capacity",
			Signature: "soak_fixture_disk_exhausted",
		},
	}
	if err := db.RecordVerification(verification); err != nil {
		t.Fatalf("RecordVerification() error = %v", err)
	}

	assertFailureClassification := func(t *testing.T, got *reporting.FailureClassification) {
		t.Helper()
		if got == nil {
			t.Fatal("FailureClassification = nil")
		}
		if got.Stage != "disk_capacity" {
			t.Errorf("Stage = %q, want disk_capacity", got.Stage)
		}
		if got.Signature != "soak_fixture_disk_exhausted" {
			t.Errorf("Signature = %q, want soak_fixture_disk_exhausted", got.Signature)
		}
	}

	listed, err := db.ListVerifications(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListVerifications() error = %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("len(ListVerifications()) = %d, want 1", len(listed))
	}
	assertFailureClassification(t, listed[0].FailureClassification)

	latest, err := db.GetLatestFailedVerification(worker.ID)
	if err != nil {
		t.Fatalf("GetLatestFailedVerification() error = %v", err)
	}
	if latest == nil {
		t.Fatal("GetLatestFailedVerification() = nil")
	}
	assertFailureClassification(t, latest.FailureClassification)

	recent, err := db.ListRecentFailedVerifications(10)
	if err != nil {
		t.Fatalf("ListRecentFailedVerifications() error = %v", err)
	}
	if len(recent) != 1 {
		t.Fatalf("len(ListRecentFailedVerifications()) = %d, want 1", len(recent))
	}
	assertFailureClassification(t, recent[0].FailureClassification)

	stats, err := db.ListVerificationStatsSince(worker.Source, completedAt.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ListVerificationStatsSince() error = %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("len(ListVerificationStatsSince()) = %d, want 1", len(stats))
	}
	assertFailureClassification(t, stats[0].FailureClassification)
}

func TestVerificationWithoutStoredClassificationRemainsReadable(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{
		ID:            "worker-legacy-classification",
		Name:          "worker-legacy-classification",
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

	startedAt := time.Date(2026, 7, 26, 1, 29, 13, 0, time.UTC)
	if _, err := db.exec(`
		INSERT INTO verifications (
			worker_id, started_at, status, check_type, source_checksum,
			restored_checksum, passed, duration_ms, error_message
		) VALUES (?, ?, 'failed', 'integrity', '', '', 0, 0, 'database or disk is full')`,
		worker.ID,
		startedAt,
	); err != nil {
		t.Fatalf("insert legacy verification: %v", err)
	}

	verifications, err := db.ListVerifications(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListVerifications() error = %v", err)
	}
	if len(verifications) != 1 {
		t.Fatalf("len(ListVerifications()) = %d, want 1", len(verifications))
	}
	if verifications[0].FailureClassification != nil {
		t.Fatalf("FailureClassification = %+v, want nil", verifications[0].FailureClassification)
	}
}

func TestMalformedStoredClassificationIsIgnored(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{
		ID:            "worker-malformed-classification",
		Name:          "worker-malformed-classification",
		Status:        WorkerDegraded,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "many-dbs-100-dir",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	startedAt := time.Date(2026, 7, 26, 1, 29, 13, 0, time.UTC)
	if _, err := db.exec(`
		INSERT INTO verifications (
			worker_id, started_at, status, check_type, source_checksum,
			restored_checksum, passed, duration_ms, error_message,
			failure_classification_json
		) VALUES (?, ?, 'failed', 'integrity', '', '', 0, 0,
			'database or disk is full',
			'{"signature":"soak_fixture_disk_exhausted"}')`,
		worker.ID,
		startedAt,
	); err != nil {
		t.Fatalf("insert malformed classification: %v", err)
	}

	assertMissing := func(t *testing.T, classification *reporting.FailureClassification) {
		t.Helper()
		if classification != nil {
			t.Fatalf("FailureClassification = %+v, want nil", classification)
		}
	}

	listed, err := db.ListVerifications(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListVerifications() error = %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("len(ListVerifications()) = %d, want 1", len(listed))
	}
	assertMissing(t, listed[0].FailureClassification)

	latest, err := db.GetLatestFailedVerification(worker.ID)
	if err != nil {
		t.Fatalf("GetLatestFailedVerification() error = %v", err)
	}
	if latest == nil {
		t.Fatal("GetLatestFailedVerification() = nil")
	}
	assertMissing(t, latest.FailureClassification)

	recent, err := db.ListRecentFailedVerifications(10)
	if err != nil {
		t.Fatalf("ListRecentFailedVerifications() error = %v", err)
	}
	if len(recent) != 1 {
		t.Fatalf("len(ListRecentFailedVerifications()) = %d, want 1", len(recent))
	}
	assertMissing(t, recent[0].FailureClassification)

	stats, err := db.ListVerificationStatsSince(worker.Source, startedAt.Add(-time.Minute))
	if err != nil {
		t.Fatalf("ListVerificationStatsSince() error = %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("len(ListVerificationStatsSince()) = %d, want 1", len(stats))
	}
	assertMissing(t, stats[0].FailureClassification)
}
