package model

import (
	"path/filepath"
	"testing"
	"time"
)

func seriesTestDB(t *testing.T) *DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seriesTestWorker(t *testing.T, db *DB, id, source string) *Worker {
	t.Helper()
	worker := &Worker{
		ID:            id,
		Name:          id,
		Status:        WorkerRunning,
		Source:        source,
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker(%s) error = %v", id, err)
	}
	return worker
}

func recordSeriesVerification(t *testing.T, db *DB, workerID string, startedAt time.Time, passed bool, status string, durationMS int) {
	t.Helper()
	completedAt := startedAt.Add(time.Duration(durationMS) * time.Millisecond)
	v := &Verification{
		WorkerID:    workerID,
		StartedAt:   startedAt,
		CompletedAt: &completedAt,
		Status:      status,
		CheckType:   "integrity",
		Passed:      passed,
		DurationMS:  durationMS,
	}
	if err := db.RecordVerification(v); err != nil {
		t.Fatalf("RecordVerification() error = %v", err)
	}
}

func TestListVerificationTicksLimitsPerWorkerAndOrdersChronologically(t *testing.T) {
	t.Parallel()

	db := seriesTestDB(t)
	workerA := seriesTestWorker(t, db, "worker-ticks-a", "main")
	workerB := seriesTestWorker(t, db, "worker-ticks-b", "pr-42")

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	for i := range 5 {
		passed := i != 3
		status := "completed"
		if !passed {
			status = "failed"
		}
		recordSeriesVerification(t, db, workerA.ID, base.Add(time.Duration(i)*time.Hour), passed, status, 1000+i)
	}
	recordSeriesVerification(t, db, workerB.ID, base, true, "completed", 500)
	recordSeriesVerification(t, db, workerB.ID, base.Add(time.Hour), false, "aborted", 600)
	recordSeriesVerification(t, db, workerB.ID, base.Add(-30*24*time.Hour), false, "failed", 700)

	ticks, err := db.ListVerificationTicks(3, base.Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("ListVerificationTicks() error = %v", err)
	}

	gotA := ticks[workerA.ID]
	if len(gotA) != 3 {
		t.Fatalf("len(ticks[%s]) = %d, want 3: %+v", workerA.ID, len(gotA), gotA)
	}
	for i := 1; i < len(gotA); i++ {
		if !gotA[i].StartedAt.After(gotA[i-1].StartedAt) {
			t.Fatalf("ticks[%s] not in chronological order: %+v", workerA.ID, gotA)
		}
	}
	if !gotA[0].StartedAt.Equal(base.Add(2 * time.Hour)) {
		t.Fatalf("ticks[%s][0].StartedAt = %v, want %v (latest 3 only)", workerA.ID, gotA[0].StartedAt, base.Add(2*time.Hour))
	}
	if gotA[1].Passed || gotA[1].Status != "failed" {
		t.Fatalf("ticks[%s][1] = %+v, want failed tick", workerA.ID, gotA[1])
	}

	gotB := ticks[workerB.ID]
	if len(gotB) != 2 {
		t.Fatalf("len(ticks[%s]) = %d, want 2 (verification older than since must be excluded): %+v", workerB.ID, len(gotB), gotB)
	}
	if gotB[1].Status != "aborted" {
		t.Fatalf("ticks[%s][1].Status = %q, want aborted", workerB.ID, gotB[1].Status)
	}
}

func TestListVerificationStatsSinceFiltersBySourceAndTime(t *testing.T) {
	t.Parallel()

	db := seriesTestDB(t)
	mainWorker := seriesTestWorker(t, db, "worker-stats-main", "main")
	prWorker := seriesTestWorker(t, db, "worker-stats-pr", "pr-99")

	cutoff := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	recordSeriesVerification(t, db, mainWorker.ID, cutoff.Add(-time.Hour), true, "completed", 800)
	recordSeriesVerification(t, db, mainWorker.ID, cutoff.Add(time.Hour), true, "completed", 900)
	recordSeriesVerification(t, db, mainWorker.ID, cutoff.Add(2*time.Hour), false, "failed", 1500)
	recordSeriesVerification(t, db, prWorker.ID, cutoff.Add(time.Hour), false, "failed", 700)

	stats, err := db.ListVerificationStatsSince("main", cutoff)
	if err != nil {
		t.Fatalf("ListVerificationStatsSince() error = %v", err)
	}

	if len(stats) != 2 {
		t.Fatalf("len(stats) = %d, want 2: %+v", len(stats), stats)
	}
	if !stats[0].StartedAt.Equal(cutoff.Add(time.Hour)) {
		t.Fatalf("stats[0].StartedAt = %v, want %v (ascending order)", stats[0].StartedAt, cutoff.Add(time.Hour))
	}
	if !stats[0].Passed || stats[0].DurationMS != 900 {
		t.Fatalf("stats[0] = %+v, want passed with duration 900", stats[0])
	}
	if stats[1].Passed || stats[1].Status != "failed" || stats[1].DurationMS != 1500 {
		t.Fatalf("stats[1] = %+v, want failed with duration 1500", stats[1])
	}
}
