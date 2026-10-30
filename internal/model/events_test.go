package model

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRecordWindowedEventAtCountsOnlyDistinctOccurrences(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	worker := &Worker{ID: "worker-replay", Name: "worker-replay", Status: WorkerRunning, Source: "main", GitSHA: "abc123", ProfileName: "burst-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	const message = "Fly platform event: main child exited with exit code: 1"
	first := time.Date(2026, time.October, 29, 18, 24, 0, 0, time.UTC)
	second := first.Add(3 * time.Minute)
	batch := []struct {
		at      time.Time
		details string
	}{
		{first, `{"timestamp":"2026-10-29T18:24:00Z","line":"crash one"}`},
		{second, `{"timestamp":"2026-10-29T18:27:00Z","line":"crash two"}`},
	}
	for replay := range 3 {
		for _, line := range batch {
			if _, err := db.RecordWindowedEventAt(worker.ID, "platform_restart", message, line.details, line.at, 10*time.Minute); err != nil {
				t.Fatalf("replay %d RecordWindowedEventAt() error = %v", replay, err)
			}
		}
	}
	if _, err := db.RecordWindowedEventAt(worker.ID, "platform_restart", message, batch[0].details, first, 10*time.Minute); err != nil {
		t.Fatalf("out-of-order replay error = %v", err)
	}

	events, err := db.ListWorkerEvents(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want one merged row: %+v", len(events), events)
	}
	if events[0].Occurrences != 2 || !events[0].CreatedAt.Equal(second) || events[0].Details != batch[1].details {
		t.Fatalf("event = %+v, want 2 distinct occurrences ending at %s with the latest payload", events[0], second)
	}
}

func TestRecordWindowedEventAtTracksRestoreOutputProvenance(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	worker := &Worker{ID: "worker-provenance", Name: "worker-provenance", Status: WorkerRunning, Source: "main", GitSHA: "abc123", ProfileName: "gate-stress", ProfileConfig: "{}"}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	const message = "Fly log reported disk pressure: no space left on device"
	first := time.Date(2026, time.October, 30, 2, 30, 0, 0, time.UTC)
	staging := `{"message":"level=ERROR msg=\"disk full while staging ltx file\" error=\"write /data/.test.db-litestream/ltx/0/0000000000000042.ltx: no space left on device\""}`
	restoreOutput := `{"message":"level=ERROR msg=\"Verification failed\" error=\"restore failed: validation failed (exit 1): Error: decode database: write page 501132: write /data/scenarios/overload-truncate0/test.db.restored.tmp: no space left on device\""}`
	for replay := range 2 {
		for i, line := range []string{staging, restoreOutput, restoreOutput} {
			if _, err := db.RecordWindowedEventAt(worker.ID, "platform_disk_full", message, line, first.Add(time.Duration(i)*2*time.Minute), 10*time.Minute); err != nil {
				t.Fatalf("replay %d line %d: %v", replay, i, err)
			}
		}
	}
	events, err := db.ListWorkerEvents(worker.ID, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events = %+v, %v; want one coalesced row", events, err)
	}
	if events[0].Occurrences != 3 || events[0].RestoreOutputOccurrences != 2 {
		t.Fatalf("occurrences = %d restore-output = %d, want 3 and 2", events[0].Occurrences, events[0].RestoreOutputOccurrences)
	}
}

func TestRecordWindowedEventAtSeedsLegacyRowsBeforeCounting(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	worker := &Worker{ID: "worker-legacy", Name: "worker-legacy", Status: WorkerRunning, Source: "main", GitSHA: "abc123", ProfileName: "burst-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	const message = "Fly platform event: main child exited with exit code: 1"
	legacyAt := time.Date(2026, time.October, 29, 18, 24, 35, 123456789, time.UTC)
	legacyDetails := `{"timestamp":"2026-10-29T18:24:35.123456789Z","line":"crash one"}`
	if _, err := db.exec(`INSERT INTO events (worker_id, event_type, message, details, created_at) VALUES (?, ?, ?, ?, ?)`, worker.ID, "platform_restart", message, legacyDetails, legacyAt); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	legacy, err := db.ListWorkerEvents(worker.ID, 10)
	if err != nil || len(legacy) != 1 || legacy[0].Occurrences != 1 {
		t.Fatalf("legacy row = %+v, %v; want occurrences 1 from the migration default", legacy, err)
	}

	for replay := range 2 {
		created, err := db.RecordWindowedEventAt(worker.ID, "platform_restart", message, legacyDetails, legacyAt, 10*time.Minute)
		if err != nil || created {
			t.Fatalf("replay %d of the legacy line = created %v, err %v", replay, created, err)
		}
	}
	events, err := db.ListWorkerEvents(worker.ID, 10)
	if err != nil || len(events) != 1 || events[0].Occurrences != 1 {
		t.Fatalf("after replaying the legacy line: %+v, %v; want occurrences still 1", events, err)
	}

	secondAt := legacyAt.Add(3 * time.Minute)
	if _, err := db.RecordWindowedEventAt(worker.ID, "platform_restart", message, `{"timestamp":"2026-10-29T18:27:35Z","line":"crash two"}`, secondAt, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecordWindowedEventAt(worker.ID, "platform_restart", message, legacyDetails, legacyAt, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	events, err = db.ListWorkerEvents(worker.ID, 10)
	if err != nil || len(events) != 1 || events[0].Occurrences != 2 || !events[0].CreatedAt.Equal(secondAt) {
		t.Fatalf("after a distinct crash and another replay: %+v, %v; want one row with 2 occurrences at %s", events, err, secondAt)
	}
}

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
	if events[0].Occurrences != 2 {
		t.Fatalf("events[0].Occurrences = %d, want 2 merged occurrences", events[0].Occurrences)
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
