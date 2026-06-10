package model

import (
	"path/filepath"
	"testing"
	"time"
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
