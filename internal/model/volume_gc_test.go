package model

import (
	"path/filepath"
	"testing"
	"time"
)

func TestVolumeGCAttemptsPersistAcrossOpen(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	attempt := VolumeGCAttempt{
		VolumeID:        "volume-one",
		AppName:         "app-one",
		VolumeName:      "soak_worker_main_low_vol",
		Region:          "ord",
		SizeGB:          10,
		VolumeCreatedAt: now.Add(-3 * time.Hour),
		FirstAttemptAt:  now.Add(-time.Hour),
		LastAttemptAt:   now,
		NextRetryAt:     now.Add(2 * time.Hour),
		RequestCount:    2,
		RequestAccepted: true,
	}
	if err := db.UpsertVolumeGCAttempt(attempt); err != nil {
		t.Fatalf("UpsertVolumeGCAttempt() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	attempts, err := db.ListVolumeGCAttempts("app-one")
	if err != nil {
		t.Fatalf("ListVolumeGCAttempts() error = %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("len(attempts) = %d, want 1", len(attempts))
	}
	if attempts[0] != attempt {
		t.Fatalf("attempt = %+v, want %+v", attempts[0], attempt)
	}

	if err := db.DeleteVolumeGCAttempt(attempt.VolumeID); err != nil {
		t.Fatalf("DeleteVolumeGCAttempt() error = %v", err)
	}
	attempts, err = db.ListVolumeGCAttempts("app-one")
	if err != nil {
		t.Fatalf("ListVolumeGCAttempts() after delete error = %v", err)
	}
	if len(attempts) != 0 {
		t.Fatalf("len(attempts) after delete = %d, want 0", len(attempts))
	}
}

func TestListVolumeGCAttemptsFiltersByApp(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	now := time.Now().UTC()
	for _, attempt := range []VolumeGCAttempt{
		{VolumeID: "volume-one", AppName: "app-one", VolumeCreatedAt: now, FirstAttemptAt: now, LastAttemptAt: now, NextRetryAt: now},
		{VolumeID: "volume-two", AppName: "app-two", VolumeCreatedAt: now, FirstAttemptAt: now, LastAttemptAt: now, NextRetryAt: now},
	} {
		if err := db.UpsertVolumeGCAttempt(attempt); err != nil {
			t.Fatalf("UpsertVolumeGCAttempt(%s) error = %v", attempt.VolumeID, err)
		}
	}

	attempts, err := db.ListVolumeGCAttempts("app-one")
	if err != nil {
		t.Fatalf("ListVolumeGCAttempts() error = %v", err)
	}
	if len(attempts) != 1 || attempts[0].VolumeID != "volume-one" {
		t.Fatalf("attempts = %+v, want only volume-one", attempts)
	}
}
