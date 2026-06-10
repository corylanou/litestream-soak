package model

import (
	"path/filepath"
	"testing"
	"time"
)

func TestStaleWorkersUTCIndependent(t *testing.T) {
	origLocal := time.Local
	t.Cleanup(func() { time.Local = origLocal })

	newWorker := func(id string) *Worker {
		return &Worker{
			ID:            id,
			Name:          id,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		}
	}

	t.Run("fresh heartbeat not stale in UTC+13", func(t *testing.T) {
		time.Local = time.FixedZone("UTC+13", 13*3600)

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		w := newWorker("worker-utc-fresh")
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}
		if err := db.UpdateWorkerHeartbeat(w.ID); err != nil {
			t.Fatalf("UpdateWorkerHeartbeat() error = %v", err)
		}

		stale, err := db.StaleWorkers(5 * time.Minute)
		if err != nil {
			t.Fatalf("StaleWorkers() error = %v", err)
		}
		if len(stale) != 0 {
			t.Fatalf("StaleWorkers() = %d workers, want 0 (fresh heartbeat should not be stale)", len(stale))
		}
	})

	t.Run("old heartbeat stale in UTC-12", func(t *testing.T) {
		time.Local = time.FixedZone("UTC-12", -12*3600)

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		w := newWorker("worker-utc-old")
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}
		if _, err := db.db.Exec(
			"UPDATE workers SET last_heartbeat_at = datetime('now','-1 hour') WHERE id = ?",
			w.ID,
		); err != nil {
			t.Fatalf("backdate heartbeat: %v", err)
		}

		stale, err := db.StaleWorkers(5 * time.Minute)
		if err != nil {
			t.Fatalf("StaleWorkers() error = %v", err)
		}
		if len(stale) != 1 {
			t.Fatalf("StaleWorkers() = %d workers, want 1 (old heartbeat should be stale)", len(stale))
		}
		if stale[0].ID != w.ID {
			t.Fatalf("stale worker ID = %q, want %q", stale[0].ID, w.ID)
		}
	})
}

func TestListExpiredWorkersUTCIndependent(t *testing.T) {
	t.Parallel()

	newWorker := func(id string, expiresAt *time.Time) *Worker {
		return &Worker{
			ID:            id,
			Name:          id,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
			ExpiresAt:     expiresAt,
		}
	}

	t.Run("past expiry in negative-offset zone is classified expired", func(t *testing.T) {
		t.Parallel()

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		negZone := time.FixedZone("UTC-5", -5*3600)
		pastInNeg := time.Now().Add(-1 * time.Hour).In(negZone)
		w := newWorker("worker-past-neg", &pastInNeg)
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}

		expired, err := db.ListExpiredWorkers()
		if err != nil {
			t.Fatalf("ListExpiredWorkers() error = %v", err)
		}
		found := false
		for _, ew := range expired {
			if ew.ID == w.ID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("past expiry in UTC-5 not in ListExpiredWorkers(); got %d workers", len(expired))
		}
	})

	t.Run("future expiry in negative-offset zone is not classified expired", func(t *testing.T) {
		t.Parallel()

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		negZone := time.FixedZone("UTC-5", -5*3600)
		futureInNeg := time.Now().Add(1 * time.Hour).In(negZone)
		w := newWorker("worker-future-neg", &futureInNeg)
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}

		expired, err := db.ListExpiredWorkers()
		if err != nil {
			t.Fatalf("ListExpiredWorkers() error = %v", err)
		}
		for _, ew := range expired {
			if ew.ID == w.ID {
				t.Fatalf("future expiry in UTC-5 wrongly appears in ListExpiredWorkers()")
			}
		}
	})

	t.Run("future expiry in positive-offset zone is not classified expired", func(t *testing.T) {
		t.Parallel()

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		posZone := time.FixedZone("UTC+5", 5*3600)
		futureInPos := time.Now().Add(1 * time.Hour).In(posZone)
		w := newWorker("worker-future-pos", &futureInPos)
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}

		expired, err := db.ListExpiredWorkers()
		if err != nil {
			t.Fatalf("ListExpiredWorkers() error = %v", err)
		}
		for _, ew := range expired {
			if ew.ID == w.ID {
				t.Fatalf("future expiry in UTC+5 wrongly appears in ListExpiredWorkers()")
			}
		}
	})
}

func TestListExpiredWorkersLegacyFormat(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	newWorker := func(id string) *Worker {
		return &Worker{
			ID:            id,
			Name:          id,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		}
	}

	pastWorker := newWorker("worker-legacy-past")
	if err := db.CreateWorker(pastWorker); err != nil {
		t.Fatalf("CreateWorker(past) error = %v", err)
	}
	if _, err := db.db.Exec(
		"UPDATE workers SET expires_at = ? WHERE id = ?",
		"2020-01-02 15:04:05.123456789 +0000 UTC",
		pastWorker.ID,
	); err != nil {
		t.Fatalf("backdate expires_at (legacy format): %v", err)
	}

	futureWorker := newWorker("worker-legacy-future")
	if err := db.CreateWorker(futureWorker); err != nil {
		t.Fatalf("CreateWorker(future) error = %v", err)
	}
	if _, err := db.db.Exec(
		"UPDATE workers SET expires_at = ? WHERE id = ?",
		"2099-01-02 15:04:05.123456789 +0000 UTC",
		futureWorker.ID,
	); err != nil {
		t.Fatalf("set future expires_at (legacy format): %v", err)
	}

	expired, err := db.ListExpiredWorkers()
	if err != nil {
		t.Fatalf("ListExpiredWorkers() error = %v", err)
	}

	foundPast := false
	foundFuture := false
	for _, w := range expired {
		if w.ID == pastWorker.ID {
			foundPast = true
		}
		if w.ID == futureWorker.ID {
			foundFuture = true
		}
	}
	if !foundPast {
		t.Fatalf("legacy past expires_at not in ListExpiredWorkers()")
	}
	if foundFuture {
		t.Fatalf("legacy future expires_at wrongly in ListExpiredWorkers()")
	}

	fetched, err := db.GetWorker(pastWorker.ID)
	if err != nil {
		t.Fatalf("GetWorker(legacy past) error = %v", err)
	}
	if fetched.ExpiresAt == nil {
		t.Fatalf("ExpiresAt after scan = nil, want value")
	}
	wantInstant := time.Date(2020, 1, 2, 15, 4, 5, 123456789, time.UTC)
	if !fetched.ExpiresAt.Equal(wantInstant) {
		t.Fatalf("ExpiresAt = %v, want %v", fetched.ExpiresAt, wantInstant)
	}
}

func TestCreateWorkerStoresExpiresAtAsUTC(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	negZone := time.FixedZone("UTC-5", -5*3600)
	expiresAt := time.Date(2030, 6, 15, 10, 0, 0, 0, negZone)
	w := &Worker{
		ID:            "worker-utc-stored",
		Name:          "worker-utc-stored",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		ExpiresAt:     &expiresAt,
	}
	if err := db.CreateWorker(w); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	var raw string
	if err := db.db.QueryRow("SELECT expires_at FROM workers WHERE id = ?", w.ID).Scan(&raw); err != nil {
		t.Fatalf("SELECT expires_at: %v", err)
	}

	wantUTCInstant := expiresAt.UTC()
	if !isStoredAsUTC(raw, wantUTCInstant) {
		t.Fatalf("expires_at raw = %q, want UTC form matching instant %v (no non-zero offset, no ` UTC` / ` MST` suffix)", raw, wantUTCInstant)
	}
}
