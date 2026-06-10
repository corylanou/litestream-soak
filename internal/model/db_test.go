package model

import (
	"path/filepath"
	"testing"
	"time"
)

func TestOpenAppliesPragmas(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var journalMode string
	if err := db.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}

	var busyTimeout int
	if err := db.db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if busyTimeout != 30000 {
		t.Fatalf("busy_timeout = %d, want 30000", busyTimeout)
	}
}

func TestOpenNormalizesLegacyNonUTCExpiresAt(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

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

	legacyLayout := "2006-01-02 15:04:05.999999999 -0700 MST"
	negZone := time.FixedZone("EST", -5*3600)
	posZone := time.FixedZone("IST", 5*3600+1800)

	futureInstant := time.Now().UTC().Add(1 * time.Hour)
	pastInstant := time.Now().UTC().Add(-1 * time.Hour)

	futureWorker := newWorker("worker-legacy-neg-future")
	pastWorker := newWorker("worker-legacy-pos-past")
	monotonicWorker := newWorker("worker-legacy-neg-future-monotonic")
	namedOffsetWorker := newWorker("worker-legacy-named-offset-future")
	emptyZoneWorker := newWorker("worker-legacy-empty-zone-future")
	for id, raw := range map[string]string{
		futureWorker.ID:      futureInstant.In(negZone).Format(legacyLayout),
		pastWorker.ID:        pastInstant.In(posZone).Format(legacyLayout),
		monotonicWorker.ID:   futureInstant.In(negZone).Format(legacyLayout) + " m=+123.456789001",
		namedOffsetWorker.ID: futureInstant.In(time.FixedZone("UTC-5", -5*3600)).String(),
		emptyZoneWorker.ID:   futureInstant.In(time.FixedZone("", -5*3600)).String(),
	} {
		w := newWorker(id)
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker(%s) error = %v", id, err)
		}
		if _, err := db.db.Exec("UPDATE workers SET expires_at = ? WHERE id = ?", raw, id); err != nil {
			t.Fatalf("set legacy expires_at for %s: %v", id, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	expired, err := db.ListExpiredWorkers()
	if err != nil {
		t.Fatalf("ListExpiredWorkers() error = %v", err)
	}
	foundPast := false
	for _, w := range expired {
		if w.ID == futureWorker.ID {
			t.Fatalf("legacy future expiry in EST wrongly in ListExpiredWorkers()")
		}
		if w.ID == monotonicWorker.ID {
			t.Fatalf("legacy future expiry with monotonic suffix wrongly in ListExpiredWorkers()")
		}
		if w.ID == namedOffsetWorker.ID {
			t.Fatalf("legacy future expiry with offset-named zone wrongly in ListExpiredWorkers()")
		}
		if w.ID == emptyZoneWorker.ID {
			t.Fatalf("legacy future expiry with empty zone name wrongly in ListExpiredWorkers()")
		}
		if w.ID == pastWorker.ID {
			foundPast = true
		}
	}
	if !foundPast {
		t.Fatalf("legacy past expiry in IST not in ListExpiredWorkers()")
	}

	for id, want := range map[string]time.Time{
		futureWorker.ID:      futureInstant,
		pastWorker.ID:        pastInstant,
		monotonicWorker.ID:   futureInstant,
		namedOffsetWorker.ID: futureInstant,
		emptyZoneWorker.ID:   futureInstant,
	} {
		var raw string
		if err := db.db.QueryRow("SELECT expires_at FROM workers WHERE id = ?", id).Scan(&raw); err != nil {
			t.Fatalf("SELECT expires_at for %s: %v", id, err)
		}
		if !isStoredAsUTC(raw, want) {
			t.Fatalf("expires_at for %s = %q, want canonical UTC form for %v", id, raw, want)
		}
	}
}

func isStoredAsUTC(raw string, want time.Time) bool {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999-07:00", time.RFC3339Nano} {
		parsed, err := time.Parse(layout, raw)
		if err != nil {
			continue
		}
		_, offset := parsed.Zone()
		return offset == 0 && parsed.Equal(want)
	}
	return false
}
