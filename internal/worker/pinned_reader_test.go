package worker

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func createPinnedReaderTestDB(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", walDSN(dbPath))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return dbPath
}

func TestPinnedReaderPauseReleasesHeldTransaction(t *testing.T) {
	dbPath := createPinnedReaderTestDB(t)

	p := newPinnedReader(dbPath, time.Hour, time.Hour)
	p.Start(t.Context())
	defer p.Stop()

	writer, err := sql.Open("sqlite", walDSN(dbPath))
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	attemptRestart := func() (busy int) {
		t.Helper()
		if _, err := writer.Exec("INSERT INTO t (v) VALUES ('x')"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var logFrames, checkpointed int
		if err := writer.QueryRow("PRAGMA wal_checkpoint(RESTART)").Scan(&busy, &logFrames, &checkpointed); err != nil {
			t.Fatalf("wal_checkpoint: %v", err)
		}
		return busy
	}

	deadline := time.Now().Add(10 * time.Second)
	held := false
	for time.Now().Before(deadline) {
		if attemptRestart() == 1 {
			held = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !held {
		t.Fatal("pinned reader never blocked a RESTART checkpoint")
	}

	if err := p.Pause(context.Background()); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	deadline = time.Now().Add(10 * time.Second)
	released := false
	for time.Now().Before(deadline) {
		if attemptRestart() == 0 {
			released = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !released {
		t.Fatal("Pause() did not release the held read transaction")
	}

	p.Resume()
}

func TestPinnedReaderStopTerminates(t *testing.T) {
	dbPath := createPinnedReaderTestDB(t)

	p := newPinnedReader(dbPath, time.Hour, time.Hour)
	p.Start(context.Background())

	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop() did not terminate the pinned reader")
	}
}
