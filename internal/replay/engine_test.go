package replay

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	_ "modernc.org/sqlite"
)

type singleRowAdapter struct{}

func (singleRowAdapter) Name() string { return "single" }

func (singleRowAdapter) CreateTables(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS replay_rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`)
	return err
}

func (singleRowAdapter) Rows() (RowIterator, error) {
	return &singleRowIterator{rows: []singleReplayRow{{ts: time.Unix(0, 0), value: "ok"}}}, nil
}

type singleReplayRow struct {
	ts    time.Time
	value string
}

type singleRowIterator struct {
	rows []singleReplayRow
	idx  int
}

func (it *singleRowIterator) Next() bool {
	if it.idx >= len(it.rows) {
		return false
	}
	it.idx++
	return true
}

func (it *singleRowIterator) Timestamp() time.Time {
	if it.idx == 0 {
		return time.Time{}
	}
	return it.rows[it.idx-1].ts
}

func (it *singleRowIterator) Insert(db *sql.DB) error {
	_, err := db.Exec(`INSERT INTO replay_rows (value) VALUES (?)`, it.rows[it.idx-1].value)
	return err
}

func (it *singleRowIterator) Err() error   { return nil }
func (it *singleRowIterator) Close() error { return nil }

func TestReplayDSNSetsBusyTimeout(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "replay.db")

	db, err := sql.Open("sqlite", replayDSN(dbPath))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var busyTimeout int
	if err := db.QueryRow(`PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
		t.Fatalf("query busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("busy_timeout=%d, want 5000", busyTimeout)
	}

	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode=%q, want \"wal\"", journalMode)
	}

	var autoCheckpoint int
	if err := db.QueryRow(`PRAGMA wal_autocheckpoint`).Scan(&autoCheckpoint); err != nil {
		t.Fatalf("query wal_autocheckpoint: %v", err)
	}
	if autoCheckpoint != 0 {
		t.Fatalf("wal_autocheckpoint=%d, want 0", autoCheckpoint)
	}
}

type failingInsertAdapter struct{}

func (failingInsertAdapter) Name() string { return "failing" }

func (failingInsertAdapter) CreateTables(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS replay_rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`)
	return err
}

func (failingInsertAdapter) Rows() (RowIterator, error) {
	return &failingInsertIterator{remaining: 3}, nil
}

type failingInsertIterator struct {
	remaining int
}

func (it *failingInsertIterator) Next() bool {
	if it.remaining == 0 {
		return false
	}
	it.remaining--
	return true
}

func (it *failingInsertIterator) Timestamp() time.Time { return time.Unix(0, 0) }

func (it *failingInsertIterator) Insert(db *sql.DB) error { return errors.New("boom") }

func (it *failingInsertIterator) Err() error   { return nil }
func (it *failingInsertIterator) Close() error { return nil }

func TestEngineCountsDroppedRows(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{
		DBPath:      filepath.Join(dir, "replay.db"),
		WorkerID:    "worker-dropped",
		ProfileName: "profile-dropped",
		Source:      "source-dropped",
	}

	engine := NewEngine(cfg, failingInsertAdapter{})
	labels := []string{"failing", cfg.WorkerID, cfg.ProfileName, cfg.Source}
	before := testutil.ToFloat64(replayDroppedRowsTotal.WithLabelValues(labels...))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := engine.Run(ctx); err != nil {
		t.Fatalf("engine run: %v", err)
	}

	after := testutil.ToFloat64(replayDroppedRowsTotal.WithLabelValues(labels...))
	if got := after - before; got != 3 {
		t.Fatalf("dropped rows increased by %v, want 3", got)
	}
}

func TestEngineRunWaitsForTransientWriterLock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "replay.db")

	bootstrapDB, err := sql.Open("sqlite", replayDSN(dbPath))
	if err != nil {
		t.Fatalf("open bootstrap db: %v", err)
	}
	if _, err := bootstrapDB.Exec(`CREATE TABLE replay_rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := bootstrapDB.Close(); err != nil {
		t.Fatalf("close bootstrap db: %v", err)
	}

	lockDB, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("open lock db: %v", err)
	}
	defer func() { _ = lockDB.Close() }()

	conn, err := lockDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire lock conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin immediate: %v", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `ROLLBACK`) }()

	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
	}()

	engine := NewEngine(Config{
		DBPath: dbPath,
	}, singleRowAdapter{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := engine.Run(ctx); err != nil {
		t.Fatalf("engine run: %v", err)
	}

	verifyDB, err := sql.Open("sqlite", replayDSN(dbPath))
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = verifyDB.Close() }()

	var count int
	if err := verifyDB.QueryRow(`SELECT count(*) FROM replay_rows`).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count=%d, want 1", count)
	}
}

type busyInsertAdapter struct{}

func (busyInsertAdapter) Name() string { return "busy-cancel" }

func (busyInsertAdapter) CreateTables(db *sql.DB) error { return nil }

func (busyInsertAdapter) Rows() (RowIterator, error) { return &busyInsertIterator{remaining: 1}, nil }

type busyInsertIterator struct{ remaining int }

func (it *busyInsertIterator) Next() bool {
	if it.remaining == 0 {
		return false
	}
	it.remaining--
	return true
}

func (it *busyInsertIterator) Timestamp() time.Time { return time.Unix(0, 0) }

func (it *busyInsertIterator) Insert(db *sql.DB) error { return errors.New("database is locked") }

func (it *busyInsertIterator) Err() error   { return nil }
func (it *busyInsertIterator) Close() error { return nil }

func TestEngineCancellationNotCountedAsDrop(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{
		DBPath:      filepath.Join(dir, "replay.db"),
		WorkerID:    "worker-cancel",
		ProfileName: "profile-cancel",
		Source:      "source-cancel",
	}

	engine := NewEngine(cfg, busyInsertAdapter{})

	labels := []string{"busy-cancel", cfg.WorkerID, cfg.ProfileName, cfg.Source}
	before := testutil.ToFloat64(replayDroppedRowsTotal.WithLabelValues(labels...))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	if err := engine.Run(ctx); err != nil {
		t.Fatalf("engine run: %v", err)
	}

	after := testutil.ToFloat64(replayDroppedRowsTotal.WithLabelValues(labels...))
	if after != before {
		t.Fatalf("dropped rows %v -> %v, want unchanged when canceled mid-retry", before, after)
	}
}
