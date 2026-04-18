package replay

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

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

func TestEngineRunWaitsForTransientWriterLock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "replay.db")

	bootstrapDB, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open bootstrap db: %v", err)
	}
	if _, err := bootstrapDB.Exec(`CREATE TABLE replay_rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	bootstrapDB.Close()

	lockDB, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open lock db: %v", err)
	}
	defer lockDB.Close()

	conn, err := lockDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire lock conn: %v", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin immediate: %v", err)
	}
	defer conn.ExecContext(context.Background(), `ROLLBACK`)

	go func() {
		time.Sleep(200 * time.Millisecond)
		conn.ExecContext(context.Background(), `ROLLBACK`)
	}()

	engine := NewEngine(Config{
		DBPath: dbPath,
	}, singleRowAdapter{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := engine.Run(ctx); err != nil {
		t.Fatalf("engine run: %v", err)
	}

	verifyDB, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer verifyDB.Close()

	var count int
	if err := verifyDB.QueryRow(`SELECT count(*) FROM replay_rows`).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("row count=%d, want 1", count)
	}
}
