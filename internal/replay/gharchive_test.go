package replay

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newGHArchiveTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "gharchive.db")
	db, err := sql.Open("sqlite", replayDSN(dbPath))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	adapter := NewGHArchiveAdapter("")
	if err := adapter.CreateTables(db); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	return db
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func TestGHArchiveInsertSkipsChildrenOnDuplicate(t *testing.T) {
	t.Parallel()

	db := newGHArchiveTestDB(t)

	it := &ghArchiveIterator{event: ghEvent{
		ID:        "evt-1",
		Type:      "PushEvent",
		Payload:   json.RawMessage(`{"ref":"refs/heads/main","commits":[{},{}]}`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}}
	it.event.Actor.Login = "octocat"
	it.event.Repo.Name = "octocat/hello"

	if err := it.Insert(db); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := it.Insert(db); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	if got := countRows(t, db, "gh_events"); got != 1 {
		t.Fatalf("gh_events count=%d, want 1", got)
	}
	if got := countRows(t, db, "gh_push_events"); got != 1 {
		t.Fatalf("gh_push_events count=%d, want 1", got)
	}
}

func TestGHArchiveInsertLogsAndSkipsBadPayload(t *testing.T) {
	t.Parallel()

	db := newGHArchiveTestDB(t)

	it := &ghArchiveIterator{event: ghEvent{
		ID:        "evt-bad",
		Type:      "PushEvent",
		Payload:   json.RawMessage(`"not-an-object"`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}}
	it.event.Actor.Login = "octocat"
	it.event.Repo.Name = "octocat/hello"

	if err := it.Insert(db); err != nil {
		t.Fatalf("insert with bad payload: %v", err)
	}

	if got := countRows(t, db, "gh_events"); got != 1 {
		t.Fatalf("gh_events count=%d, want 1", got)
	}
	if got := countRows(t, db, "gh_push_events"); got != 0 {
		t.Fatalf("gh_push_events count=%d, want 0", got)
	}
}
