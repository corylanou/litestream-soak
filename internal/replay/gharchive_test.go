package replay

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
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

// Not parallel: the before/after delta reads on the shared package-level
// counter would race with other tests that also drop a PushEvent payload.
func TestGHArchiveDroppedPayloadsCounter(t *testing.T) {
	db := newGHArchiveTestDB(t)

	eventTypes := []struct {
		eventType string
		payload   string
		table     string
	}{
		{"PushEvent", `"not-an-object"`, "gh_push_events"},
		{"IssuesEvent", `"not-an-object"`, "gh_issue_events"},
		{"PullRequestEvent", `"not-an-object"`, "gh_pr_events"},
	}

	for i, tc := range eventTypes {
		before := testutil.ToFloat64(gharchiveDroppedPayloadsTotal.WithLabelValues(tc.eventType))

		it := &ghArchiveIterator{event: ghEvent{
			ID:        fmt.Sprintf("evt-drop-%d", i),
			Type:      tc.eventType,
			Payload:   json.RawMessage(tc.payload),
			CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		}}
		it.event.Actor.Login = "octocat"
		it.event.Repo.Name = "octocat/hello"

		if err := it.Insert(db); err != nil {
			t.Fatalf("Insert() %s: %v", tc.eventType, err)
		}

		after := testutil.ToFloat64(gharchiveDroppedPayloadsTotal.WithLabelValues(tc.eventType))
		if got := after - before; got != 1 {
			t.Fatalf("Insert() dropped counter for %s = %v, want %v", tc.eventType, got, 1)
		}

		if got := countRows(t, db, tc.table); got != 0 {
			t.Fatalf("%s count=%d, want 0", tc.table, got)
		}
	}
}
