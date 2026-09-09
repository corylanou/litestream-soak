package replay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	if err := it.Insert(db); !errors.Is(err, ErrRowSkipped) {
		t.Fatalf("second insert: %v, want skipped", err)
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

func TestGHArchiveReplayPassesMutate(t *testing.T) {
	t.Parallel()
	db := newGHArchiveTestDB(t)
	path := filepath.Join(t.TempDir(), "events.jsonl")
	data := `{"id":"push","type":"PushEvent","payload":{"ref":"refs/heads/main","commits":[{}]},"created_at":"2026-01-01T00:00:00Z"}
{"id":"issue","type":"IssuesEvent","payload":{"action":"opened","issue":{"number":1,"title":"example"}},"created_at":"2026-01-01T00:00:00Z"}
{"id":"pr","type":"PullRequestEvent","payload":{"action":"opened","pull_request":{"number":2,"title":"example"}},"created_at":"2026-01-01T00:00:00Z"}
{"id":"watch","type":"WatchEvent","payload":{},"created_at":"2026-01-01T00:00:00Z"}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(Config{WorkerID: t.Name()}, NewGHArchiveAdapter(path))
	engine.db = db
	for pass := 1; pass <= 3; pass++ {
		if pass == 3 {
			engine = NewEngine(Config{WorkerID: t.Name()}, NewGHArchiveAdapter(path))
			engine.db = db
		}
		if err := engine.replayOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := countRows(t, db, "gh_events"); got != 4*pass {
			t.Fatalf("pass %d: parents=%d, want %d", pass, got, 4*pass)
		}
		for _, table := range []string{"gh_push_events", "gh_issue_events", "gh_pr_events", "gh_watch_events"} {
			if got := countRows(t, db, table); got != pass {
				t.Fatalf("pass %d: %s=%d, want %d", pass, table, got, pass)
			}
			var joined int
			if err := db.QueryRow(`SELECT count(*) FROM ` + table + ` c JOIN gh_events p ON c.event_id=p.id`).Scan(&joined); err != nil {
				t.Fatal(err)
			}
			if joined != pass {
				t.Fatalf("pass %d: %s joined=%d", pass, table, joined)
			}
		}
	}
}

func TestGHArchiveReplayCountsOnlyMutations(t *testing.T) {
	t.Parallel()
	db := newGHArchiveTestDB(t)
	if _, err := db.Exec(`CREATE TRIGGER reject_watch BEFORE INSERT ON gh_watch_events BEGIN SELECT RAISE(ABORT, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "events.jsonl")
	data := `{"id":"push","type":"PushEvent","payload":{"commits":[]},"created_at":"2026-01-01T00:00:00Z"}
{"id":"push","type":"PushEvent","payload":{"commits":[]},"created_at":"2026-01-01T00:00:00Z"}
{"id":"watch","type":"WatchEvent","payload":{},"created_at":"2026-01-01T00:00:00Z"}
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(Config{WorkerID: t.Name()}, NewGHArchiveAdapter(path))
	engine.db = db
	labels := engine.metricLabels("gharchive")
	before := testutil.ToFloat64(replayRowsTotal.WithLabelValues(labels...))
	attempted := testutil.ToFloat64(replayAttemptsTotal.WithLabelValues(labels...))
	skipped := testutil.ToFloat64(replaySkippedRowsTotal.WithLabelValues(labels...))
	failed := testutil.ToFloat64(replayDroppedRowsTotal.WithLabelValues(labels...))
	insertErrors := testutil.ToFloat64(replayErrorsTotal.WithLabelValues(labels...))
	if err := engine.replayOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(replayRowsTotal.WithLabelValues(labels...)) - before; got != 1 {
		t.Fatalf("mutated records=%v, want 1", got)
	}
	for _, tc := range []struct {
		name      string
		got, want float64
	}{
		{"attempted", testutil.ToFloat64(replayAttemptsTotal.WithLabelValues(labels...)) - attempted, 3},
		{"skipped", testutil.ToFloat64(replaySkippedRowsTotal.WithLabelValues(labels...)) - skipped, 1},
		{"failed", testutil.ToFloat64(replayDroppedRowsTotal.WithLabelValues(labels...)) - failed, 1},
		{"insert errors", testutil.ToFloat64(replayErrorsTotal.WithLabelValues(labels...)) - insertErrors, 1},
	} {
		if tc.got != tc.want {
			t.Errorf("%s=%v, want %v", tc.name, tc.got, tc.want)
		}
	}
	if got := countRows(t, db, "gh_events"); got != 1 {
		t.Fatalf("parents=%d, want 1 after rollback", got)
	}
	if got := countRows(t, db, "gh_push_events"); got != 1 {
		t.Fatalf("push children=%d, want 1", got)
	}
}
