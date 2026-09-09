package worker

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const ftsSchema = `CREATE TABLE IF NOT EXISTS fts_documents(id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL);
CREATE VIRTUAL TABLE IF NOT EXISTS fts_search USING fts5(title, body);
CREATE TABLE IF NOT EXISTS fts_progress(id INTEGER PRIMARY KEY CHECK(id=1), step INTEGER NOT NULL CHECK(step>=0));
INSERT OR IGNORE INTO fts_progress VALUES(1,0);`

func openFTS(ctx context.Context, path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	uri := url.URL{Scheme: "file", Path: path, RawQuery: url.Values{
		"_txlock": {"immediate"},
		"_pragma": {"busy_timeout(5000)", "wal_autocheckpoint(0)"},
	}.Encode()}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.ExecContext(ctx, `PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA wal_autocheckpoint=0;`+ftsSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("FTS5 unavailable or schema initialization failed: %w", err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO fts_search(fts_search,rank) VALUES('automerge',0);`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

type ftsWork struct {
	Phase   string
	Changes int64
	Deleted int64
	Queries int64
	Matches int64
}

func ftsPhase(step int64) string {
	return [...]string{"insert", "update", "delete", "query", "merge", "update", "query", "optimize"}[step%8]
}

func stepFTS(ctx context.Context, db *sql.DB) (ftsWork, error) {
	var work ftsWork
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return work, err
	}
	defer func() { _ = tx.Rollback() }()
	var step int64
	if err := tx.QueryRowContext(ctx, "SELECT step FROM fts_progress WHERE id=1").Scan(&step); err != nil {
		return work, err
	}
	if step < 0 || step > 1<<53 {
		return work, fmt.Errorf("FTS progress out of range: %d", step)
	}
	work.Phase = ftsPhase(step)
	switch work.Phase {
	case "insert":
		result, err := tx.ExecContext(ctx, "DELETE FROM fts_documents")
		if err != nil {
			return work, err
		}
		work.Deleted, err = result.RowsAffected()
		if err != nil {
			return work, err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM fts_search"); err != nil {
			return work, err
		}
		for id := 1; id <= 64; id++ {
			title := fmt.Sprintf("document %d", id)
			body := ftsBody(step/8, id, "alpha")
			if err := writeFTSDocument(ctx, tx, id, title, body, false); err != nil {
				return work, err
			}
			work.Changes++
		}
	case "update":
		token := "beta"
		parity := 0
		if step%8 == 5 {
			token = "gamma"
			parity = 1
		}
		for id := 1; id <= 64; id++ {
			if id%2 != parity || (step%8 == 5 && id%4 == 0) {
				continue
			}
			title := fmt.Sprintf("document %d", id)
			body := ftsBody(step/8, id, token)
			if err := writeFTSDocument(ctx, tx, id, title, body, true); err != nil {
				return work, err
			}
			work.Changes++
		}
	case "delete":
		result, err := tx.ExecContext(ctx, "DELETE FROM fts_documents WHERE id%4=0")
		if err != nil {
			return work, err
		}
		work.Changes, err = result.RowsAffected()
		if err != nil {
			return work, err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM fts_search WHERE rowid%4=0"); err != nil {
			return work, err
		}
	case "query":
		matches, err := checkFTSSearch(ctx, tx)
		if err != nil {
			return work, err
		}
		work.Queries = int64(len(ftsQueries()))
		work.Matches = matches
		work.Changes = work.Queries
	case "merge", "optimize":
		var before, after int64
		if err := tx.QueryRowContext(ctx, "SELECT total_changes()").Scan(&before); err != nil {
			return work, err
		}
		command := `INSERT INTO fts_search(fts_search,rank) VALUES('merge',-1)`
		if work.Phase == "optimize" {
			command = `INSERT INTO fts_search(fts_search) VALUES('optimize')`
		}
		if _, err := tx.ExecContext(ctx, command); err != nil {
			return work, err
		}
		if err := tx.QueryRowContext(ctx, "SELECT total_changes()").Scan(&after); err != nil {
			return work, err
		}
		if after-before >= 2 {
			work.Changes = after - before - 1
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE fts_progress SET step=step+1 WHERE id=1"); err != nil {
		return work, err
	}
	if err := tx.Commit(); err != nil {
		return work, err
	}
	return work, nil
}

func writeFTSDocument(ctx context.Context, tx *sql.Tx, id int, title, body string, update bool) error {
	statements := []string{"INSERT INTO fts_documents(id,title,body) VALUES(?,?,?)", "INSERT INTO fts_search(rowid,title,body) VALUES(?,?,?)"}
	args := []any{id, title, body}
	if update {
		statements = []string{"UPDATE fts_documents SET title=?,body=? WHERE id=?", "UPDATE fts_search SET title=?,body=? WHERE rowid=?"}
		args = []any{title, body, id}
	}
	for _, statement := range statements {
		result, err := tx.ExecContext(ctx, statement, args...)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return fmt.Errorf("FTS document %d affected %d rows", id, affected)
		}
	}
	return nil
}

type ftsQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type ftsQuery struct{ match, predicate string }

func ftsQueries() []ftsQuery {
	return []ftsQuery{
		{"alpha", "instr(body,'alpha ') = 1"},
		{"beta", "instr(body,'beta ') = 1"},
		{"gamma", "instr(body,'gamma ') = 1"},
		{`"quick brown"`, "instr(body,'quick brown') > 0"},
		{"rev*", "instr(body,'revised ') > 0"},
		{"nonexistenttoken", "0"},
	}
}

func checkFTSSearch(ctx context.Context, q ftsQueryer) (int64, error) {
	var mismatch int
	if err := q.QueryRowContext(ctx, `WITH missing AS (SELECT id,title,body FROM fts_documents EXCEPT SELECT rowid,title,body FROM fts_search), extra AS (SELECT rowid,title,body FROM fts_search EXCEPT SELECT id,title,body FROM fts_documents) SELECT EXISTS(SELECT 1 FROM missing) OR EXISTS(SELECT 1 FROM extra)`).Scan(&mismatch); err != nil {
		return 0, err
	}
	if mismatch != 0 {
		return 0, fmt.Errorf("FTS logical documents differ from index content")
	}
	var matches int64
	for _, query := range ftsQueries() {
		var count int64
		statement := `WITH expected AS (SELECT id FROM fts_documents WHERE ` + query.predicate + `), actual AS (SELECT rowid AS id FROM fts_search WHERE fts_search MATCH ?), missing AS (SELECT id FROM expected EXCEPT SELECT id FROM actual), extra AS (SELECT id FROM actual EXCEPT SELECT id FROM expected) SELECT EXISTS(SELECT 1 FROM missing) OR EXISTS(SELECT 1 FROM extra), (SELECT count(*) FROM actual)`
		if err := q.QueryRowContext(ctx, statement, query.match).Scan(&mismatch, &count); err != nil {
			return matches, err
		}
		if mismatch != 0 {
			return matches, fmt.Errorf("FTS search mismatch for %q", query.match)
		}
		matches += count
	}
	return matches, nil
}

func ftsBody(cycle int64, id int, token string) string {
	var body strings.Builder
	_, _ = fmt.Fprintf(&body, "%s quick brown fox cycle %d document %d", token, cycle, id)
	if token != "alpha" {
		body.WriteString(" revised ")
	}
	for term := 0; term < 128; term++ {
		_, _ = fmt.Fprintf(&body, " word%d", term)
	}
	return body.String()
}
