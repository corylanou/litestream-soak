package churn

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

func Open(ctx context.Context, path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=busy_timeout(100)&_pragma=journal_mode(WAL)"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS churn_jobs(id INTEGER PRIMARY KEY,state TEXT NOT NULL,attempts INTEGER NOT NULL,payload TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS churn_receipts(job_id INTEGER PRIMARY KEY);
 CREATE TABLE IF NOT EXISTS churn_cache(id INTEGER PRIMARY KEY,value TEXT NOT NULL,expires INTEGER NOT NULL);`)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func Apply(ctx context.Context, db *sql.DB, mode, op string, key int, tick int64, size int) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var statements []string
	var args [][]any
	switch mode {
	case "queue":
		switch op {
		case "enqueue":
			statements = []string{"INSERT OR IGNORE INTO churn_jobs VALUES(?,'ready',0,?)"}
			args = [][]any{{key, strings.Repeat("q", size)}}
		case "claim":
			statements = []string{"UPDATE churn_jobs SET state='claimed',attempts=attempts+1 WHERE id=? AND state='ready'"}
			args = [][]any{{key}}
		case "retry":
			statements = []string{"UPDATE churn_jobs SET state='ready' WHERE id=? AND state='claimed'"}
			args = [][]any{{key}}
		case "complete":
			statements = []string{"UPDATE churn_jobs SET state='done' WHERE id=? AND state='claimed'", "INSERT OR IGNORE INTO churn_receipts SELECT id FROM churn_jobs WHERE id=? AND state='done'"}
			args = [][]any{{key}, {key}}
		case "expire":
			statements = []string{"UPDATE churn_jobs SET state='expired' WHERE id=? AND state IN ('ready','claimed')"}
			args = [][]any{{key}}
		case "delete":
			statements = []string{"DELETE FROM churn_receipts WHERE job_id IN (SELECT id FROM churn_jobs WHERE id=? AND state IN ('done','expired'))", "DELETE FROM churn_jobs WHERE id=? AND state IN ('done','expired')"}
			args = [][]any{{key}, {key}}
		}
	case "cache":
		switch op {
		case "upsert":
			statements = []string{"INSERT INTO churn_cache VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value,expires=excluded.expires"}
			args = [][]any{{key, fmt.Sprintf("%d:%s", tick, strings.Repeat("c", size)), tick + 8}}
		case "evict":
			statements = []string{"DELETE FROM churn_cache WHERE expires<=?"}
			args = [][]any{{tick}}
		}
	}
	if len(statements) == 0 {
		return 0, fmt.Errorf("unknown churn operation %s/%s", mode, op)
	}
	var mutations int64
	for i, stmt := range statements {
		result, err := tx.ExecContext(ctx, stmt, args[i]...)
		if err != nil {
			return 0, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		mutations += n
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return mutations, nil
}

func Validate(ctx context.Context, db *sql.DB, mode string, slots int) error {
	var query string
	switch mode {
	case "queue":
		query = `SELECT
 (SELECT count(*) FROM churn_jobs WHERE id<0 OR id>=? OR state NOT IN ('ready','claimed','done','expired') OR attempts<0 OR (state IN ('claimed','done') AND attempts=0)) +
 (SELECT count(*) FROM churn_receipts r LEFT JOIN churn_jobs j ON j.id=r.job_id WHERE j.id IS NULL OR j.state!='done') +
 (SELECT count(*) FROM churn_jobs j LEFT JOIN churn_receipts r ON r.job_id=j.id WHERE j.state='done' AND r.job_id IS NULL)`
	case "cache":
		query = "SELECT count(*) FROM churn_cache WHERE id<0 OR id>=? OR expires<0 OR length(value)=0"
	default:
		return fmt.Errorf("unknown churn mode %q", mode)
	}
	var violations int
	if err := db.QueryRowContext(ctx, query, slots).Scan(&violations); err != nil {
		return err
	}
	if violations != 0 {
		return fmt.Errorf("%s application invariant violations: %d", mode, violations)
	}
	return nil
}
