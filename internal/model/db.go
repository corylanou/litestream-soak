package model

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var migrationSQL = `
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    app_name TEXT NOT NULL DEFAULT '',
    region TEXT NOT NULL DEFAULT '',
    fly_machine_id TEXT UNIQUE,
    fly_volume_id TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    source TEXT NOT NULL DEFAULT 'main',
    git_sha TEXT NOT NULL,
    litestream_sha TEXT NOT NULL DEFAULT '',
    pr_number INTEGER,
    profile_name TEXT NOT NULL,
    profile_config TEXT NOT NULL DEFAULT '{}',
    expires_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
    last_heartbeat_at DATETIME,
    error_message TEXT,
    last_runtime_json TEXT NOT NULL DEFAULT '',
    last_runtime_at DATETIME,
    dormant_at DATETIME,
    dormant_reason TEXT NOT NULL DEFAULT '',
    dormant_signature TEXT NOT NULL DEFAULT '',
    resume_trigger TEXT NOT NULL DEFAULT '',
    last_probe_at DATETIME
);

CREATE TABLE IF NOT EXISTS verifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id TEXT NOT NULL REFERENCES workers(id),
    started_at DATETIME NOT NULL,
    completed_at DATETIME,
    status TEXT NOT NULL DEFAULT 'running',
    check_type TEXT NOT NULL,
    source_checksum TEXT,
    restored_checksum TEXT,
    passed BOOLEAN,
    duration_ms INTEGER,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS deployments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    git_sha TEXT NOT NULL,
    litestream_sha TEXT NOT NULL DEFAULT '',
    image_ref TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'main',
    pr_number INTEGER,
    status TEXT NOT NULL DEFAULT 'building',
    started_at DATETIME NOT NULL DEFAULT (datetime('now')),
    completed_at DATETIME,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id TEXT REFERENCES workers(id),
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    details TEXT,
    created_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id TEXT REFERENCES workers(id),
    verification_id INTEGER REFERENCES verifications(id),
    alert_type TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending',
    failure_stage TEXT,
    failure_signature TEXT,
    message TEXT,
    payload TEXT,
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    sent_at DATETIME
);

CREATE TABLE IF NOT EXISTS run_archives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT '',
    worker_id TEXT NOT NULL DEFAULT '',
    archive_type TEXT NOT NULL,
    git_sha TEXT NOT NULL DEFAULT '',
    litestream_sha TEXT NOT NULL DEFAULT '',
    image_ref TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL DEFAULT '{}',
    archived_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_verifications_worker ON verifications(worker_id);
CREATE INDEX IF NOT EXISTS idx_verifications_worker_started ON verifications(worker_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_verifications_worker_failed_started ON verifications(worker_id, passed, status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_verifications_passed_status_started ON verifications(passed, status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_worker ON events(worker_id);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_events_worker_created ON events(worker_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);
CREATE INDEX IF NOT EXISTS idx_workers_source_created ON workers(source, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_workers_source_status_created ON workers(source, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_worker ON alerts(worker_id);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_deployments_source_started ON deployments(source, started_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_deployments_source_version_started ON deployments(source, git_sha, litestream_sha, started_at DESC, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_archives_unique ON run_archives(deployment_id, archive_type, worker_id);
CREATE INDEX IF NOT EXISTS idx_run_archives_source_type_archived ON run_archives(source, archive_type, archived_at DESC);
`

type DB struct {
	db *sql.DB
}

func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_time_format=sqlite&_timezone=UTC")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(migrationSQL); err != nil {
		return nil, fmt.Errorf("run migrations: %w", err)
	}
	if err := ensureWorkerColumns(db); err != nil {
		return nil, fmt.Errorf("ensure worker columns: %w", err)
	}
	if err := ensureDeploymentColumns(db); err != nil {
		return nil, fmt.Errorf("ensure deployment columns: %w", err)
	}
	if err := normalizeLegacyExpiry(db); err != nil {
		return nil, fmt.Errorf("normalize legacy expires_at: %w", err)
	}

	return &DB{db: db}, nil
}

func normalizeLegacyExpiry(db *sql.DB) error {
	rows, err := db.Query(`SELECT id, CAST(expires_at AS TEXT) FROM workers WHERE expires_at IS NOT NULL`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	type rewrite struct {
		id string
		at time.Time
	}
	var rewrites []rewrite
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return err
		}
		at, ok := parseLegacyTimestamp(raw)
		if !ok {
			continue
		}
		rewrites = append(rewrites, rewrite{id: id, at: at.UTC()})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, r := range rewrites {
		if _, err := db.Exec(`UPDATE workers SET expires_at = ? WHERE id = ?`, r.at, r.id); err != nil {
			return fmt.Errorf("rewrite expires_at for worker %s: %w", r.id, err)
		}
	}
	return nil
}

func parseLegacyTimestamp(raw string) (time.Time, bool) {
	if i := strings.Index(raw, "m="); i > 0 {
		raw = strings.TrimSpace(raw[:i])
	}
	if at, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", raw); err == nil {
		return at, true
	}
	if i := strings.LastIndexByte(raw, ' '); i > 0 {
		if at, err := time.Parse("2006-01-02 15:04:05.999999999 -0700", raw[:i]); err == nil {
			return at, true
		}
	}
	return time.Time{}, false
}

func ensureWorkerColumns(db *sql.DB) error {
	statements := []string{
		`ALTER TABLE workers ADD COLUMN app_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN region TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN last_runtime_json TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN last_runtime_at DATETIME`,
		`ALTER TABLE workers ADD COLUMN litestream_sha TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN dormant_at DATETIME`,
		`ALTER TABLE workers ADD COLUMN dormant_reason TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN dormant_signature TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN resume_trigger TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN last_probe_at DATETIME`,
	}

	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return err
		}
	}

	return nil
}

func ensureDeploymentColumns(db *sql.DB) error {
	statements := []string{
		`ALTER TABLE deployments ADD COLUMN litestream_sha TEXT NOT NULL DEFAULT ''`,
	}

	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return err
		}
	}

	return nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func nullInt(value int) any {
	if value == 0 {
		return nil
	}
	return value
}

func nullIntString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func utcTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}
