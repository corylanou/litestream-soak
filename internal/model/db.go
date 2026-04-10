package model

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
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
    pr_number INTEGER,
    profile_name TEXT NOT NULL,
    profile_config TEXT NOT NULL DEFAULT '{}',
    expires_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
    last_heartbeat_at DATETIME,
    error_message TEXT
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

CREATE INDEX IF NOT EXISTS idx_verifications_worker ON verifications(worker_id);
CREATE INDEX IF NOT EXISTS idx_events_worker ON events(worker_id);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);
`

type DB struct {
	db *sql.DB
}

func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_busy_timeout=5000")
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

	return &DB{db: db}, nil
}

func ensureWorkerColumns(db *sql.DB) error {
	statements := []string{
		`ALTER TABLE workers ADD COLUMN app_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE workers ADD COLUMN region TEXT NOT NULL DEFAULT ''`,
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

func (d *DB) CreateWorker(w *Worker) error {
	_, err := d.db.Exec(`
		INSERT INTO workers (id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, pr_number, profile_name, profile_config, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		w.ID, w.AppName, w.Region, w.FlyMachineID, w.FlyVolumeID, w.Name, w.Status, w.Source, w.GitSHA, w.PRNumber, w.ProfileName, w.ProfileConfig, w.ExpiresAt,
	)
	return err
}

func (d *DB) UpdateWorkerStatus(id string, status WorkerStatus, errMsg string) error {
	_, err := d.db.Exec(`
		UPDATE workers SET status = ?, error_message = ?, updated_at = datetime('now')
		WHERE id = ?`,
		status, errMsg, id,
	)
	return err
}

func (d *DB) UpdateWorkerMachine(id, machineID, volumeID string) error {
	_, err := d.db.Exec(`
		UPDATE workers SET fly_machine_id = ?, fly_volume_id = ?, updated_at = datetime('now')
		WHERE id = ?`,
		machineID, volumeID, id,
	)
	return err
}

func (d *DB) UpdateWorkerHeartbeat(id string) error {
	_, err := d.db.Exec(`
		UPDATE workers SET last_heartbeat_at = datetime('now'), updated_at = datetime('now')
		WHERE id = ?`,
		id,
	)
	return err
}

func (d *DB) UpsertReportedWorker(identity reporting.WorkerIdentity) error {
	name := identity.Name
	if name == "" {
		name = identity.WorkerID
	}

	_, err := d.db.Exec(`
		INSERT INTO workers (id, app_name, region, fly_machine_id, name, status, source, git_sha, profile_name, last_heartbeat_at)
		VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, datetime('now'))
		ON CONFLICT(id) DO UPDATE SET
			app_name = CASE
				WHEN excluded.app_name <> '' THEN excluded.app_name
				ELSE workers.app_name
			END,
			region = CASE
				WHEN excluded.region <> '' THEN excluded.region
				ELSE workers.region
			END,
			fly_machine_id = CASE
				WHEN excluded.fly_machine_id <> '' THEN excluded.fly_machine_id
				ELSE workers.fly_machine_id
			END,
			name = excluded.name,
			source = excluded.source,
			git_sha = excluded.git_sha,
			profile_name = excluded.profile_name,
			last_heartbeat_at = datetime('now'),
			updated_at = datetime('now')`,
		identity.WorkerID,
		identity.AppName,
		identity.Region,
		identity.MachineID,
		name,
		identity.Source,
		identity.GitSHA,
		identity.ProfileName,
	)
	return err
}

func (d *DB) UpdateWorkerVerificationState(id string, passed bool, summary string) error {
	status := WorkerRunning
	errMsg := ""
	if !passed {
		status = WorkerDegraded
		errMsg = summary
	}

	_, err := d.db.Exec(`
		UPDATE workers
		SET status = ?, error_message = ?, updated_at = datetime('now')
		WHERE id = ?`,
		status, errMsg, id,
	)
	return err
}

func (d *DB) GetWorker(id string) (*Worker, error) {
	var w Worker
	err := scanWorker(
		d.db.QueryRow(`SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message FROM workers WHERE id = ?`, id),
		&w,
	)
	if err != nil {
		return nil, err
	}
	return &w, nil
}

func (d *DB) ListWorkers(status string) ([]Worker, error) {
	query := `SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message FROM workers`
	var args []any
	if status != "" {
		query += " WHERE status = ?"
		args = append(args, status)
	}
	query += " ORDER BY created_at DESC"

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		var w Worker
		if err := scanWorker(rows, &w); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, nil
}

func (d *DB) ListExpiredWorkers() ([]Worker, error) {
	rows, err := d.db.Query(`
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message
		FROM workers
		WHERE expires_at IS NOT NULL AND expires_at < datetime('now') AND status NOT IN ('stopped', 'failed')
		ORDER BY expires_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		var w Worker
		if err := scanWorker(rows, &w); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, nil
}

func (d *DB) RecordVerification(v *Verification) error {
	_, err := d.db.Exec(`
		INSERT INTO verifications (worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		v.WorkerID, v.StartedAt, v.CompletedAt, v.Status, v.CheckType, v.SourceChecksum, v.RestoredChecksum, v.Passed, v.DurationMS, v.ErrorMessage,
	)
	return err
}

func (d *DB) ListVerifications(workerID string, limit int) ([]Verification, error) {
	rows, err := d.db.Query(`
		SELECT id, worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message
		FROM verifications WHERE worker_id = ? ORDER BY started_at DESC LIMIT ?`,
		workerID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	verifications := make([]Verification, 0)
	for rows.Next() {
		var v Verification
		var completedAt sql.NullTime
		if err := rows.Scan(&v.ID, &v.WorkerID, &v.StartedAt, &completedAt, &v.Status, &v.CheckType, &v.SourceChecksum, &v.RestoredChecksum, &v.Passed, &v.DurationMS, &v.ErrorMessage); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			v.CompletedAt = &completedAt.Time
		}
		verifications = append(verifications, v)
	}
	return verifications, nil
}

func (d *DB) CreateDeployment(dep *Deployment) (int64, error) {
	result, err := d.db.Exec(`
		INSERT INTO deployments (git_sha, image_ref, source, pr_number, status)
		VALUES (?, ?, ?, ?, ?)`,
		dep.GitSHA, dep.ImageRef, dep.Source, dep.PRNumber, dep.Status,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) UpdateDeployment(id int64, status, imageRef, errMsg string) error {
	_, err := d.db.Exec(`
		UPDATE deployments SET status = ?, image_ref = ?, error_message = ?, completed_at = datetime('now')
		WHERE id = ?`,
		status, imageRef, errMsg, id,
	)
	return err
}

func (d *DB) GetDeploymentBySHA(sha string) (*Deployment, error) {
	var dep Deployment
	var completedAt sql.NullTime
	var prNumber sql.NullInt64
	err := d.db.QueryRow(`SELECT id, git_sha, image_ref, source, pr_number, status, started_at, completed_at, error_message FROM deployments WHERE git_sha = ? ORDER BY started_at DESC LIMIT 1`, sha).Scan(
		&dep.ID, &dep.GitSHA, &dep.ImageRef, &dep.Source, &prNumber, &dep.Status, &dep.StartedAt, &completedAt, &dep.ErrorMessage,
	)
	if err != nil {
		return nil, err
	}
	if prNumber.Valid {
		dep.PRNumber = int(prNumber.Int64)
	}
	if completedAt.Valid {
		dep.CompletedAt = &completedAt.Time
	}
	return &dep, nil
}

func (d *DB) RecordEvent(workerID, eventType, message, details string) error {
	_, err := d.db.Exec(`
		INSERT INTO events (worker_id, event_type, message, details)
		VALUES (?, ?, ?, ?)`,
		workerID, eventType, message, details,
	)
	return err
}

func (d *DB) ListEvents(limit int) ([]Event, error) {
	rows, err := d.db.Query(`SELECT id, worker_id, event_type, message, details, created_at FROM events ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var e Event
		var workerID sql.NullString
		if err := rows.Scan(&e.ID, &workerID, &e.EventType, &e.Message, &e.Details, &e.CreatedAt); err != nil {
			return nil, err
		}
		if workerID.Valid {
			e.WorkerID = workerID.String
		}
		events = append(events, e)
	}
	return events, nil
}

func (d *DB) ListMainWorkers() ([]Worker, error) {
	return d.listWorkersBySource("main")
}

func (d *DB) ListWorkerEvents(workerID string, limit int) ([]Event, error) {
	rows, err := d.db.Query(`
		SELECT id, worker_id, event_type, message, details, created_at
		FROM events
		WHERE worker_id = ?
		ORDER BY created_at DESC
		LIMIT ?`,
		workerID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var e Event
		var eventWorkerID sql.NullString
		if err := rows.Scan(&e.ID, &eventWorkerID, &e.EventType, &e.Message, &e.Details, &e.CreatedAt); err != nil {
			return nil, err
		}
		if eventWorkerID.Valid {
			e.WorkerID = eventWorkerID.String
		}
		events = append(events, e)
	}

	return events, nil
}

func (d *DB) ListRecentFailedVerifications(limit int) ([]Verification, error) {
	rows, err := d.db.Query(`
		SELECT id, worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message
		FROM verifications
		WHERE passed = 0 OR status = 'failed'
		ORDER BY started_at DESC
		LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	verifications := make([]Verification, 0)
	for rows.Next() {
		var v Verification
		var completedAt sql.NullTime
		if err := rows.Scan(&v.ID, &v.WorkerID, &v.StartedAt, &completedAt, &v.Status, &v.CheckType, &v.SourceChecksum, &v.RestoredChecksum, &v.Passed, &v.DurationMS, &v.ErrorMessage); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			v.CompletedAt = &completedAt.Time
		}
		verifications = append(verifications, v)
	}

	return verifications, nil
}

func (d *DB) listWorkersBySource(source string) ([]Worker, error) {
	rows, err := d.db.Query(`
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message
		FROM workers WHERE source = ? AND status NOT IN ('stopped', 'failed')
		ORDER BY created_at`, source)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		var w Worker
		if err := scanWorker(rows, &w); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// DeleteWorker removes a worker from the database along with its verifications and events.
func (d *DB) DeleteWorker(id string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM verifications WHERE worker_id = ?", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM events WHERE worker_id = ?", id); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM workers WHERE id = ?", id); err != nil {
		return err
	}
	return tx.Commit()
}

// StaleWorkers returns workers that haven't sent a heartbeat within the given duration.
func (d *DB) StaleWorkers(timeout time.Duration) ([]Worker, error) {
	cutoff := time.Now().Add(-timeout)
	rows, err := d.db.Query(`
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message
		FROM workers
		WHERE status = 'running' AND last_heartbeat_at IS NOT NULL AND last_heartbeat_at < ?`,
		cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		var w Worker
		if err := scanWorker(rows, &w); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, nil
}

type workerScanner interface {
	Scan(dest ...any) error
}

func scanWorker(scanner workerScanner, w *Worker) error {
	var appName, region, machineID, volumeID, errorMessage sql.NullString
	var expiresAt, heartbeat sql.NullTime
	var prNumber sql.NullInt64

	if err := scanner.Scan(
		&w.ID,
		&appName,
		&region,
		&machineID,
		&volumeID,
		&w.Name,
		&w.Status,
		&w.Source,
		&w.GitSHA,
		&prNumber,
		&w.ProfileName,
		&w.ProfileConfig,
		&expiresAt,
		&w.CreatedAt,
		&w.UpdatedAt,
		&heartbeat,
		&errorMessage,
	); err != nil {
		return err
	}

	if appName.Valid {
		w.AppName = appName.String
	}
	if region.Valid {
		w.Region = region.String
	}
	if machineID.Valid {
		w.FlyMachineID = machineID.String
	}
	if volumeID.Valid {
		w.FlyVolumeID = volumeID.String
	}
	if prNumber.Valid {
		w.PRNumber = int(prNumber.Int64)
	}
	if expiresAt.Valid {
		w.ExpiresAt = &expiresAt.Time
	}
	if heartbeat.Valid {
		w.LastHeartbeatAt = &heartbeat.Time
	}
	if errorMessage.Valid {
		w.ErrorMessage = errorMessage.String
	}

	return nil
}
