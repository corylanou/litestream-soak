package model

import (
	"database/sql"
	"encoding/json"
	"errors"
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
		at, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", raw)
		if err != nil {
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

func (d *DB) CreateWorker(w *Worker) error {
	_, err := d.db.Exec(`
		INSERT INTO workers (id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, '', '', '', NULL)
		ON CONFLICT(id) DO UPDATE SET
			app_name = excluded.app_name,
			region = excluded.region,
			fly_machine_id = COALESCE(excluded.fly_machine_id, workers.fly_machine_id),
			fly_volume_id = COALESCE(excluded.fly_volume_id, workers.fly_volume_id),
			name = excluded.name,
			status = excluded.status,
			source = excluded.source,
			git_sha = excluded.git_sha,
			litestream_sha = excluded.litestream_sha,
			pr_number = excluded.pr_number,
			profile_name = excluded.profile_name,
			profile_config = excluded.profile_config,
			expires_at = excluded.expires_at,
			created_at = datetime('now'),
			last_heartbeat_at = NULL,
			last_runtime_json = '',
			last_runtime_at = NULL,
			error_message = '',
			dormant_at = NULL,
			dormant_reason = '',
			dormant_signature = '',
			resume_trigger = '',
			last_probe_at = NULL,
			updated_at = datetime('now')`,
		w.ID, w.AppName, w.Region, nullIntString(w.FlyMachineID), nullIntString(w.FlyVolumeID), w.Name, w.Status, w.Source, w.GitSHA, w.LitestreamSHA, w.PRNumber, w.ProfileName, w.ProfileConfig, utcTimePtr(w.ExpiresAt),
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
		nullIntString(machineID), nullIntString(volumeID), id,
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

func (d *DB) UpdateWorkerRuntimeSnapshot(id string, payload reporting.RuntimePayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal runtime payload: %w", err)
	}

	if !payload.LitestreamSnapshotHealthy {
		_, err = d.db.Exec(`
			UPDATE workers SET last_runtime_json = ?, updated_at = datetime('now')
			WHERE id = ?`,
			string(body), id,
		)
		return err
	}

	reportedAt := payload.SnapshotCollectedAt
	if reportedAt.IsZero() {
		reportedAt = time.Now().UTC()
	}

	_, err = d.db.Exec(`
		UPDATE workers SET last_runtime_json = ?, last_runtime_at = ?, updated_at = datetime('now')
		WHERE id = ?`,
		string(body), reportedAt.UTC(), id,
	)
	return err
}

// UpsertReportedWorker inserts or updates a worker based on an incoming report.
// A report from a stopped or failed worker preserves status and error_message;
// only last_heartbeat_at, updated_at, and identity fields refresh, which is
// harmless because staleness detection only considers running/probing workers —
// so teardown races leave an observable heartbeat trail without resurrecting
// the row. Only pending, building, and starting workers are flipped to running.
func (d *DB) UpsertReportedWorker(identity reporting.WorkerIdentity) error {
	name := identity.Name
	if name == "" {
		name = identity.WorkerID
	}

	profileConfig := identity.ProfileConfig
	if strings.TrimSpace(profileConfig) == "" {
		profileConfig = "{}"
	}

	_, err := d.db.Exec(`
		INSERT INTO workers (id, app_name, region, fly_machine_id, name, status, source, git_sha, litestream_sha, profile_name, profile_config, last_heartbeat_at)
		VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, datetime('now'))
			ON CONFLICT(id) DO UPDATE SET
				app_name = CASE
					WHEN excluded.app_name <> '' THEN excluded.app_name
					ELSE workers.app_name
				END,
			region = CASE
				WHEN excluded.region <> '' THEN excluded.region
				ELSE workers.region
			END,
			fly_machine_id = COALESCE(excluded.fly_machine_id, workers.fly_machine_id),
				name = excluded.name,
				status = CASE
					WHEN workers.status IN ('pending', 'building', 'starting') THEN 'running'
					WHEN workers.status = 'degraded' AND workers.error_message = 'worker missed heartbeat deadline' THEN 'running'
					ELSE workers.status
				END,
				error_message = CASE
					WHEN workers.status IN ('pending', 'building', 'starting') THEN ''
					WHEN workers.status = 'degraded' AND workers.error_message = 'worker missed heartbeat deadline' THEN ''
					ELSE workers.error_message
				END,
				source = excluded.source,
				git_sha = excluded.git_sha,
				litestream_sha = CASE
					WHEN excluded.litestream_sha <> '' THEN excluded.litestream_sha
					ELSE workers.litestream_sha
				END,
				profile_name = excluded.profile_name,
			profile_config = CASE
				WHEN workers.profile_config <> '{}' THEN workers.profile_config
				WHEN excluded.profile_config <> '{}' THEN excluded.profile_config
				ELSE workers.profile_config
			END,
			last_heartbeat_at = datetime('now'),
			updated_at = datetime('now')`,
		identity.WorkerID,
		identity.AppName,
		identity.Region,
		nullIntString(identity.MachineID),
		name,
		identity.Source,
		identity.GitSHA,
		identity.LitestreamSHA,
		identity.ProfileName,
		profileConfig,
	)
	return err
}

// UpdateWorkerVerificationState updates a worker's status based on a verification result.
// Verification reports never change a stopped, failed, or dormant worker; dormancy
// state is only cleared via the explicit probe/resume path (MarkWorkerProbing).
func (d *DB) UpdateWorkerVerificationState(id string, passed bool, summary string) error {
	status := WorkerRunning
	errMsg := ""
	if !passed {
		status = WorkerDegraded
		errMsg = summary
	}

	if passed {
		_, err := d.db.Exec(`
			UPDATE workers
			SET status = ?, error_message = ?, dormant_at = NULL, dormant_reason = '', dormant_signature = '', resume_trigger = '', last_probe_at = NULL, updated_at = datetime('now')
			WHERE id = ? AND status NOT IN ('stopped','failed','dormant')`,
			status, errMsg, id,
		)
		return err
	}

	_, err := d.db.Exec(`
		UPDATE workers
		SET status = ?, error_message = ?, updated_at = datetime('now')
		WHERE id = ? AND status NOT IN ('stopped','failed','dormant')`,
		status, errMsg, id,
	)
	return err
}

func (d *DB) MarkWorkerDormant(id, reason, signature, resumeTrigger string) error {
	_, err := d.db.Exec(`
		UPDATE workers
		SET status = ?, error_message = ?, dormant_at = datetime('now'), dormant_reason = ?, dormant_signature = ?, resume_trigger = ?, updated_at = datetime('now')
		WHERE id = ?`,
		WorkerDormant,
		reason,
		reason,
		signature,
		resumeTrigger,
		id,
	)
	return err
}

func (d *DB) MarkWorkerProbing(id, resumeTrigger string) error {
	_, err := d.db.Exec(`
		UPDATE workers
		SET status = ?, error_message = '', dormant_at = NULL, dormant_reason = '', dormant_signature = '', resume_trigger = ?, last_probe_at = datetime('now'), updated_at = datetime('now')
		WHERE id = ?`,
		WorkerProbing,
		resumeTrigger,
		id,
	)
	return err
}

func (d *DB) ClearWorkerDormancy(id string) error {
	_, err := d.db.Exec(`
		UPDATE workers
		SET dormant_at = NULL, dormant_reason = '', dormant_signature = '', resume_trigger = '', last_probe_at = NULL, updated_at = datetime('now')
		WHERE id = ?`,
		id,
	)
	return err
}

func (d *DB) UpdateWorkerMachineVersion(id, machineID, gitSHA, litestreamSHA string) error {
	_, err := d.db.Exec(`
		UPDATE workers
		SET fly_machine_id = ?, git_sha = ?, litestream_sha = ?, updated_at = datetime('now')
		WHERE id = ?`,
		nullIntString(machineID),
		gitSHA,
		litestreamSHA,
		id,
	)
	return err
}

func (d *DB) GetWorker(id string) (*Worker, error) {
	var w Worker
	err := scanWorker(
		d.db.QueryRow(`SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at FROM workers WHERE id = ?`, id),
		&w,
	)
	if err != nil {
		return nil, err
	}
	return &w, nil
}

func (d *DB) ListWorkers(status string) ([]Worker, error) {
	return d.ListWorkersFiltered(status, "")
}

func (d *DB) ListWorkersFiltered(status, source string) ([]Worker, error) {
	query := `SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at FROM workers`
	var args []any
	if source != "" {
		query += " WHERE source = ?"
		args = append(args, source)
	}
	if status != "" {
		if len(args) == 0 {
			query += " WHERE status = ?"
		} else {
			query += " AND status = ?"
		}
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

func (d *DB) ListDormancyWorkers() ([]Worker, error) {
	rows, err := d.db.Query(`
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at
		FROM workers
		WHERE status IN ('running', 'degraded')
		ORDER BY source, created_at`)
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

func (d *DB) ListActiveWorkerSources() ([]string, error) {
	rows, err := d.db.Query(`
		SELECT DISTINCT source
		FROM workers
		WHERE status NOT IN ('stopped', 'failed')
		ORDER BY source`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sources := make([]string, 0)
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			return nil, err
		}
		if strings.TrimSpace(source) == "" {
			source = "main"
		}
		sources = append(sources, source)
	}
	return sources, nil
}

func (d *DB) ListExpiredWorkers() ([]Worker, error) {
	rows, err := d.db.Query(`
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at
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
	result, err := d.db.Exec(`
		INSERT INTO verifications (worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		v.WorkerID, v.StartedAt, v.CompletedAt, v.Status, v.CheckType, v.SourceChecksum, v.RestoredChecksum, v.Passed, v.DurationMS, v.ErrorMessage,
	)
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	v.ID = int(id)
	return nil
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

func (d *DB) GetLatestFailedVerification(workerID string) (*Verification, error) {
	var verification Verification
	var completedAt sql.NullTime

	err := d.db.QueryRow(`
		SELECT id, worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message
		FROM verifications
		WHERE worker_id = ? AND (passed = 0 OR status = 'failed')
		ORDER BY started_at DESC
		LIMIT 1`,
		workerID,
	).Scan(
		&verification.ID,
		&verification.WorkerID,
		&verification.StartedAt,
		&completedAt,
		&verification.Status,
		&verification.CheckType,
		&verification.SourceChecksum,
		&verification.RestoredChecksum,
		&verification.Passed,
		&verification.DurationMS,
		&verification.ErrorMessage,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if completedAt.Valid {
		verification.CompletedAt = &completedAt.Time
	}
	return &verification, nil
}

func (d *DB) CreateDeployment(dep *Deployment) (int64, error) {
	result, err := d.db.Exec(`
		INSERT INTO deployments (git_sha, litestream_sha, image_ref, source, pr_number, status)
		VALUES (?, ?, ?, ?, ?, ?)`,
		dep.GitSHA, dep.LitestreamSHA, dep.ImageRef, dep.Source, dep.PRNumber, dep.Status,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) ListDeployments(source string, limit int) ([]Deployment, error) {
	query := `
		SELECT id, git_sha, litestream_sha, image_ref, source, pr_number, status, started_at, completed_at, error_message
		FROM deployments`
	args := make([]any, 0, 2)
	if source != "" {
		query += " WHERE source = ?"
		args = append(args, source)
	}
	query += " ORDER BY started_at DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deployments := make([]Deployment, 0)
	for rows.Next() {
		var dep Deployment
		if err := scanDeployment(rows, &dep); err != nil {
			return nil, err
		}
		deployments = append(deployments, dep)
	}

	return deployments, nil
}

func (d *DB) UpdateDeployment(id int64, status, imageRef, errMsg string) error {
	_, err := d.db.Exec(`
		UPDATE deployments SET status = ?, image_ref = ?, error_message = ?, completed_at = datetime('now')
		WHERE id = ?`,
		status, imageRef, errMsg, id,
	)
	return err
}

func (d *DB) UpsertReadyDeployment(dep *Deployment) error {
	existing, err := d.GetDeploymentByVersion(dep.Source, dep.GitSHA, dep.LitestreamSHA)
	switch {
	case err == nil:
		return d.UpdateDeployment(int64(existing.ID), "ready", dep.ImageRef, "")
	case errors.Is(err, sql.ErrNoRows):
		_, err := d.db.Exec(`
			INSERT INTO deployments (git_sha, litestream_sha, image_ref, source, pr_number, status, started_at, completed_at, error_message)
			VALUES (?, ?, ?, ?, ?, 'ready', datetime('now'), datetime('now'), '')`,
			dep.GitSHA, dep.LitestreamSHA, dep.ImageRef, dep.Source, dep.PRNumber,
		)
		return err
	default:
		return err
	}
}

func (d *DB) GetDeploymentBySHA(sha string) (*Deployment, error) {
	var dep Deployment
	err := scanDeployment(
		d.db.QueryRow(`SELECT id, git_sha, litestream_sha, image_ref, source, pr_number, status, started_at, completed_at, error_message FROM deployments WHERE git_sha = ? ORDER BY started_at DESC LIMIT 1`, sha),
		&dep,
	)
	if err != nil {
		return nil, err
	}
	return &dep, nil
}

func (d *DB) GetDeploymentByVersion(source, gitSHA, litestreamSHA string) (*Deployment, error) {
	var dep Deployment
	err := scanDeployment(
		d.db.QueryRow(`
			SELECT id, git_sha, litestream_sha, image_ref, source, pr_number, status, started_at, completed_at, error_message
			FROM deployments
			WHERE source = ? AND git_sha = ? AND litestream_sha = ?
			ORDER BY started_at DESC, id DESC
			LIMIT 1`,
			source,
			gitSHA,
			litestreamSHA,
		),
		&dep,
	)
	if err != nil {
		return nil, err
	}
	return &dep, nil
}

func (d *DB) GetLatestDeployment(source string) (*Deployment, error) {
	query := `
		SELECT id, git_sha, litestream_sha, image_ref, source, pr_number, status, started_at, completed_at, error_message
		FROM deployments`
	args := make([]any, 0, 1)
	if source != "" {
		query += " WHERE source = ?"
		args = append(args, source)
	}
	query += " ORDER BY started_at DESC, id DESC LIMIT 1"

	var dep Deployment
	err := scanDeployment(d.db.QueryRow(query, args...), &dep)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &dep, nil
}

func (d *DB) RecordRunArchive(archive *RunArchive) (bool, error) {
	if archive == nil {
		return false, errors.New("run archive is nil")
	}
	if strings.TrimSpace(archive.Payload) == "" {
		archive.Payload = "{}"
	}
	if archive.ArchivedAt.IsZero() {
		archive.ArchivedAt = time.Now().UTC()
	}

	result, err := d.db.Exec(`
		INSERT OR IGNORE INTO run_archives (deployment_id, source, worker_id, archive_type, git_sha, litestream_sha, image_ref, status, summary, payload, archived_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		archive.DeploymentID,
		archive.Source,
		archive.WorkerID,
		archive.ArchiveType,
		archive.GitSHA,
		archive.LitestreamSHA,
		archive.ImageRef,
		archive.Status,
		archive.Summary,
		archive.Payload,
		archive.ArchivedAt,
	)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rowsAffected == 0 {
		err := d.db.QueryRow(`
			SELECT id, archived_at
			FROM run_archives
			WHERE deployment_id = ? AND archive_type = ? AND worker_id = ?`,
			archive.DeploymentID,
			archive.ArchiveType,
			archive.WorkerID,
		).Scan(&archive.ID, &archive.ArchivedAt)
		return false, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return false, err
	}
	archive.ID = int(id)
	return true, nil
}

func (d *DB) ListRunArchives(source, archiveType string, limit int) ([]RunArchive, error) {
	if limit <= 0 {
		limit = 20
	}

	query := `
		SELECT id, deployment_id, source, worker_id, archive_type, git_sha, litestream_sha, image_ref, status, summary, payload, archived_at
		FROM run_archives`
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 2)
	if strings.TrimSpace(source) != "" {
		clauses = append(clauses, "source = ?")
		args = append(args, strings.TrimSpace(source))
	}
	if strings.TrimSpace(archiveType) != "" {
		clauses = append(clauses, "archive_type = ?")
		args = append(args, strings.TrimSpace(archiveType))
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY archived_at DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	archives := make([]RunArchive, 0)
	for rows.Next() {
		var archive RunArchive
		if err := scanRunArchive(rows, &archive); err != nil {
			return nil, err
		}
		archives = append(archives, archive)
	}
	return archives, nil
}

func (d *DB) GetRunArchive(id int) (*RunArchive, error) {
	var archive RunArchive
	err := scanRunArchive(
		d.db.QueryRow(`
			SELECT id, deployment_id, source, worker_id, archive_type, git_sha, litestream_sha, image_ref, status, summary, payload, archived_at
			FROM run_archives
			WHERE id = ?`, id),
		&archive,
	)
	if err != nil {
		return nil, err
	}
	return &archive, nil
}

func (d *DB) RecordEvent(workerID, eventType, message, details string) error {
	_, err := d.db.Exec(`
		INSERT INTO events (worker_id, event_type, message, details)
		VALUES (?, ?, ?, ?)`,
		workerID, eventType, message, details,
	)
	return err
}

func (d *DB) RecordEventAt(workerID, eventType, message, details string, createdAt time.Time) error {
	_, err := d.db.Exec(`
		INSERT INTO events (worker_id, event_type, message, details, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		workerID, eventType, message, details, createdAt,
	)
	return err
}

func (d *DB) RecordUniqueEventAt(workerID, eventType, message, details string, createdAt time.Time) (bool, error) {
	var id int
	err := d.db.QueryRow(`
		SELECT 1
		FROM events
		WHERE worker_id = ? AND event_type = ? AND message = ? AND details = ? AND created_at = ?
		LIMIT 1`,
		workerID, eventType, message, details, createdAt,
	).Scan(&id)
	switch {
	case err == nil:
		return false, nil
	case err != sql.ErrNoRows:
		return false, err
	}

	if err := d.RecordEventAt(workerID, eventType, message, details, createdAt); err != nil {
		return false, err
	}
	return true, nil
}

func (d *DB) RecordWindowedEventAt(workerID, eventType, message, details string, createdAt time.Time, window time.Duration) (bool, error) {
	if window <= 0 {
		if err := d.RecordEventAt(workerID, eventType, message, details, createdAt); err != nil {
			return false, err
		}
		return true, nil
	}

	var existingID int
	windowStart := createdAt.Add(-window)
	err := d.db.QueryRow(`
		SELECT id
		FROM events
		WHERE worker_id = ? AND event_type = ? AND message = ? AND created_at >= ? AND created_at <= ?
		ORDER BY created_at DESC
		LIMIT 1`,
		workerID, eventType, message, windowStart, createdAt,
	).Scan(&existingID)
	switch {
	case err == nil:
		_, err = d.db.Exec(`
			UPDATE events
			SET details = ?, created_at = ?
			WHERE id = ?`,
			details, createdAt, existingID,
		)
		if err != nil {
			return false, err
		}
		return false, nil
	case err != sql.ErrNoRows:
		return false, err
	}

	if err := d.RecordEventAt(workerID, eventType, message, details, createdAt); err != nil {
		return false, err
	}
	return true, nil
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

func (d *DB) ListWorkersForSource(source string) ([]Worker, error) {
	rows, err := d.db.Query(`
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at
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

func (d *DB) ListDormantWorkers(source string) ([]Worker, error) {
	query := `
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at
		FROM workers
		WHERE status = 'dormant'`
	args := make([]any, 0, 1)
	if source != "" {
		query += " AND source = ?"
		args = append(args, source)
	}
	query += " ORDER BY updated_at DESC"

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

func (d *DB) CreateAlert(alert *AlertDelivery) (int64, bool, error) {
	result, err := d.db.Exec(`
		INSERT OR IGNORE INTO alerts (worker_id, verification_id, alert_type, fingerprint, status, failure_stage, failure_signature, message, payload, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nullIntString(alert.WorkerID),
		nullInt(alert.VerificationID),
		alert.AlertType,
		alert.Fingerprint,
		alert.Status,
		nullIntString(alert.FailureStage),
		nullIntString(alert.FailureSignature),
		nullIntString(alert.Message),
		nullIntString(alert.Payload),
		nullIntString(alert.ErrorMessage),
	)
	if err != nil {
		return 0, false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if rowsAffected == 0 {
		return 0, false, nil
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

func (d *DB) UpdateAlertDelivery(id int64, status, payload, errMsg string, sentAt *time.Time) error {
	_, err := d.db.Exec(`
		UPDATE alerts
		SET status = ?, payload = ?, error_message = ?, sent_at = ?
		WHERE id = ?`,
		status,
		nullIntString(payload),
		nullIntString(errMsg),
		sentAt,
		id,
	)
	return err
}

func (d *DB) ListAlerts(limit int) ([]AlertDelivery, error) {
	rows, err := d.db.Query(`
		SELECT id, worker_id, verification_id, alert_type, fingerprint, status, failure_stage, failure_signature, message, payload, error_message, created_at, sent_at
		FROM alerts
		ORDER BY created_at DESC
		LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	alerts := make([]AlertDelivery, 0)
	for rows.Next() {
		var alert AlertDelivery
		var workerID, failureStage, failureSignature, message, payload, errorMessage sql.NullString
		var verificationID sql.NullInt64
		var sentAt sql.NullTime
		if err := rows.Scan(
			&alert.ID,
			&workerID,
			&verificationID,
			&alert.AlertType,
			&alert.Fingerprint,
			&alert.Status,
			&failureStage,
			&failureSignature,
			&message,
			&payload,
			&errorMessage,
			&alert.CreatedAt,
			&sentAt,
		); err != nil {
			return nil, err
		}
		if workerID.Valid {
			alert.WorkerID = workerID.String
		}
		if verificationID.Valid {
			alert.VerificationID = int(verificationID.Int64)
		}
		if failureStage.Valid {
			alert.FailureStage = failureStage.String
		}
		if failureSignature.Valid {
			alert.FailureSignature = failureSignature.String
		}
		if message.Valid {
			alert.Message = message.String
		}
		if payload.Valid {
			alert.Payload = payload.String
		}
		if errorMessage.Valid {
			alert.ErrorMessage = errorMessage.String
		}
		if sentAt.Valid {
			alert.SentAt = &sentAt.Time
		}
		alerts = append(alerts, alert)
	}

	return alerts, nil
}

func (d *DB) listWorkersBySource(source string) ([]Worker, error) {
	rows, err := d.db.Query(`
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at
		FROM workers WHERE source = ? AND status NOT IN ('stopped', 'failed', 'dormant')
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
	cutoff := time.Now().UTC().Add(-timeout).Format("2006-01-02 15:04:05")
	rows, err := d.db.Query(`
		SELECT id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at
		FROM workers
		WHERE status IN ('running', 'probing') AND last_heartbeat_at IS NOT NULL AND last_heartbeat_at < ?`,
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

type deploymentScanner interface {
	Scan(dest ...any) error
}

type runArchiveScanner interface {
	Scan(dest ...any) error
}

func scanRunArchive(scanner runArchiveScanner, archive *RunArchive) error {
	return scanner.Scan(
		&archive.ID,
		&archive.DeploymentID,
		&archive.Source,
		&archive.WorkerID,
		&archive.ArchiveType,
		&archive.GitSHA,
		&archive.LitestreamSHA,
		&archive.ImageRef,
		&archive.Status,
		&archive.Summary,
		&archive.Payload,
		&archive.ArchivedAt,
	)
}

func scanDeployment(scanner deploymentScanner, dep *Deployment) error {
	var completedAt sql.NullTime
	var prNumber sql.NullInt64
	if err := scanner.Scan(
		&dep.ID,
		&dep.GitSHA,
		&dep.LitestreamSHA,
		&dep.ImageRef,
		&dep.Source,
		&prNumber,
		&dep.Status,
		&dep.StartedAt,
		&completedAt,
		&dep.ErrorMessage,
	); err != nil {
		return err
	}

	if prNumber.Valid {
		dep.PRNumber = int(prNumber.Int64)
	}
	if completedAt.Valid {
		dep.CompletedAt = &completedAt.Time
	}
	return nil
}

func scanWorker(scanner workerScanner, w *Worker) error {
	var appName, region, machineID, volumeID, errorMessage, lastRuntimeJSON, dormantReason, dormantSignature, resumeTrigger, litestreamSHA sql.NullString
	var expiresAt, heartbeat, lastRuntimeAt, dormantAt, lastProbeAt sql.NullTime
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
		&litestreamSHA,
		&prNumber,
		&w.ProfileName,
		&w.ProfileConfig,
		&expiresAt,
		&w.CreatedAt,
		&w.UpdatedAt,
		&heartbeat,
		&errorMessage,
		&lastRuntimeJSON,
		&lastRuntimeAt,
		&dormantAt,
		&dormantReason,
		&dormantSignature,
		&resumeTrigger,
		&lastProbeAt,
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
	if litestreamSHA.Valid {
		w.LitestreamSHA = litestreamSHA.String
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
	if lastRuntimeJSON.Valid {
		w.LastRuntimeJSON = lastRuntimeJSON.String
	}
	if lastRuntimeAt.Valid {
		w.LastRuntimeAt = &lastRuntimeAt.Time
	}
	if dormantAt.Valid {
		w.DormantAt = &dormantAt.Time
	}
	if dormantReason.Valid {
		w.DormantReason = dormantReason.String
	}
	if dormantSignature.Valid {
		w.DormantSignature = dormantSignature.String
	}
	if resumeTrigger.Valid {
		w.ResumeTrigger = resumeTrigger.String
	}
	if lastProbeAt.Valid {
		w.LastProbeAt = &lastProbeAt.Time
	}

	return nil
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
