package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const workerColumns = "id, app_name, region, fly_machine_id, fly_volume_id, name, status, source, git_sha, litestream_sha, pr_number, profile_name, profile_config, expires_at, created_at, updated_at, last_heartbeat_at, error_message, last_runtime_json, last_runtime_at, dormant_at, dormant_reason, dormant_signature, resume_trigger, last_probe_at"

type workerScanner interface {
	Scan(dest ...any) error
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

func (d *DB) queryWorkers(query string, args ...any) ([]Worker, error) {
	rows, err := d.query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	workers := make([]Worker, 0)
	for rows.Next() {
		var w Worker
		if err := scanWorker(rows, &w); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return workers, nil
}

func (d *DB) CreateWorker(w *Worker) error {
	_, err := d.exec(`
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
			created_at = datetime('now'), -- worker ID reuse signals fresh provision; reset all runtime state including created_at
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
	_, err := d.exec(`
		UPDATE workers SET status = ?, error_message = ?, updated_at = datetime('now')
		WHERE id = ?`,
		status, errMsg, id,
	)
	return err
}

func (d *DB) UpdateWorkerMachine(id, machineID, volumeID string) error {
	_, err := d.exec(`
		UPDATE workers SET fly_machine_id = ?, fly_volume_id = ?, updated_at = datetime('now')
		WHERE id = ?`,
		nullIntString(machineID), nullIntString(volumeID), id,
	)
	return err
}

func (d *DB) UpdateWorkerHeartbeat(id string) error {
	_, err := d.exec(`
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
		_, err = d.exec(`
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

	_, err = d.exec(`
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

	_, err := d.exec(`
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
		_, err := d.exec(`
			UPDATE workers
			SET status = ?, error_message = ?, dormant_at = NULL, dormant_reason = '', dormant_signature = '', resume_trigger = '', last_probe_at = NULL, updated_at = datetime('now')
			WHERE id = ? AND status NOT IN ('stopped','failed','dormant')`,
			status, errMsg, id,
		)
		return err
	}

	_, err := d.exec(`
		UPDATE workers
		SET status = ?, error_message = ?, updated_at = datetime('now')
		WHERE id = ? AND status NOT IN ('stopped','failed','dormant')`,
		status, errMsg, id,
	)
	return err
}

func (d *DB) MarkWorkerDormant(id, reason, signature, resumeTrigger string) error {
	_, err := d.exec(`
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
	_, err := d.exec(`
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
	_, err := d.exec(`
		UPDATE workers
		SET dormant_at = NULL, dormant_reason = '', dormant_signature = '', resume_trigger = '', last_probe_at = NULL, updated_at = datetime('now')
		WHERE id = ?`,
		id,
	)
	return err
}

func (d *DB) UpdateWorkerMachineVersion(id, machineID, gitSHA, litestreamSHA string) error {
	_, err := d.exec(`
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
		d.queryRow("SELECT "+workerColumns+" FROM workers WHERE id = ?", id),
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
	query := "SELECT " + workerColumns + " FROM workers"
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
	return d.queryWorkers(query, args...)
}

func (d *DB) ListDormancyWorkers() ([]Worker, error) {
	return d.queryWorkers("SELECT " + workerColumns + " FROM workers WHERE status IN ('running', 'degraded') ORDER BY source, created_at")
}

func (d *DB) ListActiveWorkerSources() ([]string, error) {
	rows, err := d.query(`
		SELECT DISTINCT source
		FROM workers
		WHERE status NOT IN ('stopped', 'failed')
		ORDER BY source`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sources, nil
}

func (d *DB) ListExpiredWorkers() ([]Worker, error) {
	return d.queryWorkers("SELECT " + workerColumns + " FROM workers WHERE expires_at IS NOT NULL AND expires_at < datetime('now') AND status NOT IN ('stopped', 'failed') ORDER BY expires_at")
}

func (d *DB) ListMainWorkers() ([]Worker, error) {
	return d.listWorkersBySource("main")
}

func (d *DB) ListWorkersForSource(source string) ([]Worker, error) {
	return d.queryWorkers("SELECT "+workerColumns+" FROM workers WHERE source = ? AND status NOT IN ('stopped', 'failed') ORDER BY created_at", source)
}

func (d *DB) ListDormantWorkers(source string) ([]Worker, error) {
	query := "SELECT " + workerColumns + " FROM workers WHERE status = 'dormant'"
	args := make([]any, 0, 1)
	if source != "" {
		query += " AND source = ?"
		args = append(args, source)
	}
	query += " ORDER BY updated_at DESC"
	return d.queryWorkers(query, args...)
}

func (d *DB) listWorkersBySource(source string) ([]Worker, error) {
	return d.queryWorkers("SELECT "+workerColumns+" FROM workers WHERE source = ? AND status NOT IN ('stopped', 'failed', 'dormant') ORDER BY created_at", source)
}

// DeleteWorker removes a worker from the database along with its verifications and events.
func (d *DB) DeleteWorker(id string) error {
	tx, err := d.writer.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

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
	return d.queryWorkers("SELECT "+workerColumns+" FROM workers WHERE status IN ('running', 'probing') AND last_heartbeat_at IS NOT NULL AND last_heartbeat_at < ?", cutoff)
}
