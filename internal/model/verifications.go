package model

import (
	"database/sql"
	"time"
)

type VerificationTick struct {
	WorkerID   string    `json:"worker_id"`
	StartedAt  time.Time `json:"started_at"`
	Status     string    `json:"status"`
	Passed     bool      `json:"passed"`
	DurationMS int       `json:"duration_ms"`
}

type VerificationStat struct {
	ID              int       `json:"id"`
	WorkerID        string    `json:"worker_id"`
	Source          string    `json:"source"`
	WorkerCreatedAt time.Time `json:"worker_created_at"`
	StartedAt       time.Time `json:"started_at"`
	Status          string    `json:"status"`
	CheckType       string    `json:"check_type"`
	Passed          bool      `json:"passed"`
	DurationMS      int       `json:"duration_ms"`
	ErrorMessage    string    `json:"error_message,omitempty"`
	HasPriorPass    bool      `json:"has_prior_pass"`
}

func (d *DB) ListVerificationTicks(perWorker int, since time.Time) (map[string][]VerificationTick, error) {
	rows, err := d.query(`
		SELECT worker_id, started_at, status, passed, duration_ms FROM (
			SELECT worker_id, started_at, status, passed, duration_ms,
				ROW_NUMBER() OVER (PARTITION BY worker_id ORDER BY started_at DESC) AS rank
			FROM verifications
			WHERE started_at >= ?
		) WHERE rank <= ? ORDER BY worker_id, started_at ASC`,
		since, perWorker,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	ticks := make(map[string][]VerificationTick)
	for rows.Next() {
		var tick VerificationTick
		if err := rows.Scan(&tick.WorkerID, &tick.StartedAt, &tick.Status, &tick.Passed, &tick.DurationMS); err != nil {
			return nil, err
		}
		ticks[tick.WorkerID] = append(ticks[tick.WorkerID], tick)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ticks, nil
}

func (d *DB) ListVerificationStatsSince(source string, since time.Time) ([]VerificationStat, error) {
	rows, err := d.query(`
		SELECT
			v.id,
			v.worker_id,
			w.source,
			w.created_at,
			v.started_at,
			v.status,
			v.check_type,
			v.passed,
			v.duration_ms,
			COALESCE(v.error_message, ''),
			EXISTS (
				SELECT 1
				FROM verifications prior
				WHERE prior.worker_id = v.worker_id
					AND prior.started_at < v.started_at
					AND prior.passed = 1
					AND lower(trim(prior.status)) <> 'failed'
					AND lower(trim(prior.status)) <> 'aborted'
			)
		FROM verifications v
		JOIN workers w ON w.id = v.worker_id
		WHERE (? = '' OR w.source = ?) AND v.started_at >= ?
		ORDER BY v.started_at ASC`,
		source, source, since,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	stats := make([]VerificationStat, 0)
	for rows.Next() {
		var stat VerificationStat
		var hasPriorPass int
		if err := rows.Scan(
			&stat.ID,
			&stat.WorkerID,
			&stat.Source,
			&stat.WorkerCreatedAt,
			&stat.StartedAt,
			&stat.Status,
			&stat.CheckType,
			&stat.Passed,
			&stat.DurationMS,
			&stat.ErrorMessage,
			&hasPriorPass,
		); err != nil {
			return nil, err
		}
		stat.HasPriorPass = hasPriorPass != 0
		stats = append(stats, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return stats, nil
}

func (d *DB) RecordVerification(v *Verification) error {
	result, err := d.exec(`
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
	rows, err := d.query(`
		SELECT id, worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message
		FROM verifications WHERE worker_id = ? ORDER BY started_at DESC LIMIT ?`,
		workerID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return verifications, nil
}

func (d *DB) GetLatestFailedVerification(workerID string) (*Verification, error) {
	var verification Verification
	var completedAt sql.NullTime

	err := d.queryRow(`
		SELECT id, worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message
		FROM verifications
		WHERE worker_id = ? AND (passed = 0 OR lower(trim(status)) = 'failed') AND lower(trim(status)) <> 'aborted'
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

func (d *DB) ListRecentFailedVerifications(limit int) ([]Verification, error) {
	rows, err := d.query(`
		SELECT id, worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message
		FROM verifications
		WHERE (passed = 0 OR lower(trim(status)) = 'failed') AND lower(trim(status)) <> 'aborted'
		ORDER BY started_at DESC
		LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return verifications, nil
}
