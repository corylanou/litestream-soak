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
	StartedAt  time.Time `json:"started_at"`
	Status     string    `json:"status"`
	Passed     bool      `json:"passed"`
	DurationMS int       `json:"duration_ms"`
}

func (d *DB) ListVerificationTicks(perWorker int) (map[string][]VerificationTick, error) {
	rows, err := d.db.Query(`
		SELECT worker_id, started_at, status, passed, duration_ms FROM (
			SELECT worker_id, started_at, status, passed, duration_ms,
				ROW_NUMBER() OVER (PARTITION BY worker_id ORDER BY started_at DESC) AS rank
			FROM verifications
		) WHERE rank <= ? ORDER BY worker_id, started_at ASC`,
		perWorker,
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
	rows, err := d.db.Query(`
		SELECT v.started_at, v.status, v.passed, v.duration_ms
		FROM verifications v
		JOIN workers w ON w.id = v.worker_id
		WHERE w.source = ? AND v.started_at >= ?
		ORDER BY v.started_at ASC`,
		source, since,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	stats := make([]VerificationStat, 0)
	for rows.Next() {
		var stat VerificationStat
		if err := rows.Scan(&stat.StartedAt, &stat.Status, &stat.Passed, &stat.DurationMS); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return stats, nil
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

	err := d.db.QueryRow(`
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
	rows, err := d.db.Query(`
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
