package model

func (d *DB) UpsertVolumeGCAttempt(attempt VolumeGCAttempt) error {
	_, err := d.exec(`
		INSERT INTO volume_gc_attempts (
			volume_id,
			app_name,
			volume_name,
			region,
			size_gb,
			volume_created_at,
			first_attempt_at,
			last_attempt_at,
			next_retry_at,
			request_count,
			request_accepted
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(volume_id) DO UPDATE SET
			app_name = excluded.app_name,
			volume_name = excluded.volume_name,
			region = excluded.region,
			size_gb = excluded.size_gb,
			volume_created_at = excluded.volume_created_at,
			first_attempt_at = excluded.first_attempt_at,
			last_attempt_at = excluded.last_attempt_at,
			next_retry_at = excluded.next_retry_at,
			request_count = excluded.request_count,
			request_accepted = excluded.request_accepted`,
		attempt.VolumeID,
		attempt.AppName,
		attempt.VolumeName,
		attempt.Region,
		attempt.SizeGB,
		attempt.VolumeCreatedAt.UTC(),
		attempt.FirstAttemptAt.UTC(),
		attempt.LastAttemptAt.UTC(),
		attempt.NextRetryAt.UTC(),
		attempt.RequestCount,
		attempt.RequestAccepted,
	)
	return err
}

func (d *DB) ListVolumeGCAttempts(appName string) ([]VolumeGCAttempt, error) {
	rows, err := d.query(`
		SELECT
			volume_id,
			app_name,
			volume_name,
			region,
			size_gb,
			volume_created_at,
			first_attempt_at,
			last_attempt_at,
			next_retry_at,
			request_count,
			request_accepted
		FROM volume_gc_attempts
		WHERE app_name = ?
		ORDER BY next_retry_at, volume_id`,
		appName,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	attempts := make([]VolumeGCAttempt, 0)
	for rows.Next() {
		var attempt VolumeGCAttempt
		if err := rows.Scan(
			&attempt.VolumeID,
			&attempt.AppName,
			&attempt.VolumeName,
			&attempt.Region,
			&attempt.SizeGB,
			&attempt.VolumeCreatedAt,
			&attempt.FirstAttemptAt,
			&attempt.LastAttemptAt,
			&attempt.NextRetryAt,
			&attempt.RequestCount,
			&attempt.RequestAccepted,
		); err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return attempts, nil
}

func (d *DB) DeleteVolumeGCAttempt(volumeID string) error {
	_, err := d.exec(`DELETE FROM volume_gc_attempts WHERE volume_id = ?`, volumeID)
	return err
}
