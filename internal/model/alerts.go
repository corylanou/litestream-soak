package model

import (
	"database/sql"
	"time"
)

func (d *DB) CreateAlert(alert *AlertDelivery) (int64, bool, error) {
	result, err := d.exec(`
		INSERT OR IGNORE INTO alerts (worker_id, verification_id, source, alert_type, fingerprint, status, condition_status, condition_started_at, condition_resolved_at, failure_stage, failure_signature, message, payload, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nullIntString(alert.WorkerID),
		nullInt(alert.VerificationID),
		alert.Source,
		alert.AlertType,
		alert.Fingerprint,
		alert.Status,
		alert.ConditionStatus,
		utcTimePtr(alert.ConditionStartedAt),
		utcTimePtr(alert.ConditionResolvedAt),
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
	_, err := d.exec(`
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

func (d *DB) GetActiveAlertCondition(alertType, source string) (*AlertDelivery, error) {
	var alert AlertDelivery
	var workerID, failureStage, failureSignature, message, payload, errorMessage sql.NullString
	var verificationID sql.NullInt64
	var conditionStartedAt, conditionResolvedAt, sentAt sql.NullTime
	err := d.queryRow(`
		SELECT id, worker_id, verification_id, source, alert_type, fingerprint, status, condition_status, condition_started_at, condition_resolved_at, failure_stage, failure_signature, message, payload, error_message, created_at, sent_at
		FROM alerts
		WHERE alert_type = ? AND source = ? AND condition_status = 'active'
		ORDER BY created_at DESC
		LIMIT 1`,
		alertType,
		source,
	).Scan(
		&alert.ID,
		&workerID,
		&verificationID,
		&alert.Source,
		&alert.AlertType,
		&alert.Fingerprint,
		&alert.Status,
		&alert.ConditionStatus,
		&conditionStartedAt,
		&conditionResolvedAt,
		&failureStage,
		&failureSignature,
		&message,
		&payload,
		&errorMessage,
		&alert.CreatedAt,
		&sentAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	populateAlertDelivery(
		&alert,
		workerID,
		verificationID,
		conditionStartedAt,
		conditionResolvedAt,
		failureStage,
		failureSignature,
		message,
		payload,
		errorMessage,
		sentAt,
	)
	return &alert, nil
}

func (d *DB) ResolveActiveAlertCondition(alertType, source string, resolvedAt time.Time) (bool, error) {
	result, err := d.exec(`
		UPDATE alerts
		SET condition_status = 'resolved', condition_resolved_at = ?
		WHERE alert_type = ? AND source = ? AND condition_status = 'active'`,
		resolvedAt.UTC(),
		alertType,
		source,
	)
	if err != nil {
		return false, err
	}
	rowsAffected, err := result.RowsAffected()
	return rowsAffected > 0, err
}

func (d *DB) ListActiveAlertSources(alertType string) ([]string, error) {
	rows, err := d.query(`
		SELECT DISTINCT source
		FROM alerts
		WHERE alert_type = ? AND condition_status = 'active'
		ORDER BY source`,
		alertType,
	)
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
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sources, nil
}

func (d *DB) ListAlerts(limit int) ([]AlertDelivery, error) {
	rows, err := d.query(`
		SELECT id, worker_id, verification_id, source, alert_type, fingerprint, status, condition_status, condition_started_at, condition_resolved_at, failure_stage, failure_signature, message, payload, error_message, created_at, sent_at
		FROM alerts
		ORDER BY created_at DESC
		LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	alerts := make([]AlertDelivery, 0)
	for rows.Next() {
		var alert AlertDelivery
		var workerID, failureStage, failureSignature, message, payload, errorMessage sql.NullString
		var verificationID sql.NullInt64
		var conditionStartedAt, conditionResolvedAt, sentAt sql.NullTime
		if err := rows.Scan(
			&alert.ID,
			&workerID,
			&verificationID,
			&alert.Source,
			&alert.AlertType,
			&alert.Fingerprint,
			&alert.Status,
			&alert.ConditionStatus,
			&conditionStartedAt,
			&conditionResolvedAt,
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
		populateAlertDelivery(
			&alert,
			workerID,
			verificationID,
			conditionStartedAt,
			conditionResolvedAt,
			failureStage,
			failureSignature,
			message,
			payload,
			errorMessage,
			sentAt,
		)
		alerts = append(alerts, alert)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return alerts, nil
}

func populateAlertDelivery(
	alert *AlertDelivery,
	workerID sql.NullString,
	verificationID sql.NullInt64,
	conditionStartedAt sql.NullTime,
	conditionResolvedAt sql.NullTime,
	failureStage sql.NullString,
	failureSignature sql.NullString,
	message sql.NullString,
	payload sql.NullString,
	errorMessage sql.NullString,
	sentAt sql.NullTime,
) {
	if workerID.Valid {
		alert.WorkerID = workerID.String
	}
	if verificationID.Valid {
		alert.VerificationID = int(verificationID.Int64)
	}
	if conditionStartedAt.Valid {
		alert.ConditionStartedAt = &conditionStartedAt.Time
	}
	if conditionResolvedAt.Valid {
		alert.ConditionResolvedAt = &conditionResolvedAt.Time
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
}
