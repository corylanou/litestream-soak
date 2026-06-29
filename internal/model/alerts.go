package model

import (
	"database/sql"
	"time"
)

func (d *DB) CreateAlert(alert *AlertDelivery) (int64, bool, error) {
	result, err := d.exec(`
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

func (d *DB) ListAlerts(limit int) ([]AlertDelivery, error) {
	rows, err := d.query(`
		SELECT id, worker_id, verification_id, alert_type, fingerprint, status, failure_stage, failure_signature, message, payload, error_message, created_at, sent_at
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return alerts, nil
}
