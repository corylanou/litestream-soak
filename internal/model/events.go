package model

import (
	"database/sql"
	"time"
)

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
	defer func() { _ = rows.Close() }()

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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
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
	defer func() { _ = rows.Close() }()

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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
