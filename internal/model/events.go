package model

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"slices"
	"strings"
	"time"
)

func (d *DB) RecordEvent(workerID, eventType, message, details string) error {
	defer d.blobs.hold()()
	details, err := d.compactProfileSnapshot(details)
	if err != nil {
		return err
	}
	_, err = d.exec(`
		INSERT INTO events (worker_id, event_type, message, details)
		VALUES (?, ?, ?, ?)`,
		workerID, eventType, message, details,
	)
	return err
}

func (d *DB) RecordEventAt(workerID, eventType, message, details string, createdAt time.Time) error {
	defer d.blobs.hold()()
	details, err := d.compactProfileSnapshot(details)
	if err != nil {
		return err
	}
	_, err = d.exec(`
		INSERT INTO events (worker_id, event_type, message, details, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		workerID, eventType, message, details, createdAt,
	)
	return err
}

func (d *DB) RecordUniqueEventAt(workerID, eventType, message, details string, createdAt time.Time) (bool, error) {
	compact, err := d.compactProfileSnapshot(details)
	if err != nil {
		return false, err
	}
	var id int
	err = d.queryRow(`
		SELECT 1
		FROM events
		WHERE worker_id = ? AND event_type = ? AND message = ? AND details IN (?, ?) AND created_at = ?
		LIMIT 1`,
		workerID, eventType, message, details, compact, createdAt,
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

	key := eventOccurrenceKey(createdAt, details)
	var (
		existingID      int
		existingAt      time.Time
		existingDetails string
		existingKeys    string
	)
	err := d.queryRow(`
		SELECT id, created_at, details, occurrence_keys
		FROM events
		WHERE worker_id = ? AND event_type = ? AND message = ? AND created_at >= ? AND created_at <= ?
		ORDER BY created_at DESC
		LIMIT 1`,
		workerID, eventType, message, createdAt.Add(-window), createdAt.Add(window),
	).Scan(&existingID, &existingAt, &existingDetails, &existingKeys)
	switch {
	case err == nil:
		release := d.blobs.hold()
		compactDetails, err := d.compactProfileSnapshot(details)
		if err != nil {
			release()
			return false, err
		}
		if existingKeys == "" {
			// Rows recorded before occurrence tracking carry only their latest
			// occurrence; seed that identity so replaying it never counts again.
			existingKeys = eventOccurrenceKey(existingAt, existingDetails)
		}
		seen := strings.Split(existingKeys, "\n")
		if slices.Contains(seen, key) || slices.Contains(seen, eventOccurrenceKey(createdAt, compactDetails)) {
			release()
			return false, nil
		}
		keys := existingKeys + "\n" + key
		if createdAt.Before(existingAt) {
			_, err = d.exec(`
			UPDATE events
			SET occurrences = occurrences + 1, occurrence_keys = ?
			WHERE id = ?`,
				keys, existingID,
			)
		} else {
			_, err = d.exec(`
			UPDATE events
			SET details = ?, created_at = ?, occurrences = occurrences + 1, occurrence_keys = ?
			WHERE id = ?`,
				compactDetails, createdAt, keys, existingID,
			)
		}
		release()
		if err != nil {
			return false, err
		}
		return false, nil
	case err != sql.ErrNoRows:
		return false, err
	}

	defer d.blobs.hold()()
	details, err = d.compactProfileSnapshot(details)
	if err != nil {
		return false, err
	}
	_, err = d.exec(`
		INSERT INTO events (worker_id, event_type, message, details, created_at, occurrence_keys)
		VALUES (?, ?, ?, ?, ?, ?)`,
		workerID, eventType, message, details, createdAt, key,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}

// eventOccurrenceKey identifies one source occurrence of a windowed event by
// its own timestamp and payload, so a replayed log batch merges into the same
// row without counting again.
func eventOccurrenceKey(createdAt time.Time, details string) string {
	sum := sha256.Sum256([]byte(details))
	return createdAt.UTC().Format(time.RFC3339Nano) + "|" + hex.EncodeToString(sum[:8])
}

func (d *DB) ListEvents(limit int) ([]Event, error) {
	defer d.blobs.hold()()
	rows, err := d.query(`SELECT id, worker_id, event_type, message, details, created_at, occurrences FROM events ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	events := make([]Event, 0)
	for rows.Next() {
		var e Event
		var workerID sql.NullString
		if err := rows.Scan(&e.ID, &workerID, &e.EventType, &e.Message, &e.Details, &e.CreatedAt, &e.Occurrences); err != nil {
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
	_ = rows.Close()
	return d.expandEventDetails(events)
}

func (d *DB) ListWorkerEvents(workerID string, limit int) ([]Event, error) {
	defer d.blobs.hold()()
	rows, err := d.query(`
		SELECT id, worker_id, event_type, message, details, created_at, occurrences
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
		if err := rows.Scan(&e.ID, &eventWorkerID, &e.EventType, &e.Message, &e.Details, &e.CreatedAt, &e.Occurrences); err != nil {
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
	_ = rows.Close()
	return d.expandEventDetails(events)
}

func (d *DB) expandEventDetails(events []Event) ([]Event, error) {
	for i := range events {
		details, err := d.expandProfileSnapshot(events[i].Details)
		if err != nil {
			return nil, err
		}
		events[i].Details = details
	}
	return events, nil
}
