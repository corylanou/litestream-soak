package model

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
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
		if seenEventOccurrence(seen, key) || seenEventOccurrence(seen, eventOccurrenceKey(createdAt, compactDetails)) {
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
	key := createdAt.UTC().Format(time.RFC3339Nano) + "|" + hex.EncodeToString(sum[:8])
	if reporting.RestoreOutputDiskFullPath(details) {
		key += "|" + eventProvenanceRestoreOutput
	}
	return key
}

// eventProvenanceRestoreOutput tags an occurrence whose payload names an
// ENOSPC on the verifier's own restore output, so a coalesced row can say how
// many of its occurrences were harness capacity rather than real disk pressure.
const eventProvenanceRestoreOutput = "restore_output"

func sameEventOccurrence(a, b string) bool {
	return occurrenceIdentity(a) == occurrenceIdentity(b)
}

func occurrenceIdentity(key string) string {
	parts := strings.SplitN(key, "|", 3)
	if len(parts) < 2 {
		return key
	}
	return parts[0] + "|" + parts[1]
}

func seenEventOccurrence(seen []string, key string) bool {
	for _, existing := range seen {
		if sameEventOccurrence(existing, key) {
			return true
		}
	}
	return false
}

func restoreOutputOccurrences(keys string) int {
	count := 0
	for _, key := range strings.Split(keys, "\n") {
		if strings.HasSuffix(key, "|"+eventProvenanceRestoreOutput) {
			count++
		}
	}
	return count
}

func (d *DB) ListEvents(limit int) ([]Event, error) {
	defer d.blobs.hold()()
	rows, err := d.query(`SELECT id, worker_id, event_type, message, details, created_at, occurrences, occurrence_keys FROM events ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	events := make([]Event, 0)
	for rows.Next() {
		var e Event
		var workerID sql.NullString
		var keys string
		if err := rows.Scan(&e.ID, &workerID, &e.EventType, &e.Message, &e.Details, &e.CreatedAt, &e.Occurrences, &keys); err != nil {
			return nil, err
		}
		e.RestoreOutputOccurrences = restoreOutputOccurrences(keys)
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
		SELECT id, worker_id, event_type, message, details, created_at, occurrences, occurrence_keys
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
		var keys string
		if err := rows.Scan(&e.ID, &eventWorkerID, &e.EventType, &e.Message, &e.Details, &e.CreatedAt, &e.Occurrences, &keys); err != nil {
			return nil, err
		}
		e.RestoreOutputOccurrences = restoreOutputOccurrences(keys)
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
