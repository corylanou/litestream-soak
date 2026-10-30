package model

import (
	"database/sql"
	"errors"
	"time"
)

// CountWorkersWithEventBetween reports how many distinct workers recorded the
// event type inside [start, end]. The gate scoring uses it to recognise a
// control-plane restart, which marks many workers stale in the same minute.
func (d *DB) CountWorkersWithEventBetween(eventType string, start, end time.Time) (int, error) {
	var count int
	err := d.queryRow(`
		SELECT COUNT(DISTINCT worker_id)
		FROM events
		WHERE event_type = ? AND worker_id IS NOT NULL AND created_at >= ? AND created_at <= ?`,
		eventType, start.UTC(), end.UTC(),
	).Scan(&count)
	return count, err
}

// FirstHeartbeatAfter returns when the worker's first heartbeat after the given
// time reached the control plane, read from the runtime evidence journal.
func (d *DB) FirstHeartbeatAfter(source, workerID string, after time.Time) (time.Time, bool, error) {
	var receivedAt time.Time
	err := d.queryRow(`
		SELECT received_at
		FROM evidence_runtime
		WHERE source = ? AND kind = 'heartbeat' AND julianday(received_at) > julianday(?)
			AND json_extract(identity_json, '$.worker_id') = ?
		ORDER BY julianday(received_at) ASC
		LIMIT 1`,
		source, after.UTC(), workerID,
	).Scan(&receivedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return time.Time{}, false, nil
	case err != nil:
		return time.Time{}, false, err
	}
	return receivedAt.UTC(), true, nil
}
