package model

import (
	"database/sql"
	"time"
)

const meteringWindowSchema = `
CREATE TABLE IF NOT EXISTS metering_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    started_at DATETIME NOT NULL,
    ended_at DATETIME
);
CREATE INDEX IF NOT EXISTS idx_metering_windows_source ON metering_windows(source, started_at);
`

type MeteringWindow struct {
	Source    string     `json:"source"`
	Active    bool       `json:"active"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

func (d *DB) SyncMeteringWindows(sources []string, now time.Time) error {
	now = now.UTC()
	listed := make(map[string]bool, len(sources))
	for _, source := range sources {
		listed[source] = true
	}
	tx, err := d.writer.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query(`SELECT id, source FROM metering_windows WHERE ended_at IS NULL`)
	if err != nil {
		return err
	}
	open := make(map[string]bool)
	var stale []int64
	for rows.Next() {
		var id int64
		var source string
		if err := rows.Scan(&id, &source); err != nil {
			_ = rows.Close()
			return err
		}
		if listed[source] {
			open[source] = true
			continue
		}
		stale = append(stale, id)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, id := range stale {
		if _, err := tx.Exec(`UPDATE metering_windows SET ended_at = ? WHERE id = ?`, now, id); err != nil {
			return err
		}
	}
	for source := range listed {
		if open[source] {
			continue
		}
		if _, err := tx.Exec(`INSERT INTO metering_windows (source, started_at) VALUES (?, ?)`, source, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) ListMeteringWindows(source string) ([]MeteringWindow, error) {
	query := `SELECT source, started_at, ended_at FROM metering_windows`
	var args []any
	if source != "" {
		query += ` WHERE source = ?`
		args = append(args, source)
	}
	query += ` ORDER BY source, started_at`
	rows, err := d.query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var windows []MeteringWindow
	for rows.Next() {
		var window MeteringWindow
		var endedAt sql.NullTime
		if err := rows.Scan(&window.Source, &window.StartedAt, &endedAt); err != nil {
			return nil, err
		}
		window.StartedAt = window.StartedAt.UTC()
		if endedAt.Valid {
			ended := endedAt.Time.UTC()
			window.EndedAt = &ended
		}
		window.Active = window.EndedAt == nil
		windows = append(windows, window)
	}
	return windows, rows.Err()
}
