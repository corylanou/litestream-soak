package model

import (
	"database/sql"
	"errors"
	"time"
)

func (d *DB) GetComparisonSnapshot(key string) (string, time.Time, bool, error) {
	var body string
	var computedAt time.Time
	err := d.queryRow(`SELECT body, computed_at FROM comparison_snapshots WHERE cache_key = ?`, key).Scan(&body, &computedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, false, nil
	}
	if err != nil {
		return "", time.Time{}, false, err
	}
	return body, computedAt, true, nil
}

func (d *DB) PutComparisonSnapshot(key, body string, computedAt time.Time, keepAfter time.Time) error {
	if _, err := d.exec(`INSERT INTO comparison_snapshots (cache_key, computed_at, body) VALUES (?, ?, ?)
		ON CONFLICT(cache_key) DO UPDATE SET computed_at = excluded.computed_at, body = excluded.body`, key, computedAt.UTC(), body); err != nil {
		return err
	}
	_, err := d.exec(`DELETE FROM comparison_snapshots WHERE computed_at < ?`, keepAfter.UTC())
	return err
}
