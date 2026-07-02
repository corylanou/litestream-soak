package model

import "time"

// PruneVerificationsBefore deletes verification rows that started before the
// cutoff and returns how many were removed. Verification history older than
// every UI window (48h stats, 7d ticks) only grows the database.
func (d *DB) PruneVerificationsBefore(cutoff time.Time) (int64, error) {
	result, err := d.exec(`DELETE FROM verifications WHERE started_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PruneEventsBefore deletes event rows created before the cutoff and returns
// how many were removed.
func (d *DB) PruneEventsBefore(cutoff time.Time) (int64, error) {
	result, err := d.exec(`DELETE FROM events WHERE created_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// CheckpointWAL truncates the write-ahead log so space freed by pruning does
// not linger in the -wal file. The main database file is deliberately not
// VACUUMed: that requires free disk roughly equal to the database size, and
// freed pages are reused by future writes anyway.
func (d *DB) CheckpointWAL() error {
	_, err := d.exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}
