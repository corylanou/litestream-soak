package model

import (
	"context"
	"time"
)

// pruneBatchSize bounds each retention DELETE so heartbeat and report writes
// interleave between batches on the single writer connection (worker clients
// give up after 5s) and so the WAL grows a bounded amount per transaction.
// Var, not const, so tests can shrink it.
var pruneBatchSize = 5000

// PruneVerificationsBefore deletes verification rows that started before the
// cutoff, in batches, and returns how many were removed. Verification history
// older than every UI window (48h stats, 7d ticks) only grows the database.
func (d *DB) PruneVerificationsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	return d.pruneBatched(ctx,
		`DELETE FROM verifications WHERE id IN (
			SELECT id FROM verifications WHERE started_at < ? LIMIT ?)`,
		cutoff)
}

// PruneEventsBefore deletes event rows created before the cutoff, in batches,
// and returns how many were removed.
func (d *DB) PruneEventsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	return d.pruneBatched(ctx,
		`DELETE FROM events WHERE id IN (
			SELECT id FROM events WHERE created_at < ? LIMIT ?)`,
		cutoff)
}

func (d *DB) pruneBatched(ctx context.Context, query string, cutoff time.Time) (int64, error) {
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		result, err := d.exec(query, cutoff, pruneBatchSize)
		if err != nil {
			return total, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return total, err
		}
		total += affected
		if affected < int64(pruneBatchSize) {
			return total, nil
		}
	}
}

// WALCheckpointResult reports what PRAGMA wal_checkpoint(TRUNCATE) achieved.
// Busy is non-zero when concurrent readers prevented the truncate — safe, but
// worth surfacing so a WAL that never shrinks is visible.
type WALCheckpointResult struct {
	Busy         int
	LogFrames    int
	Checkpointed int
}

// CheckpointWAL truncates the write-ahead log so space freed by pruning does
// not linger in the -wal file. The main database file is deliberately not
// VACUUMed: that requires free disk roughly equal to the database size, and
// freed pages are reused by future writes anyway.
func (d *DB) CheckpointWAL() (WALCheckpointResult, error) {
	var result WALCheckpointResult
	err := d.writer.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).
		Scan(&result.Busy, &result.LogFrames, &result.Checkpointed)
	return result, err
}
