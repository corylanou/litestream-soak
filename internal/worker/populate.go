package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"modernc.org/sqlite"
)

const (
	populateCheckpointWALThreshold int64 = 64 << 20
	populateCheckpointPollInterval       = 100 * time.Millisecond
	populateCheckpointSpinLimit          = 10 * time.Second
)

var errWALCheckpointBusy = errors.New("wal checkpoint could not complete: database busy")

// populateCheckpointer bounds the WAL while litestream-test populate runs.
// The workload binary is built with SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0 so
// that Litestream owns checkpointing in steady state, but Litestream is not
// running yet during populate: without this loop every page populate writes
// stays in the WAL until its final close, and an indexed seed can need several
// times the final database size in WAL alone. Peak disk during populate is
// therefore bounded by the database plus roughly one threshold of WAL plus
// whatever populate commits during one poll interval.
type populateCheckpointer struct {
	dbPath    string
	threshold int64
	interval  time.Duration
	spinLimit time.Duration

	mu          sync.Mutex
	checkpoints int
	truncations int
	peakWAL     int64
}

type populateCheckpointStats struct {
	Checkpoints  int   `json:"checkpoints"`
	Truncations  int   `json:"truncations"`
	PeakWALBytes int64 `json:"peak_wal_bytes"`
}

func newPopulateCheckpointer(dbPath string) *populateCheckpointer {
	return &populateCheckpointer{
		dbPath:    dbPath,
		threshold: populateCheckpointWALThreshold,
		interval:  populateCheckpointPollInterval,
		spinLimit: populateCheckpointSpinLimit,
	}
}

func (c *populateCheckpointer) run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	var conns *checkpointConns
	defer func() {
		if conns != nil {
			conns.close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		size := fileSizeOrZero(walPath(c.dbPath))
		c.observeWAL(size)
		if size < c.threshold {
			continue
		}
		if conns == nil {
			opened, err := openCheckpointConns(ctx, c.dbPath)
			if err != nil {
				slog.Warn("Populate checkpointer could not open database", "error", err)
				continue
			}
			conns = opened
		}
		truncated, err := c.checkpoint(ctx, conns)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("Populate checkpoint failed", "error", err)
			}
			continue
		}
		c.recordCheckpoint(truncated)
	}
}

// checkpoint forces one bounded checkpoint while populate keeps writing.
//
// populate holds the WAL write lock for nearly its whole run, releasing it
// only for microseconds between batches, and a checkpoint that waits on
// SQLite's busy handler (millisecond sleeps between retries) starves. The
// lock connection therefore has no busy timeout and spins on BEGIN IMMEDIATE,
// which costs no I/O per attempt, until it lands in that gap. With populate
// held off on its own busy handler, the checkpoint connection backfills every
// committed frame, the lock is released, and TRUNCATE is attempted at once.
// populate is asleep in its busy handler at that moment so the truncate almost
// always wins; if it loses, populate's next transaction finds the WAL fully
// backfilled and restarts it from frame zero anyway, so the file is bounded
// either way.
func (c *populateCheckpointer) checkpoint(ctx context.Context, conns *checkpointConns) (bool, error) {
	if err := c.acquireWriteLock(ctx, conns.lock); err != nil {
		return false, err
	}
	backfillErr := walCheckpoint(ctx, conns.checkpoint, "PASSIVE")
	if _, err := conns.lock.ExecContext(ctx, "COMMIT"); err != nil {
		_, _ = conns.lock.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return false, fmt.Errorf("release wal write lock: %w", err)
	}
	if backfillErr != nil {
		return false, fmt.Errorf("backfill wal: %w", backfillErr)
	}
	switch err := walCheckpoint(ctx, conns.checkpoint, "TRUNCATE"); {
	case err == nil:
		return true, nil
	case errors.Is(err, errWALCheckpointBusy):
		return false, nil
	default:
		return false, fmt.Errorf("truncate wal: %w", err)
	}
}

func (c *populateCheckpointer) acquireWriteLock(ctx context.Context, lock *sql.Conn) error {
	deadline := time.Now().Add(c.spinLimit)
	for {
		_, err := lock.ExecContext(ctx, "BEGIN IMMEDIATE")
		if err == nil {
			return nil
		}
		if !isSQLiteBusy(err) {
			return fmt.Errorf("acquire wal write lock: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("wal write lock starved for %s", c.spinLimit)
		}
		runtime.Gosched()
	}
}

func (c *populateCheckpointer) observeWAL(size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if size > c.peakWAL {
		c.peakWAL = size
	}
}

func (c *populateCheckpointer) recordCheckpoint(truncated bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpoints++
	if truncated {
		c.truncations++
	}
}

func (c *populateCheckpointer) stats() populateCheckpointStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return populateCheckpointStats{Checkpoints: c.checkpoints, Truncations: c.truncations, PeakWALBytes: c.peakWAL}
}

// checkpointConns holds the two connections the checkpointer needs: SQLite
// refuses to checkpoint on a connection with an open transaction, so the write
// lock is held on one connection while the other checkpoints.
type checkpointConns struct {
	db         *sql.DB
	lock       *sql.Conn
	checkpoint *sql.Conn
}

func openCheckpointConns(ctx context.Context, dbPath string) (*checkpointConns, error) {
	db, err := openCheckpointDB(dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	conns := &checkpointConns{db: db}
	if conns.lock, err = db.Conn(ctx); err != nil {
		conns.close()
		return nil, err
	}
	if conns.checkpoint, err = db.Conn(ctx); err != nil {
		conns.close()
		return nil, err
	}
	return conns, nil
}

func (c *checkpointConns) close() {
	if c.lock != nil {
		_ = c.lock.Close()
	}
	if c.checkpoint != nil {
		_ = c.checkpoint.Close()
	}
	_ = c.db.Close()
}

func openCheckpointDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(0)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func isSQLiteBusy(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	code := sqliteErr.Code() & 0xff
	return code == 5 || code == 6
}

type walCheckpointer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// walCheckpoint runs PRAGMA wal_checkpoint(mode) and reports busy when SQLite
// could not complete the requested mode because another connection held the
// lock it needed.
func walCheckpoint(ctx context.Context, db walCheckpointer, mode string) error {
	var busy, logFrames, checkpointed int64
	if err := db.QueryRowContext(ctx, "PRAGMA wal_checkpoint("+mode+")").Scan(&busy, &logFrames, &checkpointed); err != nil {
		return err
	}
	if busy != 0 {
		return errWALCheckpointBusy
	}
	return nil
}

// finalizePopulateCheckpoint folds whatever populate left in the WAL into the
// database once populate has exited, so the seed is a single file before
// Litestream starts and the completion marker is written.
func finalizePopulateCheckpoint(ctx context.Context, dbPath string) error {
	db, err := openCheckpointDB(dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := walCheckpoint(ctx, db, "TRUNCATE"); err != nil {
		return err
	}
	if size := fileSizeOrZero(walPath(dbPath)); size != 0 {
		return fmt.Errorf("wal still holds %d bytes after final checkpoint", size)
	}
	return nil
}

func walPath(dbPath string) string {
	return dbPath + "-wal"
}

func fileSizeOrZero(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// runPopulateCommand runs litestream-test populate with a concurrent bounded
// checkpointer and folds the remaining WAL into the database afterwards.
func runPopulateCommand(ctx context.Context, cmd *exec.Cmd, dbPath string) (populateCheckpointStats, error) {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return populateCheckpointStats{}, fmt.Errorf("start populate: %w", err)
	}

	checkpointer := newPopulateCheckpointer(dbPath)
	checkpointCtx, stopCheckpointer := context.WithCancel(ctx)
	checkpointerDone := make(chan struct{})
	go func() {
		defer close(checkpointerDone)
		checkpointer.run(checkpointCtx)
	}()

	waitErr := cmd.Wait()
	stopCheckpointer()
	<-checkpointerDone
	stats := checkpointer.stats()
	if waitErr != nil {
		return stats, waitErr
	}
	if err := finalizePopulateCheckpoint(ctx, dbPath); err != nil {
		return stats, fmt.Errorf("final checkpoint: %w", err)
	}
	return stats, nil
}
