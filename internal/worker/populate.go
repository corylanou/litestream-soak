package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"modernc.org/sqlite"
)

const (
	populateCheckpointWALThreshold int64 = 64 << 20
	// populateCheckpointWALCeiling is the hard bound: a WAL this large means
	// checkpoints have been failing, and populate is aborted rather than
	// allowed to fill the volume.
	populateCheckpointWALCeiling   int64 = 8 * populateCheckpointWALThreshold
	populateCheckpointPollInterval       = 100 * time.Millisecond
	populateCheckpointSpinLimit          = 10 * time.Second
	// populateCheckpointLockHold caps how long the backfill may hold the WAL
	// write lock. litestream-test populate opens SQLite through mattn/go-sqlite3,
	// whose default busy timeout is 5s, so the hold stays well under it.
	populateCheckpointLockHold    = 3 * time.Second
	populateCheckpointMaxFailures = 5
)

var errWALCheckpointBusy = errors.New("wal checkpoint could not complete: database busy")

// populateCheckpointer bounds the WAL while litestream-test populate runs.
// The workload binary is built with SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0 so
// that Litestream owns checkpointing in steady state, but Litestream is not
// running yet during populate: without this loop every page populate writes
// stays in the WAL until its final close, and an indexed seed can need several
// times the final database size in WAL alone. Peak disk during populate is
// therefore bounded by the database plus roughly one threshold of WAL plus
// whatever populate commits during one poll interval, and never more than the
// database plus the ceiling: past that, or after maxFailures consecutive
// failed checkpoints, run returns an error and the caller aborts populate.
type populateCheckpointer struct {
	dbPath      string
	threshold   int64
	ceiling     int64
	interval    time.Duration
	spinLimit   time.Duration
	lockHold    time.Duration
	maxFailures int

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
		dbPath:      dbPath,
		threshold:   populateCheckpointWALThreshold,
		ceiling:     populateCheckpointWALCeiling,
		interval:    populateCheckpointPollInterval,
		spinLimit:   populateCheckpointSpinLimit,
		lockHold:    populateCheckpointLockHold,
		maxFailures: populateCheckpointMaxFailures,
	}
}

// run checkpoints until ctx is cancelled (nil) or the bound cannot be held:
// the WAL reached the ceiling, or maxFailures checkpoints in a row failed.
// Either error means populate must be stopped before it fills the disk.
func (c *populateCheckpointer) run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	var conns *checkpointConns
	defer func() {
		if conns != nil {
			conns.close()
		}
	}()

	failures := 0
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		size := fileSizeOrZero(walPath(c.dbPath))
		c.observeWAL(size)
		if size >= c.ceiling {
			return fmt.Errorf("wal is %d bytes, at or above the %d byte ceiling, after %d consecutive failed checkpoints (last: %v)", size, c.ceiling, failures, lastErr)
		}
		if size < c.threshold {
			continue
		}
		truncated, err := c.checkpointOnce(ctx, &conns)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failures++
			lastErr = err
			slog.Warn("Populate checkpoint failed", "error", err, "consecutive_failures", failures, "wal_bytes", size)
			if failures >= c.maxFailures {
				return fmt.Errorf("%d consecutive wal checkpoints failed with %d bytes of wal (last: %w)", failures, size, err)
			}
			continue
		}
		failures = 0
		lastErr = nil
		c.recordCheckpoint(truncated)
	}
}

func (c *populateCheckpointer) checkpointOnce(ctx context.Context, conns **checkpointConns) (bool, error) {
	if *conns == nil {
		opened, err := openCheckpointConns(ctx, c.dbPath)
		if err != nil {
			return false, fmt.Errorf("open database: %w", err)
		}
		*conns = opened
	}
	return c.checkpoint(ctx, *conns)
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
// either way. The backfill holds the write lock for at most lockHold so slow
// storage cannot push populate past its own busy timeout; a backfill cut
// short is retried on the next pass with less left to copy.
func (c *populateCheckpointer) checkpoint(ctx context.Context, conns *checkpointConns) (bool, error) {
	if err := c.acquireWriteLock(ctx, conns.lock); err != nil {
		return false, err
	}
	holdCtx, releaseHold := context.WithTimeout(ctx, c.lockHold)
	backfillErr := walCheckpoint(holdCtx, conns.checkpoint, "PASSIVE")
	releaseHold()
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

// populateArgs pins the schema populate creates. litestream-test populate has
// no seed flag: it decides each index with math/rand and fills int_field with
// math/rand too, so an explicit -index-ratio is the only flag-level control
// over the schema, and only 0 or 1 remove the draw entirely.
func populateArgs(cfg Config) []string {
	return []string{
		"populate",
		"-db", cfg.DBPath,
		"-target-size", cfg.InitialSize,
		"-index-ratio", strconv.FormatFloat(cfg.PopulateIndexRatio, 'f', -1, 64),
	}
}

// populateEnv seeds populate's global math/rand deterministically. Since Go
// 1.20 the top-level math/rand functions seed themselves randomly unless
// GODEBUG=randautoseed=0, which restores the fixed Seed(1) sequence, so both
// gate sides make the same index draws and insert the same int_field keys.
// Row payloads (crypto/rand) and timestamps (wall clock) stay unique to each
// run; neither affects which indexes exist.
func populateEnv(env []string) []string {
	const setting = "randautoseed=0"
	for i, entry := range env {
		if value, ok := strings.CutPrefix(entry, "GODEBUG="); ok {
			if slices.Contains(strings.Split(value, ","), setting) {
				return env
			}
			if value == "" {
				env[i] = "GODEBUG=" + setting
			} else {
				env[i] = entry + "," + setting
			}
			return env
		}
	}
	return append(env, "GODEBUG="+setting)
}

// populateMarker is the durable record that populate finished and its final
// checkpoint folded the WAL into the database. Only its presence makes an
// existing database reusable on restart; a database without it is a populate
// that was cut short and is removed before populate runs again.
type populateMarker struct {
	CompletedAt time.Time `json:"completed_at"`
	WorkloadSHA string    `json:"workload_sha,omitempty"`
	TargetSize  string    `json:"target_size"`
	IndexRatio  float64   `json:"index_ratio"`
	Indexes     []string  `json:"indexes"`
	DBSizeBytes int64     `json:"db_size_bytes"`
	Adopted     bool      `json:"adopted,omitempty"`
	populateCheckpointStats
}

func populateMarkerPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "."+filepath.Base(dbPath)+"-populated.json")
}

func readPopulateMarker(dbPath string) (*populateMarker, error) {
	body, err := os.ReadFile(populateMarkerPath(dbPath))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read populate marker: %w", err)
	}
	var marker populateMarker
	if err := json.Unmarshal(body, &marker); err != nil {
		return nil, fmt.Errorf("decode populate marker: %w", err)
	}
	return &marker, nil
}

func writePopulateMarker(dbPath string, marker populateMarker) error {
	body, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(populateMarkerPath(dbPath), append(body, '\n')); err != nil {
		return fmt.Errorf("populate marker: %w", err)
	}
	return nil
}

// populateIndexes lists the indexes populate created, so the realized schema
// is recorded with the completion marker and can be compared across a gate.
func populateIndexes(ctx context.Context, dbPath string) ([]string, error) {
	db, err := openCheckpointDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	indexes := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		indexes = append(indexes, name)
	}
	return indexes, rows.Err()
}

// removePopulateFiles deletes a partial seed so a retry never builds on it
// and the disk it consumed is released before the next attempt.
func removePopulateFiles(dbPath string) error {
	var errs []error
	for _, path := range []string{dbPath, walPath(dbPath), dbPath + "-shm", populateMarkerPath(dbPath)} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// populateSingleDB seeds cfg.DBPath, or confirms an earlier seed completed.
//
// Restart cases: a database with a completion marker is reused. A database
// without one that already has Litestream state next to it predates the
// marker and had reached replication, so it is adopted and marked. Any other
// database without a marker is a populate that did not finish; it is removed
// and populate runs again from scratch.
func populateSingleDB(ctx context.Context, cfg Config) error {
	marker, err := readPopulateMarker(cfg.DBPath)
	if err != nil {
		return err
	}
	_, statErr := os.Stat(cfg.DBPath)
	dbExists := statErr == nil
	switch {
	case marker != nil && dbExists:
		slog.Info("Database already populated, skipping populate", "path", cfg.DBPath, "completed_at", marker.CompletedAt, "indexes", marker.Indexes)
		return nil
	case dbExists && legacyPopulatedDB(cfg.DBPath):
		indexes, err := populateIndexes(ctx, cfg.DBPath)
		if err != nil {
			return fmt.Errorf("inspect existing database: %w", err)
		}
		slog.Info("Adopting database that predates the populate marker", "path", cfg.DBPath, "indexes", indexes)
		return writePopulateMarker(cfg.DBPath, populateMarker{
			CompletedAt: time.Now().UTC(),
			WorkloadSHA: cfg.WorkloadSHA,
			TargetSize:  cfg.InitialSize,
			IndexRatio:  cfg.PopulateIndexRatio,
			Indexes:     indexes,
			DBSizeBytes: fileSizeOrZero(cfg.DBPath),
			Adopted:     true,
		})
	case dbExists || marker != nil:
		slog.Warn("Removing database left by an incomplete populate", "path", cfg.DBPath, "db_size_bytes", fileSizeOrZero(cfg.DBPath), "wal_size_bytes", fileSizeOrZero(walPath(cfg.DBPath)))
		if err := removePopulateFiles(cfg.DBPath); err != nil {
			return fmt.Errorf("remove incomplete database: %w", err)
		}
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	slog.Info("Populating database", "target_size", cfg.InitialSize, "index_ratio", cfg.PopulateIndexRatio)
	cmd := exec.CommandContext(ctx, "litestream-test", populateArgs(cfg)...)
	cmd.Env = populateEnv(os.Environ())
	stats, err := runPopulateCommand(ctx, cmd, cfg.DBPath, newPopulateCheckpointer(cfg.DBPath))
	if err != nil {
		if removeErr := removePopulateFiles(cfg.DBPath); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove incomplete database: %w", removeErr))
		}
		return err
	}
	indexes, err := populateIndexes(ctx, cfg.DBPath)
	if err != nil {
		return fmt.Errorf("inspect populated database: %w", err)
	}
	marker = &populateMarker{
		CompletedAt:             time.Now().UTC(),
		WorkloadSHA:             cfg.WorkloadSHA,
		TargetSize:              cfg.InitialSize,
		IndexRatio:              cfg.PopulateIndexRatio,
		Indexes:                 indexes,
		DBSizeBytes:             fileSizeOrZero(cfg.DBPath),
		populateCheckpointStats: stats,
	}
	if err := writePopulateMarker(cfg.DBPath, *marker); err != nil {
		return err
	}
	slog.Info("Database populated", "db_size_bytes", marker.DBSizeBytes, "indexes", indexes, "checkpoints", stats.Checkpoints, "truncations", stats.Truncations, "peak_wal_bytes", stats.PeakWALBytes)
	return nil
}

// legacyPopulatedDB reports whether a database without a completion marker
// was seeded by a harness build that predates the marker: such a database has
// Litestream's local state directory next to it, which only appears once
// Litestream has started against a finished seed.
func legacyPopulatedDB(dbPath string) bool {
	info, err := os.Stat(litestreamStateDir(dbPath))
	return err == nil && info.IsDir()
}

// runPopulateCommand runs litestream-test populate with a concurrent bounded
// checkpointer and folds the remaining WAL into the database afterwards. If
// the checkpointer cannot hold its bound, populate is killed and the error
// names the bound that was lost, so the worker reports a pre-start failure
// instead of filling the volume.
func runPopulateCommand(ctx context.Context, cmd *exec.Cmd, dbPath string, checkpointer *populateCheckpointer) (populateCheckpointStats, error) {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return populateCheckpointStats{}, fmt.Errorf("start populate: %w", err)
	}

	checkpointCtx, stopCheckpointer := context.WithCancel(ctx)
	defer stopCheckpointer()
	checkpointerDone := make(chan error, 1)
	go func() { checkpointerDone <- checkpointer.run(checkpointCtx) }()
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	var waitErr error
	select {
	case waitErr = <-waitDone:
		stopCheckpointer()
		<-checkpointerDone
	case checkpointErr := <-checkpointerDone:
		if checkpointErr != nil {
			slog.Error("Aborting populate: wal bound lost", "error", checkpointErr)
			_ = cmd.Process.Kill()
			<-waitDone
			return checkpointer.stats(), fmt.Errorf("aborted populate: %w", checkpointErr)
		}
		waitErr = <-waitDone
	}
	stats := checkpointer.stats()
	if waitErr != nil {
		return stats, waitErr
	}
	if err := finalizePopulateCheckpoint(ctx, dbPath); err != nil {
		return stats, fmt.Errorf("final checkpoint: %w", err)
	}
	return stats, nil
}
