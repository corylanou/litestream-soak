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
	// populateCheckpointLockSlice bounds one attempt at the WAL write lock.
	// Every frame populate commits while the checkpointer waits is a frame the
	// TRUNCATE must copy while holding the lock, so after a slice the WAL is
	// backfilled again without the lock before the next attempt.
	populateCheckpointLockSlice = 25 * time.Millisecond
	// populateCheckpointResidualFrames is how many frames may still be
	// un-backfilled when the write lock is taken (256 pages of 4 KiB is 1 MiB),
	// and populateCheckpointResidualCap is the most the TRUNCATE may ever have
	// to copy under the lock when the lock-free passes cannot converge.
	populateCheckpointResidualFrames int64 = 256
	populateCheckpointResidualCap    int64 = 4096
	populateCheckpointMaxPasses            = 32
	populateCheckpointMaxFailures          = 5
)

var (
	errWALCheckpointBusy = errors.New("wal checkpoint could not complete: database busy")
	errWALCeiling        = errors.New("wal reached its ceiling")
)

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
	lockSlice   time.Duration
	maxFailures int
	// afterBackfillPass, when set, runs after each lock-free PASSIVE pass.
	// Tests use it to inject slow storage.
	afterBackfillPass func(pass int)

	mu          sync.Mutex
	checkpoints int
	truncations int
	peakWAL     int64
	maxLockHold time.Duration
}

type populateCheckpointStats struct {
	Checkpoints   int   `json:"checkpoints"`
	Truncations   int   `json:"truncations"`
	PeakWALBytes  int64 `json:"peak_wal_bytes"`
	MaxLockHoldMS int64 `json:"max_lock_hold_ms"`
}

func newPopulateCheckpointer(dbPath string) *populateCheckpointer {
	return &populateCheckpointer{
		dbPath:      dbPath,
		threshold:   populateCheckpointWALThreshold,
		ceiling:     populateCheckpointWALCeiling,
		interval:    populateCheckpointPollInterval,
		spinLimit:   populateCheckpointSpinLimit,
		lockSlice:   populateCheckpointLockSlice,
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
		if err := c.checkCeiling(size); err != nil {
			return fmt.Errorf("%w after %d consecutive failed checkpoints (last: %v)", err, failures, lastErr)
		}
		if size < c.threshold {
			continue
		}
		truncated, err := c.checkpointOnce(ctx, &conns)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, errWALCeiling) {
				return fmt.Errorf("%w while waiting for the write lock, after %d consecutive failed checkpoints (last: %v)", err, failures, lastErr)
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

// checkpoint forces one bounded checkpoint while populate keeps writing,
// without ever holding the WAL write lock for longer than a small, bounded
// copy. populate opens SQLite through mattn/go-sqlite3 with a 5s busy
// timeout; a hold that long fails populate with "database is locked".
//
// All the expensive work happens without the write lock: PASSIVE passes
// backfill committed frames into the database while populate keeps writing,
// repeated until only a small residual is left. populate holds the write lock
// for nearly its whole run, releasing it only for microseconds between
// batches, and a checkpoint that waits on SQLite's busy handler (millisecond
// sleeps) starves, so the lock connection spins on BEGIN IMMEDIATE, which
// costs no I/O, for one short slice; if the slice passes, the frames populate
// committed meanwhile are backfilled again before the next slice. Once the
// lock is won, populate is parked in its own busy handler, the lock is
// released at once, and TRUNCATE runs immediately: it copies only the residual
// (at most residualCap frames), fsyncs, and truncates, so the hold is the cost
// of that small copy. If TRUNCATE loses the lock race, populate's next
// transaction finds a backfilled WAL and the next cycle retries.
func (c *populateCheckpointer) checkpoint(ctx context.Context, conns *checkpointConns) (bool, error) {
	deadline := time.Now().Add(c.spinLimit)
	for {
		if err := c.backfill(ctx, conns.checkpoint); err != nil {
			return false, err
		}
		// TRUNCATE fsyncs the WAL before it copies and the database after.
		// populate never fsyncs (synchronous=NORMAL), so after a catch-up the
		// kernel can hold hundreds of megabytes of dirty WAL pages, and an fsync
		// under the lock would wait for all of them. Drain both files here,
		// without the lock, so the fsyncs under the lock see only the residual.
		if err := syncFiles(c.dbPath, walPath(c.dbPath)); err != nil {
			return false, err
		}
		acquired, err := c.acquireWriteLock(ctx, conns.lock)
		if err != nil {
			return false, err
		}
		if acquired {
			break
		}
		if time.Now().After(deadline) {
			return false, fmt.Errorf("wal write lock starved for %s", c.spinLimit)
		}
	}

	held := time.Now()
	if _, err := conns.lock.ExecContext(ctx, "COMMIT"); err != nil {
		_, _ = conns.lock.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return false, fmt.Errorf("release wal write lock: %w", err)
	}
	truncateErr := walCheckpoint(ctx, conns.checkpoint, "TRUNCATE")
	c.observeLockHold(time.Since(held))
	switch {
	case truncateErr == nil:
		return true, nil
	case errors.Is(truncateErr, errWALCheckpointBusy):
		return false, nil
	default:
		return false, fmt.Errorf("truncate wal: %w", truncateErr)
	}
}

// backfill runs lock-free PASSIVE checkpoints until the un-backfilled residual
// is small enough for the TRUNCATE to copy quickly under the lock. If populate
// outpaces the passes it still proceeds when the residual is under the cap,
// and otherwise reports a failure so a storage device that cannot keep up
// surfaces as an aborted populate rather than a long lock hold.
func (c *populateCheckpointer) backfill(ctx context.Context, conn walCheckpointer) error {
	var residual int64
	for pass := 1; pass <= populateCheckpointMaxPasses; pass++ {
		logFrames, checkpointed, err := walCheckpointFrames(ctx, conn, "PASSIVE")
		if err != nil {
			return fmt.Errorf("backfill wal: %w", err)
		}
		if c.afterBackfillPass != nil {
			c.afterBackfillPass(pass)
		}
		if err := c.checkCeiling(fileSizeOrZero(walPath(c.dbPath))); err != nil {
			return err
		}
		residual = logFrames - checkpointed
		if residual <= populateCheckpointResidualFrames {
			return nil
		}
	}
	if residual <= populateCheckpointResidualCap {
		return nil
	}
	return fmt.Errorf("backfill cannot keep up with populate: %d frames still un-backfilled after %d passes", residual, populateCheckpointMaxPasses)
}

// acquireWriteLock spins on BEGIN IMMEDIATE for one lock slice. It reports
// false when the slice passes without the lock, so the caller backfills again
// before retrying; the ceiling is checked on every spin.
func (c *populateCheckpointer) acquireWriteLock(ctx context.Context, lock *sql.Conn) (bool, error) {
	deadline := time.Now().Add(c.lockSlice)
	for {
		_, err := lock.ExecContext(ctx, "BEGIN IMMEDIATE")
		if err == nil {
			return true, nil
		}
		if !isSQLiteBusy(err) {
			return false, fmt.Errorf("acquire wal write lock: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		if err := c.checkCeiling(fileSizeOrZero(walPath(c.dbPath))); err != nil {
			return false, err
		}
		runtime.Gosched()
	}
}

func syncFiles(paths ...string) error {
	for _, path := range paths {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("open %s for sync: %w", filepath.Base(path), err)
		}
		syncErr := f.Sync()
		closeErr := f.Close()
		if err := errors.Join(syncErr, closeErr); err != nil {
			return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
		}
	}
	return nil
}

func (c *populateCheckpointer) observeLockHold(hold time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hold > c.maxLockHold {
		c.maxLockHold = hold
	}
}

// checkCeiling is the hard bound. It runs on every poll and on every spin of
// the write-lock wait, the only window in which populate keeps appending while
// the checkpointer is busy, so a breach is caught while it happens rather than
// hidden by a checkpoint that later succeeds.
func (c *populateCheckpointer) checkCeiling(size int64) error {
	c.observeWAL(size)
	if size >= c.ceiling {
		return fmt.Errorf("%w: wal is %d bytes, at or above the %d byte ceiling", errWALCeiling, size, c.ceiling)
	}
	return nil
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
	return populateCheckpointStats{Checkpoints: c.checkpoints, Truncations: c.truncations, PeakWALBytes: c.peakWAL, MaxLockHoldMS: c.maxLockHold.Milliseconds()}
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
	_, _, err := walCheckpointFrames(ctx, db, mode)
	return err
}

// walCheckpointFrames runs PRAGMA wal_checkpoint(mode) and returns the frame
// count in the WAL and how many of them are now backfilled, so a caller can
// size the residual. PASSIVE reports busy only when readers pinned frames; the
// counts are still meaningful then, so busy is returned alongside them.
func walCheckpointFrames(ctx context.Context, db walCheckpointer, mode string) (logFrames, checkpointed int64, err error) {
	var busy int64
	if err := db.QueryRowContext(ctx, "PRAGMA wal_checkpoint("+mode+")").Scan(&busy, &logFrames, &checkpointed); err != nil {
		return 0, 0, err
	}
	if busy != 0 && mode != "PASSIVE" {
		return logFrames, checkpointed, errWALCheckpointBusy
	}
	return max(logFrames, 0), max(checkpointed, 0), nil
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

// populateAttempts is how many times the worker runs populate before it gives
// up. One retry absorbs a transient failure; a deterministic one (no disk, a
// lost WAL bound) fails both attempts and is then reported exactly once.
const populateAttempts = 2

// PopulateFailure is the error Run returns when populate failed on every
// attempt. The failure has already been reported as a worker_failed event, and
// retrying from a fresh process would only repeat it, so the worker process
// exits cleanly on it instead of letting the platform restart it ten times.
type PopulateFailure struct {
	err error
}

func (e *PopulateFailure) Error() string { return e.err.Error() }

func (e *PopulateFailure) Unwrap() error { return e.err }

// IsPopulateFailure reports whether err is a reported, final populate failure.
func IsPopulateFailure(err error) bool {
	var failure *PopulateFailure
	return errors.As(err, &failure)
}

// populateSingleDBWithRetry runs populateSingleDB up to attempts times. Each
// failed attempt has already removed its partial seed, so a retry starts
// clean; the returned error carries every attempt's failure.
func populateSingleDBWithRetry(ctx context.Context, cfg Config, attempts int) error {
	var errs []error
	for attempt := 1; attempt <= attempts; attempt++ {
		err := populateSingleDB(ctx, cfg)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("attempt %d of %d: %w", attempt, attempts, err))
		if ctx.Err() != nil || attempt == attempts {
			break
		}
		slog.Warn("Populate failed; retrying once from a clean database", "attempt", attempt, "error", err)
	}
	return errors.Join(errs...)
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
