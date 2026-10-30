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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	// populateBinary is litestream-test built from the pinned WORKLOAD_SHA
	// without SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0, so SQLite's own automatic
	// checkpoint runs inside populate's connection after each commit once the
	// WAL passes 1000 pages. The load binary keeps that flag because Litestream
	// owns checkpointing in steady state; populate runs before Litestream
	// exists, so it needs SQLite to bound the WAL itself. No second writer or
	// checkpointer touches the database while populate runs.
	populateBinary = "litestream-test-populate"
	// populateFallbackBinary is used only when populateBinary is not on PATH
	// (local rigs built from the Makefile); its WAL is unbounded, and the
	// ceiling watcher is then the only bound.
	populateFallbackBinary = "litestream-test"
	// populateWALCeiling is the hard bound an independent watcher enforces: if
	// the WAL reaches it while populate runs, populate is killed and the seed
	// discarded rather than letting the volume fill. SQLite's autocheckpoint
	// keeps a healthy populate's WAL near 4 MiB.
	populateWALCeiling    int64 = 512 << 20
	populateWatchInterval       = 100 * time.Millisecond
	// populateAttempts is how many times the worker runs populate before it
	// gives up. One retry absorbs a transient failure; a deterministic one (no
	// disk, a WAL past the ceiling) fails both attempts and is reported once.
	populateAttempts = 2
)

var errWALCheckpointBusy = errors.New("wal checkpoint could not complete: database busy")

// PopulateFailure is the error Run returns when the single-database initial
// seed failed on every attempt without being cancelled. The failure has been
// reported as a worker_failed event, and a fresh process would only repeat it,
// so the worker exits cleanly on it instead of letting the platform restart it
// ten times. Other populate paths (many-db, churn, FTS) return plain errors and
// keep the normal restart-on-failure behaviour.
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

type populateStats struct {
	Binary       string `json:"binary"`
	PeakWALBytes int64  `json:"peak_wal_bytes"`
}

// populateCommand resolves the populate binary. It prefers the autocheckpoint
// build and falls back to the load binary with a warning so local rigs that
// only built litestream-test still work.
func populateCommand() string {
	if _, err := exec.LookPath(populateBinary); err == nil {
		return populateBinary
	}
	slog.Warn("Populate binary with WAL autocheckpoint not found; falling back to the load binary, whose WAL is bounded only by the ceiling watcher", "binary", populateBinary, "fallback", populateFallbackBinary)
	return populateFallbackBinary
}

// walWatcher samples the WAL while populate runs. It is the only concurrent
// observer of the database and never opens it: it stats the WAL file, records
// the peak, and reports a breach of the ceiling so the caller can kill
// populate. Sampling never blocks on database I/O, so the ceiling is enforced
// even while SQLite is inside a slow checkpoint.
type walWatcher struct {
	dbPath   string
	ceiling  int64
	interval time.Duration

	mu      sync.Mutex
	peak    int64
	breach  int64
	breachC chan struct{}
	once    sync.Once
}

func newWALWatcher(dbPath string, ceiling int64) *walWatcher {
	return &walWatcher{dbPath: dbPath, ceiling: ceiling, interval: populateWatchInterval, breachC: make(chan struct{})}
}

// run samples until ctx is cancelled. The first sample at or above the
// ceiling closes breached() and is recorded; sampling continues so the peak
// stays accurate.
func (w *walWatcher) run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.observe(fileSizeOrZero(walPath(w.dbPath)))
		}
	}
}

func (w *walWatcher) observe(size int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if size > w.peak {
		w.peak = size
	}
	if size >= w.ceiling {
		w.breach = size
		w.once.Do(func() { close(w.breachC) })
	}
}

func (w *walWatcher) breached() <-chan struct{} { return w.breachC }

func (w *walWatcher) stats() (peak, breach int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.peak, w.breach
}

// runPopulateCommand runs populate alone, with only the WAL watcher beside it,
// and folds whatever WAL is left into the database once populate has exited
// and nothing is writing. A ceiling breach kills populate and returns an
// aborted populate error so the worker reports a pre-start failure instead of
// filling the volume.
func runPopulateCommand(ctx context.Context, cmd *exec.Cmd, dbPath string, watcher *walWatcher) (populateStats, error) {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return populateStats{}, fmt.Errorf("start populate: %w", err)
	}

	watchCtx, stopWatcher := context.WithCancel(ctx)
	defer stopWatcher()
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		watcher.run(watchCtx)
	}()
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	var waitErr error
	select {
	case waitErr = <-waitDone:
	case <-watcher.breached():
		_, breach := watcher.stats()
		slog.Error("Aborting populate: wal reached its ceiling", "wal_bytes", breach, "ceiling_bytes", watcher.ceiling)
		_ = cmd.Process.Kill()
		<-waitDone
		stopWatcher()
		<-watcherDone
		peak, _ := watcher.stats()
		return populateStats{Binary: cmd.Path, PeakWALBytes: peak}, fmt.Errorf("aborted populate: wal is %d bytes, at or above the %d byte ceiling", breach, watcher.ceiling)
	}
	stopWatcher()
	<-watcherDone
	peak, _ := watcher.stats()
	stats := populateStats{Binary: cmd.Path, PeakWALBytes: peak}
	if waitErr != nil {
		return stats, waitErr
	}
	if err := finalizePopulateCheckpoint(ctx, dbPath); err != nil {
		return stats, fmt.Errorf("final checkpoint: %w", err)
	}
	return stats, nil
}

func openCheckpointDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

type walCheckpointer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// walCheckpoint runs PRAGMA wal_checkpoint(mode) and reports busy when SQLite
// could not complete the requested mode because another connection held a
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
// Litestream starts and the completion marker is written. Nothing else is
// writing at this point, so the TRUNCATE never contends with populate.
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

// populateSingleDBWithRetry runs populateSingleDB up to attempts times. Each
// failed attempt has already removed its partial seed, so a retry starts
// clean. When every attempt fails and the run was not cancelled, the joined
// error is returned as a PopulateFailure so the process stops instead of
// restarting; a cancelled run returns the plain error.
func populateSingleDBWithRetry(ctx context.Context, cfg Config, attempts int) error {
	var errs []error
	for attempt := 1; attempt <= attempts; attempt++ {
		err := populateSingleDB(ctx, cfg)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("attempt %d of %d: %w", attempt, attempts, err))
		if ctx.Err() != nil {
			return errors.Join(errs...)
		}
		if attempt < attempts {
			slog.Warn("Populate failed; retrying once from a clean database", "attempt", attempt, "error", err)
		}
	}
	return &PopulateFailure{err: errors.Join(errs...)}
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
	populateStats
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

	binary := populateCommand()
	slog.Info("Populating database", "binary", binary, "target_size", cfg.InitialSize, "index_ratio", cfg.PopulateIndexRatio)
	cmd := exec.CommandContext(ctx, binary, populateArgs(cfg)...)
	cmd.Env = populateEnv(os.Environ())
	stats, err := runPopulateCommand(ctx, cmd, cfg.DBPath, newWALWatcher(cfg.DBPath, populateWALCeiling))
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
		CompletedAt:   time.Now().UTC(),
		WorkloadSHA:   cfg.WorkloadSHA,
		TargetSize:    cfg.InitialSize,
		IndexRatio:    cfg.PopulateIndexRatio,
		Indexes:       indexes,
		DBSizeBytes:   fileSizeOrZero(cfg.DBPath),
		populateStats: stats,
	}
	if err := writePopulateMarker(cfg.DBPath, *marker); err != nil {
		return err
	}
	slog.Info("Database populated", "binary", stats.Binary, "db_size_bytes", marker.DBSizeBytes, "indexes", indexes, "peak_wal_bytes", stats.PeakWALBytes)
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
