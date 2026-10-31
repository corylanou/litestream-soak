package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	_ "modernc.org/sqlite"
)

type manyDBLoad struct {
	cfg *Config
	now func() time.Time

	cancel context.CancelFunc
	jobs   chan manyDBWrite
	wg     sync.WaitGroup
	next   atomic.Uint64

	changedMu        sync.Mutex
	changed          map[string]manyDBChange
	changeGeneration uint64
	lastAttempt      string
	stateMu          sync.Mutex
	stateDirty       bool
	saveState        func([]string) error
	writeRow         func(context.Context, string, int, int64) error
	onStateEvent     func(eventType, message string)

	mu      sync.Mutex
	paused  bool
	active  int
	stopped bool
}

type manyDBWrite struct {
	dbPath  string
	rowSlot int64
}

const manyDBPruneBatchSize = 10_000

func populateManyDBs(ctx context.Context, cfg Config) error {
	if err := os.MkdirAll(cfg.ManyDBDir(), 0755); err != nil {
		return fmt.Errorf("create many database dir: %w", err)
	}
	fresh := true
	for _, dbPath := range cfg.ManyDBPaths() {
		if _, err := os.Stat(dbPath); err == nil {
			fresh = false
			if err := pruneManyDBRows(ctx, dbPath, cfg.manyDBMaxRowsPerDatabase()); err != nil {
				return fmt.Errorf("prune many database %s: %w", dbPath, err)
			}
			continue
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("stat many database %s: %w", dbPath, err)
		}
		if err := seedManyDB(ctx, dbPath); err != nil {
			return fmt.Errorf("seed many database %s: %w", dbPath, err)
		}
	}
	if fresh {
		if err := writeManyDBWrittenState(cfg, nil); err != nil {
			return fmt.Errorf("record empty written set: %w", err)
		}
	}
	return nil
}

// manyDBWrittenStateFile records, on the data volume, the databases the
// workload wrote to that have not been verified since. A worker restart
// builds a new tracker, and without this file every one of those writes
// would come back as seeded backlog and could leave a batch without ever
// being verified.
const manyDBWrittenStateFile = ".written-pending.json"

type manyDBWrittenState struct {
	Written []string `json:"written"`
}

func manyDBWrittenStatePath(cfg *Config) string {
	return filepath.Join(cfg.ManyDBDir(), manyDBWrittenStateFile)
}

func writeManyDBWrittenState(cfg Config, written []string) error {
	body, err := json.Marshal(manyDBWrittenState{Written: written})
	if err != nil {
		return err
	}
	return writeFileAtomic(manyDBWrittenStatePath(&cfg), body)
}

func readManyDBWrittenState(cfg *Config) ([]string, error) {
	body, err := os.ReadFile(manyDBWrittenStatePath(cfg))
	if err != nil {
		return nil, err
	}
	var state manyDBWrittenState
	if err := json.Unmarshal(body, &state); err != nil {
		return nil, fmt.Errorf("decode %s: %w", manyDBWrittenStatePath(cfg), err)
	}
	return state.Written, nil
}

// manyDBStateOnVolume reports whether any database already exists, which
// tells a restart apart from a fresh volume.
func manyDBStateOnVolume(cfg *Config) bool {
	for _, dbPath := range cfg.ManyDBPaths() {
		if _, err := os.Stat(dbPath); err == nil {
			return true
		}
	}
	return false
}

func pruneManyDBRows(ctx context.Context, dbPath string, maxRows int) error {
	if maxRows <= 0 {
		return fmt.Errorf("max rows must be positive")
	}
	db, err := sql.Open("sqlite", manyDBDSN(dbPath))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	for {
		result, err := db.ExecContext(ctx, `
			DELETE FROM soak_writes
			WHERE id IN (
				SELECT id FROM soak_writes
				WHERE id > ?
				ORDER BY id DESC
				LIMIT ?
			)
		`, maxRows, manyDBPruneBatchSize)
		if err != nil {
			return fmt.Errorf("delete rows above retention limit: %w", err)
		}
		rowsDeleted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read deleted row count: %w", err)
		}
		if rowsDeleted < manyDBPruneBatchSize {
			return nil
		}
	}
}

func seedManyDB(ctx context.Context, dbPath string) error {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return fmt.Errorf("create database parent: %w", err)
	}
	db, err := sql.Open("sqlite", manyDBDSN(dbPath))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("enable WAL: %w", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS soak_payloads (id INTEGER PRIMARY KEY, body BLOB NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"); err != nil {
		return fmt.Errorf("create payload table: %w", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO soak_payloads (body) VALUES (zeroblob(65536))"); err != nil {
		return fmt.Errorf("seed payload table: %w", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS soak_writes (id INTEGER PRIMARY KEY, body BLOB NOT NULL, written_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"); err != nil {
		return fmt.Errorf("create writes table: %w", err)
	}
	return nil
}

func manyDBDSN(dbPath string) string {
	return dbPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
}

// newManyDBLoad seeds every database into the pending set so each one is
// verified at least once; seeded entries are not written, so they are a
// backlog the verifier drains behind the databases the workload touched. The
// written set a previous run left on the volume is restored on top; when the
// volume holds databases but the written set cannot be read, every database
// counts as written until it is verified, so no write is lost to a restart.
func newManyDBLoad(cfg *Config) *manyDBLoad {
	load := &manyDBLoad{cfg: cfg, now: time.Now, changed: make(map[string]manyDBChange), writeRow: writeManyDBRow}
	load.saveState = func(written []string) error { return writeManyDBWrittenState(*cfg, written) }
	for _, path := range cfg.ManyDBPaths() {
		load.mark(path, false)
	}
	load.restoreWrittenState()
	return load
}

func (l *manyDBLoad) restoreWrittenState() {
	written, err := readManyDBWrittenState(l.cfg)
	switch {
	case err == nil:
		known := make(map[string]struct{}, len(l.changed))
		for path := range l.changed {
			known[path] = struct{}{}
		}
		for _, path := range written {
			if _, ok := known[path]; ok {
				l.setWritten(path)
			}
		}
		if len(written) > 0 {
			slog.Info("Restored many database written set", "written", len(written))
		}
	case !manyDBStateOnVolume(l.cfg):
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		slog.Warn("Many database written set unreadable on an empty volume", "error", err)
	default:
		slog.Warn("Many database written set unavailable with databases on the volume; treating every database as written until verified", "error", err)
		for path := range l.changed {
			l.setWritten(path)
		}
	}
}

func (l *manyDBLoad) setWritten(path string) {
	l.changedMu.Lock()
	change := l.changed[path]
	change.written = true
	l.changed[path] = change
	l.changedMu.Unlock()
}

const (
	manyDBStateDirtyEvent     = "many_db_written_state_dirty"
	manyDBStateRecoveredEvent = "many_db_written_state_recovered"
)

// persistWrittenState rewrites the written set on the volume. It runs when a
// path enters or leaves the set, and again on every later mark or
// acknowledgement while a save has failed, and only once the database
// directory exists, so an unpopulated configuration never writes anything.
// The snapshot and the save happen under stateMu, so a save never writes an
// older view of the set than one already on disk. A failed save marks the
// state dirty and removes the stale file, so a restart before a save
// succeeds takes the all-written fallback instead of trusting old contents.
func (l *manyDBLoad) persistWrittenState() {
	if _, err := os.Stat(l.cfg.ManyDBDir()); err != nil {
		return
	}
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	l.changedMu.Lock()
	written := make([]string, 0, len(l.changed))
	for path, change := range l.changed {
		if change.written {
			written = append(written, path)
		}
	}
	l.changedMu.Unlock()
	sort.Strings(written)
	if err := l.saveState(written); err != nil {
		if removeErr := os.Remove(manyDBWrittenStatePath(l.cfg)); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			slog.Warn("Failed to remove stale many database written set", "error", removeErr)
		}
		if !l.stateDirty {
			l.stateDirty = true
			message := fmt.Sprintf("many database written set could not be saved; a restart before it recovers treats every database as written: %v", err)
			slog.Warn("Many database written set dirty", "error", err)
			l.emitStateEvent(manyDBStateDirtyEvent, message)
		}
		return
	}
	if l.stateDirty {
		l.stateDirty = false
		slog.Info("Many database written set recovered", "written", len(written))
		l.emitStateEvent(manyDBStateRecoveredEvent, fmt.Sprintf("many database written set saved again with %d written databases", len(written)))
	}
}

func (l *manyDBLoad) emitStateEvent(eventType, message string) {
	if l.onStateEvent != nil {
		l.onStateEvent(eventType, message)
	}
}

func (l *manyDBLoad) stateIsDirty() bool {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	return l.stateDirty
}

func (l *manyDBLoad) Start(ctx context.Context) error {
	if err := prometheus.Register(l); err != nil {
		return fmt.Errorf("register pending coverage metrics: %w", err)
	}
	activePaths := l.currentActivePaths()
	if len(activePaths) == 0 || l.cfg.WriteRate <= 0 {
		slog.Info("Many database load idle", "active_databases", len(activePaths), "write_rate", l.cfg.WriteRate)
		SetLoadRunning(false)
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.jobs = make(chan manyDBWrite)

	workerCount := l.cfg.Workers
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(activePaths) {
		workerCount = len(activePaths)
	}

	l.wg.Add(1)
	go l.dispatch(runCtx)
	for i := 0; i < workerCount; i++ {
		l.wg.Add(1)
		go l.writeWorker(runCtx)
	}
	SetLoadRunning(true)
	slog.Info("Started many database load", "active_databases", len(activePaths), "write_rate", l.cfg.WriteRate, "workers", workerCount)
	return nil
}

func (l *manyDBLoad) currentActivePaths() []string {
	now := time.Now()
	if l.now != nil {
		now = l.now()
	}
	return l.cfg.ManyDBActivePathsAt(now)
}

func (l *manyDBLoad) currentActiveGeneration() int64 {
	now := time.Now()
	if l.now != nil {
		now = l.now()
	}
	return l.cfg.manyDBActiveGeneration(now)
}

func (l *manyDBLoad) dispatch(ctx context.Context) {
	defer l.wg.Done()
	defer close(l.jobs)

	interval := time.Second / time.Duration(l.cfg.WriteRate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var activePaths []string
	var activeGeneration int64
	nextSlots := make(map[string]int64)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			generation := l.currentActiveGeneration()
			if activePaths == nil || generation != activeGeneration {
				activePaths = l.currentActivePaths()
				activeGeneration = generation
				l.next.Store(0)
				slog.Info("Rotated many database active set", "active_databases", len(activePaths), "generation", generation)
			}
			if len(activePaths) == 0 {
				continue
			}
			idx := int(l.next.Add(1)-1) % len(activePaths)
			dbPath := activePaths[idx]
			rowSlot := nextSlots[dbPath] + 1
			if rowSlot > int64(l.cfg.manyDBMaxRowsPerDatabase()) {
				rowSlot = 1
			}
			nextSlots[dbPath] = rowSlot
			select {
			case <-ctx.Done():
				return
			case l.jobs <- manyDBWrite{dbPath: dbPath, rowSlot: rowSlot}:
			}
		}
	}
}

func (l *manyDBLoad) writeWorker(ctx context.Context) {
	defer l.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case write, ok := <-l.jobs:
			if !ok {
				return
			}
			if !l.beginWrite(ctx) {
				return
			}
			l.performWrite(ctx, write)
			l.endWrite()
		}
	}
}

// performWrite records the database as written, persisting the written set
// when it enters, before the row is committed: a crash between the commit and
// the record would otherwise leave a valid file that omits the write. A write
// that then fails leaves the database marked written, and it is verified
// anyway.
func (l *manyDBLoad) performWrite(ctx context.Context, write manyDBWrite) {
	l.markChanged(write.dbPath)
	if err := l.writeRow(ctx, write.dbPath, l.cfg.PayloadSize, write.rowSlot); err != nil {
		slog.Warn("Many database write failed", "db", write.dbPath, "error", err)
	}
}

func (l *manyDBLoad) beginWrite(ctx context.Context) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		l.mu.Lock()
		if l.stopped {
			l.mu.Unlock()
			return false
		}
		if !l.paused {
			l.active++
			l.mu.Unlock()
			return true
		}
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func (l *manyDBLoad) endWrite() {
	l.mu.Lock()
	if l.active > 0 {
		l.active--
	}
	l.mu.Unlock()
}

func writeManyDBRow(ctx context.Context, dbPath string, payloadSize int, rowSlot int64) error {
	if payloadSize <= 0 {
		payloadSize = 512
	}
	if rowSlot <= 0 {
		return fmt.Errorf("row slot must be positive")
	}
	defer harnessWriters.begin("many_db_write", dbPath)()
	db, err := sql.Open("sqlite", manyDBDSN(dbPath))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO soak_writes (id, body) VALUES (?, randomblob(?))
		ON CONFLICT(id) DO UPDATE SET body = excluded.body, written_at = CURRENT_TIMESTAMP
	`, rowSlot, payloadSize); err != nil {
		return fmt.Errorf("write row slot: %w", err)
	}
	return nil
}

func (l *manyDBLoad) Pause(ctx context.Context) error {
	l.mu.Lock()
	l.paused = true
	l.mu.Unlock()
	SetLoadRunning(false)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		l.mu.Lock()
		active := l.active
		l.mu.Unlock()
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (l *manyDBLoad) Resume() {
	l.mu.Lock()
	stopped := l.stopped
	l.paused = false
	l.mu.Unlock()
	if !stopped && len(l.currentActivePaths()) > 0 && l.cfg.WriteRate > 0 {
		SetLoadRunning(true)
	}
}

func (l *manyDBLoad) Stop() {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	l.stopped = true
	cancel := l.cancel
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	l.wg.Wait()
	prometheus.Unregister(l)
	SetLoadRunning(false)
}

// manyDBChange is one database awaiting verification of its current
// generation. written is set once the workload wrote to it since it was last
// verified; a seeded entry that was never written is backlog, not a change.
type manyDBChange struct {
	path       string
	generation uint64
	since      time.Time
	written    bool
}

func (l *manyDBLoad) markChanged(dbPath string) {
	l.mark(dbPath, true)
}

func (l *manyDBLoad) mark(dbPath string, written bool) {
	if dbPath == "" {
		return
	}
	l.changedMu.Lock()
	defer l.changedMu.Unlock()
	if l.changed == nil {
		l.changed = make(map[string]manyDBChange)
	}
	change, exists := l.changed[dbPath]
	if !exists {
		change = manyDBChange{path: dbPath, since: l.now()}
	}
	l.changeGeneration++
	change.generation = l.changeGeneration
	entered := written && !change.written
	change.written = change.written || written
	l.changed[dbPath] = change
	l.changedMu.Unlock()
	if entered || l.stateIsDirty() {
		l.persistWrittenState()
	}
	l.changedMu.Lock()
}

// pendingManyDBChanges lists the pending set with written databases ahead of
// the seeded backlog, each group in round-robin order from the last attempt,
// so a truncated batch always covers what the workload touched first.
func (l *manyDBLoad) pendingManyDBChanges() []manyDBChange {
	l.changedMu.Lock()
	defer l.changedMu.Unlock()
	changes := make([]manyDBChange, 0, len(l.changed))
	for _, change := range l.changed {
		changes = append(changes, change)
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].written != changes[j].written {
			return changes[i].written
		}
		afterI, afterJ := changes[i].path > l.lastAttempt, changes[j].path > l.lastAttempt
		if afterI != afterJ {
			return afterI
		}
		return changes[i].path < changes[j].path
	})
	return changes
}

func (l *manyDBLoad) attemptManyDBChange(change manyDBChange) {
	l.changedMu.Lock()
	defer l.changedMu.Unlock()
	l.lastAttempt = change.path
}

func (l *manyDBLoad) acknowledgeManyDBChange(change manyDBChange) {
	l.changedMu.Lock()
	current, ok := l.changed[change.path]
	verified := ok && current.generation == change.generation
	if verified {
		delete(l.changed, change.path)
	}
	l.changedMu.Unlock()
	if (verified && current.written) || l.stateIsDirty() {
		l.persistWrittenState()
	}
}

// manyDBCoverage is the pending set at one instant: every database awaiting
// verification, how many of them the workload wrote to, and the age of the
// oldest. All three are read under one lock so they describe the same set.
type manyDBCoverage struct {
	pending   int
	written   int
	oldestAge float64
}

func (l *manyDBLoad) manyDBPendingCoverage() manyDBCoverage {
	l.changedMu.Lock()
	defer l.changedMu.Unlock()
	now := l.now()
	oldest := now
	coverage := manyDBCoverage{pending: len(l.changed)}
	for _, change := range l.changed {
		if change.since.Before(oldest) {
			oldest = change.since
		}
		if change.written {
			coverage.written++
		}
	}
	coverage.oldestAge = max(0, now.Sub(oldest).Seconds())
	return coverage
}

func selectManyDBVerificationTargets(cfg Config, changed []string) ([]string, int) {
	if len(changed) == 0 {
		changed = cfg.ManyDBActivePaths()
	}
	if len(changed) == 0 {
		return nil, 0
	}

	allowed := make(map[string]struct{}, cfg.NumDatabases)
	for _, path := range cfg.ManyDBPaths() {
		allowed[path] = struct{}{}
	}

	seen := make(map[string]struct{}, len(changed))
	targets := make([]string, 0, len(changed))
	for _, path := range changed {
		if _, ok := allowed[path]; !ok {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		targets = append(targets, path)
	}

	totalChanged := len(targets)
	limit := cfg.manyDBVerifyChangedLimit()
	if limit > 0 && len(targets) > limit {
		targets = targets[:limit]
	}
	return targets, totalChanged
}

func (l *manyDBLoad) coverageDescriptors() (*prometheus.Desc, *prometheus.Desc, *prometheus.Desc) {
	labels := prometheus.Labels{"worker_id": l.cfg.WorkerID, "profile": l.cfg.ProfileName, "source": l.cfg.Source, "region": l.cfg.Region}
	return prometheus.NewDesc("soak_many_db_pending_count", "Databases awaiting successful verification of their current generation, including untouched databases.", nil, labels),
		prometheus.NewDesc("soak_many_db_oldest_pending_age_seconds", "Seconds since the oldest outstanding database first required verification; zero when coverage is current.", nil, labels),
		prometheus.NewDesc("soak_many_db_written_pending_count", "Databases the workload wrote to that still await successful verification of their current generation.", nil, labels)
}

func (l *manyDBLoad) Describe(ch chan<- *prometheus.Desc) {
	count, age, written := l.coverageDescriptors()
	ch <- count
	ch <- age
	ch <- written
}

func (l *manyDBLoad) Collect(ch chan<- prometheus.Metric) {
	countDesc, ageDesc, writtenDesc := l.coverageDescriptors()
	coverage := l.manyDBPendingCoverage()
	ch <- prometheus.MustNewConstMetric(countDesc, prometheus.GaugeValue, float64(coverage.pending))
	ch <- prometheus.MustNewConstMetric(ageDesc, prometheus.GaugeValue, coverage.oldestAge)
	ch <- prometheus.MustNewConstMetric(writtenDesc, prometheus.GaugeValue, float64(coverage.written))
}
