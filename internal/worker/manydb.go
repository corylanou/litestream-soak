package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

type manyDBLoad struct {
	cfg *Config
	now func() time.Time

	cancel context.CancelFunc
	jobs   chan string
	wg     sync.WaitGroup
	next   atomic.Uint64

	changedMu sync.Mutex
	changed   map[string]struct{}

	mu      sync.Mutex
	paused  bool
	active  int
	stopped bool
}

func populateManyDBs(ctx context.Context, cfg Config) error {
	if err := os.MkdirAll(cfg.ManyDBDir(), 0755); err != nil {
		return fmt.Errorf("create many database dir: %w", err)
	}
	for _, dbPath := range cfg.ManyDBPaths() {
		if _, err := os.Stat(dbPath); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("stat many database %s: %w", dbPath, err)
		}
		if err := seedManyDB(ctx, dbPath); err != nil {
			return fmt.Errorf("seed many database %s: %w", dbPath, err)
		}
	}
	return nil
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

func newManyDBLoad(cfg *Config) *manyDBLoad {
	return &manyDBLoad{
		cfg:     cfg,
		now:     time.Now,
		changed: make(map[string]struct{}),
	}
}

func (l *manyDBLoad) Start(ctx context.Context) error {
	activePaths := l.currentActivePaths()
	if len(activePaths) == 0 || l.cfg.WriteRate <= 0 {
		slog.Info("Many database load idle", "active_databases", len(activePaths), "write_rate", l.cfg.WriteRate)
		SetLoadRunning(false)
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.jobs = make(chan string)

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
			select {
			case <-ctx.Done():
				return
			case l.jobs <- activePaths[idx]:
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
		case dbPath, ok := <-l.jobs:
			if !ok {
				return
			}
			if !l.beginWrite(ctx) {
				return
			}
			if err := writeManyDBRow(ctx, dbPath, l.cfg.PayloadSize); err != nil {
				slog.Warn("Many database write failed", "db", dbPath, "error", err)
			} else {
				l.markChanged(dbPath)
			}
			l.endWrite()
		}
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

func writeManyDBRow(ctx context.Context, dbPath string, payloadSize int) error {
	if payloadSize <= 0 {
		payloadSize = 512
	}
	db, err := sql.Open("sqlite", manyDBDSN(dbPath))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "INSERT INTO soak_writes (body) VALUES (zeroblob(?))", payloadSize); err != nil {
		return fmt.Errorf("insert write row: %w", err)
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
	SetLoadRunning(false)
}

func (l *manyDBLoad) markChanged(dbPath string) {
	if dbPath == "" {
		return
	}
	l.changedMu.Lock()
	if l.changed == nil {
		l.changed = make(map[string]struct{})
	}
	l.changed[dbPath] = struct{}{}
	l.changedMu.Unlock()
}

func (l *manyDBLoad) manyDBChangedPathsAndReset() []string {
	l.changedMu.Lock()
	defer l.changedMu.Unlock()

	paths := make([]string, 0, len(l.changed))
	for path := range l.changed {
		paths = append(paths, path)
	}
	clear(l.changed)
	sort.Strings(paths)
	return paths
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
	sort.Strings(targets)

	totalChanged := len(targets)
	limit := cfg.manyDBVerifyChangedLimit()
	if limit > 0 && len(targets) > limit {
		targets = targets[:limit]
	}
	return targets, totalChanged
}
