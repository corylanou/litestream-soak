package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

type manyDBLoad struct {
	cfg     *Config
	dbPaths []string

	cancel context.CancelFunc
	jobs   chan string
	wg     sync.WaitGroup
	next   atomic.Uint64

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
		dbPaths: cfg.ManyDBActivePaths(),
	}
}

func (l *manyDBLoad) Start(ctx context.Context) error {
	if len(l.dbPaths) == 0 || l.cfg.WriteRate <= 0 {
		slog.Info("Many database load idle", "active_databases", len(l.dbPaths), "write_rate", l.cfg.WriteRate)
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
	if workerCount > len(l.dbPaths) {
		workerCount = len(l.dbPaths)
	}

	l.wg.Add(1)
	go l.dispatch(runCtx)
	for i := 0; i < workerCount; i++ {
		l.wg.Add(1)
		go l.writeWorker(runCtx)
	}
	SetLoadRunning(true)
	slog.Info("Started many database load", "active_databases", len(l.dbPaths), "write_rate", l.cfg.WriteRate, "workers", workerCount)
	return nil
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

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idx := int(l.next.Add(1)-1) % len(l.dbPaths)
			select {
			case <-ctx.Done():
				return
			case l.jobs <- l.dbPaths[idx]:
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
	if !stopped && len(l.dbPaths) > 0 && l.cfg.WriteRate > 0 {
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

func selectManyDBVerificationSample(cfg Config, rng *rand.Rand) []string {
	paths := cfg.ManyDBPaths()
	if len(paths) == 0 {
		return nil
	}
	size := cfg.manyDBVerifySampleSize()
	if size >= len(paths) {
		return append([]string(nil), paths...)
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	active := cfg.ManyDBActivePaths()
	activeSet := make(map[string]bool, len(active))
	for _, path := range active {
		activeSet[path] = true
	}
	idle := make([]string, 0, len(paths)-len(active))
	for _, path := range paths {
		if !activeSet[path] {
			idle = append(idle, path)
		}
	}

	sample := make([]string, 0, size)
	seen := make(map[string]bool, size)
	add := func(path string) {
		if path == "" || seen[path] || len(sample) >= size {
			return
		}
		seen[path] = true
		sample = append(sample, path)
	}

	if len(active) > 0 {
		add(active[rng.Intn(len(active))])
	}
	if len(idle) > 0 {
		add(idle[rng.Intn(len(idle))])
	}

	shuffled := append([]string(nil), paths...)
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	for _, path := range shuffled {
		add(path)
	}
	return sample
}
