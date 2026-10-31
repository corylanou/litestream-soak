package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// The cache_probe table reproduces the geometry of benbjohnson/litestream#1608:
// 2048 rows of 3500-byte payloads is roughly 8 MiB at 4 KiB pages, far more
// than the 1 MiB page cache the VFS reader runs with, so a full scan inside a
// read transaction has to re-read pages the VFS may have replaced underneath
// it. The writer bumps one stripe of rows at a time, so every bump touches
// pages spread across the whole table without rewriting all of it.
const (
	cacheProbeTable        = "cache_probe"
	cacheProbeRows         = 2048
	cacheProbePayloadBytes = 3500
	cacheProbeStripes      = 16
	cacheProbeInterval     = 5 * time.Second
	cacheProbeBusyTimeout  = 5 * time.Second
	cacheProbeQuery        = "SELECT count(*), min(generation), max(generation), sum(generation) FROM " + cacheProbeTable + " NOT INDEXED"
)

func openCacheProbeDB(path, mode string) (*sql.DB, error) {
	uri := url.URL{Scheme: "file", Path: path, RawQuery: fmt.Sprintf("mode=%s&_pragma=busy_timeout(%d)", mode, cacheProbeBusyTimeout.Milliseconds())}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// ensureCacheProbe creates the probe table and fills any missing rows. It runs
// during populate, before Litestream starts, and is a no-op on a seeded
// database.
func ensureCacheProbe(ctx context.Context, path string) error {
	db, err := openCacheProbeDB(path, "rwc")
	if err != nil {
		return fmt.Errorf("open cache probe: %w", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+cacheProbeTable+" (id INTEGER PRIMARY KEY, generation INTEGER NOT NULL, payload BLOB NOT NULL)"); err != nil {
		return fmt.Errorf("create cache probe: %w", err)
	}
	var rows int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+cacheProbeTable).Scan(&rows); err != nil {
		return fmt.Errorf("count cache probe: %w", err)
	}
	if rows >= cacheProbeRows {
		return nil
	}
	fill := fmt.Sprintf("WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < %d) INSERT OR IGNORE INTO %s(id, generation, payload) SELECT n, 0, zeroblob(%d) FROM seq", cacheProbeRows, cacheProbeTable, cacheProbePayloadBytes)
	if _, err := db.ExecContext(ctx, fill); err != nil {
		return fmt.Errorf("fill cache probe: %w", err)
	}
	slog.Info("Seeded cache probe table", "db", path, "rows", cacheProbeRows, "payload_bytes", cacheProbePayloadBytes)
	return nil
}

func bumpCacheProbe(ctx context.Context, path string, stripe int) error {
	defer harnessWriters.begin("cache_probe", path)()
	db, err := openCacheProbeDB(path, "rw")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(ctx, "UPDATE "+cacheProbeTable+" SET generation = generation + 1 WHERE id % ? = ?", cacheProbeStripes, stripe%cacheProbeStripes)
	return err
}

// cacheProbeWriter advances the probe table on the primary between
// verification cycles. It implements loadPauser so the verifier can quiesce
// it with the rest of the load: every write runs under the writer's lock, so
// Pause returns only once no write is in flight and nothing commits after it.
type cacheProbeWriter struct {
	dbPath   string
	interval time.Duration
	bump     func(context.Context, string, int) error
	position func(context.Context) (uint64, error)

	mu      sync.Mutex
	paused  bool
	stripe  int
	commits []cacheProbeCommit
	stop    context.CancelFunc
	done    chan struct{}
}

// cacheProbeCommit is one committed probe update and the Litestream txid by
// which it must be in the replica: the position Litestream reported after the
// commit plus one, since the next sync after a commit carries its frames. A
// zero txid means the position could not be read.
type cacheProbeCommit struct {
	txid uint64
	at   time.Time
}

const cacheProbeCommitHistory = 256

func newCacheProbeWriter(dbPath string, interval time.Duration) *cacheProbeWriter {
	if interval <= 0 {
		interval = cacheProbeInterval
	}
	return &cacheProbeWriter{dbPath: dbPath, interval: interval, bump: bumpCacheProbe, position: func(context.Context) (uint64, error) { return 0, errors.New("litestream position unavailable") }}
}

// litestreamPosition reads the primary's current Litestream txid over IPC.
func litestreamPosition(cfg Config) func(context.Context) (uint64, error) {
	client := newIPCClient(cfg.SocketPath, cacheProbeBusyTimeout)
	return func(ctx context.Context) (uint64, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/txid?path="+cfg.DBPath, nil)
		if err != nil {
			return 0, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, fmt.Errorf("read txid: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()
		var result struct {
			TXID uint64 `json:"txid"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return 0, fmt.Errorf("decode txid: %w", err)
		}
		return result.TXID, nil
	}
}

// Commits returns the recent probe commits, oldest first.
func (w *cacheProbeWriter) Commits() []cacheProbeCommit {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]cacheProbeCommit(nil), w.commits...)
}

func (w *cacheProbeWriter) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	w.stop = cancel
	w.done = make(chan struct{})
	go w.run(runCtx)
}

func (w *cacheProbeWriter) Stop() {
	w.mu.Lock()
	stop, done := w.stop, w.done
	w.mu.Unlock()
	if stop != nil {
		stop()
	}
	if done != nil {
		<-done
	}
}

func (w *cacheProbeWriter) Pause(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = true
	return nil
}

func (w *cacheProbeWriter) Resume() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = false
}

func (w *cacheProbeWriter) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		w.writeOnce(ctx)
	}
}

func (w *cacheProbeWriter) writeOnce(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.paused {
		return
	}
	stripe := w.stripe
	w.stripe++
	if err := w.bump(ctx, w.dbPath, stripe); err != nil {
		if ctx.Err() == nil {
			slog.Warn("Cache probe update failed", "error", err)
		}
		return
	}
	commit := cacheProbeCommit{at: time.Now().UTC()}
	if position, err := w.position(ctx); err == nil {
		commit.txid = position + 1
	} else if ctx.Err() == nil {
		slog.Warn("Cache probe commit position unavailable", "error", err)
	}
	w.commits = append(w.commits, commit)
	if len(w.commits) > cacheProbeCommitHistory {
		w.commits = w.commits[len(w.commits)-cacheProbeCommitHistory:]
	}
}
