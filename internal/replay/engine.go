package replay

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	replayRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_replay_rows_total",
		Help: "Total rows replayed by dataset.",
	}, []string{"dataset", "worker_id", "profile", "source"})

	replayErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_replay_errors_total",
		Help: "Total replay errors by dataset.",
	}, []string{"dataset", "worker_id", "profile", "source"})

	replayDroppedRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_replay_dropped_rows_total",
		Help: "Total rows dropped after insert retries were exhausted.",
	}, []string{"dataset", "worker_id", "profile", "source"})

	replayLagSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_replay_lag_seconds",
		Help: "Delay between scheduled and actual insert time.",
	}, []string{"dataset", "worker_id", "profile", "source"})

	replayActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_replay_active",
		Help: "Whether a replay dataset is currently running (1=yes).",
	}, []string{"dataset", "worker_id", "profile", "source"})
)

type Config struct {
	Dataset         string
	DataPath        string
	DBPath          string
	SpeedMultiplier float64
	Loop            bool
	WorkerID        string
	ProfileName     string
	Source          string
}

type Adapter interface {
	Name() string
	CreateTables(db *sql.DB) error
	Rows() (RowIterator, error)
}

type RowIterator interface {
	Next() bool
	Timestamp() time.Time
	Insert(db *sql.DB) error
	Err() error
	Close() error
}

type Engine struct {
	cfg     Config
	adapter Adapter
	db      *sql.DB

	mu        sync.Mutex
	running   bool
	paused    bool
	resumeCh  chan struct{}
	ackCh     chan struct{}
	ackClosed bool
}

func NewEngine(cfg Config, adapter Adapter) *Engine {
	return &Engine{cfg: cfg, adapter: adapter}
}

// Pause blocks new inserts and waits until the engine is quiesced
// (parked between inserts) or ctx is done.
func (e *Engine) Pause(ctx context.Context) error {
	e.mu.Lock()
	if !e.paused {
		e.paused = true
		e.resumeCh = make(chan struct{})
		e.ackCh = make(chan struct{})
		e.ackClosed = false
	}
	if !e.running {
		e.closeAckLocked()
	}
	ack := e.ackCh
	e.mu.Unlock()

	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Resume releases a paused engine. Idempotent.
func (e *Engine) Resume() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.paused {
		return
	}
	e.paused = false
	close(e.resumeCh)
	e.resumeCh = nil
	e.ackCh = nil
	e.ackClosed = false
}

func (e *Engine) closeAckLocked() {
	if !e.ackClosed {
		close(e.ackCh)
		e.ackClosed = true
	}
}

func (e *Engine) waitIfPaused(ctx context.Context) error {
	e.mu.Lock()
	if !e.paused {
		e.mu.Unlock()
		return ctx.Err()
	}
	e.closeAckLocked()
	resume := e.resumeCh
	e.mu.Unlock()

	select {
	case <-resume:
	case <-ctx.Done():
	}
	return ctx.Err()
}

func replayDSN(dbPath string) string {
	return dbPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)"
}

func (e *Engine) Run(ctx context.Context) error {
	e.mu.Lock()
	e.running = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.running = false
		if e.paused {
			e.closeAckLocked()
		}
		e.mu.Unlock()
	}()

	var err error
	e.db, err = sql.Open("sqlite", replayDSN(e.cfg.DBPath))
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	e.db.SetMaxOpenConns(1)
	e.db.SetMaxIdleConns(1)
	defer func() { _ = e.db.Close() }()

	if err := e.adapter.CreateTables(e.db); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	name := e.adapter.Name()
	labels := e.metricLabels(name)
	replayActive.WithLabelValues(labels...).Set(1)
	defer replayActive.WithLabelValues(labels...).Set(0)

	slog.Info("Starting replay", "dataset", name, "speed", e.cfg.SpeedMultiplier, "loop", e.cfg.Loop)

	for {
		if err := e.replayOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if !e.cfg.Loop {
			slog.Info("Replay complete", "dataset", name)
			return nil
		}
		slog.Info("Replay loop restarting", "dataset", name)
	}
}

func (e *Engine) replayOnce(ctx context.Context) error {
	// Loop mode can cycle passes without ever reaching the per-row gate
	// (e.g. an empty dataset), so a pending Pause must be acknowledged here.
	if err := e.waitIfPaused(ctx); err != nil {
		return err
	}

	name := e.adapter.Name()
	labels := e.metricLabels(name)
	iter, err := e.adapter.Rows()
	if err != nil {
		return fmt.Errorf("open rows: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var prevTS time.Time
	var count int64
	var dropped int64

	for iter.Next() {
		if err := e.waitIfPaused(ctx); err != nil {
			return err
		}

		ts := iter.Timestamp()

		if !prevTS.IsZero() && ts.After(prevTS) {
			delay := ts.Sub(prevTS)
			if e.cfg.SpeedMultiplier > 0 {
				delay = time.Duration(float64(delay) / e.cfg.SpeedMultiplier)
			}
			if delay > 0 && delay < 10*time.Second {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
		}
		prevTS = ts

		if err := e.waitIfPaused(ctx); err != nil {
			return err
		}

		start := time.Now()
		if err := e.insertWithRetry(ctx, func() error { return iter.Insert(e.db) }); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			replayErrorsTotal.WithLabelValues(labels...).Inc()
			replayDroppedRowsTotal.WithLabelValues(labels...).Inc()
			dropped++
			slog.Error("Replay insert failed, row dropped", "dataset", name, "error", err, "dropped_total", dropped)
			continue
		}

		replayRowsTotal.WithLabelValues(labels...).Inc()
		replayLagSeconds.WithLabelValues(labels...).Set(time.Since(start).Seconds())
		count++

		if count%10000 == 0 {
			slog.Info("Replay progress", "dataset", name, "rows", count)
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	slog.Info("Replay pass complete", "dataset", name, "total_rows", count)
	return nil
}

func (e *Engine) insertWithRetry(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if !isBusyError(err) || time.Now().After(deadline) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked")
}

func (e *Engine) metricLabels(dataset string) []string {
	return []string{dataset, e.cfg.WorkerID, e.cfg.ProfileName, e.cfg.Source}
}
