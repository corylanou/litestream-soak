package replay

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "modernc.org/sqlite"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	replayRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_replay_rows_total",
		Help: "Total rows replayed by dataset.",
	}, []string{"dataset"})

	replayErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_replay_errors_total",
		Help: "Total replay errors by dataset.",
	}, []string{"dataset"})

	replayLagSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_replay_lag_seconds",
		Help: "Delay between scheduled and actual insert time.",
	}, []string{"dataset"})

	replayActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_replay_active",
		Help: "Whether a replay dataset is currently running (1=yes).",
	}, []string{"dataset"})
)

type Config struct {
	Dataset         string
	DataPath        string
	DBPath          string
	SpeedMultiplier float64
	Loop            bool
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
}

func NewEngine(cfg Config, adapter Adapter) *Engine {
	return &Engine{cfg: cfg, adapter: adapter}
}

func (e *Engine) Run(ctx context.Context) error {
	var err error
	e.db, err = sql.Open("sqlite", e.cfg.DBPath+"?_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer e.db.Close()

	if err := e.adapter.CreateTables(e.db); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}

	name := e.adapter.Name()
	replayActive.WithLabelValues(name).Set(1)
	defer replayActive.WithLabelValues(name).Set(0)

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
	name := e.adapter.Name()
	iter, err := e.adapter.Rows()
	if err != nil {
		return fmt.Errorf("open rows: %w", err)
	}
	defer iter.Close()

	var prevTS time.Time
	var count int64

	for iter.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
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

		start := time.Now()
		if err := iter.Insert(e.db); err != nil {
			replayErrorsTotal.WithLabelValues(name).Inc()
			slog.Error("Replay insert failed", "dataset", name, "error", err)
			continue
		}

		replayRowsTotal.WithLabelValues(name).Inc()
		replayLagSeconds.WithLabelValues(name).Set(time.Since(start).Seconds())
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
