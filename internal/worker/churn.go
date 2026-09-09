package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/corylanou/litestream-soak/internal/churn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"modernc.org/sqlite"
)

var churnMutations = promauto.NewCounterVec(prometheus.CounterOpts{Name: "soak_churn_mutations_total", Help: "Committed affected rows, including queue receipts."}, []string{"mode", "operation", "worker_id", "profile", "source"})
var churnErrors = promauto.NewCounterVec(prometheus.CounterOpts{Name: "soak_churn_errors_total", Help: "All failed churn attempts, retained after recovery."}, []string{"mode", "operation", "kind", "worker_id", "profile", "source"})
var churnLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "soak_churn_operation_seconds", Help: "Transaction attempt latency including database waits, excluding pacing."}, []string{"mode", "operation", "worker_id", "profile", "source"})

func (c Config) churnEnabled() bool { return c.LoadMode == "queue" || c.LoadMode == "cache" }

func loadChurnConfig(c *Config, getenv func(string) string) error {
	if !c.churnEnabled() {
		return nil
	}
	if c.ManyDBEnabled() {
		return fmt.Errorf("churn cannot use many database mode")
	}
	c.Churn = churn.Config{Slots: 1024, HotPercent: 80, Workers: 1, Rate: 100, PayloadSize: 256, Seed: 1}
	if raw := getenv("CHURN_CONFIG"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.Churn); err != nil {
			return fmt.Errorf("CHURN_CONFIG: %w", err)
		}
	}
	return c.Churn.Validate()
}

func populateChurn(ctx context.Context, cfg Config) error {
	db, err := churn.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	return db.Close()
}

type churnLoad struct {
	engine *churn.Engine
	db     *sql.DB
}

func startChurn(ctx context.Context, cfg Config, observe churn.Observer) (*churnLoad, error) {
	db, err := churn.Open(ctx, cfg.DBPath)
	if err != nil {
		return nil, err
	}
	if observe == nil {
		observe = func(op string, n int64, d time.Duration, err error) { recordChurn(cfg, op, n, d, err) }
	}
	engine := churn.New(cfg.LoadMode, cfg.Churn, observe)
	if err := engine.Start(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &churnLoad{engine: engine, db: db}, nil
}

func (l *churnLoad) Stop() {
	l.engine.Stop()
	if err := l.db.Close(); err != nil {
		slog.Error("Close churn database", "error", err)
	}
}

func recordChurn(cfg Config, op string, n int64, d time.Duration, err error) {
	labels := []string{cfg.LoadMode, op, cfg.WorkerID, cfg.ProfileName, cfg.Source}
	churnLatency.WithLabelValues(labels...).Observe(d.Seconds())
	if err != nil {
		kind := "other"
		var sqliteErr *sqlite.Error
		if errors.As(err, &sqliteErr) && (sqliteErr.Code()&255 == 5 || sqliteErr.Code()&255 == 6) {
			kind = "busy"
		}
		churnErrors.WithLabelValues(cfg.LoadMode, op, kind, cfg.WorkerID, cfg.ProfileName, cfg.Source).Inc()
		slog.Warn("Churn attempt failed", "mode", cfg.LoadMode, "operation", op, "error", err)
		return
	}
	churnMutations.WithLabelValues(labels...).Add(float64(n))
}

func validateChurnRestore(ctx context.Context, cfg Config, path string) error {
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if err := churn.Validate(ctx, db, cfg.LoadMode, cfg.Churn.Slots); err != nil {
		return fmt.Errorf("restored churn invariants: %w", err)
	}
	return nil
}
