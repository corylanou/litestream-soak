package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ftsCapability = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "soak_fts_available", Help: "FTS5 initialization succeeded (1) or failed (0)."}, []string{"worker_id"})
	ftsOperations = promauto.NewCounterVec(prometheus.CounterOpts{Name: "soak_fts_operations_total", Help: "Committed phase operations, including maintenance no-ops."}, []string{"worker_id", "phase"})
	ftsChanges    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "soak_fts_changes_total", Help: "Committed document rows, checked queries, or maintenance internal changes excluding the command row."}, []string{"worker_id", "phase"})
	ftsMatches    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "soak_fts_matches_total", Help: "Search result rows checked against logical documents."}, []string{"worker_id"})
	ftsFailures   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "soak_fts_failures_total", Help: "FTS initialization or phase failures; never reset after recovery."}, []string{"worker_id"})
	ftsDuration   = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "soak_fts_phase_seconds", Help: "FTS phase execution duration excluding profile capture.", Buckets: prometheus.DefBuckets}, []string{"worker_id", "phase"})
)

type ftsLoad struct {
	cfg          *Config
	db           *sql.DB
	gate         chan struct{}
	resume       chan struct{}
	done         chan struct{}
	cancel       context.CancelFunc
	failure      error
	capture      func(context.Context, string)
	beginProfile func(context.Context, string) func()
	closeOnce    sync.Once
}

func newFTSLoad(cfg *Config) *ftsLoad {
	return &ftsLoad{cfg: cfg, gate: make(chan struct{}, 1)}
}

func (l *ftsLoad) Start(ctx context.Context, onFailure func(error)) error {
	if l.cfg.WriteRate < 1 || l.cfg.WriteRate > 1000 || l.cfg.ManyDBEnabled() {
		return fmt.Errorf("FTS requires WRITE_RATE between 1 and 1000 and a single database")
	}
	db, err := openFTS(ctx, l.cfg.DBPath)
	if err != nil {
		ftsCapability.WithLabelValues(l.cfg.WorkerID).Set(0)
		ftsFailures.WithLabelValues(l.cfg.WorkerID).Inc()
		return err
	}
	l.db = db
	ftsCapability.WithLabelValues(l.cfg.WorkerID).Set(1)
	slog.Info("FTS capability available", "worker_id", l.cfg.WorkerID, "candidate", l.cfg.LitestreamSHA, "workload_version", "fts-v1")
	runCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.done = make(chan struct{})
	go func() {
		defer close(l.done)
		defer func() {
			if err := db.Close(); err != nil {
				slog.Error("Close FTS database", "error", err)
			}
		}()
		SetLoadRunning(true)
		defer SetLoadRunning(false)
		if err := l.run(runCtx); err != nil && runCtx.Err() == nil {
			l.gate <- struct{}{}
			l.failure = err
			<-l.gate
			ftsFailures.WithLabelValues(l.cfg.WorkerID).Inc()
			onFailure(fmt.Errorf("FTS workload failed: %w", err))
		}
	}()
	return nil
}

func (l *ftsLoad) run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second / time.Duration(l.cfg.WriteRate))
	defer ticker.Stop()
	captured := make(map[string]time.Time)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if err := l.lock(ctx); err != nil {
			return err
		}
		if l.resume != nil {
			resume := l.resume
			<-l.gate
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-resume:
			}
			continue
		}
		SetLoadRunning(true)
		var step int64
		err := l.db.QueryRowContext(ctx, "SELECT step FROM fts_progress WHERE id=1").Scan(&step)
		if err != nil {
			<-l.gate
			return err
		}
		if step < 0 {
			<-l.gate
			return fmt.Errorf("negative FTS progress")
		}
		phase := ftsPhase(step)
		profile := l.capture != nil && time.Since(captured[phase]) >= time.Hour
		if profile {
			l.capture(ctx, "fts-"+phase+"-before")
		}
		finishProfile := func() {}
		if profile && l.beginProfile != nil {
			finishProfile = l.beginProfile(ctx, phase)
		}
		started := time.Now()
		work, err := stepFTS(ctx, l.db)
		ftsDuration.WithLabelValues(l.cfg.WorkerID, phase).Observe(time.Since(started).Seconds())
		finishProfile()
		if err == nil {
			ftsOperations.WithLabelValues(l.cfg.WorkerID, phase).Inc()
			ftsChanges.WithLabelValues(l.cfg.WorkerID, phase).Add(float64(work.Changes))
			ftsChanges.WithLabelValues(l.cfg.WorkerID, "delete").Add(float64(work.Deleted))
			ftsMatches.WithLabelValues(l.cfg.WorkerID).Add(float64(work.Matches))
			slog.Info("FTS phase committed", "phase", phase, "step", step, "changes", work.Changes, "deleted", work.Deleted, "queries", work.Queries, "matches", work.Matches)
		}
		if profile {
			label := "fts-" + phase + "-after"
			if err != nil {
				label = "fts-" + phase + "-failed"
			}
			l.capture(ctx, label)
			captured[phase] = time.Now()
		}
		<-l.gate
		if err != nil {
			return fmt.Errorf("phase %s step %d: %w", phase, step, err)
		}
	}
}

func (l *ftsLoad) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.gate <- struct{}{}:
		return nil
	}
}

func (l *ftsLoad) Pause(ctx context.Context) error {
	if err := l.lock(ctx); err != nil {
		return err
	}
	defer func() { <-l.gate }()
	if l.failure != nil {
		return l.failure
	}
	if l.resume == nil {
		l.resume = make(chan struct{})
	}
	SetLoadRunning(false)
	return nil
}

func (l *ftsLoad) Resume() {
	l.gate <- struct{}{}
	defer func() { <-l.gate }()
	if l.resume != nil {
		close(l.resume)
		l.resume = nil
	}
}

func (l *ftsLoad) Stop() {
	l.closeOnce.Do(func() {
		if l.cancel != nil {
			l.cancel()
			<-l.done
		}
	})
}

func (r *Runner) startFTS(ctx context.Context, onFailure func(error)) (*ftsLoad, error) {
	load := newFTSLoad(&r.cfg)
	if r.cfg.PprofCaptureEnabled {
		load.capture = r.profiles.captureSet
		load.beginProfile = r.profiles.beginFTSProfile
	}
	if err := load.Start(ctx, onFailure); err != nil {
		return nil, err
	}
	return load, nil
}
