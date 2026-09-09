package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type deploymentObserver struct {
	ctx      context.Context
	cancel   context.CancelFunc
	refresh  func(context.Context, []string) error
	interval time.Duration
	budget   time.Duration
	mu       sync.Mutex
	pending  map[string]struct{}
	running  bool
	next     time.Time
	done     chan struct{}
}

func (a *API) initializeDeploymentObserver() {
	a.deploymentObserverOnce.Do(func() {
		ctx := a.backgroundContext
		if ctx == nil {
			ctx = context.Background()
		}
		ctx, cancel := context.WithCancel(ctx)
		a.deploymentObserver = &deploymentObserver{ctx: ctx, cancel: cancel, refresh: a.refreshDeploymentState, interval: 5 * time.Second, budget: 10 * time.Second}
	})
}

func (a *API) observeLatestDeploymentState(source string) {
	a.initializeDeploymentObserver()
	a.deploymentObserver.request(source)
}

func (o *deploymentObserver) request(source string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ctx.Err() != nil {
		return
	}
	if o.pending == nil {
		o.pending = make(map[string]struct{})
	}
	o.pending[source] = struct{}{}
	if o.running {
		return
	}
	o.running = true
	o.done = make(chan struct{})
	go o.run()
}

func (o *deploymentObserver) run() {
	for {
		o.mu.Lock()
		if o.ctx.Err() != nil || len(o.pending) == 0 {
			o.pending = nil
			o.running = false
			close(o.done)
			o.mu.Unlock()
			return
		}
		delay := time.Until(o.next)
		if delay > 0 {
			o.mu.Unlock()
			timer := time.NewTimer(delay)
			select {
			case <-o.ctx.Done():
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		sources := make([]string, 0, len(o.pending))
		for source := range o.pending {
			sources = append(sources, source)
		}
		o.pending = nil
		o.mu.Unlock()
		sort.Strings(sources)
		ctx, cancel := context.WithTimeout(o.ctx, o.budget)
		err := errors.Join(o.refresh(ctx, sources), ctx.Err())
		observeDeploymentRefreshResult(err)
		if err != nil {
			slog.Warn("Deployment gauge refresh incomplete", "error", err)
		}
		cancel()
		o.mu.Lock()
		if err != nil && o.ctx.Err() == nil {
			if o.pending == nil {
				o.pending = make(map[string]struct{})
			}
			for _, source := range sources {
				o.pending[source] = struct{}{}
			}
		}
		o.next = time.Now().Add(o.interval)
		o.mu.Unlock()
	}
}

var deploymentRefreshFailures = promauto.NewCounter(prometheus.CounterOpts{Name: "soak_control_deployment_refresh_failures_total", Help: "Failed complete deployment gauge refreshes, retained after recovery."})
var deploymentRefreshAttempt = promauto.NewGauge(prometheus.GaugeOpts{Name: "soak_control_deployment_refresh_last_attempt_unixtime", Help: "Last completed deployment gauge refresh attempt."})
var deploymentRefreshSuccess = promauto.NewGauge(prometheus.GaugeOpts{Name: "soak_control_deployment_refresh_last_success_unixtime", Help: "Last complete successful deployment gauge refresh."})
var deploymentRefreshHealthy = promauto.NewGauge(prometheus.GaugeOpts{Name: "soak_control_deployment_refresh_healthy", Help: "Whether the latest complete deployment gauge refresh succeeded."})

func observeDeploymentRefreshResult(err error) {
	deploymentRefreshAttempt.SetToCurrentTime()
	if err != nil {
		deploymentRefreshFailures.Inc()
		deploymentRefreshHealthy.Set(0)
		return
	}
	deploymentRefreshSuccess.SetToCurrentTime()
	deploymentRefreshHealthy.Set(1)
}

func (m *controlMetrics) refreshDeploymentMetrics(ctx context.Context, db *model.DB) error {
	sequence := m.latestDeploymentSequence.Add(1)
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		return err
	}
	var rollout *DeploymentRolloutResponse
	if deployment != nil {
		value, err := buildDeploymentRollout(db, *deployment)
		if err != nil {
			return err
		}
		rollout = &value
	}
	comparison, err := m.prepareLatestDeploymentComparison(db)
	if err != nil {
		return err
	}
	sources, err := m.prepareSourceComparisons(db)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if rollout != nil {
		m.publishDeploymentRollout(*rollout, sequence)
	}
	comparison()
	sources()
	return nil
}

var deploymentAlertRefreshFailures = promauto.NewCounterVec(prometheus.CounterOpts{Name: "soak_control_deployment_alert_refresh_failures_total", Help: "Failed deployment alert reads by source, retained after recovery."}, []string{"source"})

func (a *API) WaitForBackground(ctx context.Context) error {
	a.initializeDeploymentObserver()
	observer := a.deploymentObserver
	observer.cancel()
	rolloutErr := a.WaitForRollouts(ctx)
	observer.mu.Lock()
	running, done := observer.running, observer.done
	observer.mu.Unlock()
	if !running {
		return rolloutErr
	}
	select {
	case <-done:
		return rolloutErr
	case <-ctx.Done():
		return errors.Join(rolloutErr, ctx.Err())
	}
}
