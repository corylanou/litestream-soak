package orchestrator

import (
	"context"
	"log/slog"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func (m *Manager) RunExpiryLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.cleanExpiredWorkers(ctx)
		}
	}
}

func (m *Manager) cleanExpiredWorkers(ctx context.Context) {
	workers, err := m.db.ListExpiredWorkers()
	if err != nil {
		slog.Error("Failed to list expired workers", "error", err)
		return
	}

	for _, w := range workers {
		slog.Info("Cleaning expired worker", "name", w.Name, "worker_id", w.ID, "expires_at", w.ExpiresAt)
		if err := m.DestroyWorker(ctx, w.ID); err != nil {
			slog.Error("Failed to destroy expired worker", "worker_id", w.ID, "error", err)
		}
	}
}

func (m *Manager) RunHeartbeatMonitor(ctx context.Context, timeout time.Duration) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkStaleWorkers(ctx, timeout)
		}
	}
}

func (m *Manager) checkStaleWorkers(ctx context.Context, timeout time.Duration) {
	workers, err := m.db.StaleWorkers(timeout)
	if err != nil {
		slog.Error("Failed to list stale workers", "error", err)
		return
	}

	for _, w := range workers {
		slog.Warn("Stale worker detected", "name", w.Name, "worker_id", w.ID, "last_heartbeat", w.LastHeartbeatAt)
		if err := m.db.UpdateWorkerStatus(w.ID, model.WorkerDegraded, "worker missed heartbeat deadline"); err != nil {
			slog.Error("Failed to update stale worker status", "worker_id", w.ID, "error", err)
			continue
		}
		m.db.RecordEvent(w.ID, "worker_stale", "Worker missed heartbeat deadline", "")
		m.observeWorkerByID(w.ID)
	}
}
