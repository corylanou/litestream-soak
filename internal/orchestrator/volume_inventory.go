package orchestrator

import (
	"context"
	"log/slog"
	"time"
)

func (m *Manager) RunVolumeInventoryMonitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 || m.metrics == nil || m.fly == nil {
		return
	}

	m.syncVolumeInventory(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.syncVolumeInventory(ctx)
		}
	}
}

func (m *Manager) syncVolumeInventory(ctx context.Context) {
	volumes, err := m.fly.ListVolumes(ctx)
	if err != nil {
		slog.Warn("Failed to list Fly volumes", "app", m.appName, "error", err)
		return
	}
	m.metrics.observeVolumes(m.appName, volumes)
}
