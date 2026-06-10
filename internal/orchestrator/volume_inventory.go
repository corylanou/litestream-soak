package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
)

const workerVolumeNamePrefix = "soak_worker_"

func (m *Manager) RunVolumeInventoryMonitor(ctx context.Context, interval, unattachedVolumeTTL time.Duration) {
	if interval <= 0 || m.metrics == nil || m.fly == nil {
		return
	}

	m.syncVolumeInventory(ctx, unattachedVolumeTTL)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.syncVolumeInventory(ctx, unattachedVolumeTTL)
		}
	}
}

func (m *Manager) syncVolumeInventory(ctx context.Context, unattachedVolumeTTL time.Duration) {
	volumes, err := m.fly.ListVolumes(ctx)
	if err != nil {
		slog.Warn("Failed to list Fly volumes", "app", m.appName, "error", err)
		return
	}
	m.metrics.observeVolumes(m.appName, volumes)
	m.destroyStaleUnattachedWorkerVolumes(ctx, volumes, unattachedVolumeTTL)
}

func (m *Manager) destroyStaleUnattachedWorkerVolumes(ctx context.Context, volumes []flyapi.Volume, ttl time.Duration) {
	staleVolumes := staleUnattachedWorkerVolumes(volumes, time.Now().UTC(), ttl)
	for _, volume := range staleVolumes {
		if err := m.fly.DestroyVolume(ctx, volume.ID); err != nil {
			if flyapi.IsNotFound(err) {
				slog.Warn("Unattached worker volume already gone", "volume_id", volume.ID, "volume_name", volume.Name)
				continue
			}
			slog.Warn("Failed to destroy stale unattached worker volume", "volume_id", volume.ID, "volume_name", volume.Name, "size_gb", volume.SizeGB, "created_at", volume.CreatedAt, "error", err)
			continue
		}
		message := fmt.Sprintf("Destroyed unattached worker volume %s (%s, %dGB)", volume.Name, volume.ID, volume.SizeGB)
		if m.db != nil {
			_ = m.db.RecordEvent("", "volume_gc_destroyed", message, "")
		}
		slog.Info("Destroyed stale unattached worker volume", "volume_id", volume.ID, "volume_name", volume.Name, "size_gb", volume.SizeGB, "created_at", volume.CreatedAt)
	}
}

func staleUnattachedWorkerVolumes(volumes []flyapi.Volume, now time.Time, ttl time.Duration) []flyapi.Volume {
	if ttl <= 0 {
		return nil
	}

	cutoff := now.Add(-ttl)
	staleVolumes := make([]flyapi.Volume, 0)
	for _, volume := range volumes {
		if !isStaleUnattachedWorkerVolume(volume, cutoff) {
			continue
		}
		staleVolumes = append(staleVolumes, volume)
	}
	return staleVolumes
}

func isStaleUnattachedWorkerVolume(volume flyapi.Volume, cutoff time.Time) bool {
	if strings.TrimSpace(volume.AttachedMachineID) != "" {
		return false
	}
	if !strings.HasPrefix(strings.TrimSpace(volume.Name), workerVolumeNamePrefix) {
		return false
	}
	if volume.CreatedAt.IsZero() {
		return false
	}
	return !volume.CreatedAt.After(cutoff)
}
