package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
)

const workerVolumeNamePrefix = "soak_worker_"

type volumeInventoryProvider struct {
	client   *flyapi.Client
	mu       sync.Mutex
	cache    map[string]cachedVolumeInventory
	inFlight map[string]*volumeInventoryCall
}

type cachedVolumeInventory struct {
	volumes   []flyapi.Volume
	fetchedAt time.Time
}

type volumeInventoryCall struct {
	done    chan struct{}
	volumes []flyapi.Volume
	err     error
}

func newVolumeInventoryProvider(client *flyapi.Client) *volumeInventoryProvider {
	return &volumeInventoryProvider{
		client:   client,
		cache:    make(map[string]cachedVolumeInventory),
		inFlight: make(map[string]*volumeInventoryCall),
	}
}

func (p *volumeInventoryProvider) listFresh(ctx context.Context, appName string) ([]flyapi.Volume, error) {
	return p.list(ctx, appName, 0, true)
}

func (p *volumeInventoryProvider) listCached(ctx context.Context, appName string, maxAge time.Duration) ([]flyapi.Volume, error) {
	return p.list(ctx, appName, maxAge, false)
}

func (p *volumeInventoryProvider) list(ctx context.Context, appName string, maxAge time.Duration, force bool) ([]flyapi.Volume, error) {
	appName = strings.TrimSpace(appName)
	if appName == "" {
		appName = p.client.AppName()
	}

	p.mu.Lock()
	if !force {
		if cached, ok := p.cache[appName]; ok && maxAge > 0 && time.Since(cached.fetchedAt) <= maxAge {
			p.mu.Unlock()
			return cached.volumes, nil
		}
	}
	if call, ok := p.inFlight[appName]; ok {
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			return call.volumes, call.err
		}
	}

	call := &volumeInventoryCall{done: make(chan struct{})}
	p.inFlight[appName] = call
	p.mu.Unlock()

	volumes, err := p.client.ForApp(appName).ListVolumes(ctx)

	p.mu.Lock()
	call.volumes = volumes
	call.err = err
	if err == nil {
		p.cache[appName] = cachedVolumeInventory{
			volumes:   volumes,
			fetchedAt: time.Now(),
		}
	}
	delete(p.inFlight, appName)
	close(call.done)
	p.mu.Unlock()
	return volumes, err
}

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
	volumes, err := m.inventoryProvider().listFresh(ctx, m.appName)
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
