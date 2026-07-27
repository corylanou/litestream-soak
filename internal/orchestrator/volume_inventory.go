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

const (
	workerVolumeNamePrefix        = "soak_worker_"
	volumeGCInitialRetryBackoff   = time.Hour
	volumeGCMaximumRetryBackoff   = 24 * time.Hour
	volumeGCOperatorEventWindow   = 24 * time.Hour
	volumeGCStateCreated          = "created"
	volumeGCStateUnknown          = "unknown"
	volumeGCEventDestroyRequested = "volume_gc_destroy_requested"
	volumeGCEventDestroyScheduled = "volume_gc_destroy_scheduled"
	volumeGCEventDestroyConfirmed = "volume_gc_destroy_confirmed"
	volumeGCEventDestroyFailed    = "volume_gc_destroy_failed"
	volumeGCEventDestroyStalled   = "volume_gc_destroy_stalled"
	volumeGCEventUnexpectedState  = "volume_gc_skipped_state"
)

type volumeGCAttempt struct {
	volume              flyapi.Volume
	firstAttemptAt      time.Time
	lastAttemptAt       time.Time
	nextRetryAt         time.Time
	requestCount        int
	requestAccepted     bool
	transitionConfirmed bool
}

type volumeInventoryProvider struct {
	client   *flyapi.Client
	mu       sync.Mutex
	cache    map[string]cachedVolumeInventory
	inFlight map[string]volumeInventoryFlights
}

type cachedVolumeInventory struct {
	volumes   []flyapi.Volume
	startedAt time.Time
}

type volumeInventoryFlights struct {
	cached *volumeInventoryCall
	fresh  *volumeInventoryCall
}

type volumeInventoryCall struct {
	done      chan struct{}
	startedAt time.Time
	volumes   []flyapi.Volume
	err       error
}

func newVolumeInventoryProvider(client *flyapi.Client) *volumeInventoryProvider {
	return &volumeInventoryProvider{
		client:   client,
		cache:    make(map[string]cachedVolumeInventory),
		inFlight: make(map[string]volumeInventoryFlights),
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
		if cached, ok := p.cache[appName]; ok && maxAge > 0 && time.Since(cached.startedAt) <= maxAge {
			p.mu.Unlock()
			return cached.volumes, nil
		}
	}
	flights := p.inFlight[appName]
	call := flights.fresh
	if call == nil && !force {
		call = flights.cached
	}
	if call != nil {
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
			return call.volumes, call.err
		}
	}

	call = &volumeInventoryCall{
		done:      make(chan struct{}),
		startedAt: time.Now(),
	}
	if force {
		flights.fresh = call
	} else {
		flights.cached = call
	}
	p.inFlight[appName] = flights
	p.mu.Unlock()

	volumes, err := p.client.ForApp(appName).ListVolumes(ctx)

	p.mu.Lock()
	call.volumes = volumes
	call.err = err
	if err == nil {
		cached, ok := p.cache[appName]
		if !ok || !call.startedAt.Before(cached.startedAt) {
			p.cache[appName] = cachedVolumeInventory{
				volumes:   volumes,
				startedAt: call.startedAt,
			}
		}
	}
	flights = p.inFlight[appName]
	if force && flights.fresh == call {
		flights.fresh = nil
	}
	if !force && flights.cached == call {
		flights.cached = nil
	}
	if flights.cached == nil && flights.fresh == nil {
		delete(p.inFlight, appName)
	} else {
		p.inFlight[appName] = flights
	}
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
	if ttl <= 0 {
		return
	}

	now := time.Now().UTC()
	cutoff := now.Add(-ttl)
	inventory := make(map[string]flyapi.Volume, len(volumes))
	for _, volume := range volumes {
		inventory[volume.ID] = volume
	}

	m.volumeGCMu.Lock()
	defer m.volumeGCMu.Unlock()

	for volumeID, attempt := range m.volumeGCAttempts {
		volume, ok := inventory[volumeID]
		if !ok {
			m.confirmStaleVolumeAbsent(*attempt, now)
			delete(m.volumeGCAttempts, volumeID)
			continue
		}
		m.reconcileVolumeGCAttempt(ctx, volume, attempt, cutoff, now)
	}

	for _, volume := range volumes {
		if !isStaleUnattachedWorkerVolumeCandidate(volume, cutoff) {
			continue
		}
		if _, ok := m.volumeGCAttempts[volume.ID]; ok {
			continue
		}

		state := normalizeVolumeState(volume.State)
		switch {
		case state == volumeGCStateCreated:
			m.requestStaleVolumeDestruction(ctx, volume, nil, now)
		case isVolumeDeletionState(state):
			continue
		default:
			m.surfaceUnexpectedStaleVolumeState(volume, false, now)
		}
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
	return isStaleUnattachedWorkerVolumeCandidate(volume, cutoff) &&
		normalizeVolumeState(volume.State) == volumeGCStateCreated
}

func isStaleUnattachedWorkerVolumeCandidate(volume flyapi.Volume, cutoff time.Time) bool {
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

func (m *Manager) reconcileVolumeGCAttempt(ctx context.Context, volume flyapi.Volume, attempt *volumeGCAttempt, cutoff, now time.Time) {
	state := normalizeVolumeState(volume.State)
	switch {
	case isVolumeDeletionState(state):
		if !attempt.transitionConfirmed {
			attempt.transitionConfirmed = true
			attempt.volume = volume
			message := fmt.Sprintf("Destruction scheduled for stale unattached worker volume %s (%s, %dGB)", volume.Name, volume.ID, volume.SizeGB)
			details := fmt.Sprintf("state=%s requests=%d", state, attempt.requestCount)
			m.recordVolumeGCEvent(volumeGCEventDestroyScheduled, message, details, now, 0)
			slog.Info("Stale unattached worker volume destruction scheduled", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", state, "request_count", attempt.requestCount)
		}
		return
	case state != volumeGCStateCreated:
		m.surfaceUnexpectedStaleVolumeState(volume, true, now)
		return
	case !isStaleUnattachedWorkerVolumeCandidate(volume, cutoff):
		message := fmt.Sprintf("Destroy request target %s (%s) remains created but is no longer eligible for volume GC", volume.Name, volume.ID)
		details := fmt.Sprintf("state=%s attached_machine_id=%s requests=%d", state, strings.TrimSpace(volume.AttachedMachineID), attempt.requestCount)
		m.recordVolumeGCEvent(volumeGCEventDestroyStalled, message, details, now, volumeGCOperatorEventWindow)
		slog.Warn("Stale worker volume remains created after accepted destroy request but is no longer eligible for GC", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", state, "attached_machine_id", volume.AttachedMachineID, "request_count", attempt.requestCount)
		return
	}

	if attempt.requestAccepted {
		message := fmt.Sprintf("Stale unattached worker volume %s (%s) remains created after an accepted destroy request", volume.Name, volume.ID)
		details := fmt.Sprintf("state=%s requests=%d first_requested_at=%s last_requested_at=%s next_retry_at=%s", state, attempt.requestCount, attempt.firstAttemptAt.Format(time.RFC3339), attempt.lastAttemptAt.Format(time.RFC3339), attempt.nextRetryAt.Format(time.RFC3339))
		m.recordVolumeGCEvent(volumeGCEventDestroyStalled, message, details, now, volumeGCOperatorEventWindow)
		slog.Warn("Stale unattached worker volume remains created after accepted destroy request", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", state, "request_count", attempt.requestCount, "first_requested_at", attempt.firstAttemptAt, "last_requested_at", attempt.lastAttemptAt, "next_retry_at", attempt.nextRetryAt)
	}
	if now.Before(attempt.nextRetryAt) {
		return
	}
	m.requestStaleVolumeDestruction(ctx, volume, attempt, now)
}

func (m *Manager) requestStaleVolumeDestruction(ctx context.Context, volume flyapi.Volume, attempt *volumeGCAttempt, now time.Time) {
	if attempt == nil {
		attempt = &volumeGCAttempt{
			volume:         volume,
			firstAttemptAt: now,
		}
		if m.volumeGCAttempts == nil {
			m.volumeGCAttempts = make(map[string]*volumeGCAttempt)
		}
		m.volumeGCAttempts[volume.ID] = attempt
	}

	attempt.volume = volume
	attempt.lastAttemptAt = now
	attempt.requestCount++
	attempt.nextRetryAt = now.Add(volumeGCRetryBackoff(attempt.requestCount))
	attempt.transitionConfirmed = false

	if err := m.fly.DestroyVolume(ctx, volume.ID); err != nil {
		attempt.requestAccepted = false
		if flyapi.IsNotFound(err) {
			message := fmt.Sprintf("Stale unattached worker volume %s (%s) was already absent when destruction was requested", volume.Name, volume.ID)
			m.recordVolumeGCEvent(volumeGCEventDestroyConfirmed, message, "result=not_found", now, 0)
			slog.Info("Stale unattached worker volume already absent at destroy time", "volume_id", volume.ID, "volume_name", volume.Name)
			delete(m.volumeGCAttempts, volume.ID)
			return
		}

		message := fmt.Sprintf("Failed to request destruction of stale unattached worker volume %s (%s)", volume.Name, volume.ID)
		details := fmt.Sprintf("state=%s requests=%d next_retry_at=%s error=%v", normalizeVolumeState(volume.State), attempt.requestCount, attempt.nextRetryAt.Format(time.RFC3339), err)
		m.recordVolumeGCEvent(volumeGCEventDestroyFailed, message, details, now, volumeGCOperatorEventWindow)
		slog.Warn("Failed to request destruction of stale unattached worker volume", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", normalizeVolumeState(volume.State), "size_gb", volume.SizeGB, "created_at", volume.CreatedAt, "request_count", attempt.requestCount, "next_retry_at", attempt.nextRetryAt, "error", err)
		return
	}

	attempt.requestAccepted = true
	message := fmt.Sprintf("Destroy request accepted for stale unattached worker volume %s (%s, %dGB)", volume.Name, volume.ID, volume.SizeGB)
	details := fmt.Sprintf("state=%s requests=%d next_confirmation=next_fresh_inventory next_retry_at=%s", normalizeVolumeState(volume.State), attempt.requestCount, attempt.nextRetryAt.Format(time.RFC3339))
	m.recordVolumeGCEvent(volumeGCEventDestroyRequested, message, details, now, 0)
	if attempt.requestCount == 1 {
		slog.Info("Stale unattached worker volume destroy request accepted", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", normalizeVolumeState(volume.State), "size_gb", volume.SizeGB, "created_at", volume.CreatedAt, "next_confirmation", "next_fresh_inventory")
		return
	}
	slog.Warn("Retry destroy request accepted for stale unattached worker volume still in created state", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", normalizeVolumeState(volume.State), "request_count", attempt.requestCount, "next_retry_at", attempt.nextRetryAt)
}

func (m *Manager) confirmStaleVolumeAbsent(attempt volumeGCAttempt, now time.Time) {
	message := fmt.Sprintf("Stale unattached worker volume %s (%s) is no longer present after a destroy request", attempt.volume.Name, attempt.volume.ID)
	details := fmt.Sprintf("requests=%d first_requested_at=%s last_requested_at=%s", attempt.requestCount, attempt.firstAttemptAt.Format(time.RFC3339), attempt.lastAttemptAt.Format(time.RFC3339))
	m.recordVolumeGCEvent(volumeGCEventDestroyConfirmed, message, details, now, 0)
	slog.Info("Stale unattached worker volume no longer present after destroy request", "volume_id", attempt.volume.ID, "volume_name", attempt.volume.Name, "request_count", attempt.requestCount)
}

func (m *Manager) surfaceUnexpectedStaleVolumeState(volume flyapi.Volume, destroyRequested bool, now time.Time) {
	state := normalizeVolumeState(volume.State)
	message := fmt.Sprintf("Skipped stale unattached worker volume %s (%s) in unexpected state %s", volume.Name, volume.ID, state)
	details := fmt.Sprintf("state=%s destroy_requested=%t created_at=%s", state, destroyRequested, volume.CreatedAt.Format(time.RFC3339))
	m.recordVolumeGCEvent(volumeGCEventUnexpectedState, message, details, now, volumeGCOperatorEventWindow)
	slog.Warn("Skipping stale unattached worker volume in unexpected state", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", state, "size_gb", volume.SizeGB, "created_at", volume.CreatedAt, "destroy_requested", destroyRequested)
}

func (m *Manager) recordVolumeGCEvent(eventType, message, details string, now time.Time, window time.Duration) {
	if m.db == nil {
		return
	}

	var err error
	if window > 0 {
		_, err = m.db.RecordWindowedEventAt("", eventType, message, details, now, window)
	} else {
		err = m.db.RecordEventAt("", eventType, message, details, now)
	}
	if err != nil {
		slog.Warn("Failed to record volume GC event", "event_type", eventType, "error", err)
	}
}

func volumeGCRetryBackoff(requestCount int) time.Duration {
	backoff := volumeGCInitialRetryBackoff
	for request := 1; request < requestCount; request++ {
		if backoff >= volumeGCMaximumRetryBackoff/2 {
			return volumeGCMaximumRetryBackoff
		}
		backoff *= 2
	}
	return backoff
}

func normalizeVolumeState(state string) string {
	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		return volumeGCStateUnknown
	}
	return state
}

func isVolumeDeletionState(state string) bool {
	switch normalizeVolumeState(state) {
	case "destroyed", "deleting", "scheduling_destroy", "pending_destroy":
		return true
	default:
		return false
	}
}
