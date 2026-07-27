package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
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
	volumeGCEventConfirmFailed    = "volume_gc_confirmation_failed"
)

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

	if err := m.loadVolumeGCAttempts(); err != nil {
		slog.Warn("Failed to load persisted volume GC attempts; skipping deletion", "app", m.appName, "error", err)
		return
	}

	for volumeID, attempt := range m.volumeGCAttempts {
		volume, ok := inventory[volumeID]
		if !ok {
			m.verifyMissingVolumeGCAttempt(ctx, attempt, cutoff, now)
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

func (m *Manager) reconcileVolumeGCAttempt(ctx context.Context, volume flyapi.Volume, attempt *model.VolumeGCAttempt, cutoff, now time.Time) {
	state := normalizeVolumeState(volume.State)
	switch {
	case isVolumeDeletionState(state):
		if err := m.removeVolumeGCAttempt(volume.ID); err != nil {
			slog.Warn("Failed to clear persisted volume GC attempt after observing deletion state", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", state, "error", err)
			return
		}
		message := fmt.Sprintf("Destruction scheduled for stale unattached worker volume %s (%s, %dGB)", volume.Name, volume.ID, volume.SizeGB)
		details := fmt.Sprintf("state=%s requests=%d", state, attempt.RequestCount)
		m.recordVolumeGCEvent(volumeGCEventDestroyScheduled, message, details, now, 0)
		slog.Info("Stale unattached worker volume destruction scheduled", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", state, "request_count", attempt.RequestCount)
		return
	case state != volumeGCStateCreated:
		m.surfaceUnexpectedStaleVolumeState(volume, true, now)
		return
	case !isStaleUnattachedWorkerVolumeCandidate(volume, cutoff):
		message := fmt.Sprintf("Destroy request target %s (%s) remains created but is no longer eligible for volume GC", volume.Name, volume.ID)
		details := fmt.Sprintf("state=%s attached_machine_id=%s requests=%d", state, strings.TrimSpace(volume.AttachedMachineID), attempt.RequestCount)
		m.recordVolumeGCEvent(volumeGCEventDestroyStalled, message, details, now, volumeGCOperatorEventWindow)
		slog.Warn("Stale worker volume remains created after accepted destroy request but is no longer eligible for GC", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", state, "attached_machine_id", volume.AttachedMachineID, "request_count", attempt.RequestCount)
		return
	}

	if attempt.RequestAccepted {
		message := fmt.Sprintf("Stale unattached worker volume %s (%s) remains created after an accepted destroy request", volume.Name, volume.ID)
		details := fmt.Sprintf("state=%s requests=%d first_requested_at=%s last_requested_at=%s next_retry_at=%s", state, attempt.RequestCount, attempt.FirstAttemptAt.Format(time.RFC3339), attempt.LastAttemptAt.Format(time.RFC3339), attempt.NextRetryAt.Format(time.RFC3339))
		m.recordVolumeGCEvent(volumeGCEventDestroyStalled, message, details, now, volumeGCOperatorEventWindow)
		slog.Warn("Stale unattached worker volume remains created after accepted destroy request", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", state, "request_count", attempt.RequestCount, "first_requested_at", attempt.FirstAttemptAt, "last_requested_at", attempt.LastAttemptAt, "next_retry_at", attempt.NextRetryAt)
	}
	if now.Before(attempt.NextRetryAt) {
		return
	}
	m.requestStaleVolumeDestruction(ctx, volume, attempt, now)
}

func (m *Manager) requestStaleVolumeDestruction(ctx context.Context, volume flyapi.Volume, attempt *model.VolumeGCAttempt, now time.Time) {
	existing := attempt != nil
	if attempt == nil {
		attempt = &model.VolumeGCAttempt{
			VolumeID:        volume.ID,
			AppName:         m.appName,
			VolumeName:      volume.Name,
			Region:          volume.Region,
			SizeGB:          volume.SizeGB,
			VolumeCreatedAt: volume.CreatedAt,
			FirstAttemptAt:  now,
		}
		m.volumeGCAttempts[volume.ID] = attempt
	}

	previous := *attempt
	attempt.VolumeName = volume.Name
	attempt.Region = volume.Region
	attempt.SizeGB = volume.SizeGB
	attempt.VolumeCreatedAt = volume.CreatedAt
	attempt.LastAttemptAt = now
	attempt.RequestCount++
	attempt.RequestAccepted = false
	attempt.NextRetryAt = now.Add(volumeGCRetryBackoff(attempt.RequestCount))

	if err := m.persistVolumeGCAttempt(*attempt); err != nil {
		if existing {
			*attempt = previous
		} else {
			delete(m.volumeGCAttempts, volume.ID)
		}
		slog.Warn("Failed to persist volume GC attempt; skipping destroy request", "volume_id", volume.ID, "volume_name", volume.Name, "error", err)
		return
	}

	if err := m.fly.DestroyVolume(ctx, volume.ID); err != nil {
		if flyapi.IsNotFound(err) {
			if removeErr := m.removeVolumeGCAttempt(volume.ID); removeErr != nil {
				slog.Warn("Failed to clear persisted volume GC attempt after not-found response", "volume_id", volume.ID, "volume_name", volume.Name, "error", removeErr)
				return
			}
			message := fmt.Sprintf("Stale unattached worker volume %s (%s) was already absent when destruction was requested", volume.Name, volume.ID)
			m.recordVolumeGCEvent(volumeGCEventDestroyConfirmed, message, "result=not_found", now, 0)
			slog.Info("Stale unattached worker volume already absent at destroy time", "volume_id", volume.ID, "volume_name", volume.Name)
			return
		}

		message := fmt.Sprintf("Failed to request destruction of stale unattached worker volume %s (%s)", volume.Name, volume.ID)
		details := fmt.Sprintf("state=%s requests=%d next_retry_at=%s error=%v", normalizeVolumeState(volume.State), attempt.RequestCount, attempt.NextRetryAt.Format(time.RFC3339), err)
		m.recordVolumeGCEvent(volumeGCEventDestroyFailed, message, details, now, volumeGCOperatorEventWindow)
		slog.Warn("Failed to request destruction of stale unattached worker volume", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", normalizeVolumeState(volume.State), "size_gb", volume.SizeGB, "created_at", volume.CreatedAt, "request_count", attempt.RequestCount, "next_retry_at", attempt.NextRetryAt, "error", err)
		return
	}

	attempt.RequestAccepted = true
	if err := m.persistVolumeGCAttempt(*attempt); err != nil {
		slog.Warn("Destroy request succeeded but accepted state was not persisted", "volume_id", volume.ID, "volume_name", volume.Name, "error", err)
	}
	message := fmt.Sprintf("Destroy request accepted for stale unattached worker volume %s (%s, %dGB)", volume.Name, volume.ID, volume.SizeGB)
	details := fmt.Sprintf("state=%s requests=%d next_confirmation=next_fresh_inventory next_retry_at=%s", normalizeVolumeState(volume.State), attempt.RequestCount, attempt.NextRetryAt.Format(time.RFC3339))
	m.recordVolumeGCEvent(volumeGCEventDestroyRequested, message, details, now, 0)
	if attempt.RequestCount == 1 {
		slog.Info("Stale unattached worker volume destroy request accepted", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", normalizeVolumeState(volume.State), "size_gb", volume.SizeGB, "created_at", volume.CreatedAt, "next_confirmation", "next_fresh_inventory")
		return
	}
	slog.Warn("Retry destroy request accepted for stale unattached worker volume still in created state", "volume_id", volume.ID, "volume_name", volume.Name, "volume_state", normalizeVolumeState(volume.State), "request_count", attempt.RequestCount, "next_retry_at", attempt.NextRetryAt)
}

func (m *Manager) verifyMissingVolumeGCAttempt(ctx context.Context, attempt *model.VolumeGCAttempt, cutoff, now time.Time) {
	volume, err := m.fly.GetVolume(ctx, attempt.VolumeID)
	if err != nil {
		if flyapi.IsNotFound(err) {
			if removeErr := m.removeVolumeGCAttempt(attempt.VolumeID); removeErr != nil {
				slog.Warn("Failed to clear persisted volume GC attempt after confirmation", "volume_id", attempt.VolumeID, "volume_name", attempt.VolumeName, "error", removeErr)
				return
			}
			message := fmt.Sprintf("Fly confirmed stale unattached worker volume %s (%s) is absent", attempt.VolumeName, attempt.VolumeID)
			details := fmt.Sprintf("result=not_found requests=%d first_requested_at=%s last_requested_at=%s", attempt.RequestCount, attempt.FirstAttemptAt.Format(time.RFC3339), attempt.LastAttemptAt.Format(time.RFC3339))
			m.recordVolumeGCEvent(volumeGCEventDestroyConfirmed, message, details, now, 0)
			slog.Info("Fly confirmed stale unattached worker volume is absent", "volume_id", attempt.VolumeID, "volume_name", attempt.VolumeName, "request_count", attempt.RequestCount)
			return
		}
		message := fmt.Sprintf("Failed to verify stale unattached worker volume %s (%s) missing from inventory", attempt.VolumeName, attempt.VolumeID)
		details := fmt.Sprintf("requests=%d error=%v", attempt.RequestCount, err)
		m.recordVolumeGCEvent(volumeGCEventConfirmFailed, message, details, now, volumeGCOperatorEventWindow)
		slog.Warn("Failed to verify stale unattached worker volume missing from inventory", "volume_id", attempt.VolumeID, "volume_name", attempt.VolumeName, "request_count", attempt.RequestCount, "error", err)
		return
	}
	if strings.TrimSpace(volume.ID) != attempt.VolumeID {
		message := fmt.Sprintf("Fly returned invalid evidence while verifying stale unattached worker volume %s (%s)", attempt.VolumeName, attempt.VolumeID)
		details := fmt.Sprintf("requests=%d returned_volume_id=%s returned_state=%s", attempt.RequestCount, strings.TrimSpace(volume.ID), normalizeVolumeState(volume.State))
		m.recordVolumeGCEvent(volumeGCEventConfirmFailed, message, details, now, volumeGCOperatorEventWindow)
		slog.Warn("Fly returned invalid volume verification response", "volume_id", attempt.VolumeID, "volume_name", attempt.VolumeName, "returned_volume_id", volume.ID, "returned_state", normalizeVolumeState(volume.State))
		return
	}
	m.reconcileVolumeGCAttempt(ctx, *volume, attempt, cutoff, now)
}

func (m *Manager) loadVolumeGCAttempts() error {
	if m.volumeGCAttemptsLoaded {
		return nil
	}
	if m.volumeGCAttempts == nil {
		m.volumeGCAttempts = make(map[string]*model.VolumeGCAttempt)
	}
	if m.db != nil {
		attempts, err := m.db.ListVolumeGCAttempts(m.appName)
		if err != nil {
			return err
		}
		for i := range attempts {
			attempt := attempts[i]
			m.volumeGCAttempts[attempt.VolumeID] = &attempt
		}
	}
	m.volumeGCAttemptsLoaded = true
	return nil
}

func (m *Manager) persistVolumeGCAttempt(attempt model.VolumeGCAttempt) error {
	if m.db == nil {
		return nil
	}
	return m.db.UpsertVolumeGCAttempt(attempt)
}

func (m *Manager) removeVolumeGCAttempt(volumeID string) error {
	if m.db != nil {
		if err := m.db.DeleteVolumeGCAttempt(volumeID); err != nil {
			return err
		}
	}
	delete(m.volumeGCAttempts, volumeID)
	return nil
}

func (m *Manager) surfaceUnexpectedStaleVolumeState(volume flyapi.Volume, destroyRequested bool, now time.Time) {
	state := normalizeVolumeState(volume.State)
	controlVolumeGCSkipped.WithLabelValues(m.appName, volume.Region, volume.ID, volume.Name, state, fmt.Sprintf("%t", destroyRequested)).Inc()
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
