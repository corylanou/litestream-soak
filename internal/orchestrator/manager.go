package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/s3util"
	"github.com/corylanou/litestream-soak/internal/workload"
	"github.com/google/uuid"
)

type WorkerRequest struct {
	WorkerID      string
	Name          string
	Source        string
	GitSHA        string
	LitestreamSHA string
	PRNumber      int
	ProfileName   string
	ImageRef      string
	Region        string
	VolumeSizeGB  int
	ExpiresAt     *time.Time
	Workload      workload.Config
}

type ReplicaConfig struct {
	Bucket    string
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
}

type contextLock struct {
	available chan struct{}
}

func newContextLock() *contextLock {
	lock := &contextLock{available: make(chan struct{}, 1)}
	lock.available <- struct{}{}
	return lock
}

func (l *contextLock) lock(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.available:
		if err := ctx.Err(); err != nil {
			l.available <- struct{}{}
			return nil, err
		}
		return func() { l.available <- struct{}{} }, nil
	}
}

type Manager struct {
	fly              *flyapi.Client
	db               *model.DB
	metrics          *controlMetrics
	alerts           *AlertDispatcher
	appName          string
	replica          ReplicaConfig
	controlBaseURL   string
	platformLogToken string

	locks       sync.Mutex
	workerLocks map[string]*sync.Mutex
	sourceLocks map[string]*contextLock
}

func (m *Manager) keyedLock(table *map[string]*sync.Mutex, key string) func() {
	m.locks.Lock()
	if *table == nil {
		*table = make(map[string]*sync.Mutex)
	}
	mu, ok := (*table)[key]
	if !ok {
		mu = &sync.Mutex{}
		(*table)[key] = mu
	}
	m.locks.Unlock()

	mu.Lock()
	return mu.Unlock
}

func (m *Manager) lockWorker(id string) func() {
	return m.keyedLock(&m.workerLocks, id)
}

func (m *Manager) lockSource(ctx context.Context, source string) (func(), error) {
	m.locks.Lock()
	if m.sourceLocks == nil {
		m.sourceLocks = make(map[string]*contextLock)
	}
	lock, ok := m.sourceLocks[source]
	if !ok {
		lock = newContextLock()
		m.sourceLocks[source] = lock
	}
	m.locks.Unlock()

	unlock, err := lock.lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire source lock for %s: %w", source, err)
	}
	return unlock, nil
}

func NewManager(fly *flyapi.Client, db *model.DB, metrics *controlMetrics, alerts *AlertDispatcher, appName string, replica ReplicaConfig, controlBaseURL, platformLogToken string) *Manager {
	return &Manager{
		fly:              fly,
		db:               db,
		metrics:          metrics,
		alerts:           alerts,
		appName:          appName,
		replica:          replica,
		controlBaseURL:   controlBaseURL,
		platformLogToken: platformLogToken,
	}
}

func (m *Manager) newWorkerRecord(req WorkerRequest) *model.Worker {
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		workerID = uuid.New().String()
	}
	region := req.Region
	if region == "" {
		region = "ord"
	}

	return &model.Worker{
		ID:            workerID,
		AppName:       m.appName,
		Name:          req.Name,
		Status:        model.WorkerPending,
		Source:        req.Source,
		GitSHA:        req.GitSHA,
		LitestreamSHA: req.LitestreamSHA,
		PRNumber:      req.PRNumber,
		ProfileName:   req.ProfileName,
		ProfileConfig: normalizeWorkloadConfig(req.Workload).JSON(),
		Region:        region,
		ExpiresAt:     req.ExpiresAt,
	}
}

func (m *Manager) CreateWorker(ctx context.Context, req WorkerRequest) (*model.Worker, error) {
	if id := strings.TrimSpace(req.WorkerID); id != "" {
		unlock := m.lockWorker(id)
		defer unlock()
	}
	return m.createWorker(ctx, req)
}

func (m *Manager) createWorker(ctx context.Context, req WorkerRequest) (*model.Worker, error) {
	worker := m.newWorkerRecord(req)
	workerID := worker.ID
	workloadCfg := normalizeWorkloadConfig(req.Workload)

	if err := m.db.CreateWorker(worker); err != nil {
		return nil, fmt.Errorf("create worker record: %w", err)
	}
	m.observeWorkerByID(workerID)

	if err := m.db.RecordEvent(workerID, "worker_creating", fmt.Sprintf("Creating worker %s with profile %s", req.Name, req.ProfileName), ""); err != nil {
		return nil, fmt.Errorf("record worker creating event: %w", err)
	}

	volSize := req.VolumeSizeGB
	if volSize == 0 {
		volSize = 10
	}

	vol, err := m.createWorkerVolume(ctx, *worker, volSize)
	if err != nil {
		if updateErr := m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error()); updateErr != nil {
			return nil, fmt.Errorf("create volume: %w", errors.Join(err, fmt.Errorf("mark worker failed: %w", updateErr)))
		}
		m.observeWorkerByID(workerID)
		return nil, fmt.Errorf("create volume: %w", err)
	}
	worker.FlyVolumeID = vol.ID

	if err := m.clearWorkerReplicaPrefix(ctx, *worker); err != nil {
		if destroyErr := m.fly.DestroyVolume(ctx, vol.ID); destroyErr != nil && !flyapi.IsNotFound(destroyErr) {
			slog.Warn("Failed to destroy worker volume after replica prefix clear failure", "worker_id", workerID, "volume_id", vol.ID, "error", destroyErr)
		}
		if updateErr := m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error()); updateErr != nil {
			return nil, fmt.Errorf("clear replica prefix: %w", errors.Join(err, fmt.Errorf("mark worker failed: %w", updateErr)))
		}
		m.observeWorkerByID(workerID)
		return nil, fmt.Errorf("clear replica prefix: %w", err)
	}

	machine, err := m.createWorkerMachine(ctx, *worker, req.ImageRef, vol.ID, workloadCfg)
	if err != nil {
		if destroyErr := m.fly.DestroyVolume(ctx, vol.ID); destroyErr != nil && !flyapi.IsNotFound(destroyErr) {
			slog.Warn("Failed to destroy worker volume after machine creation failure", "worker_id", workerID, "volume_id", vol.ID, "error", destroyErr)
		}
		if updateErr := m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error()); updateErr != nil {
			return nil, fmt.Errorf("create machine: %w", errors.Join(err, fmt.Errorf("mark worker failed: %w", updateErr)))
		}
		m.observeWorkerByID(workerID)
		return nil, fmt.Errorf("create machine: %w", err)
	}

	if err := m.db.UpdateWorkerMachine(workerID, machine.ID, vol.ID); err != nil {
		if destroyErr := m.fly.DestroyMachine(ctx, machine.ID, true); destroyErr != nil && !flyapi.IsNotFound(destroyErr) {
			slog.Warn("Failed to destroy worker machine after database update failure", "worker_id", workerID, "machine_id", machine.ID, "error", destroyErr)
		}
		if destroyErr := m.fly.DestroyVolume(ctx, vol.ID); destroyErr != nil && !flyapi.IsNotFound(destroyErr) {
			slog.Warn("Failed to destroy worker volume after database update failure", "worker_id", workerID, "volume_id", vol.ID, "error", destroyErr)
		}
		return nil, fmt.Errorf("update worker machine: %w", err)
	}
	if err := m.db.UpdateWorkerStatus(workerID, model.WorkerRunning, ""); err != nil {
		return nil, fmt.Errorf("mark worker running: %w", err)
	}
	m.observeWorkerByID(workerID)
	_ = m.db.RecordEvent(workerID, "worker_started", fmt.Sprintf("Worker %s started (machine %s)", req.Name, machine.ID), "")

	slog.Info("Worker created", "name", req.Name, "machine_id", machine.ID, "volume_id", vol.ID, "profile", req.ProfileName)

	return worker, nil
}

func (m *Manager) createWorkerVolume(ctx context.Context, worker model.Worker, volSize int) (*flyapi.Volume, error) {
	sourceWorker, freshReason, err := m.forkSourceForWorker(worker)
	if err != nil {
		return nil, err
	}

	volumeName := flyVolumeName(worker.Name)
	client := m.flyClientForWorker(worker)
	if sourceWorker != nil {
		vol, err := client.ForkVolume(ctx, sourceWorker.FlyVolumeID, volumeName)
		if err == nil {
			m.recordWorkerVolumeForked(worker, *sourceWorker, vol.ID)
			return vol, nil
		}
		freshReason = "fork_volume_failed"
		slog.Warn("Failed to fork worker volume; creating fresh volume", "worker_id", worker.ID, "source_worker_id", sourceWorker.ID, "source_volume_id", sourceWorker.FlyVolumeID, "error", err)
	}

	vol, err := client.CreateVolume(ctx, flyapi.CreateVolumeRequest{
		Name:      volumeName,
		SizeGB:    volSize,
		Region:    worker.Region,
		Encrypted: true,
	})
	if err != nil {
		return nil, err
	}
	m.recordWorkerVolumeFresh(worker, freshReason, sourceWorker, vol.ID)
	return vol, nil
}

func (m *Manager) forkSourceForWorker(worker model.Worker) (*model.Worker, string, error) {
	source := strings.TrimSpace(worker.Source)
	if source == "" || source == "main" {
		return nil, "main_source", nil
	}

	mainWorkers, err := m.db.ListWorkersForSource("main")
	if err != nil {
		return nil, "", fmt.Errorf("list main workers for volume fork: %w", err)
	}

	profileName := strings.TrimSpace(worker.ProfileName)
	region := strings.TrimSpace(worker.Region)
	reason := "no_matching_main_worker"
	for i := range mainWorkers {
		candidate := mainWorkers[i]
		if strings.TrimSpace(candidate.ProfileName) != profileName {
			continue
		}
		reason = "main_worker_not_running"
		if candidate.Status != model.WorkerRunning {
			continue
		}
		reason = "main_worker_missing_volume"
		if strings.TrimSpace(candidate.FlyVolumeID) == "" {
			continue
		}
		reason = "main_worker_region_mismatch"
		if strings.TrimSpace(candidate.Region) != region {
			continue
		}
		return &candidate, "", nil
	}
	return nil, reason, nil
}

func (m *Manager) recordWorkerVolumeForked(worker, sourceWorker model.Worker, volumeID string) {
	if m.db == nil {
		return
	}
	details := workerVolumeEventDetails("fork", "", volumeID, &sourceWorker)
	_ = m.db.RecordEvent(worker.ID, "worker_volume_forked", fmt.Sprintf("Forked volume for worker %s from main worker %s", worker.Name, sourceWorker.Name), details)
}

func (m *Manager) recordWorkerVolumeFresh(worker model.Worker, reason string, sourceWorker *model.Worker, volumeID string) {
	if m.db == nil {
		return
	}
	details := workerVolumeEventDetails("fresh", reason, volumeID, sourceWorker)
	_ = m.db.RecordEvent(worker.ID, "worker_volume_fresh", fmt.Sprintf("Created fresh volume for worker %s", worker.Name), details)
}

func workerVolumeEventDetails(path, reason, volumeID string, sourceWorker *model.Worker) string {
	details := map[string]string{
		"path":      path,
		"volume_id": volumeID,
	}
	if strings.TrimSpace(reason) != "" {
		details["reason"] = reason
	}
	if sourceWorker != nil {
		details["source_worker_id"] = sourceWorker.ID
		details["source_worker_name"] = sourceWorker.Name
		details["source_volume_id"] = sourceWorker.FlyVolumeID
	}
	body, err := json.Marshal(details)
	if err != nil {
		return ""
	}
	return string(body)
}

func workerReplicaPath(worker model.Worker) string {
	base := fmt.Sprintf("soak/%s", worker.Name)
	if volumeID := strings.Trim(strings.TrimSpace(worker.FlyVolumeID), "/"); volumeID != "" {
		return fmt.Sprintf("%s/%s", base, volumeID)
	}
	return base
}

func (m *Manager) clearWorkerReplicaPrefix(ctx context.Context, worker model.Worker) error {
	bucket := strings.TrimSpace(m.replica.Bucket)
	if bucket == "" {
		return nil
	}

	prefix := strings.Trim(workerReplicaPath(worker), "/")
	if prefix == "" {
		return fmt.Errorf("worker replica prefix is empty")
	}

	clearCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	replicaURL := fmt.Sprintf("s3://%s/%s/", bucket, prefix)
	deleted, err := s3util.DeletePrefix(clearCtx, s3util.Config{
		Bucket:    bucket,
		Endpoint:  m.replica.Endpoint,
		AccessKey: m.replica.AccessKey,
		SecretKey: m.replica.SecretKey,
		Region:    m.replica.Region,
	}, prefix)
	if clearCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("timed out clearing %s", replicaURL)
	}
	if err != nil {
		return fmt.Errorf("clear %s: %w", replicaURL, err)
	}

	if m.db != nil {
		_ = m.db.RecordEvent(worker.ID, "replica_prefix_cleared", fmt.Sprintf("Cleared %d object(s) from replica prefix %s", deleted, replicaURL), "")
	}
	slog.Info("Cleared worker replica prefix", "worker_id", worker.ID, "prefix", replicaURL, "objects", deleted)
	return nil
}

func (m *Manager) clearOldWorkerReplicaPrefix(ctx context.Context, w model.Worker) {
	if strings.TrimSpace(w.FlyVolumeID) == "" {
		return
	}
	if err := m.clearWorkerReplicaPrefix(ctx, w); err != nil {
		slog.Warn("Failed to clear old worker replica prefix during update", "worker_id", w.ID, "error", err)
	}
}

func flyVolumeName(workerName string) string {
	var b strings.Builder
	b.Grow(len(workerName) + 5)
	b.WriteString("soak_")

	lastUnderscore := true
	for _, r := range strings.ToLower(workerName) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastUnderscore = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}

	name := strings.Trim(b.String(), "_")
	if name == "" {
		name = "soak_worker"
	}
	if len(name) <= 30 {
		return name
	}

	sum := fnv.New32a()
	_, _ = sum.Write([]byte(workerName))
	suffix := fmt.Sprintf("%05x", sum.Sum32()%0x100000)
	return fmt.Sprintf("%s_%s", name[:24], suffix)
}

func (m *Manager) StopWorker(ctx context.Context, workerID string) error {
	unlock := m.lockWorker(workerID)
	defer unlock()
	return m.stopWorker(ctx, workerID)
}

func (m *Manager) stopWorker(ctx context.Context, workerID string) error {
	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return fmt.Errorf("get worker: %w", err)
	}

	if worker.FlyMachineID != "" {
		if err := m.fly.StopMachine(ctx, worker.FlyMachineID); err != nil {
			slog.Warn("Failed to stop machine", "machine_id", worker.FlyMachineID, "error", err)
		}
	}

	if err := m.db.UpdateWorkerStatus(workerID, model.WorkerStopped, ""); err != nil {
		return fmt.Errorf("mark worker stopped: %w", err)
	}
	m.observeWorkerByID(workerID)
	_ = m.db.RecordEvent(workerID, "worker_stopped", fmt.Sprintf("Worker %s stopped", worker.Name), "")
	return nil
}

func (m *Manager) DestroyWorker(ctx context.Context, workerID string) error {
	unlock := m.lockWorker(workerID)
	defer unlock()
	return m.destroyWorker(ctx, workerID)
}

func (m *Manager) destroyWorker(ctx context.Context, workerID string) error {
	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return fmt.Errorf("get worker: %w", err)
	}

	if worker.FlyMachineID != "" {
		if err := m.fly.DestroyMachine(ctx, worker.FlyMachineID, true); err != nil {
			slog.Warn("Failed to destroy machine", "machine_id", worker.FlyMachineID, "error", err)
		}
	}

	if worker.FlyVolumeID != "" {
		if err := m.fly.DestroyVolume(ctx, worker.FlyVolumeID); err != nil {
			slog.Warn("Failed to destroy volume", "volume_id", worker.FlyVolumeID, "error", err)
		}
	}
	if err := m.clearWorkerReplicaPrefix(ctx, *worker); err != nil {
		slog.Warn("Failed to clear worker replica prefix", "worker_id", workerID, "error", err)
	}

	if err := m.db.UpdateWorkerStatus(workerID, model.WorkerStopped, ""); err != nil {
		return fmt.Errorf("mark worker stopped: %w", err)
	}
	m.observeWorkerByID(workerID)
	_ = m.db.RecordEvent(workerID, "worker_destroyed", fmt.Sprintf("Worker %s destroyed", worker.Name), "")
	return nil
}

func (m *Manager) RollingUpdate(ctx context.Context, newImageRef, newSHA, newLitestreamSHA string) error {
	return m.RollingUpdateSource(ctx, "main", newImageRef, newSHA, newLitestreamSHA)
}

func (m *Manager) RollingUpdateSource(ctx context.Context, source, newImageRef, newSHA, newLitestreamSHA string) error {
	source = firstNonEmpty(strings.TrimSpace(source), "main")

	unlockSource, err := m.lockSource(ctx, source)
	if err != nil {
		return err
	}
	defer unlockSource()

	return m.rollingUpdateSourceLocked(ctx, source, newImageRef, newSHA, newLitestreamSHA)
}

func (m *Manager) rollingUpdateSourceLocked(ctx context.Context, source, newImageRef, newSHA, newLitestreamSHA string) error {
	current, latest, err := m.latestReadyDeploymentMatches(source, newImageRef, newSHA, newLitestreamSHA)
	if err != nil {
		return err
	}
	if !current {
		slog.Info("Skipping superseded rolling update", "source", source, "sha", newSHA, "litestream_sha", newLitestreamSHA, "image", newImageRef, "latest_sha", latest.GitSHA, "latest_litestream_sha", latest.LitestreamSHA, "latest_image", latest.ImageRef)
		return nil
	}

	workers, err := m.db.ListWorkersForSource(source)
	if err != nil {
		return fmt.Errorf("list %s workers: %w", source, err)
	}

	deployment := model.Deployment{GitSHA: newSHA, LitestreamSHA: newLitestreamSHA}
	slog.Info("Starting rolling update", "source", source, "workers", len(workers), "sha", newSHA, "image", newImageRef)

	for _, listed := range workers {
		newWorker, err := func() (*model.Worker, error) {
			unlock := m.lockWorker(listed.ID)
			defer unlock()

			w, err := m.db.GetWorker(listed.ID)
			if err != nil {
				return nil, fmt.Errorf("reload worker: %w", err)
			}
			if w.Status == model.WorkerStopped || w.Status == model.WorkerFailed || w.Status == model.WorkerDormant {
				return nil, nil
			}
			if workerMatchesDeployment(*w, deployment) {
				return nil, nil
			}
			return m.replaceWorker(ctx, *w, newImageRef, newSHA, newLitestreamSHA)
		}()
		if err != nil {
			slog.Error("Failed to create updated worker", "name", listed.Name, "error", err)
			continue
		}
		if newWorker != nil {
			slog.Info("Worker updated", "name", listed.Name, "new_id", newWorker.ID)
		}
	}

	return nil
}

func (m *Manager) latestReadyDeploymentMatches(source, imageRef, gitSHA, litestreamSHA string) (bool, *model.Deployment, error) {
	latest, err := m.db.GetLatestReadyDeployment(source)
	if err != nil {
		return false, nil, fmt.Errorf("get latest ready deployment for %s: %w", source, err)
	}
	if latest == nil {
		return true, nil, nil
	}
	if deploymentMatchesTarget(*latest, imageRef, gitSHA, litestreamSHA) {
		return true, latest, nil
	}
	return false, latest, nil
}

func deploymentMatchesTarget(deployment model.Deployment, imageRef, gitSHA, litestreamSHA string) bool {
	return strings.TrimSpace(deployment.ImageRef) == strings.TrimSpace(imageRef) &&
		strings.TrimSpace(deployment.GitSHA) == strings.TrimSpace(gitSHA) &&
		strings.TrimSpace(deployment.LitestreamSHA) == strings.TrimSpace(litestreamSHA)
}

func (m *Manager) RollWorker(ctx context.Context, workerID, newImageRef, newSHA, newLitestreamSHA string) (*model.Worker, error) {
	unlock := m.lockWorker(workerID)
	defer unlock()

	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return nil, fmt.Errorf("get worker: %w", err)
	}
	if workerMatchesDeployment(*worker, model.Deployment{GitSHA: newSHA, LitestreamSHA: newLitestreamSHA}) {
		return worker, nil
	}
	return m.replaceWorker(ctx, *worker, newImageRef, newSHA, newLitestreamSHA)
}

func (m *Manager) replaceWorker(ctx context.Context, w model.Worker, newImageRef, newSHA, newLitestreamSHA string) (*model.Worker, error) {
	slog.Info("Updating worker", "name", w.Name, "old_sha", w.GitSHA, "new_sha", newSHA, "old_litestream_sha", w.LitestreamSHA, "new_litestream_sha", newLitestreamSHA)
	_ = m.db.RecordEvent(w.ID, "rolling_update", fmt.Sprintf("Updating %s from soak %s / litestream %s to soak %s / litestream %s", w.Name, shortVersionValue(w.GitSHA), shortVersionValue(w.LitestreamSHA), shortVersionValue(newSHA), shortVersionValue(newLitestreamSHA)), "")

	if w.FlyMachineID != "" {
		if err := m.fly.StopMachine(ctx, w.FlyMachineID); err != nil {
			if flyapi.IsNotFound(err) {
				slog.Warn("Old worker machine already gone during update", "machine_id", w.FlyMachineID, "worker_id", w.ID)
			} else {
				return nil, fmt.Errorf("stop machine for update: %w", err)
			}
		} else if err := m.fly.DestroyMachine(ctx, w.FlyMachineID, true); err != nil {
			if flyapi.IsNotFound(err) {
				slog.Warn("Old worker machine already destroyed during update", "machine_id", w.FlyMachineID, "worker_id", w.ID)
			} else {
				return nil, fmt.Errorf("destroy old machine: %w", err)
			}
		}
	}
	if w.FlyVolumeID != "" {
		if err := m.flyClientForWorker(w).DestroyVolume(ctx, w.FlyVolumeID); err != nil {
			if flyapi.IsNotFound(err) {
				slog.Warn("Old worker volume already gone during update", "volume_id", w.FlyVolumeID, "worker_id", w.ID)
			} else {
				return nil, fmt.Errorf("destroy old worker volume: %w", err)
			}
		}
	}

	m.clearOldWorkerReplicaPrefix(ctx, w)

	return m.createWorker(ctx, replacementRequest(w, newImageRef, newSHA, newLitestreamSHA))
}

func replacementRequest(w model.Worker, newImageRef, newSHA, newLitestreamSHA string) WorkerRequest {
	workloadCfg := resolveWorkerWorkload(w)
	return WorkerRequest{
		WorkerID:      w.ID,
		Name:          w.Name,
		Source:        w.Source,
		GitSHA:        newSHA,
		LitestreamSHA: newLitestreamSHA,
		PRNumber:      w.PRNumber,
		ProfileName:   w.ProfileName,
		ImageRef:      newImageRef,
		Region:        w.Region,
		VolumeSizeGB:  resolveWorkerVolumeSize(w, workloadCfg),
		ExpiresAt:     w.ExpiresAt,
		Workload:      workloadCfg,
	}
}

func (m *Manager) observeWorkerByID(workerID string) {
	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return
	}

	if m.metrics != nil {
		m.metrics.observeWorker(*worker)
	}
	if m.metrics != nil {
		if events, err := m.db.ListWorkerEvents(workerID, 20); err == nil {
			m.metrics.observePlatformEvent(*worker, latestPlatformEvent(coalesceEventFeed(events)))
		}
	}

	verifications, err := m.db.ListVerifications(workerID, 1)
	if err != nil || len(verifications) == 0 {
		m.observeLatestDeploymentState(worker.Source)
		return
	}
	if m.metrics != nil {
		m.metrics.observeVerification(*worker, verifications[0])
	}
	m.observeLatestDeploymentState(worker.Source)
}

func (m *Manager) observeLatestDeploymentState(source string) {
	if m.metrics != nil {
		m.metrics.observeLatestDeployment(m.db)
	}
	if m.alerts == nil {
		return
	}

	rollout, err := buildLatestDeploymentRollout(m.db, source)
	if err != nil || rollout == nil {
		return
	}
	m.alerts.NotifyDeploymentAttention(*rollout)
}
