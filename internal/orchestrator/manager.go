package orchestrator

import (
	"context"
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
	sourceLocks map[string]*sync.Mutex
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

func (m *Manager) lockSource(source string) func() {
	return m.keyedLock(&m.sourceLocks, source)
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
	region := worker.Region
	workloadCfg := normalizeWorkloadConfig(req.Workload)

	if err := m.db.CreateWorker(worker); err != nil {
		return nil, fmt.Errorf("create worker record: %w", err)
	}
	m.observeWorkerByID(workerID)

	m.db.RecordEvent(workerID, "worker_creating", fmt.Sprintf("Creating worker %s with profile %s", req.Name, req.ProfileName), "")

	volSize := req.VolumeSizeGB
	if volSize == 0 {
		volSize = 10
	}

	vol, err := m.fly.CreateVolume(ctx, flyapi.CreateVolumeRequest{
		Name:      flyVolumeName(req.Name),
		SizeGB:    volSize,
		Region:    region,
		Encrypted: true,
	})
	if err != nil {
		m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error())
		m.observeWorkerByID(workerID)
		return nil, fmt.Errorf("create volume: %w", err)
	}
	worker.FlyVolumeID = vol.ID

	if err := m.clearWorkerReplicaPrefix(ctx, *worker); err != nil {
		if destroyErr := m.fly.DestroyVolume(ctx, vol.ID); destroyErr != nil && !flyapi.IsNotFound(destroyErr) {
			slog.Warn("Failed to destroy worker volume after replica prefix clear failure", "worker_id", workerID, "volume_id", vol.ID, "error", destroyErr)
		}
		m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error())
		m.observeWorkerByID(workerID)
		return nil, fmt.Errorf("clear replica prefix: %w", err)
	}

	machine, err := m.createWorkerMachine(ctx, *worker, req.ImageRef, vol.ID, workloadCfg)
	if err != nil {
		m.fly.DestroyVolume(ctx, vol.ID)
		m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error())
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
	m.db.UpdateWorkerStatus(workerID, model.WorkerRunning, "")
	m.observeWorkerByID(workerID)
	m.db.RecordEvent(workerID, "worker_started", fmt.Sprintf("Worker %s started (machine %s)", req.Name, machine.ID), "")

	slog.Info("Worker created", "name", req.Name, "machine_id", machine.ID, "volume_id", vol.ID, "profile", req.ProfileName)

	return worker, nil
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

	m.db.UpdateWorkerStatus(workerID, model.WorkerStopped, "")
	m.observeWorkerByID(workerID)
	m.db.RecordEvent(workerID, "worker_stopped", fmt.Sprintf("Worker %s stopped", worker.Name), "")
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

	m.db.UpdateWorkerStatus(workerID, model.WorkerStopped, "")
	m.observeWorkerByID(workerID)
	m.db.RecordEvent(workerID, "worker_destroyed", fmt.Sprintf("Worker %s destroyed", worker.Name), "")
	return nil
}

func (m *Manager) RollingUpdate(ctx context.Context, newImageRef, newSHA, newLitestreamSHA string) error {
	return m.RollingUpdateSource(ctx, "main", newImageRef, newSHA, newLitestreamSHA)
}

func (m *Manager) RollingUpdateSource(ctx context.Context, source, newImageRef, newSHA, newLitestreamSHA string) error {
	source = firstNonEmpty(strings.TrimSpace(source), "main")

	unlockSource := m.lockSource(source)
	defer unlockSource()

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
	m.db.RecordEvent(w.ID, "rolling_update", fmt.Sprintf("Updating %s from soak %s / litestream %s to soak %s / litestream %s", w.Name, shortVersionValue(w.GitSHA), shortVersionValue(w.LitestreamSHA), shortVersionValue(newSHA), shortVersionValue(newLitestreamSHA)), "")

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
