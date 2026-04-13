package orchestrator

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
	"github.com/google/uuid"
)

type WorkerRequest struct {
	WorkerID     string
	Name         string
	Source       string
	GitSHA       string
	PRNumber     int
	ProfileName  string
	ImageRef     string
	Region       string
	VolumeSizeGB int
	ExpiresAt    *string
	Workload     workload.Config
}

type Manager struct {
	fly            *flyapi.Client
	db             *model.DB
	metrics        *controlMetrics
	alerts         *AlertDispatcher
	appName        string
	s3Bucket       string
	s3Endpoint     string
	controlBaseURL string
}

func NewManager(fly *flyapi.Client, db *model.DB, metrics *controlMetrics, alerts *AlertDispatcher, appName, s3Bucket, s3Endpoint, controlBaseURL string) *Manager {
	return &Manager{
		fly:            fly,
		db:             db,
		metrics:        metrics,
		alerts:         alerts,
		appName:        appName,
		s3Bucket:       s3Bucket,
		s3Endpoint:     s3Endpoint,
		controlBaseURL: controlBaseURL,
	}
}

func (m *Manager) CreateWorker(ctx context.Context, req WorkerRequest) (*model.Worker, error) {
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		workerID = uuid.New().String()
	}
	region := req.Region
	if region == "" {
		region = "ord"
	}
	workloadCfg := req.Workload
	workloadCfg = normalizeWorkloadConfig(workloadCfg)

	worker := &model.Worker{
		ID:            workerID,
		AppName:       m.appName,
		Name:          req.Name,
		Status:        model.WorkerPending,
		Source:        req.Source,
		GitSHA:        req.GitSHA,
		PRNumber:      req.PRNumber,
		ProfileName:   req.ProfileName,
		ProfileConfig: workloadCfg.JSON(),
		Region:        region,
	}

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

	machine, err := m.createWorkerMachine(ctx, *worker, req.ImageRef, vol.ID, workloadCfg)
	if err != nil {
		m.fly.DestroyVolume(ctx, vol.ID)
		m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error())
		m.observeWorkerByID(workerID)
		return nil, fmt.Errorf("create machine: %w", err)
	}

	if err := m.db.UpdateWorkerMachine(workerID, machine.ID, vol.ID); err != nil {
		return nil, fmt.Errorf("update worker machine: %w", err)
	}
	m.db.UpdateWorkerStatus(workerID, model.WorkerRunning, "")
	m.observeWorkerByID(workerID)
	m.db.RecordEvent(workerID, "worker_started", fmt.Sprintf("Worker %s started (machine %s)", req.Name, machine.ID), "")

	slog.Info("Worker created", "name", req.Name, "machine_id", machine.ID, "volume_id", vol.ID, "profile", req.ProfileName)

	return worker, nil
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

	m.db.UpdateWorkerStatus(workerID, model.WorkerStopped, "")
	m.observeWorkerByID(workerID)
	m.db.RecordEvent(workerID, "worker_destroyed", fmt.Sprintf("Worker %s destroyed", worker.Name), "")
	return nil
}

func (m *Manager) RollingUpdate(ctx context.Context, newImageRef, newSHA string) error {
	workers, err := m.db.ListMainWorkers()
	if err != nil {
		return fmt.Errorf("list main workers: %w", err)
	}

	slog.Info("Starting rolling update", "workers", len(workers), "sha", newSHA, "image", newImageRef)

	for _, w := range workers {
		slog.Info("Updating worker", "name", w.Name, "old_sha", w.GitSHA, "new_sha", newSHA)
		m.db.RecordEvent(w.ID, "rolling_update", fmt.Sprintf("Updating %s from %s to %s", w.Name, w.GitSHA, newSHA), "")

		if w.FlyMachineID != "" {
			if err := m.fly.StopMachine(ctx, w.FlyMachineID); err != nil {
				slog.Error("Failed to stop machine for update", "machine_id", w.FlyMachineID, "error", err)
				continue
			}
			if err := m.fly.DestroyMachine(ctx, w.FlyMachineID, true); err != nil {
				slog.Error("Failed to destroy old machine", "machine_id", w.FlyMachineID, "error", err)
				continue
			}
		}

		workloadCfg := resolveWorkerWorkload(w)

		newWorker, err := m.CreateWorker(ctx, WorkerRequest{
			WorkerID:    w.Name,
			Name:        w.Name,
			Source:      "main",
			GitSHA:      newSHA,
			ProfileName: w.ProfileName,
			ImageRef:    newImageRef,
			Workload:    workloadCfg,
		})
		if err != nil {
			slog.Error("Failed to create updated worker", "name", w.Name, "error", err)
			continue
		}
		slog.Info("Worker updated", "name", w.Name, "new_id", newWorker.ID)
	}

	return nil
}

func (m *Manager) observeWorkerByID(workerID string) {
	if m.metrics == nil {
		return
	}

	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return
	}
	m.metrics.observeWorker(*worker)

	verifications, err := m.db.ListVerifications(workerID, 1)
	if err != nil || len(verifications) == 0 {
		m.metrics.observeLatestDeployment(m.db)
		return
	}
	m.metrics.observeVerification(*worker, verifications[0])
	m.metrics.observeLatestDeployment(m.db)
}
