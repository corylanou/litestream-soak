package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/google/uuid"
)

type WorkerRequest struct {
	Name        string
	Source      string
	GitSHA      string
	PRNumber    int
	ProfileName string
	ImageRef    string
	Region      string
	VolumeSizeGB int
	ExpiresAt   *string

	// Profile overrides
	WriteRate   int
	Pattern     string
	PayloadSize int
	Workers     int
	InitialSize string
}

type Manager struct {
	fly      *flyapi.Client
	db       *model.DB
	appName  string
	s3Bucket string
	s3Endpoint string
}

func NewManager(fly *flyapi.Client, db *model.DB, appName, s3Bucket, s3Endpoint string) *Manager {
	return &Manager{
		fly:        fly,
		db:         db,
		appName:    appName,
		s3Bucket:   s3Bucket,
		s3Endpoint: s3Endpoint,
	}
}

func (m *Manager) CreateWorker(ctx context.Context, req WorkerRequest) (*model.Worker, error) {
	workerID := uuid.New().String()

	profileConfig, _ := json.Marshal(map[string]any{
		"write_rate":   req.WriteRate,
		"pattern":      req.Pattern,
		"payload_size": req.PayloadSize,
		"workers":      req.Workers,
		"initial_size": req.InitialSize,
	})

	worker := &model.Worker{
		ID:            workerID,
		Name:          req.Name,
		Status:        model.WorkerPending,
		Source:        req.Source,
		GitSHA:        req.GitSHA,
		PRNumber:      req.PRNumber,
		ProfileName:   req.ProfileName,
		ProfileConfig: string(profileConfig),
	}

	if err := m.db.CreateWorker(worker); err != nil {
		return nil, fmt.Errorf("create worker record: %w", err)
	}

	m.db.RecordEvent(workerID, "worker_creating", fmt.Sprintf("Creating worker %s with profile %s", req.Name, req.ProfileName), "")

	region := req.Region
	if region == "" {
		region = "ord"
	}
	volSize := req.VolumeSizeGB
	if volSize == 0 {
		volSize = 10
	}

	vol, err := m.fly.CreateVolume(ctx, flyapi.CreateVolumeRequest{
		Name:      fmt.Sprintf("soak_%s", req.Name),
		SizeGB:    volSize,
		Region:    region,
		Encrypted: true,
	})
	if err != nil {
		m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error())
		return nil, fmt.Errorf("create volume: %w", err)
	}

	s3Path := fmt.Sprintf("soak/%s", req.Name)
	env := map[string]string{
		"WORKER_ID":         workerID,
		"GIT_SHA":           req.GitSHA,
		"SOURCE":            req.Source,
		"PROFILE":           req.ProfileName,
		"INITIAL_SIZE":      req.InitialSize,
		"VERIFY_INTERVAL":   "30m",
		"SNAPSHOT_INTERVAL":  "10m",
		"REPLICA_TYPE":      "s3",
		"S3_BUCKET":         m.s3Bucket,
		"S3_PATH":           s3Path,
		"S3_ENDPOINT":       m.s3Endpoint,
	}

	if req.WriteRate > 0 {
		env["WRITE_RATE"] = fmt.Sprintf("%d", req.WriteRate)
	}
	if req.Pattern != "" {
		env["PATTERN"] = req.Pattern
	}
	if req.PayloadSize > 0 {
		env["PAYLOAD_SIZE"] = fmt.Sprintf("%d", req.PayloadSize)
	}
	if req.Workers > 0 {
		env["LOAD_WORKERS"] = fmt.Sprintf("%d", req.Workers)
	}

	machine, err := m.fly.CreateMachine(ctx, flyapi.CreateMachineRequest{
		Name:   req.Name,
		Region: region,
		Config: flyapi.MachineConfig{
			Image: req.ImageRef,
			Env:   env,
			Guest: flyapi.Guest{
				CPUKind:  "shared",
				CPUs:     1,
				MemoryMB: 256,
			},
			Mounts: []flyapi.Mount{{
				Volume: vol.ID,
				Path:   "/data",
			}},
			Metrics: &flyapi.MetricsConfig{
				Port: 9091,
				Path: "/metrics",
			},
		},
	})
	if err != nil {
		m.fly.DestroyVolume(ctx, vol.ID)
		m.db.UpdateWorkerStatus(workerID, model.WorkerFailed, err.Error())
		return nil, fmt.Errorf("create machine: %w", err)
	}

	if err := m.db.UpdateWorkerMachine(workerID, machine.ID, vol.ID); err != nil {
		return nil, fmt.Errorf("update worker machine: %w", err)
	}
	m.db.UpdateWorkerStatus(workerID, model.WorkerRunning, "")
	m.db.RecordEvent(workerID, "worker_started", fmt.Sprintf("Worker %s started (machine %s)", req.Name, machine.ID), "")

	slog.Info("Worker created", "name", req.Name, "machine_id", machine.ID, "volume_id", vol.ID, "profile", req.ProfileName)

	return worker, nil
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

		var profileConfig map[string]any
		json.Unmarshal([]byte(w.ProfileConfig), &profileConfig)

		writeRate, _ := profileConfig["write_rate"].(float64)
		pattern, _ := profileConfig["pattern"].(string)
		payloadSize, _ := profileConfig["payload_size"].(float64)
		workers, _ := profileConfig["workers"].(float64)
		initialSize, _ := profileConfig["initial_size"].(string)

		newWorker, err := m.CreateWorker(ctx, WorkerRequest{
			Name:         w.Name,
			Source:       "main",
			GitSHA:       newSHA,
			ProfileName:  w.ProfileName,
			ImageRef:     newImageRef,
			WriteRate:    int(writeRate),
			Pattern:      pattern,
			PayloadSize:  int(payloadSize),
			Workers:      int(workers),
			InitialSize:  initialSize,
		})
		if err != nil {
			slog.Error("Failed to create updated worker", "name", w.Name, "error", err)
			continue
		}

		m.db.UpdateWorkerStatus(w.ID, model.WorkerStopped, "replaced by rolling update")
		slog.Info("Worker updated", "name", w.Name, "new_id", newWorker.ID)
	}

	return nil
}
