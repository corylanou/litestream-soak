package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

type DesiredWorker struct {
	WorkerID     string
	Name         string
	Source       string
	GitSHA       string
	ProfileName  string
	Region       string
	VolumeSizeGB int
	Workload     workload.Config
}

type FleetSpec struct {
	Workers []DesiredWorker
}

func DefaultMainFleet() FleetSpec {
	return FleetSpec{
		Workers: []DesiredWorker{
			{
				WorkerID:    "worker-main-low-vol",
				Name:        "worker-main-low-vol",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "low-volume",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "synthetic",
					WriteRate:        10,
					Pattern:          "constant",
					PayloadSize:      1024,
					ReadRatio:        0.2,
					Workers:          1,
					InitialSize:      "5MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
			{
				WorkerID:    "worker-main-high-vol",
				Name:        "worker-main-high-vol",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "high-volume",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "synthetic",
					WriteRate:        500,
					Pattern:          "wave",
					PayloadSize:      4096,
					ReadRatio:        0.2,
					Workers:          8,
					InitialSize:      "50MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
			{
				WorkerID:    "worker-main-burst-vol",
				Name:        "worker-main-burst-vol",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "burst-volume",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "synthetic",
					WriteRate:        1000,
					Pattern:          "burst",
					PayloadSize:      2048,
					ReadRatio:        0.2,
					Workers:          4,
					InitialSize:      "20MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
			{
				WorkerID:    "worker-main-read-heavy",
				Name:        "worker-main-read-heavy",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "read-heavy",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "synthetic",
					WriteRate:        80,
					Pattern:          "constant",
					PayloadSize:      512,
					ReadRatio:        0.95,
					Workers:          6,
					InitialSize:      "10MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
			{
				WorkerID:    "worker-main-gharchive",
				Name:        "worker-main-gharchive",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "gharchive-replay",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "replay",
					InitialSize:      "5MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					ReplayDataset:    "gharchive",
					ReplayDataURL:    "https://data.gharchive.org/2025-01-01-0.json.gz",
					ReplaySpeed:      300,
					ReplayLoop:       true,
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
			{
				WorkerID:    "worker-main-gharchive-mixed",
				Name:        "worker-main-gharchive-mixed",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "gharchive-mixed",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "both",
					WriteRate:        50,
					Pattern:          "wave",
					PayloadSize:      1024,
					ReadRatio:        0.2,
					Workers:          2,
					InitialSize:      "10MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					ReplayDataset:    "gharchive",
					ReplayDataURL:    "https://data.gharchive.org/2025-01-01-0.json.gz",
					ReplaySpeed:      120,
					ReplayLoop:       true,
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
			{
				WorkerID:    "worker-main-taxi-mixed",
				Name:        "worker-main-taxi-mixed",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "taxi-mixed",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "both",
					WriteRate:        40,
					Pattern:          "wave",
					PayloadSize:      1024,
					ReadRatio:        0.4,
					Workers:          2,
					InitialSize:      "10MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					ReplayDataset:    "taxi",
					ReplayDataPath:   "/opt/soak/datasets/taxi_sample.csv",
					ReplaySpeed:      60,
					ReplayLoop:       true,
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
			{
				WorkerID:    "worker-main-taxi-replay",
				Name:        "worker-main-taxi-replay",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "taxi-replay",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "replay",
					InitialSize:      "5MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					ReplayDataset:    "taxi",
					ReplayDataPath:   "/opt/soak/datasets/taxi_sample.csv",
					ReplaySpeed:      90,
					ReplayLoop:       true,
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
			{
				WorkerID:    "worker-main-orders-replay",
				Name:        "worker-main-orders-replay",
				Source:      "main",
				GitSHA:      "main",
				ProfileName: "orders-replay",
				Region:      "ord",
				Workload: workload.Config{
					LoadMode:         "replay",
					InitialSize:      "5MB",
					VerifyInterval:   "30m",
					VerifyType:       "integrity",
					SnapshotInterval: "10m",
					SyncInterval:     "1s",
					ReplayDataset:    "orders",
					ReplayDataPath:   "/opt/soak/datasets/orders_sample.jsonl",
					ReplaySpeed:      45,
					ReplayLoop:       true,
					MemoryMB:         1024,
					CPUs:             1,
				},
			},
		},
	}
}

func (m *Manager) RunFleetReconciler(ctx context.Context, spec FleetSpec, interval time.Duration) {
	m.reconcileFleet(ctx, spec)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcileFleet(ctx, spec)
		}
	}
}

func (m *Manager) reconcileFleet(ctx context.Context, spec FleetSpec) {
	if len(spec.Workers) == 0 {
		return
	}

	imageRef, err := m.currentWorkerImage(ctx)
	if err != nil {
		slog.Error("Failed to resolve worker image for fleet reconciliation", "error", err)
		return
	}

	activeWorkers, err := m.db.ListWorkers("")
	if err != nil {
		slog.Error("Failed to list current main workers", "error", err)
		return
	}

	byName := make(map[string]model.Worker, len(activeWorkers))
	for _, worker := range activeWorkers {
		if worker.Source != "main" {
			continue
		}
		if worker.Status == model.WorkerStopped || worker.Status == model.WorkerFailed {
			continue
		}
		byName[worker.Name] = worker
	}

	for _, desired := range spec.Workers {
		if _, ok := byName[desired.Name]; ok {
			continue
		}

		request := WorkerRequest{
			WorkerID:     firstNonEmpty(desired.WorkerID, desired.Name),
			Name:         desired.Name,
			Source:       firstNonEmpty(desired.Source, "main"),
			GitSHA:       firstNonEmpty(desired.GitSHA, "main"),
			ProfileName:  desired.ProfileName,
			ImageRef:     imageRef,
			Region:       desired.Region,
			VolumeSizeGB: desired.VolumeSizeGB,
			Workload:     desired.Workload,
		}

		if _, err := m.CreateWorker(ctx, request); err != nil {
			slog.Error("Failed to create desired fleet worker", "name", desired.Name, "error", err)
			continue
		}

		slog.Info("Created desired fleet worker", "name", desired.Name, "profile", desired.ProfileName, "load_mode", desired.Workload.LoadMode)
	}
}

func (m *Manager) currentWorkerImage(ctx context.Context) (string, error) {
	machines, err := m.fly.ListMachines(ctx)
	if err != nil {
		return "", fmt.Errorf("list machines: %w", err)
	}

	var newestStarted *flyapi.Machine
	for _, machine := range machines {
		if strings.TrimSpace(machine.Config.Image) == "" || machine.State != "started" {
			continue
		}
		if newestStarted == nil || machine.UpdatedAt.After(newestStarted.UpdatedAt) {
			candidate := machine
			newestStarted = &candidate
		}
	}
	if newestStarted != nil {
		return newestStarted.Config.Image, nil
	}

	var newestAny *flyapi.Machine
	for _, machine := range machines {
		if strings.TrimSpace(machine.Config.Image) == "" {
			continue
		}
		if newestAny == nil || machine.UpdatedAt.After(newestAny.UpdatedAt) {
			candidate := machine
			newestAny = &candidate
		}
	}
	if newestAny != nil {
		return newestAny.Config.Image, nil
	}

	return "", fmt.Errorf("no worker image found in %s", m.fly.AppName())
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
