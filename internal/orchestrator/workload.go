package orchestrator

import (
	"log/slog"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func resolveWorkerWorkload(worker model.Worker) workload.Config {
	if desired, ok := defaultFleetDesiredWorker(worker.Source, worker.ID, worker.Name); ok {
		return desired.Workload
	}

	cfg, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		slog.Warn("Invalid worker profile config, falling back to defaults",
			"worker_id", worker.ID, "worker_name", worker.Name,
			"profile_name", worker.ProfileName, "error", err)
	}
	return normalizeWorkload(cfg)
}

func resolveWorkerVolumeSize(worker model.Worker, workloadCfg workload.Config) int {
	if desired, ok := defaultFleetDesiredWorker(worker.Source, worker.ID, worker.Name); ok && desired.VolumeSizeGB != 0 {
		return desired.VolumeSizeGB
	}
	return workloadCfg.VolumeSizeGB
}

func defaultFleetDesiredWorker(source, workerID, workerName string) (DesiredWorker, bool) {
	for _, desired := range DefaultFleetForSource(source, "", "").Workers {
		if desired.WorkerID == workerID || desired.WorkerID == workerName || desired.Name == workerID || desired.Name == workerName {
			return desired, true
		}
	}
	return DesiredWorker{}, false
}

func normalizeWorkload(cfg workload.Config) workload.Config {
	switch cfg.MetricLoadMode() {
	case "replay":
		cfg.WriteRate = 0
		cfg.Pattern = ""
		cfg.PayloadSize = 0
		cfg.ReadRatio = 0
		cfg.Workers = 0
	case "synthetic":
		cfg.ReplayDataset = ""
		cfg.ReplayDataPath = ""
		cfg.ReplayDataURL = ""
		cfg.ReplaySpeed = 0
		cfg.ReplayLoop = false
	}

	return cfg
}
