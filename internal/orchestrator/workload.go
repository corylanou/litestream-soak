package orchestrator

import (
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func resolveWorkerWorkload(worker model.Worker) workload.Config {
	if worker.Source == "main" {
		if desired, ok := defaultMainFleetWorkload(worker.ID); ok {
			return desired
		}
		if desired, ok := defaultMainFleetWorkload(worker.Name); ok {
			return desired
		}
	}

	return normalizeWorkload(workload.ParseConfig(worker.ProfileConfig))
}

func defaultMainFleetWorkload(workerID string) (workload.Config, bool) {
	for _, desired := range DefaultMainFleet().Workers {
		if desired.WorkerID == workerID || desired.Name == workerID {
			return desired.Workload, true
		}
	}
	return workload.Config{}, false
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
