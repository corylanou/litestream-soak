package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

type DesiredWorker struct {
	WorkerID      string
	Name          string
	Source        string
	GitSHA        string
	LitestreamSHA string
	PRNumber      int
	ProfileName   string
	Region        string
	VolumeSizeGB  int
	Workload      workload.Config
}

type FleetSpec struct {
	Workers []DesiredWorker
}

func DefaultMainFleet() FleetSpec {
	workers := []DesiredWorker{
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
			WorkerID:     "worker-main-high-vol",
			Name:         "worker-main-high-vol",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "high-volume",
			Region:       "ord",
			VolumeSizeGB: 100,
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
				S3PartSize:       "16MB",
				S3Concurrency:    8,
				VolumeSizeGB:     100,
				MemoryMB:         1024,
				CPUs:             1,
			},
		},
		{
			WorkerID:    "worker-main-low-vol-syd",
			Name:        "worker-main-low-vol-syd",
			Source:      "main",
			GitSHA:      "main",
			ProfileName: "low-vol-syd",
			Region:      "syd",
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
			WorkerID:     "worker-main-high-vol-ams",
			Name:         "worker-main-high-vol-ams",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "high-vol-ams",
			Region:       "ams",
			VolumeSizeGB: 100,
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
				S3PartSize:       "16MB",
				S3Concurrency:    8,
				VolumeSizeGB:     100,
				MemoryMB:         1024,
				CPUs:             1,
			},
		},
		{
			WorkerID:     "worker-main-burst-vol",
			Name:         "worker-main-burst-vol",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "burst-volume",
			Region:       "ord",
			VolumeSizeGB: 100,
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
				VolumeSizeGB:     100,
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
			WorkerID:     "worker-main-gharchive",
			Name:         "worker-main-gharchive",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "gharchive-replay",
			Region:       "ord",
			VolumeSizeGB: 50,
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
				VolumeSizeGB:     50,
				MemoryMB:         1024,
				CPUs:             1,
			},
		},
		{
			WorkerID:     "worker-main-gharchive-mixed",
			Name:         "worker-main-gharchive-mixed",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "gharchive-mixed",
			Region:       "ord",
			VolumeSizeGB: 50,
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
				VolumeSizeGB:     50,
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
		{
			WorkerID:     "worker-main-overload-truncate0",
			Name:         "worker-main-overload-truncate0",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "overload-truncate0",
			Region:       "ord",
			VolumeSizeGB: 100,
			Workload: workload.Config{
				LoadMode:         "synthetic",
				WriteRate:        600,
				Pattern:          "constant",
				PayloadSize:      2048,
				ReadRatio:        0.1,
				Workers:          8,
				InitialSize:      "50MB",
				VerifyInterval:   "30m",
				VerifyType:       "integrity",
				SnapshotInterval: "10m",
				SyncInterval:     "1s",
				S3PartSize:       "16MB",
				S3Concurrency:    8,
				TruncatePageN:    intPtr(0),
				VolumeSizeGB:     100,
				MemoryMB:         2048,
				CPUs:             2,
			},
		},
		{
			WorkerID:     "worker-main-pinned-reader",
			Name:         "worker-main-pinned-reader",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "pinned-reader",
			Region:       "ord",
			VolumeSizeGB: 100,
			Workload: workload.Config{
				LoadMode:          "synthetic",
				WriteRate:         200,
				Pattern:           "constant",
				PayloadSize:       2048,
				ReadRatio:         0.1,
				Workers:           4,
				InitialSize:       "20MB",
				VerifyInterval:    "30m",
				VerifyType:        "integrity",
				SnapshotInterval:  "10m",
				SyncInterval:      "1s",
				PinnedReaderHold:  "4m",
				PinnedReaderPause: "45s",
				VolumeSizeGB:      100,
				MemoryMB:          2048,
				CPUs:              2,
			},
		},
	}
	if manyDBFleetEnabled() {
		workers = append(workers, manyDB100FleetWorkers()...)
		if manyDB500FleetEnabled() {
			workers = append(workers, manyDB500FleetWorkers()...)
		}
		if manyDB1000FleetEnabled() {
			workers = append(workers, manyDB1000FleetWorker())
		}
	}
	return FleetSpec{Workers: workers}
}

func manyDBFleetEnabled() bool {
	return envFlagEnabled("SOAK_ENABLE_MANY_DB_FLEET")
}

func manyDB500FleetEnabled() bool {
	return envFlagEnabled("SOAK_ENABLE_MANY_DB_500")
}

func manyDB1000FleetEnabled() bool {
	return envFlagEnabled("SOAK_ENABLE_MANY_DB_1000")
}

func withManyDBObserveProxy(workers []DesiredWorker) []DesiredWorker {
	// Opt-in until the proxy re-signs forwarded requests: it currently
	// forwards litestream's Host header, which Tigris resolves as bucket
	// "127.0.0.1" (NoSuchBucket) — see issue #146. This is the fleet-spec
	// twin of the worker-profile gate from #147; fleet-created workers take
	// their workload config from here, not from worker profile defaults.
	if !envFlagEnabled("SOAK_ENABLE_S3_OBSERVE_PROXY") {
		return workers
	}
	for i := range workers {
		workers[i].Workload.S3FaultProxyEnabled = true
		workers[i].Workload.S3FaultProxyMode = "observe"
	}
	return workers
}

func envFlagEnabled(name string) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	return value == "1" || value == "true" || value == "yes"
}

func manyDB100FleetWorkers() []DesiredWorker {
	return withManyDBObserveProxy([]DesiredWorker{
		{
			WorkerID:     "worker-main-many-dbs-100-list",
			Name:         "worker-main-many-dbs-100-list",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "many-dbs-100-list",
			Region:       "ord",
			VolumeSizeGB: 10,
			Workload: workload.Config{
				LoadMode:                "many-db",
				WriteRate:               20,
				Pattern:                 "constant",
				PayloadSize:             512,
				Workers:                 2,
				InitialSize:             "5MB",
				VerifyInterval:          "30m",
				VerifyType:              "integrity",
				SnapshotInterval:        "10m",
				SyncInterval:            "1s",
				NumDatabases:            100,
				ActivePercent:           2,
				ActiveRotateInterval:    "30m",
				ActiveSetSeed:           1,
				ConfigMode:              "list",
				VerifySampleSize:        5,
				VerifyChangedLimit:      100,
				ReplicationLagThreshold: 0,
				VolumeSizeGB:            10,
				MemoryMB:                2048,
				CPUs:                    1,
			},
		},
		{
			WorkerID:     "worker-main-many-dbs-100-dir",
			Name:         "worker-main-many-dbs-100-dir",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "many-dbs-100-dir",
			Region:       "ord",
			VolumeSizeGB: 10,
			Workload: workload.Config{
				LoadMode:                "many-db",
				WriteRate:               20,
				Pattern:                 "constant",
				PayloadSize:             512,
				Workers:                 2,
				InitialSize:             "5MB",
				VerifyInterval:          "30m",
				VerifyType:              "integrity",
				SnapshotInterval:        "10m",
				SyncInterval:            "1s",
				NumDatabases:            100,
				ActivePercent:           2,
				ActiveRotateInterval:    "30m",
				ActiveSetSeed:           1,
				ConfigMode:              "dir",
				VerifySampleSize:        5,
				VerifyChangedLimit:      100,
				ReplicationLagThreshold: 0,
				VolumeSizeGB:            10,
				MemoryMB:                2048,
				CPUs:                    1,
			},
		},
	})
}

func manyDB500FleetWorkers() []DesiredWorker {
	return withManyDBObserveProxy([]DesiredWorker{
		{
			WorkerID:     "worker-main-many-dbs-500-list",
			Name:         "worker-main-many-dbs-500-list",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "many-dbs-500-list",
			Region:       "ord",
			VolumeSizeGB: 15,
			Workload: workload.Config{
				LoadMode:                "many-db",
				WriteRate:               20,
				Pattern:                 "constant",
				PayloadSize:             512,
				Workers:                 3,
				InitialSize:             "5MB",
				VerifyInterval:          "30m",
				VerifyType:              "integrity",
				SnapshotInterval:        "10m",
				SyncInterval:            "1s",
				NumDatabases:            500,
				ActivePercent:           2,
				ActiveRotateInterval:    "30m",
				ActiveSetSeed:           1,
				ConfigMode:              "list",
				VerifySampleSize:        5,
				VerifyChangedLimit:      100,
				ReplicationLagThreshold: 0,
				VolumeSizeGB:            15,
				MemoryMB:                3072,
				CPUs:                    2,
			},
		},
		{
			WorkerID:     "worker-main-many-dbs-500-dir",
			Name:         "worker-main-many-dbs-500-dir",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "many-dbs-500-dir",
			Region:       "ord",
			VolumeSizeGB: 15,
			Workload: workload.Config{
				LoadMode:                "many-db",
				WriteRate:               20,
				Pattern:                 "constant",
				PayloadSize:             512,
				Workers:                 3,
				InitialSize:             "5MB",
				VerifyInterval:          "30m",
				VerifyType:              "integrity",
				SnapshotInterval:        "10m",
				SyncInterval:            "1s",
				NumDatabases:            500,
				ActivePercent:           2,
				ActiveRotateInterval:    "30m",
				ActiveSetSeed:           1,
				ConfigMode:              "dir",
				VerifySampleSize:        5,
				VerifyChangedLimit:      100,
				ReplicationLagThreshold: 0,
				VolumeSizeGB:            15,
				MemoryMB:                3072,
				CPUs:                    2,
			},
		},
		{
			WorkerID:     "worker-main-many-dbs-500-dir-lowfreq",
			Name:         "worker-main-many-dbs-500-dir-lowfreq",
			Source:       "main",
			GitSHA:       "main",
			ProfileName:  "many-dbs-500-dir-lowfreq",
			Region:       "ord",
			VolumeSizeGB: 15,
			Workload: workload.Config{
				LoadMode:                 "many-db",
				WriteRate:                20,
				Pattern:                  "constant",
				PayloadSize:              512,
				Workers:                  3,
				InitialSize:              "5MB",
				VerifyInterval:           "30m",
				VerifyType:               "integrity",
				SnapshotInterval:         "1h",
				SyncInterval:             "1s",
				NumDatabases:             500,
				ActivePercent:            2,
				ActiveRotateInterval:     "30m",
				ActiveSetSeed:            1,
				ConfigMode:               "dir",
				VerifySampleSize:         5,
				VerifyChangedLimit:       100,
				ReplicationLagThreshold:  0,
				L1CompactionInterval:     "5m",
				L2CompactionInterval:     "30m",
				L3CompactionInterval:     "6h",
				L0Retention:              "1h",
				L0RetentionCheckInterval: "2m",
				VolumeSizeGB:             15,
				MemoryMB:                 3072,
				CPUs:                     2,
			},
		},
	})
}

func manyDB1000FleetWorker() DesiredWorker {
	return withManyDBObserveProxy([]DesiredWorker{{
		WorkerID:     "worker-main-many-dbs-1000-dir",
		Name:         "worker-main-many-dbs-1000-dir",
		Source:       "main",
		GitSHA:       "main",
		ProfileName:  "many-dbs-1000-dir",
		Region:       "ord",
		VolumeSizeGB: 20,
		Workload: workload.Config{
			LoadMode:                "many-db",
			WriteRate:               20,
			Pattern:                 "constant",
			PayloadSize:             512,
			Workers:                 4,
			InitialSize:             "5MB",
			VerifyInterval:          "30m",
			VerifyType:              "integrity",
			SnapshotInterval:        "10m",
			SyncInterval:            "1s",
			NumDatabases:            1000,
			ActivePercent:           2,
			ActiveRotateInterval:    "30m",
			ActiveSetSeed:           1,
			ConfigMode:              "dir",
			VerifySampleSize:        5,
			VerifyChangedLimit:      100,
			ReplicationLagThreshold: 0,
			VolumeSizeGB:            20,
			MemoryMB:                4096,
			CPUs:                    2,
		},
	}})[0]
}

func DefaultFleetForSource(source, gitSHA, litestreamSHA string) FleetSpec {
	spec := DefaultMainFleet()
	normalizedSource := firstNonEmpty(source, "main")
	prNumber := sourcePRNumber(normalizedSource)
	workers := make([]DesiredWorker, 0, len(spec.Workers))
	for _, desired := range spec.Workers {
		rewritten := desired
		rewritten.Source = normalizedSource
		if strings.TrimSpace(gitSHA) != "" {
			rewritten.GitSHA = gitSHA
		}
		if strings.TrimSpace(litestreamSHA) != "" {
			rewritten.LitestreamSHA = litestreamSHA
		}
		rewritten.PRNumber = prNumber
		rewritten.WorkerID = workerNameForSource(normalizedSource, firstNonEmpty(desired.WorkerID, desired.Name))
		rewritten.Name = workerNameForSource(normalizedSource, desired.Name)
		workers = append(workers, rewritten)
	}
	return FleetSpec{Workers: workers}
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

	workersBySource := make(map[string][]DesiredWorker)
	sources := make([]string, 0)
	for _, desired := range spec.Workers {
		source := firstNonEmpty(strings.TrimSpace(desired.Source), "main")
		if _, ok := workersBySource[source]; !ok {
			sources = append(sources, source)
		}
		workersBySource[source] = append(workersBySource[source], desired)
	}

	for _, source := range sources {
		deployment, err := resolveReadyDeploymentTarget(m.db, source, "", "", "")
		if err != nil {
			slog.Warn("Skipping fleet reconciliation without a ready source deployment", "source", source, "error", err)
			continue
		}

		resolved := make([]DesiredWorker, 0, len(workersBySource[source]))
		for _, desired := range workersBySource[source] {
			desired.Source = source
			desired.GitSHA = deployment.GitSHA
			desired.LitestreamSHA = deployment.LitestreamSHA
			desired.PRNumber = sourcePRNumber(source)
			resolved = append(resolved, desired)
		}
		if err := m.ensureFleetSpec(ctx, FleetSpec{Workers: resolved}, deployment.ImageRef); err != nil {
			slog.Error("Failed to reconcile source fleet", "source", source, "error", err)
		}
	}
}

func (m *Manager) EnsureSourceFleet(ctx context.Context, source, gitSHA, litestreamSHA, imageRef string) error {
	if !supportsDefaultFleetSource(source) {
		return nil
	}
	deployment, err := resolveReadyDeploymentTarget(m.db, source, imageRef, gitSHA, litestreamSHA)
	if err != nil {
		return fmt.Errorf("resolve source fleet deployment: %w", err)
	}
	if err := m.ensureFleetSpec(
		ctx,
		DefaultFleetForSource(source, deployment.GitSHA, deployment.LitestreamSHA),
		deployment.ImageRef,
	); err != nil {
		return fmt.Errorf("ensure source fleet: %w", err)
	}
	return nil
}

func (m *Manager) ensureFleetSpec(ctx context.Context, spec FleetSpec, imageRef string) error {
	activeWorkers, err := m.db.ListWorkers("")
	if err != nil {
		return fmt.Errorf("list current workers for fleet reconciliation: %w", err)
	}

	byName := make(map[string]model.Worker, len(activeWorkers))
	for _, worker := range activeWorkers {
		if worker.Status == model.WorkerStopped || worker.Status == model.WorkerFailed {
			continue
		}
		byName[worker.Name] = worker
	}

	var ensureErrors []error
	for _, desired := range spec.Workers {
		current, ok := byName[desired.Name]
		if ok && current.Status == model.WorkerDormant {
			continue
		}
		if ok && workerMatchesDesiredSpec(current, desired) {
			continue
		}
		if ok {
			_, err := func() (*model.Worker, error) {
				unlock, err := m.lockWorker(ctx, current.ID)
				if err != nil {
					return nil, err
				}
				defer unlock()

				worker, err := m.db.GetWorker(current.ID)
				if err != nil {
					return nil, fmt.Errorf("reload worker: %w", err)
				}
				if worker.Status == model.WorkerStopped || worker.Status == model.WorkerFailed || worker.Status == model.WorkerDormant {
					return nil, nil
				}
				if workerMatchesDesiredSpec(*worker, desired) {
					return nil, nil
				}

				request := workerRequestForDesired(desired, imageRef, worker.ID, worker.ExpiresAt)
				details := fmt.Sprintf(
					"profile=%s region=%s volume_size_gb=%d",
					request.ProfileName,
					request.Region,
					effectiveDesiredVolumeSize(desired, request.Workload),
				)
				if err := m.db.RecordEvent(worker.ID, "worker_spec_drift", fmt.Sprintf("Reconciling desired fleet spec for %s", worker.Name), details); err != nil {
					return nil, fmt.Errorf("record worker spec drift: %w", err)
				}
				return m.replaceWorkerWithRequest(ctx, *worker, request)
			}()
			if err != nil {
				ensureErrors = append(ensureErrors, fmt.Errorf("%s: %w", desired.Name, err))
			}
			continue
		}

		request := workerRequestForDesired(desired, imageRef, "", nil)

		if _, err := m.CreateWorker(ctx, request); err != nil {
			ensureErrors = append(ensureErrors, fmt.Errorf("%s: %w", desired.Name, err))
			continue
		}

		slog.Info("Created desired fleet worker", "name", desired.Name, "source", desired.Source, "profile", desired.ProfileName, "load_mode", desired.Workload.LoadMode)
	}
	return errors.Join(ensureErrors...)
}

func workerRequestForDesired(desired DesiredWorker, imageRef, workerID string, expiresAt *time.Time) WorkerRequest {
	workloadCfg := normalizeWorkloadConfig(desired.Workload)
	volumeSizeGB := effectiveDesiredVolumeSize(desired, workloadCfg)
	workloadCfg.VolumeSizeGB = volumeSizeGB
	return WorkerRequest{
		WorkerID:      firstNonEmpty(workerID, desired.WorkerID, desired.Name),
		Name:          desired.Name,
		Source:        firstNonEmpty(desired.Source, "main"),
		GitSHA:        firstNonEmpty(desired.GitSHA, "main"),
		LitestreamSHA: strings.TrimSpace(desired.LitestreamSHA),
		PRNumber:      desired.PRNumber,
		ProfileName:   desired.ProfileName,
		ImageRef:      imageRef,
		Region:        normalizedWorkerRegion(desired.Region),
		VolumeSizeGB:  volumeSizeGB,
		ExpiresAt:     expiresAt,
		Workload:      workloadCfg,
	}
}

func workerMatchesDesiredSpec(worker model.Worker, desired DesiredWorker) bool {
	if firstNonEmpty(strings.TrimSpace(worker.Source), "main") != firstNonEmpty(strings.TrimSpace(desired.Source), "main") {
		return false
	}
	if worker.PRNumber != desired.PRNumber {
		return false
	}
	if strings.TrimSpace(worker.ProfileName) != strings.TrimSpace(desired.ProfileName) {
		return false
	}
	if normalizedWorkerRegion(worker.Region) != normalizedWorkerRegion(desired.Region) {
		return false
	}

	applied, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		return false
	}
	applied = normalizeWorkloadConfig(applied)
	wanted := normalizeWorkloadConfig(desired.Workload)
	if effectiveAppliedVolumeSize(applied) != effectiveDesiredVolumeSize(desired, wanted) {
		return false
	}

	applied.VolumeSizeGB = 0
	wanted.VolumeSizeGB = 0
	return applied.JSON() == wanted.JSON()
}

func workerPhysicalSpecMatchesDesired(worker model.Worker, desired DesiredWorker) bool {
	if normalizedWorkerRegion(worker.Region) != normalizedWorkerRegion(desired.Region) {
		return false
	}
	applied, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		return false
	}
	applied = normalizeWorkloadConfig(applied)
	wanted := normalizeWorkloadConfig(desired.Workload)
	return effectiveAppliedVolumeSize(applied) == effectiveDesiredVolumeSize(desired, wanted)
}

func effectiveAppliedVolumeSize(cfg workload.Config) int {
	if cfg.VolumeSizeGB > 0 {
		return cfg.VolumeSizeGB
	}
	return 10
}

func effectiveDesiredVolumeSize(desired DesiredWorker, cfg workload.Config) int {
	if desired.VolumeSizeGB > 0 {
		return desired.VolumeSizeGB
	}
	return effectiveAppliedVolumeSize(cfg)
}

func normalizedWorkerRegion(region string) string {
	return firstNonEmpty(strings.TrimSpace(region), "ord")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func intPtr(n int) *int {
	return &n
}

func supportsDefaultFleetSource(source string) bool {
	source = strings.TrimSpace(source)
	return source == "main" || sourcePRNumber(source) > 0
}

func sourcePRNumber(source string) int {
	source = strings.TrimSpace(source)
	if !strings.HasPrefix(source, "pr-") {
		return 0
	}
	prNumber, err := strconv.Atoi(strings.TrimPrefix(source, "pr-"))
	if err != nil || prNumber <= 0 {
		return 0
	}
	return prNumber
}

func workerNameForSource(source, baseName string) string {
	source = strings.TrimSpace(source)
	if source == "" || source == "main" {
		return baseName
	}
	if strings.HasPrefix(baseName, "worker-main-") {
		return "worker-" + source + "-" + strings.TrimPrefix(baseName, "worker-main-")
	}
	if strings.HasPrefix(baseName, "worker-") {
		return "worker-" + source + "-" + strings.TrimPrefix(baseName, "worker-")
	}
	return "worker-" + source + "-" + baseName
}
