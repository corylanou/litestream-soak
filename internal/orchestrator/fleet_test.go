package orchestrator

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestReconcileFleetUsesLatestReadySourceDeployment(t *testing.T) {
	db := openTestDB(t)
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const litestreamSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const imageRef = "registry.fly.io/litestream-soak:sha-aaaaaaaaaaaa-ls-bbbbbbbbbbbb"
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ImageRef:      imageRef,
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	manager.reconcileFleet(context.Background(), FleetSpec{Workers: []DesiredWorker{{
		WorkerID:    "worker-main-low-vol",
		Name:        "worker-main-low-vol",
		Source:      "main",
		GitSHA:      "main",
		ProfileName: "low-volume",
		Region:      "ord",
		Workload:    workload.Config{LoadMode: "synthetic", InitialSize: "5MB"},
	}}})

	worker := mustWorker(t, db, "worker-main-low-vol")
	if worker.GitSHA != sha {
		t.Fatalf("GitSHA = %q, want %q", worker.GitSHA, sha)
	}
	if worker.LitestreamSHA != litestreamSHA {
		t.Fatalf("LitestreamSHA = %q, want %q", worker.LitestreamSHA, litestreamSHA)
	}
	machines := fly.machineRequests()
	if len(machines) != 1 {
		t.Fatalf("len(machine requests) = %d, want 1", len(machines))
	}
	if machines[0].Config.Image != imageRef {
		t.Fatalf("machine image = %q, want %q", machines[0].Config.Image, imageRef)
	}
}

func TestReconcileFleetWithoutReadyDeploymentWaitsForBootstrap(t *testing.T) {
	db := openTestDB(t)
	const unrelatedImageRef = "registry.fly.io/litestream-soak:sha-cccccccccccc-pr-1345-ls-dddddddddddd"
	fly := newDeployTestFlyServer(
		t,
		db,
		"pr-1345",
		"cccccccccccccccccccccccccccccccccccccccc",
		"dddddddddddddddddddddddddddddddddddddddd",
		unrelatedImageRef,
	)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	manager.reconcileFleet(context.Background(), FleetSpec{Workers: []DesiredWorker{{
		WorkerID:    "worker-main-low-vol",
		Name:        "worker-main-low-vol",
		Source:      "main",
		GitSHA:      "main",
		ProfileName: "low-volume",
		Region:      "ord",
		Workload:    workload.Config{LoadMode: "synthetic", InitialSize: "5MB"},
	}}})

	workers, err := db.ListWorkersForSource("main")
	if err != nil {
		t.Fatalf("ListWorkersForSource() error = %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("len(workers) = %d, want 0 until deployment-ready bootstraps main", len(workers))
	}
	fly.assertCreateCounts(t, 0, 0)
}

func TestEnsureFleetSpecSkipsNonFiniteDesiredConfig(t *testing.T) {
	db := openTestDB(t)
	const workerID = "worker-main-low-vol"
	const profileConfig = `{"load_mode":"synthetic","write_rate":10}`
	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		AppName:       "litestream-soak",
		Name:          workerID,
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		LitestreamSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ProfileName:   "low-volume",
		ProfileConfig: profileConfig,
		Region:        "ord",
		FlyMachineID:  "old-machine",
		FlyVolumeID:   "old-volume",
	})

	spec := FleetSpec{Workers: []DesiredWorker{{
		WorkerID:    workerID,
		Name:        workerID,
		Source:      "main",
		ProfileName: "low-volume",
		Region:      "ord",
		Workload: workload.Config{
			LoadMode:  "synthetic",
			WriteRate: 10,
			ReadRatio: math.NaN(),
		},
	}}}
	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")

	for cycle := 1; cycle <= 2; cycle++ {
		err := manager.ensureFleetSpec(context.Background(), spec, "registry.fly.io/litestream-soak:test")
		if err == nil || !strings.Contains(err.Error(), "unsupported value") {
			t.Fatalf("cycle %d ensureFleetSpec() error = %v, want marshal error", cycle, err)
		}
	}

	worker := mustWorker(t, db, workerID)
	if worker.ProfileConfig != profileConfig {
		t.Fatalf("ProfileConfig = %q, want unchanged %q", worker.ProfileConfig, profileConfig)
	}
	if got := len(fly.volumeRequests()); got != 0 {
		t.Fatalf("volume creates = %d, want 0", got)
	}
	if got := len(fly.machineRequests()); got != 0 {
		t.Fatalf("machine creates = %d, want 0", got)
	}
	stops, machineDeletes, volumeDeletes := fly.replacementCounts()
	if stops != 0 || machineDeletes != 0 || volumeDeletes != 0 {
		t.Fatalf("replacement operations = stops:%d machine deletes:%d volume deletes:%d, want zero", stops, machineDeletes, volumeDeletes)
	}
}

func TestEnsureFleetSpecCapsConfigDriftReplacementsPerCycle(t *testing.T) {
	db := openTestDB(t)
	spec := FleetSpec{}
	for i := 1; i <= 3; i++ {
		workerID := fmt.Sprintf("worker-main-drift-%d", i)
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			AppName:       "litestream-soak",
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			LitestreamSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			ProfileName:   "low-volume",
			ProfileConfig: normalizeWorkloadConfig(workload.Config{WriteRate: 1}).JSON(),
			Region:        "ord",
			FlyMachineID:  fmt.Sprintf("old-machine-%d", i),
			FlyVolumeID:   fmt.Sprintf("old-volume-%d", i),
		})
		spec.Workers = append(spec.Workers, DesiredWorker{
			WorkerID:      workerID,
			Name:          workerID,
			Source:        "main",
			GitSHA:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			LitestreamSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			ProfileName:   "low-volume",
			Region:        "ord",
			Workload:      workload.Config{WriteRate: 10},
		})
	}

	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	if err := manager.ensureFleetSpec(context.Background(), spec, "registry.fly.io/litestream-soak:test"); err != nil {
		t.Fatalf("first ensureFleetSpec() error = %v", err)
	}
	if got := len(fly.machineRequests()); got != 1 {
		t.Fatalf("machine creates after first cycle = %d, want 1", got)
	}

	if err := manager.ensureFleetSpec(context.Background(), spec, "registry.fly.io/litestream-soak:test"); err != nil {
		t.Fatalf("second ensureFleetSpec() error = %v", err)
	}
	if got := len(fly.machineRequests()); got != 2 {
		t.Fatalf("machine creates after second cycle = %d, want 2", got)
	}
}

func TestReconcileFleetReplacesConfigDriftOnce(t *testing.T) {
	db := openTestDB(t)
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const litestreamSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const imageRef = "registry.fly.io/litestream-soak:sha-aaaaaaaaaaaa-ls-bbbbbbbbbbbb"
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ImageRef:      imageRef,
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	const workerID = "worker-main-many-dbs-100-dir"
	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		AppName:       "litestream-soak",
		Name:          workerID,
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ProfileName:   "many-dbs-100-dir",
		ProfileConfig: workload.Config{
			LoadMode:           "many-db",
			NumDatabases:       100,
			MaxRowsPerDatabase: 40_000,
			ConfigMode:         "dir",
			VolumeSizeGB:       10,
			MemoryMB:           2048,
			CPUs:               1,
		}.JSON(),
		Region:       "ord",
		FlyMachineID: "old-machine",
		FlyVolumeID:  "old-volume",
	})

	spec := FleetSpec{Workers: []DesiredWorker{{
		WorkerID:     workerID,
		Name:         workerID,
		Source:       "main",
		ProfileName:  "many-dbs-100-dir",
		Region:       "ord",
		VolumeSizeGB: 10,
		Workload: workload.Config{
			LoadMode:     "many-db",
			NumDatabases: 100,
			ConfigMode:   "dir",
			VolumeSizeGB: 10,
			MemoryMB:     2048,
			CPUs:         1,
		},
	}}}
	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")

	manager.reconcileFleet(context.Background(), spec)

	worker := mustWorker(t, db, workerID)
	config, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	if config.MaxRowsPerDatabase != 50_000 {
		t.Fatalf("MaxRowsPerDatabase = %d, want 50000", config.MaxRowsPerDatabase)
	}
	if got := len(fly.volumeRequests()); got != 1 {
		t.Fatalf("volume creates after first reconciliation = %d, want 1", got)
	}
	if got := len(fly.machineRequests()); got != 1 {
		t.Fatalf("machine creates after first reconciliation = %d, want 1", got)
	}
	stops, machineDeletes, volumeDeletes := fly.replacementCounts()
	if stops != 1 || machineDeletes != 1 || volumeDeletes != 1 {
		t.Fatalf("replacement operations after first reconciliation = stops:%d machine deletes:%d volume deletes:%d, want 1 each", stops, machineDeletes, volumeDeletes)
	}

	manager.reconcileFleet(context.Background(), spec)

	if got := len(fly.volumeRequests()); got != 1 {
		t.Fatalf("volume creates after second reconciliation = %d, want 1", got)
	}
	if got := len(fly.machineRequests()); got != 1 {
		t.Fatalf("machine creates after second reconciliation = %d, want 1", got)
	}
	stops, machineDeletes, volumeDeletes = fly.replacementCounts()
	if stops != 1 || machineDeletes != 1 || volumeDeletes != 1 {
		t.Fatalf("replacement operations after second reconciliation = stops:%d machine deletes:%d volume deletes:%d, want unchanged at 1 each", stops, machineDeletes, volumeDeletes)
	}
}

func TestReconcileFleetDoesNotReplaceSemanticallyEquivalentConfig(t *testing.T) {
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const litestreamSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const imageRef = "registry.fly.io/litestream-soak:sha-aaaaaaaaaaaa-ls-bbbbbbbbbbbb"

	tests := []struct {
		name          string
		profileName   string
		profileConfig string
		workload      workload.Config
	}{
		{
			name:          "omitted and explicit defaults",
			profileName:   "low-volume",
			profileConfig: `{"load_mode":"synthetic","initial_size":"5MB","verify_interval":"30m","snapshot_interval":"10m","sync_interval":"1s","memory_mb":1024,"cpus":1}`,
			workload:      workload.Config{},
		},
		{
			name:          "zero and unset numeric defaults",
			profileName:   "many-dbs-100-dir",
			profileConfig: `{"load_mode":"many-db","num_databases":100,"active_percent":2,"active_rotate_interval":"30m","active_set_seed":1,"config_mode":"dir","verify_sample_size":5,"verify_changed_limit":100,"memory_mb":1024,"cpus":1}`,
			workload: workload.Config{
				LoadMode:     "many-db",
				NumDatabases: 100,
				ConfigMode:   "dir",
			},
		},
		{
			name:          "empty and absent strings",
			profileName:   "low-volume",
			profileConfig: `{"load_mode":"","initial_size":"","verify_interval":"","snapshot_interval":"","sync_interval":"","memory_mb":1024,"cpus":1}`,
			workload:      workload.Config{},
		},
		{
			name:          "worker defaulted many db retention bound",
			profileName:   "many-dbs-100-dir",
			profileConfig: `{"load_mode":"many-db","num_databases":100,"max_rows_per_database":50000,"active_percent":2,"active_rotate_interval":"30m","active_set_seed":1,"config_mode":"dir","verify_sample_size":5,"verify_changed_limit":100,"memory_mb":1024,"cpus":1}`,
			workload: workload.Config{
				LoadMode:     "many-db",
				NumDatabases: 100,
				ConfigMode:   "dir",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			if err := db.UpsertReadyDeployment(&model.Deployment{
				GitSHA:        sha,
				LitestreamSHA: litestreamSHA,
				ImageRef:      imageRef,
				Source:        "main",
				Status:        "ready",
			}); err != nil {
				t.Fatalf("UpsertReadyDeployment() error = %v", err)
			}

			workerID := "worker-main-" + strings.ReplaceAll(test.name, " ", "-")
			createTestWorker(t, db, model.Worker{
				ID:            workerID,
				AppName:       "litestream-soak",
				Name:          workerID,
				Status:        model.WorkerRunning,
				Source:        "main",
				GitSHA:        sha,
				LitestreamSHA: litestreamSHA,
				ProfileName:   test.profileName,
				ProfileConfig: test.profileConfig,
				Region:        "ord",
				FlyMachineID:  "old-machine",
				FlyVolumeID:   "old-volume",
			})

			fly := newCreateWorkerFlyServer(t)
			manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
			spec := FleetSpec{Workers: []DesiredWorker{{
				WorkerID:    workerID,
				Name:        workerID,
				Source:      "main",
				ProfileName: test.profileName,
				Region:      "ord",
				Workload:    test.workload,
			}}}
			manager.reconcileFleet(context.Background(), spec)
			manager.reconcileFleet(context.Background(), spec)

			if got := len(fly.volumeRequests()); got != 0 {
				t.Fatalf("volume creates = %d, want 0", got)
			}
			if got := len(fly.machineRequests()); got != 0 {
				t.Fatalf("machine creates = %d, want 0", got)
			}
			stops, machineDeletes, volumeDeletes := fly.replacementCounts()
			if stops != 0 || machineDeletes != 0 || volumeDeletes != 0 {
				t.Fatalf("replacement operations = stops:%d machine deletes:%d volume deletes:%d, want zero", stops, machineDeletes, volumeDeletes)
			}
		})
	}
}

func TestReconcileFleetReplacesPRWorkerWithDesiredConfigAndResources(t *testing.T) {
	db := openTestDB(t)
	const source = "pr-149"
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const litestreamSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const imageRef = "registry.fly.io/litestream-soak:sha-aaaaaaaaaaaa-pr-149-ls-bbbbbbbbbbbb"
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ImageRef:      imageRef,
		Source:        source,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	const workerID = "worker-pr-149-many-dbs-100-dir"
	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		AppName:       "litestream-soak",
		Name:          workerID,
		Status:        model.WorkerRunning,
		Source:        source,
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		PRNumber:      149,
		ProfileName:   "many-dbs-100-dir",
		ProfileConfig: workload.Config{
			LoadMode:         "many-db",
			NumDatabases:     100,
			ConfigMode:       "dir",
			SnapshotInterval: "1h",
			VolumeSizeGB:     10,
			MemoryMB:         1024,
			CPUs:             1,
		}.JSON(),
		Region:       "ord",
		FlyMachineID: "old-machine",
		FlyVolumeID:  "old-volume",
	})

	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	manager.reconcileFleet(context.Background(), FleetSpec{Workers: []DesiredWorker{{
		WorkerID:     workerID,
		Name:         workerID,
		Source:       source,
		ProfileName:  "many-dbs-100-dir",
		Region:       "syd",
		VolumeSizeGB: 20,
		Workload: workload.Config{
			LoadMode:         "many-db",
			NumDatabases:     100,
			ConfigMode:       "dir",
			SnapshotInterval: "10m",
			VolumeSizeGB:     10,
			MemoryMB:         2048,
			CPUs:             2,
		},
	}}})

	volumes := fly.volumeRequests()
	if len(volumes) != 1 {
		t.Fatalf("len(volume requests) = %d, want 1", len(volumes))
	}
	if volumes[0].Region != "syd" || volumes[0].SizeGB != 20 {
		t.Fatalf("volume request = region:%q size:%d, want syd/20", volumes[0].Region, volumes[0].SizeGB)
	}
	machines := fly.machineRequests()
	if len(machines) != 1 {
		t.Fatalf("len(machine requests) = %d, want 1", len(machines))
	}
	if machines[0].Region != "syd" {
		t.Fatalf("machine region = %q, want syd", machines[0].Region)
	}
	if machines[0].Config.Guest.CPUs != 2 || machines[0].Config.Guest.MemoryMB != 2048 {
		t.Fatalf("machine guest = CPUs:%d memory:%d, want 2/2048", machines[0].Config.Guest.CPUs, machines[0].Config.Guest.MemoryMB)
	}
	if machines[0].Config.Env["SNAPSHOT_INTERVAL"] != "10m" {
		t.Fatalf("SNAPSHOT_INTERVAL = %q, want 10m", machines[0].Config.Env["SNAPSHOT_INTERVAL"])
	}
	if machines[0].Config.Env["MAX_ROWS_PER_DATABASE"] != "50000" {
		t.Fatalf("MAX_ROWS_PER_DATABASE = %q, want 50000", machines[0].Config.Env["MAX_ROWS_PER_DATABASE"])
	}

	worker := mustWorker(t, db, workerID)
	if worker.Region != "syd" {
		t.Fatalf("worker.Region = %q, want syd", worker.Region)
	}
	config, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	if config.SnapshotInterval != "10m" || config.MaxRowsPerDatabase != 50_000 {
		t.Fatalf("stored config = snapshot:%q max rows:%d, want 10m/50000", config.SnapshotInterval, config.MaxRowsPerDatabase)
	}
	if config.VolumeSizeGB != 20 {
		t.Fatalf("stored volume size = %d, want effective desired size 20", config.VolumeSizeGB)
	}

	manager.reconcileFleet(context.Background(), FleetSpec{Workers: []DesiredWorker{{
		WorkerID:     workerID,
		Name:         workerID,
		Source:       source,
		ProfileName:  "many-dbs-100-dir",
		Region:       "syd",
		VolumeSizeGB: 20,
		Workload: workload.Config{
			LoadMode:         "many-db",
			NumDatabases:     100,
			ConfigMode:       "dir",
			SnapshotInterval: "10m",
			VolumeSizeGB:     10,
			MemoryMB:         2048,
			CPUs:             2,
		},
	}}})
	if got := len(fly.volumeRequests()); got != 1 {
		t.Fatalf("volume creates after second reconciliation = %d, want unchanged at 1", got)
	}
	if got := len(fly.machineRequests()); got != 1 {
		t.Fatalf("machine creates after second reconciliation = %d, want unchanged at 1", got)
	}
}

func TestReconcileFleetDoesNotReplaceDormantWorkerForConfigDrift(t *testing.T) {
	db := openTestDB(t)
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const litestreamSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const imageRef = "registry.fly.io/litestream-soak:sha-aaaaaaaaaaaa-ls-bbbbbbbbbbbb"
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ImageRef:      imageRef,
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	const workerID = "worker-main-many-dbs-100-dir"
	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		AppName:       "litestream-soak",
		Name:          workerID,
		Status:        model.WorkerDormant,
		Source:        "main",
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ProfileName:   "many-dbs-100-dir",
		ProfileConfig: workload.Config{
			LoadMode:           "many-db",
			NumDatabases:       100,
			MaxRowsPerDatabase: 40_000,
			ConfigMode:         "dir",
		}.JSON(),
		Region:       "ord",
		FlyMachineID: "old-machine",
		FlyVolumeID:  "old-volume",
	})

	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	spec := FleetSpec{Workers: []DesiredWorker{{
		WorkerID:    workerID,
		Name:        workerID,
		Source:      "main",
		ProfileName: "many-dbs-100-dir",
		Region:      "ord",
		Workload: workload.Config{
			LoadMode:     "many-db",
			NumDatabases: 100,
			ConfigMode:   "dir",
		},
	}}}
	manager.reconcileFleet(context.Background(), spec)
	manager.reconcileFleet(context.Background(), spec)

	worker := mustWorker(t, db, workerID)
	if worker.Status != model.WorkerDormant {
		t.Fatalf("worker.Status = %q, want dormant", worker.Status)
	}
	if got := len(fly.volumeRequests()); got != 0 {
		t.Fatalf("volume creates = %d, want 0", got)
	}
	if got := len(fly.machineRequests()); got != 0 {
		t.Fatalf("machine creates = %d, want 0", got)
	}
	stops, machineDeletes, volumeDeletes := fly.replacementCounts()
	if stops != 0 || machineDeletes != 0 || volumeDeletes != 0 {
		t.Fatalf("replacement operations = stops:%d machine deletes:%d volume deletes:%d, want zero", stops, machineDeletes, volumeDeletes)
	}
}

func TestSourcePRNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		want   int
	}{
		{source: "main", want: 0},
		{source: "pr-1221", want: 1221},
		{source: "pr-0", want: 0},
		{source: "pr-nope", want: 0},
	}

	for _, tc := range tests {
		if got := sourcePRNumber(tc.source); got != tc.want {
			t.Fatalf("sourcePRNumber(%q) = %d, want %d", tc.source, got, tc.want)
		}
	}
}

func TestWorkerNameForSource(t *testing.T) {
	t.Parallel()

	if got := workerNameForSource("main", "worker-main-low-vol"); got != "worker-main-low-vol" {
		t.Fatalf("workerNameForSource(main) = %q", got)
	}
	if got := workerNameForSource("pr-1221", "worker-main-low-vol"); got != "worker-pr-1221-low-vol" {
		t.Fatalf("workerNameForSource(pr-1221) = %q, want worker-pr-1221-low-vol", got)
	}
}

func TestDefaultFleetForSource(t *testing.T) {
	t.Parallel()

	spec := DefaultFleetForSource("pr-1221", "soak-sha", "litestream-sha")
	if len(spec.Workers) == 0 {
		t.Fatal("DefaultFleetForSource() returned no workers")
	}

	first := spec.Workers[0]
	if first.Source != "pr-1221" {
		t.Fatalf("first.Source = %q, want pr-1221", first.Source)
	}
	if first.GitSHA != "soak-sha" {
		t.Fatalf("first.GitSHA = %q, want soak-sha", first.GitSHA)
	}
	if first.LitestreamSHA != "litestream-sha" {
		t.Fatalf("first.LitestreamSHA = %q, want litestream-sha", first.LitestreamSHA)
	}
	if first.PRNumber != 1221 {
		t.Fatalf("first.PRNumber = %d, want 1221", first.PRNumber)
	}
	if first.Name != "worker-pr-1221-sentinel-single" {
		t.Fatalf("first.Name = %q, want worker-pr-1221-sentinel-single", first.Name)
	}

	for _, worker := range spec.Workers {
		if worker.VolumeSizeGB != 10 || worker.Workload.VolumeSizeGB != worker.VolumeSizeGB {
			t.Fatalf("%s volume = %d/%d, want 10/10", worker.ProfileName, worker.VolumeSizeGB, worker.Workload.VolumeSizeGB)
		}
	}
	if desired, ok := defaultFleetDesiredWorker("pr-1221", "worker-pr-1221-sentinel-manydb", "worker-pr-1221-sentinel-manydb"); !ok {
		t.Fatal("defaultFleetDesiredWorker() missing PR many-db sentinel worker")
	} else if desired.Name != "worker-pr-1221-sentinel-manydb" {
		t.Fatalf("desired.Name = %q, want worker-pr-1221-sentinel-manydb", desired.Name)
	}
}

func TestDefaultMainFleetExcludesFixtureSensitiveFaultProfiles(t *testing.T) {
	t.Parallel()

	spec := DefaultMainFleet()

	profiles := map[string]bool{}
	for _, worker := range spec.Workers {
		profiles[worker.ProfileName] = true
	}

	for _, profile := range []string{
		"constrained-disk",
		"compaction-source-stream-drop",
		"uploadpart-retry-quota",
		"provider-http-408",
		"provider-request-canceled",
		"s3-flap",
	} {
		if profiles[profile] {
			t.Fatalf("DefaultMainFleet() includes fixture-sensitive profile %q", profile)
		}
	}
}

func TestDefaultMainFleetIsTwoSentinels(t *testing.T) {
	spec := DefaultMainFleet()
	got := make([]string, 0, len(spec.Workers))
	byName := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		got = append(got, worker.Name)
		byName[worker.Name] = worker
	}
	want := []string{"worker-main-sentinel-single", "worker-main-sentinel-manydb"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultMainFleet() workers = %v, want %v", got, want)
	}

	for _, tc := range []struct {
		name    string
		profile string
	}{
		{name: "worker-main-sentinel-single", profile: "sentinel-single"},
		{name: "worker-main-sentinel-manydb", profile: "sentinel-manydb"},
	} {
		worker := byName[tc.name]
		if worker.WorkerID != tc.name || worker.Source != "main" || worker.GitSHA != "main" {
			t.Fatalf("%s identity = %q/%q/%q, want %s/main/main", tc.name, worker.WorkerID, worker.Source, worker.GitSHA, tc.name)
		}
		if worker.ProfileName != tc.profile {
			t.Fatalf("%s ProfileName = %q, want %q", tc.name, worker.ProfileName, tc.profile)
		}
		if worker.Region != "ord" {
			t.Fatalf("%s Region = %q, want ord", tc.name, worker.Region)
		}
		if worker.Workload.CPUs != 1 || worker.Workload.MemoryMB != 2048 {
			t.Fatalf("%s CPUs/MemoryMB = %d/%d, want 1/2048", tc.name, worker.Workload.CPUs, worker.Workload.MemoryMB)
		}
		if worker.VolumeSizeGB != 10 || worker.Workload.VolumeSizeGB != 10 {
			t.Fatalf("%s volume = %d/%d, want 10/10", tc.name, worker.VolumeSizeGB, worker.Workload.VolumeSizeGB)
		}
	}

	single := byName["worker-main-sentinel-single"].Workload
	if single.LoadMode != "synthetic" || single.WriteRate != 10 || single.Pattern != "constant" || single.PayloadSize != 1024 || single.ReadRatio != 0.2 || single.Workers != 1 || single.InitialSize != "5MB" {
		t.Fatalf("sentinel-single workload = %+v, want low-volume shape", single)
	}
	if single.VerifyInterval != "30m" || single.VerifyType != "integrity" || single.SnapshotInterval != "10m" || single.SyncInterval != "1s" {
		t.Fatalf("sentinel-single verify/snapshot/sync = %q/%q/%q/%q, want 30m/integrity/10m/1s", single.VerifyInterval, single.VerifyType, single.SnapshotInterval, single.SyncInterval)
	}
	if single.L0Retention != "" || single.L0RetentionCheckInterval != "" || single.L1CompactionInterval != "" || single.L2CompactionInterval != "" || single.L3CompactionInterval != "" {
		t.Fatalf("sentinel-single overrides Litestream defaults: %+v", single)
	}

	many := byName["worker-main-sentinel-manydb"].Workload
	if many.LoadMode != "many-db" || many.ConfigMode != "dir" || many.NumDatabases != 100 || many.ActivePercent != 2 || many.ActiveRotateInterval != "30m" || many.ActiveSetSeed != 1 {
		t.Fatalf("sentinel-manydb workload = %+v, want many-dbs-100-dir shape", many)
	}
	if many.Workers != 2 || many.WriteRate != 20 || many.VerifySampleSize != 5 || many.VerifyChangedLimit != 100 {
		t.Fatalf("sentinel-manydb load/verify = %d/%d/%d/%d, want 2/20/5/100", many.Workers, many.WriteRate, many.VerifySampleSize, many.VerifyChangedLimit)
	}
	if many.L0RetentionCheckInterval != "2m" || many.L1CompactionInterval != "5m" || many.L2CompactionInterval != "15m" || many.L3CompactionInterval != "1h" {
		t.Fatalf("sentinel-manydb L0 check/L1/L2/L3 = %q/%q/%q/%q, want 2m/5m/15m/1h", many.L0RetentionCheckInterval, many.L1CompactionInterval, many.L2CompactionInterval, many.L3CompactionInterval)
	}
	if many.S3FaultProxyEnabled {
		t.Fatal("sentinel-manydb S3FaultProxyEnabled = true, want false by default: the observe proxy is opt-in")
	}
}

func TestDefaultMainFleetIgnoresManyDBTierFlags(t *testing.T) {
	t.Setenv("SOAK_ENABLE_MANY_DB_FLEET", "true")
	t.Setenv("SOAK_ENABLE_MANY_DB_500", "true")
	t.Setenv("SOAK_ENABLE_MANY_DB_1000", "true")

	for _, spec := range []FleetSpec{DefaultMainFleet(), DefaultFleetForSource("pr-1221", "soak-sha", "litestream-sha")} {
		if len(spec.Workers) != 2 {
			t.Fatalf("fleet with many-db tier flags has %d workers, want 2", len(spec.Workers))
		}
		for _, worker := range spec.Workers {
			if strings.HasPrefix(worker.ProfileName, "many-dbs-") {
				t.Fatalf("fleet includes tier profile %q with many-db tier flags set", worker.ProfileName)
			}
		}
	}
}

func TestManyDBProfilesExcludedFromReleaseQuality(t *testing.T) {
	t.Parallel()

	for _, profile := range []string{"many-dbs-100-dir", "many-dbs-500-dir-lowfreq", "sentinel-manydb"} {
		if workerIncludedInReleaseQuality(model.Worker{ProfileName: profile, Region: "ord"}) {
			t.Fatalf("%s should be excluded from release quality", profile)
		}
	}
	for _, profile := range []string{"low-volume", "sentinel-single"} {
		if !workerIncludedInReleaseQuality(model.Worker{ProfileName: profile, Region: "ord"}) {
			t.Fatalf("%s in ord should be included in release quality", profile)
		}
	}
}

func TestDefaultFleetForSourceRewritesSentinelWorkers(t *testing.T) {
	t.Parallel()

	spec := DefaultFleetForSource("pr-1221", "soak-sha", "litestream-sha")
	got := make([]string, 0, len(spec.Workers))
	for _, worker := range spec.Workers {
		if worker.WorkerID != worker.Name {
			t.Fatalf("WorkerID = %q, want Name %q", worker.WorkerID, worker.Name)
		}
		if worker.Source != "pr-1221" || worker.GitSHA != "soak-sha" || worker.LitestreamSHA != "litestream-sha" || worker.PRNumber != 1221 {
			t.Fatalf("%s identity = %q/%q/%q/%d, want pr-1221/soak-sha/litestream-sha/1221", worker.Name, worker.Source, worker.GitSHA, worker.LitestreamSHA, worker.PRNumber)
		}
		got = append(got, worker.Name)
	}
	want := []string{"worker-pr-1221-sentinel-single", "worker-pr-1221-sentinel-manydb"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultFleetForSource() workers = %v, want %v", got, want)
	}
}

func TestResolveWorkerVolumeSizeUsesDefaultFleetForRollouts(t *testing.T) {
	t.Parallel()

	worker := model.Worker{
		ID:            "worker-pr-1228-sentinel-manydb",
		Name:          "worker-pr-1228-sentinel-manydb",
		Source:        "pr-1228",
		ProfileName:   "sentinel-manydb",
		ProfileConfig: workload.Config{LoadMode: "many-db", VolumeSizeGB: 3}.JSON(),
	}

	parsedCfg, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		t.Fatalf("ParseConfig(%q) error = %v, want nil", worker.ProfileConfig, err)
	}
	if got := resolveWorkerVolumeSize(worker, normalizeWorkload(parsedCfg)); got != 10 {
		t.Fatalf("resolveWorkerVolumeSize() = %d, want 10", got)
	}
}

func TestManyDBObserveProxyOptInViaEnv(t *testing.T) {
	t.Setenv("SOAK_ENABLE_S3_OBSERVE_PROXY", "true")

	for _, worker := range DefaultMainFleet().Workers {
		wantProxy := worker.ProfileName == "sentinel-manydb"
		if worker.Workload.S3FaultProxyEnabled != wantProxy {
			t.Fatalf("%s S3FaultProxyEnabled = %v, want %v when SOAK_ENABLE_S3_OBSERVE_PROXY set", worker.ProfileName, worker.Workload.S3FaultProxyEnabled, wantProxy)
		}
		if wantProxy && worker.Workload.S3FaultProxyMode != "observe" {
			t.Fatalf("%s S3FaultProxyMode = %q, want observe", worker.ProfileName, worker.Workload.S3FaultProxyMode)
		}
	}
}
