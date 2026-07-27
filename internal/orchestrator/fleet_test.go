package orchestrator

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
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
			WorkerID:    workerID,
			Name:        workerID,
			Source:      "main",
			ProfileName: "low-volume",
			Region:      "ord",
			Workload:    workload.Config{WriteRate: 10},
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
	if first.Name != "worker-pr-1221-low-vol" {
		t.Fatalf("first.Name = %q, want worker-pr-1221-low-vol", first.Name)
	}

	volumeSizes := map[string]int{}
	for _, worker := range spec.Workers {
		if worker.VolumeSizeGB != 0 {
			volumeSizes[worker.ProfileName] = worker.VolumeSizeGB
		}
		if worker.VolumeSizeGB != 0 && worker.Workload.VolumeSizeGB != worker.VolumeSizeGB {
			t.Fatalf("%s Workload.VolumeSizeGB = %d, want %d", worker.ProfileName, worker.Workload.VolumeSizeGB, worker.VolumeSizeGB)
		}
	}
	if got := volumeSizes["high-volume"]; got != 100 {
		t.Fatalf("high-volume VolumeSizeGB = %d, want 100", got)
	}
	if got := volumeSizes["burst-volume"]; got != 100 {
		t.Fatalf("burst-volume VolumeSizeGB = %d, want 100", got)
	}
	if got := volumeSizes["gharchive-replay"]; got != 50 {
		t.Fatalf("gharchive-replay VolumeSizeGB = %d, want 50", got)
	}
	if got := volumeSizes["gharchive-mixed"]; got != 50 {
		t.Fatalf("gharchive-mixed VolumeSizeGB = %d, want 50", got)
	}
	if desired, ok := defaultFleetDesiredWorker("pr-1221", "worker-pr-1221-high-vol", "worker-pr-1221-high-vol"); !ok {
		t.Fatal("defaultFleetDesiredWorker() missing PR high-volume worker")
	} else if desired.Name != "worker-pr-1221-high-vol" {
		t.Fatalf("desired.Name = %q, want worker-pr-1221-high-vol", desired.Name)
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

func TestDefaultMainFleetKeepsAlwaysOnLoadProfiles(t *testing.T) {
	t.Parallel()

	spec := DefaultMainFleet()
	profiles := map[string]bool{}
	for _, worker := range spec.Workers {
		profiles[worker.ProfileName] = true
	}

	for _, profile := range []string{
		"low-volume",
		"high-volume",
		"burst-volume",
		"read-heavy",
		"gharchive-replay",
		"gharchive-mixed",
		"taxi-replay",
		"taxi-mixed",
		"orders-replay",
		"low-vol-syd",
		"high-vol-ams",
	} {
		if !profiles[profile] {
			t.Fatalf("DefaultMainFleet() missing always-on load profile %q", profile)
		}
	}
}

func TestDefaultMainFleetIncludesRegionalWorkers(t *testing.T) {
	t.Parallel()

	spec := DefaultMainFleet()
	regional := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		if worker.Region == "ord" {
			continue
		}
		regional[worker.ProfileName] = worker
	}

	lowVol, ok := regional["low-vol-syd"]
	if !ok {
		t.Fatal("DefaultMainFleet() missing low-vol-syd regional worker")
	}
	if lowVol.Region != "syd" {
		t.Fatalf("low-vol-syd Region = %q, want syd", lowVol.Region)
	}
	if lowVol.Workload.WriteRate != 10 || lowVol.Workload.Pattern != "constant" {
		t.Fatalf("low-vol-syd workload = %+v, want low-volume shape", lowVol.Workload)
	}

	highVol, ok := regional["high-vol-ams"]
	if !ok {
		t.Fatal("DefaultMainFleet() missing high-vol-ams regional worker")
	}
	if highVol.Region != "ams" {
		t.Fatalf("high-vol-ams Region = %q, want ams", highVol.Region)
	}
	if highVol.VolumeSizeGB != 100 {
		t.Fatalf("high-vol-ams VolumeSizeGB = %d, want 100", highVol.VolumeSizeGB)
	}
	if highVol.Workload.WriteRate != 500 || highVol.Workload.Pattern != "wave" {
		t.Fatalf("high-vol-ams workload = %+v, want high-volume shape", highVol.Workload)
	}
}

func TestDefaultMainFleetTunesHighVolumeS3Uploads(t *testing.T) {
	t.Parallel()

	spec := DefaultMainFleet()
	highVolume := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		switch worker.ProfileName {
		case "high-volume", "high-vol-ams":
			highVolume[worker.ProfileName] = worker
		}
	}

	for _, profile := range []string{"high-volume", "high-vol-ams"} {
		worker, ok := highVolume[profile]
		if !ok {
			t.Fatalf("DefaultMainFleet() missing %s", profile)
		}
		if worker.Workload.S3PartSize != "16MB" {
			t.Fatalf("%s S3PartSize = %q, want 16MB", profile, worker.Workload.S3PartSize)
		}
		if worker.Workload.S3Concurrency != 8 {
			t.Fatalf("%s S3Concurrency = %d, want 8", profile, worker.Workload.S3Concurrency)
		}
	}
}

func manyDBProfiles(spec FleetSpec) map[string]DesiredWorker {
	many := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		if strings.HasPrefix(worker.ProfileName, "many-dbs-") {
			many[worker.ProfileName] = worker
		}
	}
	return many
}

func TestManyDBFleetGating(t *testing.T) {
	tests := []struct {
		name     string
		mainFlag string
		k500     string
		k1000    string
		want     []string
	}{
		{name: "all off", mainFlag: "", k500: "", k1000: "", want: []string{}},
		{name: "main only enables 100 tier", mainFlag: "true", k500: "", k1000: "", want: []string{"many-dbs-100-dir", "many-dbs-100-list"}},
		{name: "500 flag without main is inert", mainFlag: "", k500: "true", k1000: "", want: []string{}},
		{name: "1000 flag without main is inert", mainFlag: "", k500: "", k1000: "true", want: []string{}},
		{name: "main plus 500 adds the 500 tier", mainFlag: "true", k500: "true", k1000: "", want: []string{"many-dbs-100-dir", "many-dbs-100-list", "many-dbs-500-dir", "many-dbs-500-dir-lowfreq", "many-dbs-500-list"}},
		{name: "main plus 1000 enables three", mainFlag: "true", k500: "", k1000: "true", want: []string{"many-dbs-100-dir", "many-dbs-100-list", "many-dbs-1000-dir"}},
		{name: "all flags enable all six", mainFlag: "true", k500: "true", k1000: "true", want: []string{"many-dbs-100-dir", "many-dbs-100-list", "many-dbs-1000-dir", "many-dbs-500-dir", "many-dbs-500-dir-lowfreq", "many-dbs-500-list"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SOAK_ENABLE_MANY_DB_FLEET", tc.mainFlag)
			t.Setenv("SOAK_ENABLE_MANY_DB_500", tc.k500)
			t.Setenv("SOAK_ENABLE_MANY_DB_1000", tc.k1000)

			many := manyDBProfiles(DefaultMainFleet())
			got := make([]string, 0, len(many))
			for name := range many {
				got = append(got, name)
			}
			sort.Strings(got)
			want := append([]string{}, tc.want...)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("many-dbs profiles = %v, want %v", got, want)
			}
		})
	}
}

func TestDefaultMainFleetIncludesManyDBProfilesWhenEnabled(t *testing.T) {
	t.Setenv("SOAK_ENABLE_MANY_DB_FLEET", "true")
	t.Setenv("SOAK_ENABLE_MANY_DB_500", "true")
	t.Setenv("SOAK_ENABLE_MANY_DB_1000", "true")

	spec := DefaultMainFleet()
	many := manyDBProfiles(spec)

	tests := []struct {
		profile      string
		numDatabases int
		configMode   string
		volumeGB     int
		memoryMB     int
		cpus         int
		workers      int
	}{
		{profile: "many-dbs-100-list", numDatabases: 100, configMode: "list", volumeGB: 10, memoryMB: 2048, cpus: 1, workers: 2},
		{profile: "many-dbs-100-dir", numDatabases: 100, configMode: "dir", volumeGB: 10, memoryMB: 2048, cpus: 1, workers: 2},
		{profile: "many-dbs-500-list", numDatabases: 500, configMode: "list", volumeGB: 15, memoryMB: 3072, cpus: 2, workers: 3},
		{profile: "many-dbs-500-dir", numDatabases: 500, configMode: "dir", volumeGB: 15, memoryMB: 3072, cpus: 2, workers: 3},
		{profile: "many-dbs-500-dir-lowfreq", numDatabases: 500, configMode: "dir", volumeGB: 15, memoryMB: 3072, cpus: 2, workers: 3},
		{profile: "many-dbs-1000-dir", numDatabases: 1000, configMode: "dir", volumeGB: 20, memoryMB: 4096, cpus: 2, workers: 4},
	}

	for _, tc := range tests {
		worker, ok := many[tc.profile]
		if !ok {
			t.Fatalf("DefaultMainFleet() missing %s", tc.profile)
		}
		if worker.Workload.Workers != tc.workers {
			t.Fatalf("%s Workers = %d, want %d", tc.profile, worker.Workload.Workers, tc.workers)
		}
		if worker.Workload.NumDatabases != tc.numDatabases {
			t.Fatalf("%s NumDatabases = %d, want %d", tc.profile, worker.Workload.NumDatabases, tc.numDatabases)
		}
		if worker.Workload.ActivePercent != 2 {
			t.Fatalf("%s ActivePercent = %v, want 2", tc.profile, worker.Workload.ActivePercent)
		}
		if worker.Workload.ActiveRotateInterval != "30m" {
			t.Fatalf("%s ActiveRotateInterval = %q, want 30m", tc.profile, worker.Workload.ActiveRotateInterval)
		}
		if worker.Workload.ActiveSetSeed != 1 {
			t.Fatalf("%s ActiveSetSeed = %d, want 1", tc.profile, worker.Workload.ActiveSetSeed)
		}
		if worker.Workload.ConfigMode != tc.configMode {
			t.Fatalf("%s ConfigMode = %q, want %q", tc.profile, worker.Workload.ConfigMode, tc.configMode)
		}
		if worker.Workload.VerifySampleSize != 5 {
			t.Fatalf("%s VerifySampleSize = %d, want 5", tc.profile, worker.Workload.VerifySampleSize)
		}
		if worker.Workload.VerifyChangedLimit != 100 {
			t.Fatalf("%s VerifyChangedLimit = %d, want 100", tc.profile, worker.Workload.VerifyChangedLimit)
		}
		if worker.VolumeSizeGB != tc.volumeGB || worker.Workload.VolumeSizeGB != tc.volumeGB {
			t.Fatalf("%s volume = %d/%d, want %d", tc.profile, worker.VolumeSizeGB, worker.Workload.VolumeSizeGB, tc.volumeGB)
		}
		if worker.Workload.MemoryMB != tc.memoryMB {
			t.Fatalf("%s MemoryMB = %d, want %d", tc.profile, worker.Workload.MemoryMB, tc.memoryMB)
		}
		if worker.Workload.CPUs != tc.cpus {
			t.Fatalf("%s CPUs = %d, want %d", tc.profile, worker.Workload.CPUs, tc.cpus)
		}
	}

	for profile, worker := range many {
		if worker.Workload.S3FaultProxyEnabled {
			t.Fatalf("%s S3FaultProxyEnabled = true, want false by default until the proxy re-signs requests (issue #146)", profile)
		}
	}

	lowfreq := many["many-dbs-500-dir-lowfreq"]
	if lowfreq.Workload.SnapshotInterval != "1h" {
		t.Fatalf("lowfreq SnapshotInterval = %q, want 1h", lowfreq.Workload.SnapshotInterval)
	}
	if lowfreq.Workload.L1CompactionInterval != "5m" {
		t.Fatalf("lowfreq L1CompactionInterval = %q, want 5m", lowfreq.Workload.L1CompactionInterval)
	}
	if lowfreq.Workload.L2CompactionInterval != "30m" {
		t.Fatalf("lowfreq L2CompactionInterval = %q, want 30m", lowfreq.Workload.L2CompactionInterval)
	}
	if lowfreq.Workload.L3CompactionInterval != "6h" {
		t.Fatalf("lowfreq L3CompactionInterval = %q, want 6h", lowfreq.Workload.L3CompactionInterval)
	}
	if lowfreq.Workload.L0Retention != "1h" {
		t.Fatalf("lowfreq L0Retention = %q, want 1h", lowfreq.Workload.L0Retention)
	}
	if lowfreq.Workload.L0RetentionCheckInterval != "2m" {
		t.Fatalf("lowfreq L0RetentionCheckInterval = %q, want 2m", lowfreq.Workload.L0RetentionCheckInterval)
	}

	for _, profile := range []string{"many-dbs-500-list", "many-dbs-500-dir"} {
		w := many[profile].Workload
		if w.L1CompactionInterval != "" || w.L2CompactionInterval != "" || w.L3CompactionInterval != "" ||
			w.L0Retention != "" || w.L0RetentionCheckInterval != "" {
			t.Fatalf("%s compaction/retention knobs = %q/%q/%q/%q/%q, want all empty",
				profile, w.L1CompactionInterval, w.L2CompactionInterval, w.L3CompactionInterval,
				w.L0Retention, w.L0RetentionCheckInterval)
		}
	}
}

func TestManyDBProfilesExcludedFromReleaseQuality(t *testing.T) {
	t.Parallel()

	if workerIncludedInReleaseQuality(model.Worker{ProfileName: "many-dbs-100-dir", Region: "ord"}) {
		t.Fatal("many-dbs-100-dir should be excluded from release quality")
	}
	if workerIncludedInReleaseQuality(model.Worker{ProfileName: "many-dbs-500-dir-lowfreq", Region: "ord"}) {
		t.Fatal("many-dbs-500-dir-lowfreq should be excluded from release quality")
	}
	if !workerIncludedInReleaseQuality(model.Worker{ProfileName: "low-volume", Region: "ord"}) {
		t.Fatal("low-volume in ord should be included in release quality")
	}
}

func TestDefaultFleetForSourceRewritesRegionalWorkers(t *testing.T) {
	t.Parallel()

	spec := DefaultFleetForSource("pr-1221", "soak-sha", "litestream-sha")
	regional := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		if worker.Region == "ord" {
			continue
		}
		regional[worker.ProfileName] = worker
	}

	lowVol, ok := regional["low-vol-syd"]
	if !ok {
		t.Fatal("DefaultFleetForSource() missing low-vol-syd regional worker")
	}
	if lowVol.WorkerID != "worker-pr-1221-low-vol-syd" {
		t.Fatalf("low-vol-syd WorkerID = %q, want worker-pr-1221-low-vol-syd", lowVol.WorkerID)
	}
	if lowVol.Name != "worker-pr-1221-low-vol-syd" {
		t.Fatalf("low-vol-syd Name = %q, want worker-pr-1221-low-vol-syd", lowVol.Name)
	}
	if lowVol.Region != "syd" {
		t.Fatalf("low-vol-syd Region = %q, want syd", lowVol.Region)
	}
}

func TestResolveWorkerVolumeSizeUsesDefaultFleetForRollouts(t *testing.T) {
	t.Parallel()

	worker := model.Worker{
		ID:            "worker-pr-1228-high-vol",
		Name:          "worker-pr-1228-high-vol",
		Source:        "pr-1228",
		ProfileName:   "high-volume",
		ProfileConfig: workload.Config{LoadMode: "synthetic"}.JSON(),
	}

	parsedCfg, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		t.Fatalf("ParseConfig(%q) error = %v, want nil", worker.ProfileConfig, err)
	}
	if got := resolveWorkerVolumeSize(worker, normalizeWorkload(parsedCfg)); got != 100 {
		t.Fatalf("resolveWorkerVolumeSize() = %d, want 100", got)
	}
}

func TestManyDBObserveProxyOptInViaEnv(t *testing.T) {
	t.Setenv("SOAK_ENABLE_MANY_DB_FLEET", "true")
	t.Setenv("SOAK_ENABLE_S3_OBSERVE_PROXY", "true")

	for _, worker := range DefaultMainFleet().Workers {
		if !strings.HasPrefix(worker.ProfileName, "many-dbs-") {
			continue
		}
		if !worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != "observe" {
			t.Fatalf("%s proxy = %v/%q, want enabled observe when SOAK_ENABLE_S3_OBSERVE_PROXY set", worker.ProfileName, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode)
		}
	}
}
