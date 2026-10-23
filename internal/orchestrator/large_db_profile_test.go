package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	workerconfig "github.com/corylanou/litestream-soak/internal/worker"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func largeDBWorker(t *testing.T, spec FleetSpec) DesiredWorker {
	t.Helper()
	for _, worker := range spec.Workers {
		if worker.ProfileName == "large-db" {
			return worker
		}
	}
	t.Fatal("large-db profile missing")
	return DesiredWorker{}
}

func TestLargeDBProfileInMainAndPRFleets(t *testing.T) {
	for _, tc := range []struct {
		spec FleetSpec
		name string
	}{
		{DefaultMainFleet(), "worker-main-large-db"},
		{DefaultFleetForSource("pr-1568", "soak-sha", "litestream-sha"), "worker-pr-1568-large-db"},
	} {
		worker := largeDBWorker(t, tc.spec)
		cfg := worker.Workload
		if worker.Name != tc.name || worker.WorkerID != tc.name {
			t.Fatalf("name = %q id = %q, want %q", worker.Name, worker.WorkerID, tc.name)
		}
		if cfg.LoadMode != "synthetic" || cfg.InitialSize != "6GB" {
			t.Fatalf("workload = %+v, want synthetic 6GB", cfg)
		}
		if worker.VolumeSizeGB != 100 || cfg.VolumeSizeGB != 100 || cfg.MemoryMB != 4096 || cfg.CPUs != 2 {
			t.Fatalf("resources volume=%d/%d memory=%d cpus=%d", worker.VolumeSizeGB, cfg.VolumeSizeGB, cfg.MemoryMB, cfg.CPUs)
		}
		var total time.Duration
		for _, raw := range []string{cfg.VerifySourceSnapshotBudget, cfg.VerifyRestoreBudget, cfg.VerifyIntegrityCheckBudget, cfg.VerifyCompareBudget} {
			budget, err := time.ParseDuration(raw)
			if err != nil || budget <= 0 {
				t.Fatalf("budget %q err=%v", raw, err)
			}
			total += budget
		}
		interval, err := time.ParseDuration(cfg.VerifyInterval)
		if err != nil {
			t.Fatal(err)
		}
		if total > interval || total >= 2*time.Hour {
			t.Fatalf("phase budgets %s exceed verify interval %s or running-verification stale window", total, interval)
		}
	}
}

func TestOnlyLargeDBSetsVerifierPhaseBudgets(t *testing.T) {
	for _, worker := range DefaultMainFleet().Workers {
		cfg := worker.Workload
		set := cfg.VerifySourceSnapshotBudget != "" || cfg.VerifyRestoreBudget != "" || cfg.VerifyIntegrityCheckBudget != "" || cfg.VerifyCompareBudget != ""
		if set != (worker.ProfileName == "large-db") {
			t.Fatalf("%s budgets set = %v", worker.ProfileName, set)
		}
	}
}

func TestLargeDBWorkerEnvCarriesSeedAndBudgets(t *testing.T) {
	desired := largeDBWorker(t, DefaultMainFleet())
	request, err := workerRequestForDesired(desired, "registry.fly.io/litestream-soak:test", "", nil)
	if err != nil {
		t.Fatalf("workerRequestForDesired() error = %v", err)
	}
	if request.VolumeSizeGB != 100 {
		t.Fatalf("request VolumeSizeGB = %d, want 100", request.VolumeSizeGB)
	}
	m := &Manager{}
	env := m.workerEnv(model.Worker{ID: desired.WorkerID, Name: desired.Name, ProfileName: desired.ProfileName}, request.Workload)
	want := map[string]string{
		"INITIAL_SIZE":                  "6GB",
		"VERIFY_SOURCE_SNAPSHOT_BUDGET": desired.Workload.VerifySourceSnapshotBudget,
		"VERIFY_RESTORE_BUDGET":         desired.Workload.VerifyRestoreBudget,
		"VERIFY_INTEGRITY_CHECK_BUDGET": desired.Workload.VerifyIntegrityCheckBudget,
		"VERIFY_COMPARE_BUDGET":         desired.Workload.VerifyCompareBudget,
	}
	for key, value := range want {
		if env[key] != value {
			t.Fatalf("env[%s] = %q, want %q", key, env[key], value)
		}
	}
	effective, err := workerconfig.WorkloadFromEnvironment(env)
	if err != nil {
		t.Fatalf("WorkloadFromEnvironment() error = %v", err)
	}
	if effective.InitialSize != "6GB" || effective.VerifyRestoreBudget == "" || effective.VerifyCompareBudget == "" {
		t.Fatalf("effective workload = %+v", effective)
	}

	low := m.workerEnv(model.Worker{ProfileName: "low-volume"}, normalizeWorkloadConfig(workload.Config{LoadMode: "synthetic"}))
	for key := range want {
		if key == "INITIAL_SIZE" {
			continue
		}
		if _, ok := low[key]; ok {
			t.Fatalf("low-volume env gained %s", key)
		}
	}
}

func TestReconcileFleetAddsLargeDBWithoutReplacingRunningWorkers(t *testing.T) {
	db := openTestDB(t)
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const litestreamSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ImageRef:      "registry.fly.io/litestream-soak:test",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	spec := DefaultMainFleet()
	for _, desired := range spec.Workers {
		if desired.ProfileName == "large-db" {
			continue
		}
		cfg := normalizeWorkloadConfig(desired.Workload)
		cfg.VolumeSizeGB = effectiveDesiredVolumeSize(desired, cfg)
		createTestWorker(t, db, model.Worker{
			ID:            desired.WorkerID,
			AppName:       "litestream-soak",
			Name:          desired.Name,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        sha,
			LitestreamSHA: litestreamSHA,
			ProfileName:   desired.ProfileName,
			ProfileConfig: cfg.JSON(),
			Region:        desired.Region,
			FlyMachineID:  desired.WorkerID + "-machine",
			FlyVolumeID:   desired.WorkerID + "-volume",
		})
	}

	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	manager.reconcileFleet(context.Background(), spec)

	machines := fly.machineRequests()
	if len(machines) != 1 {
		t.Fatalf("machine creates = %d, want 1", len(machines))
	}
	if got := machines[0].Config.Env["PROFILE"]; got != "large-db" {
		t.Fatalf("created PROFILE = %q, want large-db", got)
	}
	volumes := fly.volumeRequests()
	if len(volumes) != 1 || volumes[0].SizeGB != 100 {
		t.Fatalf("volume creates = %+v, want one 100GB volume", volumes)
	}
	stops, machineDeletes, volumeDeletes := fly.replacementCounts()
	if stops != 0 || machineDeletes != 0 || volumeDeletes != 0 {
		t.Fatalf("replacements stops:%d machine deletes:%d volume deletes:%d, want none", stops, machineDeletes, volumeDeletes)
	}
}
