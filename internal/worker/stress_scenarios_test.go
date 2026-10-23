package worker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func stressScheduleConfig(t *testing.T) Config {
	t.Helper()
	env := map[string]string{
		"PROFILE":               "gate-stress",
		"WRITE_RATE":            "600",
		"TRUNCATE_PAGE_N":       "0",
		"STRESS_SCENARIOS":      "overload-truncate0,pinned-reader",
		"STRESS_SCENARIO_PHASE": "6h",
	}
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("configFromLookup() error = %v", err)
	}
	return cfg
}

func TestStressScenarioConfigRoundTrips(t *testing.T) {
	cfg := stressScheduleConfig(t)
	if strings.Join(cfg.StressScenarios, ",") != "overload-truncate0,pinned-reader" || cfg.StressScenarioPhase != 6*time.Hour {
		t.Fatalf("schedule = %v / %s", cfg.StressScenarios, cfg.StressScenarioPhase)
	}
	wl := cfg.WorkloadConfig()
	if wl.StressScenarios != "overload-truncate0,pinned-reader" || wl.StressScenarioPhase != "6h0m0s" {
		t.Fatalf("workload = %+v", wl)
	}
	for _, env := range []map[string]string{
		{"STRESS_SCENARIOS": "overload-truncate0,unknown", "STRESS_SCENARIO_PHASE": "6h"},
		{"STRESS_SCENARIOS": "pinned-reader"},
		{"STRESS_SCENARIOS": "pinned-reader", "STRESS_SCENARIO_PHASE": "-1h"},
		{"STRESS_SCENARIOS": "pinned-reader,pinned-reader", "STRESS_SCENARIO_PHASE": "1h"},
	} {
		if _, err := configFromLookup(func(key string) string { return env[key] }); err == nil {
			t.Fatalf("configFromLookup(%v) accepted an invalid schedule", env)
		}
	}
}

func TestStressScenarioPhasesAreIsolatedAndKeepIdentity(t *testing.T) {
	cfg := stressScheduleConfig(t)
	cfg.DataDir = "/data"
	cfg.DBPath = "/data/soak.db"
	cfg.ReplicaPath = "/data/replicas"
	cfg.S3Path = "soak/worker/volume"
	baseIdentity := workerIdentity(cfg)

	overload, err := cfg.ForStressScenario("overload-truncate0")
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := cfg.ForStressScenario("pinned-reader")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.ForStressScenario("merged"); err == nil {
		t.Fatal("unknown scenario accepted")
	}

	if overload.TruncatePageN == nil || *overload.TruncatePageN != 0 || overload.PinnedReaderHold != 0 || overload.WriteRate != 600 {
		t.Fatalf("overload phase = truncate %v hold %s rate %d", overload.TruncatePageN, overload.PinnedReaderHold, overload.WriteRate)
	}
	if pinned.TruncatePageN != nil || pinned.PinnedReaderHold != 4*time.Minute || pinned.PinnedReaderPause != 45*time.Second || pinned.WriteRate != 200 {
		t.Fatalf("pinned phase = truncate %v hold %s pause %s rate %d", pinned.TruncatePageN, pinned.PinnedReaderHold, pinned.PinnedReaderPause, pinned.WriteRate)
	}
	if overload.DBPath == pinned.DBPath || overload.DataDir == pinned.DataDir || overload.ReplicaPath == pinned.ReplicaPath || overload.S3Path == pinned.S3Path {
		t.Fatal("scenario phases share a database or replica prefix")
	}
	if overload.DBPath != filepath.Join("/data", "scenarios", "overload-truncate0", "soak.db") || pinned.S3Path != "soak/worker/volume/scenarios/pinned-reader" {
		t.Fatalf("paths = %s %s", overload.DBPath, pinned.S3Path)
	}
	for name, phase := range map[string]Config{"overload-truncate0": overload, "pinned-reader": pinned} {
		identity := workerIdentity(phase)
		if identity.Scenario != name {
			t.Fatalf("identity scenario = %q, want %q", identity.Scenario, name)
		}
		if identity.ProfileConfig != baseIdentity.ProfileConfig || identity.ProfileHash != baseIdentity.ProfileHash {
			t.Fatal("scenario phase changed the attributed profile identity")
		}
	}
	if baseIdentity.Scenario != "" {
		t.Fatalf("unscheduled identity scenario = %q", baseIdentity.Scenario)
	}
}

func TestStressScenarioScheduleAlternatesNonOverlappingPhases(t *testing.T) {
	cfg := stressScheduleConfig(t)
	cfg.StressScenarioPhase = 40 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu      sync.Mutex
		running int
		order   []string
	)
	run := func(phaseCtx context.Context, phase Config) error {
		mu.Lock()
		running++
		if running > 1 {
			mu.Unlock()
			return errors.New("overlapping phases")
		}
		order = append(order, phase.StressScenario())
		if len(order) == 5 {
			cancel()
		}
		mu.Unlock()
		<-phaseCtx.Done()
		mu.Lock()
		running--
		mu.Unlock()
		return context.Cause(phaseCtx)
	}
	if err := RunStressScenarioSchedule(ctx, cfg, run); err != nil {
		t.Fatalf("RunStressScenarioSchedule() error = %v", err)
	}
	if len(order) != 5 {
		t.Fatalf("phases = %v", order)
	}
	for i := 1; i < len(order); i++ {
		if order[i] == order[i-1] {
			t.Fatalf("phases did not alternate: %v", order)
		}
	}

	failing := func(context.Context, Config) error { return errors.New("litestream crashed") }
	if err := RunStressScenarioSchedule(context.Background(), cfg, failing); err == nil || !strings.Contains(err.Error(), "litestream crashed") {
		t.Fatalf("phase failure err = %v", err)
	}
}

func TestStressScenarioAtUsesWallClockBoundaries(t *testing.T) {
	phase := 6 * time.Hour
	start := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	first, end := stressScenarioAt(start, 2, phase)
	second, _ := stressScenarioAt(start.Add(phase), 2, phase)
	if first == second {
		t.Fatalf("adjacent phases share index %d", first)
	}
	if !end.Equal(start.Add(phase)) {
		t.Fatalf("end = %s", end)
	}
	again, _ := stressScenarioAt(start.Add(2*phase+time.Minute), 2, phase)
	if again != first {
		t.Fatalf("index = %d, want %d", again, first)
	}
}
