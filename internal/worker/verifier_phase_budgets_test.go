package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigFromEnvReadsVerifierPhaseBudgets(t *testing.T) {
	env := map[string]string{
		"INITIAL_SIZE":                  "6GB",
		"VERIFY_SOURCE_SNAPSHOT_BUDGET": "20m",
		"VERIFY_RESTORE_BUDGET":         "45m",
		"VERIFY_INTEGRITY_CHECK_BUDGET": "20m",
		"VERIFY_COMPARE_BUDGET":         "30m",
	}
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("configFromLookup() error = %v", err)
	}
	if cfg.InitialSize != "6GB" {
		t.Fatalf("InitialSize = %q, want 6GB", cfg.InitialSize)
	}
	want := map[string]time.Duration{
		"source_snapshot": 20 * time.Minute,
		"restore":         45 * time.Minute,
		"integrity_check": 20 * time.Minute,
		"compare":         30 * time.Minute,
	}
	for phase, budget := range want {
		if got := cfg.verificationPhaseBudget(phase, 6<<30); got != budget {
			t.Fatalf("%s budget = %s, want %s", phase, got, budget)
		}
	}

	wl := cfg.WorkloadConfig()
	if wl.VerifySourceSnapshotBudget != "20m0s" || wl.VerifyRestoreBudget != "45m0s" || wl.VerifyIntegrityCheckBudget != "20m0s" || wl.VerifyCompareBudget != "30m0s" {
		t.Fatalf("WorkloadConfig() budgets = %+v", wl)
	}
	roundTrip, err := WorkloadFromEnvironment(env)
	if err != nil {
		t.Fatalf("WorkloadFromEnvironment() error = %v", err)
	}
	if roundTrip.JSON() != wl.JSON() {
		t.Fatalf("round trip = %s, want %s", roundTrip.JSON(), wl.JSON())
	}
}

func TestConfigFromEnvRejectsInvalidVerifierPhaseBudgets(t *testing.T) {
	for _, key := range []string{"VERIFY_SOURCE_SNAPSHOT_BUDGET", "VERIFY_RESTORE_BUDGET", "VERIFY_INTEGRITY_CHECK_BUDGET", "VERIFY_COMPARE_BUDGET"} {
		for _, value := range []string{"soon", "-1m"} {
			env := map[string]string{key: value}
			if _, err := configFromLookup(func(k string) string { return env[k] }); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("%s=%s err = %v", key, value, err)
			}
		}
	}
}

func TestVerifierPhaseBudgetDefaultsUnchanged(t *testing.T) {
	cfg := DefaultConfig()
	for _, phase := range []string{"source_snapshot", "restore", "integrity_check", "compare"} {
		for _, size := range []int64{0, 1 << 30, 8 << 30} {
			if got, want := cfg.verificationPhaseBudget(phase, size), verificationPhaseBudget(cfg.LogicalTimeout, size); got != want {
				t.Fatalf("%s size=%d budget = %s, want %s", phase, size, got, want)
			}
		}
	}
	wl := cfg.WorkloadConfig()
	if strings.Contains(wl.JSON(), "budget") {
		t.Fatalf("default workload config gained budget fields: %s", wl.JSON())
	}
}

func TestVerifierRestoreBudgetOverrideReachesValidation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.LogicalTimeout = time.Hour
	cfg.VerifyRestoreBudget = time.Second
	logicalTestDB(t, cfg.DBPath, "CREATE TABLE t(id INTEGER)")
	startBoundarySyncFixture(t, &cfg, 42)
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	v := NewVerifier(cfg)
	started := time.Now()
	passed, err := v.validate(context.Background(), 42)
	if passed || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "restore: verifier deadline exceeded") {
		t.Fatalf("passed=%v err=%v", passed, err)
	}
	if elapsed := time.Since(started); elapsed > 8*time.Second {
		t.Fatalf("restore override ignored: elapsed %s", elapsed)
	}
	var snapshotDeadline, restoreDeadline time.Time
	for _, step := range v.validationSteps {
		switch step.Name {
		case "source_snapshot":
			snapshotDeadline = *step.DeadlineAt
		case "restore":
			restoreDeadline = *step.DeadlineAt
		}
	}
	if snapshotDeadline.Sub(started) < 50*time.Minute {
		t.Fatalf("source_snapshot deadline %s lost default budget", snapshotDeadline.Sub(started))
	}
	if restoreDeadline.Sub(started) > 5*time.Second {
		t.Fatalf("restore deadline %s ignored override", restoreDeadline.Sub(started))
	}
}
