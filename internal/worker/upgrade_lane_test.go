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

const (
	testUpgradeBaseSHA      = "9f3af35a71e0763815cdf6ad2d3667aea41f9f75"
	testUpgradeCandidateSHA = "a25ac0119b033c8e878376f0121432672b247a37"
)

func upgradeLaneConfig(t *testing.T, dir string) Config {
	t.Helper()
	baseBinary := filepath.Join(dir, "litestream-base")
	if err := os.WriteFile(baseBinary, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"PROFILE":                        "gate-upgrade-lane",
		"UPGRADE_LANE":                   "true",
		"UPGRADE_WARMUP":                 "2h",
		"UPGRADE_POST_SNAPSHOT_INTERVAL": "1h",
		"SNAPSHOT_INTERVAL":              "24h",
		"LITESTREAM_SHA":                 testUpgradeCandidateSHA,
		"LITESTREAM_BASE_SHA":            testUpgradeBaseSHA,
		"LITESTREAM_BASE_BINARY":         baseBinary,
		"FOLLOW_CHECK":                   "true",
		"COMPACTION_PROGRESS_CHECK":      "true",
		"DATA_DIR":                       dir,
	}
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("configFromLookup() error = %v", err)
	}
	if err := cfg.validateUpgradeLane(os.Stat); err != nil {
		t.Fatalf("validateUpgradeLane() error = %v", err)
	}
	return cfg
}

func TestUpgradeLaneConfigFromEnv(t *testing.T) {
	dir := t.TempDir()
	baseBinary := filepath.Join(dir, "litestream-base")
	if err := os.WriteFile(baseBinary, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	valid := map[string]string{
		"UPGRADE_LANE":           "true",
		"LITESTREAM_SHA":         testUpgradeCandidateSHA,
		"LITESTREAM_BASE_SHA":    testUpgradeBaseSHA,
		"LITESTREAM_BASE_BINARY": baseBinary,
	}
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{name: "valid", env: valid},
		{name: "missing base binary", env: map[string]string{"LITESTREAM_BASE_BINARY": filepath.Join(dir, "missing")}, wantErr: "base litestream binary"},
		{name: "missing base sha", env: map[string]string{"LITESTREAM_BASE_SHA": ""}, wantErr: "LITESTREAM_BASE_SHA"},
		{name: "same build", env: map[string]string{"LITESTREAM_BASE_SHA": testUpgradeCandidateSHA}, wantErr: "same commit"},
		{name: "zero warm-up", env: map[string]string{"UPGRADE_WARMUP": "0s"}, wantErr: "UPGRADE_WARMUP"},
		{name: "stress scenarios", env: map[string]string{"STRESS_SCENARIOS": "pinned-reader", "STRESS_SCENARIO_PHASE": "1h"}, wantErr: "STRESS_SCENARIOS"},
		{name: "many databases", env: map[string]string{"NUM_DATABASES": "10"}, wantErr: "many-database"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{}
			for key, value := range valid {
				env[key] = value
			}
			for key, value := range tt.env {
				env[key] = value
			}
			cfg, err := configFromLookup(func(key string) string { return env[key] })
			if err == nil {
				cfg.LitestreamSHA = env["LITESTREAM_SHA"]
				err = cfg.validateUpgradeLane(os.Stat)
			}
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !cfg.UpgradeLane || cfg.UpgradeWarmup != 2*time.Hour || cfg.LitestreamBaseBinary != baseBinary {
					t.Fatalf("config = lane %v warm-up %s binary %s", cfg.UpgradeLane, cfg.UpgradeWarmup, cfg.LitestreamBaseBinary)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestUpgradePhasesKeepIdentityAndSwitchBinary(t *testing.T) {
	cfg := upgradeLaneConfig(t, t.TempDir())
	baseIdentity := workerIdentity(cfg)
	tests := []struct {
		phase            string
		wantBinary       string
		wantActiveSHA    string
		wantSnapshot     time.Duration
		wantFollowCheck  bool
		wantIdentityBase bool
	}{
		{phase: UpgradePhasePre, wantBinary: cfg.LitestreamBaseBinary, wantActiveSHA: testUpgradeBaseSHA, wantSnapshot: 24 * time.Hour, wantFollowCheck: false, wantIdentityBase: true},
		{phase: UpgradePhasePost, wantBinary: "litestream", wantActiveSHA: testUpgradeCandidateSHA, wantSnapshot: time.Hour, wantFollowCheck: true, wantIdentityBase: true},
	}
	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			phase, err := cfg.ForUpgradePhase(tt.phase)
			if err != nil {
				t.Fatal(err)
			}
			if phase.litestreamCommand() != tt.wantBinary || phase.activeLitestreamSHA != tt.wantActiveSHA || phase.SnapshotInterval != tt.wantSnapshot || phase.FollowCheck != tt.wantFollowCheck {
				t.Fatalf("phase = binary %s sha %s snapshot %s follow %v", phase.litestreamCommand(), phase.activeLitestreamSHA, phase.SnapshotInterval, phase.FollowCheck)
			}
			if phase.DBPath != cfg.DBPath || phase.DataDir != cfg.DataDir || phase.S3Path != cfg.S3Path || phase.ReplicaPath != cfg.ReplicaPath {
				t.Fatal("upgrade phase moved the database or replica prefix")
			}
			identity := workerIdentity(phase)
			if identity.UpgradePhase != tt.phase || identity.ActiveLitestreamSHA != tt.wantActiveSHA {
				t.Fatalf("identity phase %q active sha %q", identity.UpgradePhase, identity.ActiveLitestreamSHA)
			}
			if identity.LitestreamSHA != testUpgradeCandidateSHA {
				t.Fatalf("identity litestream sha = %q, want the image's pinned build", identity.LitestreamSHA)
			}
			if identity.ProfileConfig != baseIdentity.ProfileConfig || identity.ProfileHash != baseIdentity.ProfileHash {
				t.Fatal("upgrade phase changed the attributed profile identity")
			}
		})
	}
	if _, err := cfg.ForUpgradePhase("sideways"); err == nil {
		t.Fatal("unknown phase accepted")
	}
	if baseIdentity.UpgradePhase != "" || baseIdentity.ActiveLitestreamSHA != "" || cfg.litestreamCommand() != "litestream" {
		t.Fatalf("unscheduled identity = %+v", baseIdentity)
	}
	workload := cfg.WorkloadConfig()
	if !workload.UpgradeLane || workload.UpgradeWarmup != "2h0m0s" || workload.UpgradePostSnapshotInterval != "1h0m0s" || !workload.FollowCheck || !workload.CompactionProgressCheck {
		t.Fatalf("workload config = %+v", workload)
	}
}

type phaseRun struct {
	phase    string
	binary   string
	sha      string
	preTXID  uint64
	snapshot time.Duration
}

func TestUpgradeLanePhaseSwitch(t *testing.T) {
	start := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		state        *upgradeLaneState
		clockOffset  time.Duration
		preErr       error
		preTXID      uint64
		wantRuns     []phaseRun
		wantErr      string
		wantPhase    string
		wantSwitched bool
	}{
		{
			name:         "fresh volume runs base then candidate",
			preTXID:      0x42,
			wantRuns:     []phaseRun{{phase: UpgradePhasePre, sha: testUpgradeBaseSHA, snapshot: 24 * time.Hour}, {phase: UpgradePhasePost, sha: testUpgradeCandidateSHA, preTXID: 0x42, snapshot: time.Hour}},
			wantPhase:    UpgradePhasePost,
			wantSwitched: true,
		},
		{
			name:         "restart after warm-up elapsed switches at once",
			state:        &upgradeLaneState{Phase: UpgradePhasePre, StartedAt: start.Add(-3 * time.Hour), BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA, PreUpgradeTXID: 0x10},
			wantRuns:     []phaseRun{{phase: UpgradePhasePre, sha: testUpgradeBaseSHA, snapshot: 24 * time.Hour}, {phase: UpgradePhasePost, sha: testUpgradeCandidateSHA, preTXID: 0x10, snapshot: time.Hour}},
			wantPhase:    UpgradePhasePost,
			wantSwitched: true,
		},
		{
			name:      "restart after the switch never runs the base build",
			state:     &upgradeLaneState{Phase: UpgradePhasePost, StartedAt: start.Add(-5 * time.Hour), BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA, PreUpgradeTXID: 0x99},
			wantRuns:  []phaseRun{{phase: UpgradePhasePost, sha: testUpgradeCandidateSHA, preTXID: 0x99, snapshot: time.Hour}},
			wantPhase: UpgradePhasePost,
		},
		{
			name:      "pre-upgrade failure does not switch",
			preErr:    errors.New("litestream exited unexpectedly"),
			wantRuns:  []phaseRun{{phase: UpgradePhasePre, sha: testUpgradeBaseSHA, snapshot: 24 * time.Hour}},
			wantErr:   "pre-upgrade phase: litestream exited unexpectedly",
			wantPhase: UpgradePhasePre,
		},
		{
			name:      "state for other builds is refused",
			state:     &upgradeLaneState{Phase: UpgradePhasePre, StartedAt: start, BaseSHA: "0000000000000000000000000000000000000000", CandidateSHA: testUpgradeCandidateSHA},
			wantErr:   "upgrade lane state on this volume is for",
			wantPhase: UpgradePhasePre,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := upgradeLaneConfig(t, dir)
			cfg.UpgradeWarmup = 50 * time.Millisecond
			if tt.state != nil {
				if err := writeUpgradeLaneState(dir, *tt.state); err != nil {
					t.Fatal(err)
				}
			}
			now := start
			var runs []phaseRun
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			run := func(runCtx context.Context, phase Config) error {
				runs = append(runs, phaseRun{phase: phase.upgradePhase, binary: phase.litestreamCommand(), sha: phase.activeLitestreamSHA, preTXID: phase.upgradePreTXID, snapshot: phase.SnapshotInterval})
				switch phase.upgradePhase {
				case UpgradePhasePre:
					if tt.preErr != nil {
						return tt.preErr
					}
					if tt.preTXID != 0 {
						if err := recordUpgradePreTXID(dir, tt.preTXID); err != nil {
							t.Fatal(err)
						}
					}
					<-runCtx.Done()
					if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, context.Canceled) {
						return cause
					}
					return nil
				default:
					cancel()
					return nil
				}
			}
			err := runUpgradeLane(ctx, cfg, func() time.Time { return now }, replicaL9Purger{}, run)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("runUpgradeLane() error = %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("runUpgradeLane() error = %v, want %q", err, tt.wantErr)
			}
			if len(runs) != len(tt.wantRuns) {
				t.Fatalf("runs = %+v, want %+v", runs, tt.wantRuns)
			}
			for i, want := range tt.wantRuns {
				got := runs[i]
				wantBinary := "litestream"
				if want.phase == UpgradePhasePre {
					wantBinary = cfg.LitestreamBaseBinary
				}
				if got.phase != want.phase || got.binary != wantBinary || got.sha != want.sha || got.preTXID != want.preTXID || got.snapshot != want.snapshot {
					t.Fatalf("run %d = %+v, want %+v with binary %s", i, got, want, wantBinary)
				}
			}
			state, err := readUpgradeLaneState(dir)
			if err != nil {
				t.Fatal(err)
			}
			if state.Phase != tt.wantPhase || (tt.wantSwitched && state.SwitchedAt == nil) {
				t.Fatalf("state = %+v, want phase %s switched %v", state, tt.wantPhase, tt.wantSwitched)
			}
		})
	}
}

func TestRecordUpgradePreTXIDOnlyAdvancesDuringPreUpgrade(t *testing.T) {
	dir := t.TempDir()
	if err := writeUpgradeLaneState(dir, upgradeLaneState{Phase: UpgradePhasePre, StartedAt: time.Now().UTC(), BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA}); err != nil {
		t.Fatal(err)
	}
	for _, txid := range []uint64{0x20, 0x10, 0x30} {
		if err := recordUpgradePreTXID(dir, txid); err != nil {
			t.Fatal(err)
		}
	}
	state, err := readUpgradeLaneState(dir)
	if err != nil || state.PreUpgradeTXID != 0x30 {
		t.Fatalf("pre-upgrade txid = %#x, %v; want 0x30", state.PreUpgradeTXID, err)
	}
	state.Phase = UpgradePhasePost
	if err := writeUpgradeLaneState(dir, state); err != nil {
		t.Fatal(err)
	}
	if err := recordUpgradePreTXID(dir, 0x40); err != nil {
		t.Fatal(err)
	}
	state, err = readUpgradeLaneState(dir)
	if err != nil || state.PreUpgradeTXID != 0x30 {
		t.Fatalf("post-upgrade txid moved the pre-upgrade marker to %#x, %v", state.PreUpgradeTXID, err)
	}
}

func TestPhaseEndedSkipsWorkerFailureEvent(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want bool
	}{
		{errUpgradeWarmupEnded, true},
		{errStressScenarioPhaseEnded, true},
		{errors.New("litestream exited unexpectedly"), false},
	} {
		if got := phaseEnded(tt.err); got != tt.want {
			t.Fatalf("phaseEnded(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
