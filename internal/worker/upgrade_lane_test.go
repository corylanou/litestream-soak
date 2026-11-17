package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
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
			state:        &upgradeLaneState{Phase: UpgradePhasePre, StartedAt: start.Add(-3 * time.Hour), BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA, PreUpgradeTXID: 0x10, PreRestoreModes: slices.Clone(restoreModeRotation)},
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
						for _, mode := range restoreModeRotation {
							if err := recordUpgradePreEvidence(dir, tt.preTXID, mode); err != nil {
								t.Fatal(err)
							}
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
		if err := recordUpgradePreEvidence(dir, txid, restoreModeTXID); err != nil {
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
	if err := recordUpgradePreEvidence(dir, 0x40, restoreModeLatest); err != nil {
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

func shortenUpgradeEvidenceWait(t *testing.T, grace time.Duration) {
	t.Helper()
	poll, graceFor := upgradeEvidencePoll, upgradeEvidenceGrace
	upgradeEvidencePoll = 5 * time.Millisecond
	upgradeEvidenceGrace = func(time.Duration) time.Duration { return grace }
	t.Cleanup(func() { upgradeEvidencePoll, upgradeEvidenceGrace = poll, graceFor })
}

func TestUpgradeLaneWaitsForARestoreInEveryModeBeforeSwitching(t *testing.T) {
	shortenUpgradeEvidenceWait(t, 5*time.Second)
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.UpgradeWarmup = 60 * time.Millisecond
	cfg.VerifyRestoreRotation = true
	cfg.VerifyInterval = 25 * time.Millisecond
	const cycleDuration = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	var modes []string
	var cyclesAtWarmupEnd, preTXIDAtSwitch int
	var switchedAfter time.Duration
	run := func(runCtx context.Context, phase Config) error {
		if phase.upgradePhase == UpgradePhasePost {
			switchedAfter = time.Since(started)
			preTXIDAtSwitch = int(phase.upgradePreTXID)
			cancel()
			return nil
		}
		verifier := &Verifier{cfg: phase}
		for txid := uint64(1); ; txid++ {
			select {
			case <-runCtx.Done():
				return context.Cause(runCtx)
			case <-time.After(phase.VerifyInterval):
			}
			mode := verifier.restoreModeAt(time.Now())
			time.Sleep(cycleDuration)
			if runCtx.Err() != nil {
				return context.Cause(runCtx)
			}
			if time.Since(started) < cfg.UpgradeWarmup {
				cyclesAtWarmupEnd++
			}
			modes = append(modes, mode)
			if err := recordUpgradePreEvidence(dir, txid, mode); err != nil {
				t.Error(err)
			}
		}
	}
	if err := runUpgradeLane(ctx, cfg, time.Now, replicaL9Purger{}, run); err != nil {
		t.Fatalf("runUpgradeLane() error = %v", err)
	}
	if cyclesAtWarmupEnd >= len(restoreModeRotation) {
		t.Fatalf("%d cycles finished inside the warm-up; the test needs the warm-up to end before every mode has run", cyclesAtWarmupEnd)
	}
	if len(modes) < len(restoreModeRotation) || !slices.Equal(modes[:len(restoreModeRotation)], restoreModeRotation) {
		t.Fatalf("pre-upgrade modes = %v, want one cycle per mode %v whatever the wall-clock slot", modes, restoreModeRotation)
	}
	if switchedAfter < cfg.UpgradeWarmup || switchedAfter < 3*(cfg.VerifyInterval+cycleDuration) {
		t.Fatalf("switched after %s, want it held past the %s warm-up until the third cycle finished", switchedAfter, cfg.UpgradeWarmup)
	}
	if preTXIDAtSwitch < len(restoreModeRotation) {
		t.Fatalf("pre-upgrade txid at the switch = %d, want the last pre-upgrade cycle's", preTXIDAtSwitch)
	}
	state, err := readUpgradeLaneState(dir)
	if err != nil || state.Phase != UpgradePhasePost || !state.preEvidenceComplete() {
		t.Fatalf("state = %+v, %v; want the switch recorded with complete evidence", state, err)
	}
}

func TestUpgradeLaneStaysOnTheBaseBuildWithoutABaseline(t *testing.T) {
	shortenUpgradeEvidenceWait(t, 80*time.Millisecond)
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.UpgradeWarmup = 30 * time.Millisecond

	blocked := func() *time.Time {
		state, err := readUpgradeLaneState(dir)
		if err != nil {
			t.Error(err)
		}
		return state.SwitchBlockedAt
	}
	var phases []string
	run := func(complete bool) func(context.Context, Config) error {
		return func(runCtx context.Context, phase Config) error {
			phases = append(phases, phase.upgradePhase)
			modes := []string{restoreModeTXID}
			if complete {
				modes = restoreModeRotation
			}
			for _, mode := range modes {
				if err := recordUpgradePreEvidence(dir, 7, mode); err != nil {
					t.Error(err)
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			for blocked() == nil && time.Now().Before(deadline) && runCtx.Err() == nil {
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(40 * time.Millisecond)
			if cause := context.Cause(runCtx); cause != nil {
				return cause
			}
			return nil
		}
	}
	started := time.Now()
	if err := runUpgradeLane(context.Background(), cfg, time.Now, replicaL9Purger{}, run(false)); err != nil {
		t.Fatalf("runUpgradeLane() error = %v, want the lane left running on the base build", err)
	}
	firstBlock := blocked()
	if firstBlock == nil || time.Since(started) < cfg.UpgradeWarmup+80*time.Millisecond {
		t.Fatalf("blocked at %v after %s, want the block recorded only after the warm-up and the grace", firstBlock, time.Since(started))
	}

	if err := runUpgradeLane(context.Background(), cfg, time.Now, replicaL9Purger{}, run(true)); err != nil {
		t.Fatalf("restarted runUpgradeLane() error = %v", err)
	}
	state, err := readUpgradeLaneState(dir)
	if err != nil || state.Phase != UpgradePhasePre || state.SwitchedAt != nil || state.SwitchBlockedAt == nil || !state.SwitchBlockedAt.Equal(*firstBlock) {
		t.Fatalf("state = %+v, %v; want the lane still pre-upgrade with the first block kept", state, err)
	}
	if len(phases) != 2 || phases[0] != UpgradePhasePre || phases[1] != UpgradePhasePre {
		t.Fatalf("phases = %v, want the candidate build never started, even once the evidence completed after the block", phases)
	}
}

func TestUpgradeLaneRestartDoesNotExtendTheEvidenceGrace(t *testing.T) {
	shortenUpgradeEvidenceWait(t, time.Hour)
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.UpgradeWarmup = time.Hour
	started := time.Now().UTC().Add(-2*time.Hour - time.Minute)
	if err := writeUpgradeLaneState(dir, upgradeLaneState{Phase: UpgradePhasePre, StartedAt: started, BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA, PreUpgradeTXID: 7, PreRestoreModes: []string{restoreModeTXID}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := func(runCtx context.Context, phase Config) error {
		if phase.upgradePhase != UpgradePhasePre {
			t.Errorf("phase %s started without a baseline", phase.upgradePhase)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if state, err := readUpgradeLaneState(dir); err == nil && state.SwitchBlockedAt != nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		return nil
	}
	if err := runUpgradeLane(ctx, cfg, time.Now, replicaL9Purger{}, run); err != nil {
		t.Fatalf("runUpgradeLane() error = %v", err)
	}
	state, err := readUpgradeLaneState(dir)
	if err != nil || state.SwitchBlockedAt == nil {
		t.Fatalf("state = %+v, %v; want a restart past start + warm-up + grace blocked at once instead of given a fresh hour", state, err)
	}
}

func TestUpgradeLaneBlockSurvivesConcurrentEvidenceUpdates(t *testing.T) {
	shortenUpgradeEvidenceWait(t, time.Millisecond)
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.UpgradeWarmup = time.Millisecond
	if err := writeUpgradeLaneState(dir, upgradeLaneState{Phase: UpgradePhasePre, StartedAt: time.Now().UTC().Add(-time.Hour), BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA, PreUpgradeTXID: 1, PreRestoreModes: []string{restoreModeTXID}}); err != nil {
		t.Fatal(err)
	}
	blocked, err := blockUpgradeSwitch(cfg)
	if err != nil || blocked.SwitchBlockedAt == nil {
		t.Fatalf("blockUpgradeSwitch() = %+v, %v", blocked, err)
	}

	const writers, rounds = 8, 40
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < rounds; i++ {
				if err := recordUpgradePreEvidence(dir, uint64(w*rounds+i+2), restoreModeRotation[(w+i)%len(restoreModeRotation)]); err != nil {
					t.Error(err)
				}
				if _, err := blockUpgradeSwitch(cfg); err != nil {
					t.Error(err)
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < rounds; i++ {
			_, err := updateUpgradeLaneState(dir, func(state *upgradeLaneState) bool {
				state.SwitchBlockedAt, state.SwitchBlockedReason = nil, ""
				state.Phase = UpgradePhasePost
				return true
			})
			if !errors.Is(err, errUpgradeSwitchBlocked) {
				t.Errorf("update leaving a blocked pre-upgrade phase: error = %v, want %v", err, errUpgradeSwitchBlocked)
			}
			if _, err := updateUpgradeLaneState(dir, func(state *upgradeLaneState) bool {
				state.SwitchBlockedAt, state.SwitchBlockedReason = nil, ""
				return true
			}); err != nil {
				t.Error(err)
			}
		}
	}()
	close(start)
	wg.Wait()

	state, err := readUpgradeLaneState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if state.SwitchBlockedAt == nil || !state.SwitchBlockedAt.Equal(*blocked.SwitchBlockedAt) || state.SwitchBlockedReason != blocked.SwitchBlockedReason || state.Phase != UpgradePhasePre {
		t.Fatalf("state = %+v, want the first block kept through every concurrent update", state)
	}
	if !state.preEvidenceComplete() || state.PreUpgradeTXID != writers*rounds+1 {
		t.Fatalf("state = %+v, want every concurrent evidence update kept (txid %d)", state, writers*rounds+1)
	}

	var phases []string
	if err := runUpgradeLane(context.Background(), cfg, time.Now, replicaL9Purger{}, func(_ context.Context, phase Config) error {
		phases = append(phases, phase.upgradePhase)
		time.Sleep(30 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatalf("restarted runUpgradeLane() error = %v", err)
	}
	if len(phases) != 1 || phases[0] != UpgradePhasePre {
		t.Fatalf("phases = %v, want a restart with complete evidence to stay on the base build once blocked", phases)
	}
}

func TestUpgradeLaneRetriesTheBlockedEventAcrossARestart(t *testing.T) {
	shortenUpgradeEvidenceWait(t, 20*time.Millisecond)
	backoff, maxBackoff := upgradeBlockedEventBackoff, upgradeBlockedEventMaxBackoff
	upgradeBlockedEventBackoff, upgradeBlockedEventMaxBackoff = 5*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { upgradeBlockedEventBackoff, upgradeBlockedEventMaxBackoff = backoff, maxBackoff })

	var mu sync.Mutex
	failing := true
	var attempts int
	var delivered []reporting.WorkerEventPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload reporting.WorkerEventPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if payload.EventType != upgradeSwitchBlockedEvent {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		attempts++
		if failing {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		delivered = append(delivered, payload)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.UpgradeWarmup = 10 * time.Millisecond
	cfg.ControlBaseURL = server.URL
	count := func() (int, int) {
		mu.Lock()
		defer mu.Unlock()
		return attempts, len(delivered)
	}
	waitFor := func(what string, done func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !done() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	run := func(until func() bool) func(context.Context, Config) error {
		return func(_ context.Context, phase Config) error {
			if phase.upgradePhase != UpgradePhasePre {
				t.Errorf("phase %s started on a blocked lane", phase.upgradePhase)
			}
			waitFor("the blocked event attempts", until)
			return nil
		}
	}

	if err := runUpgradeLane(context.Background(), cfg, time.Now, replicaL9Purger{}, run(func() bool { n, _ := count(); return n >= 3 })); err != nil {
		t.Fatal(err)
	}
	state, err := readUpgradeLaneState(dir)
	if err != nil || state.SwitchBlockedAt == nil || state.BlockedEventDeliveredAt != nil {
		t.Fatalf("state = %+v, %v; want the block recorded and its event still undelivered after failed POSTs", state, err)
	}
	if _, ok := count(); ok != 0 {
		t.Fatalf("delivered = %d, want none while the control plane rejects the event", ok)
	}
	firstBlock := *state.SwitchBlockedAt

	mu.Lock()
	failing = false
	mu.Unlock()
	if err := runUpgradeLane(context.Background(), cfg, time.Now, replicaL9Purger{}, run(func() bool {
		state, err := readUpgradeLaneState(dir)
		return err == nil && state.BlockedEventDeliveredAt != nil
	})); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := slices.Clone(delivered)
	mu.Unlock()
	if len(got) != 1 || got[0].Message != state.SwitchBlockedReason || !got[0].SentAt.Equal(firstBlock) {
		t.Fatalf("delivered = %+v, want the original blocked event resent once after the restart", got)
	}

	before, _ := count()
	if err := runUpgradeLane(context.Background(), cfg, time.Now, replicaL9Purger{}, func(context.Context, Config) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if after, _ := count(); after != before {
		t.Fatalf("attempts went from %d to %d, want no resend once the delivery is on record", before, after)
	}
}
