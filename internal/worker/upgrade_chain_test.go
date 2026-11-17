package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/s3util/s3fake"
)

func quickenChainPreservation(t *testing.T) {
	t.Helper()
	settle, delay := preservedChainSettle, replicaL9PurgeRetryDelay
	preservedChainSettle, replicaL9PurgeRetryDelay = 2*time.Millisecond, time.Millisecond
	t.Cleanup(func() { preservedChainSettle, replicaL9PurgeRetryDelay = settle, delay })
}

var laneReplicaKeys = []string{
	"ltx/0/0000000000000443-0000000000000443.ltx",
	"ltx/0/0000000000000444-0000000000000444.ltx",
	"ltx/1/0000000000000430-0000000000000442.ltx",
	"ltx/9/0000000000000001-000000000000042f.ltx",
}

func seedLaneReplica(t *testing.T, cfg Config) {
	t.Helper()
	for _, key := range laneReplicaKeys {
		writeLaneFile(t, filepath.Join(cfg.ReplicaPath, filepath.FromSlash(key)), key)
	}
}

func writeLaneFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func preservedLaneFile(cfg Config, key string) string {
	return filepath.Join(preservedChainConfig(cfg).ReplicaPath, filepath.FromSlash(key))
}

func fileLane(t *testing.T) (Config, Config) {
	t.Helper()
	quickenChainPreservation(t)
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.ReplicaPath = filepath.Join(dir, "replica")
	base, err := cfg.ForUpgradePhase(UpgradePhasePre)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, base
}

func TestUpgradeChainPreserveCopiesTheReplicaAndProvesItWithTheBaseBuild(t *testing.T) {
	cfg, base := fileLane(t)
	seedLaneReplica(t, cfg)
	writeLaneFile(t, preservedLaneFile(cfg, "ltx/0/0000000000000999-0000000000000999.ltx"), "left by an earlier lane")
	writeLaneFile(t, preservedLaneFile(cfg, laneReplicaKeys[0]), "torn copy of the same length as nothing")
	kept := preservedLaneFile(cfg, laneReplicaKeys[3])
	writeLaneFile(t, kept, laneReplicaKeys[3])
	copiedEarlier := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes(kept, copiedEarlier, copiedEarlier); err != nil {
		t.Fatal(err)
	}
	var restored []uint64
	preserver := &upgradeChainPreserver{restore: func(_ context.Context, build, preserved Config, txid uint64) error {
		if build.litestreamCommand() != cfg.LitestreamBaseBinary {
			t.Errorf("restore ran %s, want the base build", build.litestreamCommand())
		}
		for _, key := range laneReplicaKeys {
			body, err := os.ReadFile(filepath.Join(preserved.ReplicaPath, filepath.FromSlash(key)))
			if err != nil || string(body) != key {
				t.Errorf("preserved %s = %q, %v when the base build restores it", key, body, err)
			}
		}
		restored = append(restored, txid)
		return nil
	}}

	record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
	if err != nil {
		t.Fatalf("preserve() error = %v", err)
	}
	wantBytes := int64(0)
	for _, key := range laneReplicaKeys {
		wantBytes += int64(len(key))
	}
	if !record.succeeded() || record.TXID != "0000000000000444" || record.Objects != 4 || record.Bytes != wantBytes || record.Prefix != cfg.ReplicaPath+reporting.PreservedChainPrefixSuffix {
		t.Fatalf("record = %+v, want the newest replicated txid proven on a four-object copy", record)
	}
	if len(restored) != 1 || restored[0] != 0x444 {
		t.Fatalf("base restores = %x, want one restore of txid 444", restored)
	}
	if _, err := os.Stat(preservedLaneFile(cfg, "ltx/0/0000000000000999-0000000000000999.ltx")); !os.IsNotExist(err) {
		t.Fatalf("object from an earlier lane survived in the preserved prefix: %v", err)
	}
	if info, err := os.Stat(kept); err != nil || !info.ModTime().Equal(copiedEarlier) {
		t.Fatalf("object an earlier attempt had already copied was copied again: %v, %v", info, err)
	}
	if err := os.Remove(filepath.Join(cfg.ReplicaPath, filepath.FromSlash(laneReplicaKeys[0]))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(preservedLaneFile(cfg, laneReplicaKeys[0])); err != nil {
		t.Fatalf("preserved copy lost a file once the live replica dropped it: %v", err)
	}
}

func TestUpgradeChainPreserveSeparatesBaseBuildFailuresFromLaneFaults(t *testing.T) {
	cfg, base := fileLane(t)
	proof := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error {
		return fmt.Errorf("%w: integrity check: database disk image is malformed", errBaseProofFailed)
	}}
	record, err := proof.preserve(context.Background(), cfg, base, time.Now)
	if err == nil || record.succeeded() || record.BaseProofFailed || !strings.Contains(record.Error, "no LTX objects") {
		t.Fatalf("empty replica: record = %+v, %v; want a lane fault, not a base build failure", record, err)
	}
	seedLaneReplica(t, cfg)
	record, err = proof.preserve(context.Background(), cfg, base, time.Now)
	if !errors.Is(err, errBaseProofFailed) || record.succeeded() || !record.BaseProofFailed || !strings.Contains(record.Error, "txid 0000000000000444 from the preserved copy") {
		t.Fatalf("record = %+v, %v; want the base build's failed restore on record", record, err)
	}
	capacity := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error {
		return errors.New("restore capacity: 1 MiB free, 2 MiB needed")
	}}
	record, err = capacity.preserve(context.Background(), cfg, base, time.Now)
	if err == nil || errors.Is(err, errBaseProofFailed) || record.BaseProofFailed {
		t.Fatalf("record = %+v, %v; want a restore the harness could not run kept apart from a base build failure", record, err)
	}
}

func TestUpgradeChainPreserveWaitsForTheReplicaToStopChanging(t *testing.T) {
	cfg, base := fileLane(t)
	seedLaneReplica(t, cfg)
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.WriteFile(filepath.Join(cfg.ReplicaPath, "ltx", "0", fmt.Sprintf("%016x-%016x.ltx", 0x500+i, 0x500+i)), []byte("late upload"), 0o644)
			time.Sleep(200 * time.Microsecond)
		}
	}()
	restores := 0
	preserver := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error {
		restores++
		return nil
	}}
	record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
	close(stop)
	writer.Wait()
	if err == nil || record.succeeded() || record.BaseProofFailed || !strings.Contains(record.Error, "still changing") || restores != 0 {
		t.Fatalf("record = %+v, %v, %d restore(s); want no copy and no proof while uploads are still landing", record, err, restores)
	}
	record, err = preserver.preserve(context.Background(), cfg, base, time.Now)
	if err != nil || !record.succeeded() || restores != 1 {
		t.Fatalf("settled replica: record = %+v, %v, %d restore(s)", record, err, restores)
	}
}

func TestMatchReplicaObjects(t *testing.T) {
	want := map[string]replicaObject{"ltx/0000/a.ltx": {Size: 10, ETag: "aa"}, "ltx/0009/b.ltx": {Size: 900, ETag: "bb-3"}}
	tests := []struct {
		name      string
		got       map[string]replicaObject
		multipart bool
		wantErr   string
	}{
		{name: "identical", got: map[string]replicaObject{"ltx/0000/a.ltx": {Size: 10, ETag: "aa"}, "ltx/0009/b.ltx": {Size: 900, ETag: "bb-3"}}},
		{name: "missing object", got: map[string]replicaObject{"ltx/0000/a.ltx": {Size: 10, ETag: "aa"}, "ltx/0009/c.ltx": {Size: 900, ETag: "bb-3"}}, wantErr: "ltx/0009/b.ltx is missing"},
		{name: "extra object", got: map[string]replicaObject{"ltx/0000/a.ltx": {Size: 10, ETag: "aa"}, "ltx/0009/b.ltx": {Size: 900, ETag: "bb-3"}, "x": {}}, wantErr: "3 object(s), want 2"},
		{name: "same name, different size", got: map[string]replicaObject{"ltx/0000/a.ltx": {Size: 11, ETag: "aa"}, "ltx/0009/b.ltx": {Size: 900, ETag: "bb-3"}}, wantErr: "ltx/0000/a.ltx is 11 bytes, want 10"},
		{name: "same name and size, different content", got: map[string]replicaObject{"ltx/0000/a.ltx": {Size: 10, ETag: "zz"}, "ltx/0009/b.ltx": {Size: 900, ETag: "bb-3"}}, multipart: true, wantErr: "ltx/0000/a.ltx has ETag zz, want aa"},
		{name: "copied multipart upload gets a new ETag", got: map[string]replicaObject{"ltx/0000/a.ltx": {Size: 10, ETag: "aa"}, "ltx/0009/b.ltx": {Size: 900, ETag: "cc"}}, multipart: true},
		{name: "multipart ETag change on the source itself", got: map[string]replicaObject{"ltx/0000/a.ltx": {Size: 10, ETag: "aa"}, "ltx/0009/b.ltx": {Size: 900, ETag: "cc"}}, wantErr: "ltx/0009/b.ltx has ETag cc, want bb-3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := matchReplicaObjects(want, tt.got, tt.multipart)
			if (tt.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("matchReplicaObjects() = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func chainLanePurger(calls *[]string, restore func() error) replicaL9Purger {
	return replicaL9Purger{
		list: func(_ context.Context, cfg Config) ([]string, error) {
			return listFileReplicaObjects(cfg)
		},
		remove: func(_ context.Context, cfg Config, key string) error {
			*calls = append(*calls, "purge "+key)
			return os.Remove(filepath.Join(replicaOwnPrefix(cfg), key))
		},
		chain: &upgradeChainPreserver{restore: func(_ context.Context, _, _ Config, txid uint64) error {
			*calls = append(*calls, "base-restore "+formatTXID(txid))
			return restore()
		}},
	}
}

func switchedLaneState(t *testing.T, cfg Config, mutate func(*upgradeLaneState)) {
	t.Helper()
	state := upgradeLaneState{Phase: UpgradePhasePre, StartedAt: time.Now().UTC().Add(-3 * time.Hour), BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA, PreUpgradeTXID: 0x433, PreRestoreModes: restoreModeRotation}
	if mutate != nil {
		mutate(&state)
	}
	if err := writeUpgradeLaneState(cfg.DataDir, state); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeLaneHandsTheCandidateThePreservedChain(t *testing.T) {
	shortenUpgradeEvidenceWait(t, time.Second)
	cfg, _, store := s3Lane(t)
	cfg.UpgradeDeleteL9AtSwitch = true
	switchedLaneState(t, cfg, nil)
	const snapshot = "/0009/0000000000000001-000000000000042f.ltx"

	var calls []string
	var post []Config
	run := func(ctx context.Context, phase Config) error {
		calls = append(calls, "run "+phase.upgradePhase)
		if phase.upgradePhase == UpgradePhasePost {
			post = append(post, phase)
			return nil
		}
		<-ctx.Done()
		return nil
	}
	purger := replicaL9Purger{
		list: func(_ context.Context, cfg Config) ([]string, error) {
			return store.Keys(replicaOwnPrefix(cfg) + "/"), nil
		},
		remove: func(_ context.Context, _ Config, key string) error {
			calls = append(calls, "purge "+key)
			store.Delete(key)
			return nil
		},
		chain: &upgradeChainPreserver{restore: func(_ context.Context, _, _ Config, txid uint64) error {
			calls = append(calls, "base-restore "+formatTXID(txid))
			if state, err := readUpgradeLaneState(cfg.DataDir); err != nil || state.Phase != UpgradePhasePre || state.SwitchedAt != nil {
				t.Errorf("state = %+v, %v while the base build proves the copy; want the switch not committed yet", state, err)
			}
			return nil
		}},
	}
	for range 2 {
		if err := runUpgradeLane(context.Background(), cfg, time.Now, purger, run); err != nil {
			t.Fatalf("runUpgradeLane() error = %v", err)
		}
	}
	want := []string{"run pre-upgrade", "base-restore 0000000000000444", "purge " + laneS3Prefix + snapshot, "run post-upgrade", "run post-upgrade"}
	if strings.Join(calls, "; ") != strings.Join(want, "; ") {
		t.Fatalf("calls = %v\nwant    %v: the chain is copied and proven once, before the purge and before the candidate", calls, want)
	}
	for _, phase := range post {
		if phase.upgradePreTXID != 0x444 || phase.upgradePreservedChain != laneS3Prefix+reporting.PreservedChainPrefixSuffix {
			t.Fatalf("post-upgrade config txid %x chain %q, want the preserved chain's proven txid, not the saved boundary 433", phase.upgradePreTXID, phase.upgradePreservedChain)
		}
	}
	if _, ok := store.Get(laneS3Prefix + reporting.PreservedChainPrefixSuffix + snapshot); !ok {
		t.Fatal("preserved chain lost the base build's snapshot to the L9 purge")
	}
	if _, ok := store.Get(laneS3Prefix + snapshot); ok {
		t.Fatal("L9 purge left the live replica's snapshot in place")
	}
	state, err := readUpgradeLaneState(cfg.DataDir)
	if err != nil || !state.PreservedChain.succeeded() || state.PreservedChain.Attempt != 1 || state.SwitchBlockedAt != nil {
		t.Fatalf("state = %+v, %v; want one successful preservation on record", state.PreservedChain, err)
	}
}

func TestUpgradeLaneCopyFailureIsALaneFaultBeforeTheSwitch(t *testing.T) {
	shortenUpgradeEvidenceWait(t, time.Second)
	cfg, _ := fileLane(t)
	seedLaneReplica(t, cfg)
	switchedLaneState(t, cfg, nil)

	var mu sync.Mutex
	var events []reporting.WorkerEventPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event reporting.WorkerEventPayload
		_ = json.NewDecoder(r.Body).Decode(&event)
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	cfg.ControlBaseURL = server.URL

	var calls []string
	run := func(ctx context.Context, phase Config) error {
		calls = append(calls, "run "+phase.upgradePhase)
		if phase.upgradePhase == UpgradePhasePre {
			<-ctx.Done()
		}
		return nil
	}
	err := runUpgradeLane(context.Background(), cfg, time.Now, chainLanePurger(&calls, func() error { return errors.New("restore did not finish inside its 30m0s budget") }), run)
	if !errors.Is(err, errUpgradeLaneHalted) || !strings.Contains(err.Error(), "pre-upgrade chain preservation") {
		t.Fatalf("runUpgradeLane() error = %v, want the failed preservation", err)
	}
	for _, call := range calls {
		if call == "run post-upgrade" {
			t.Fatalf("calls = %v, want the candidate never started", calls)
		}
	}
	state, err := readUpgradeLaneState(cfg.DataDir)
	if err != nil || state.Phase != UpgradePhasePre || state.SwitchedAt != nil || state.SwitchBlockedAt != nil || state.BaseProofFailure != nil {
		t.Fatalf("state = %+v, %v; want the lane still pre-upgrade, neither switched nor blocked", state, err)
	}
	if state.PreservedChain == nil || state.PreservedChain.succeeded() || state.PreservedChain.Attempt != replicaL9PurgeAttempts || state.PreservedChain.BaseProofFailed {
		t.Fatalf("preserved chain = %+v; want every failed attempt on record as a lane fault", state.PreservedChain)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 || events[0].EventType != "worker_failed" {
		t.Fatalf("events = %+v, want one worker_failed", events)
	}
}

func TestUpgradeLaneBaseProofFailureBlocksTheSwitchAsABaselineFailure(t *testing.T) {
	shortenUpgradeEvidenceWait(t, time.Second)
	cfg, _ := fileLane(t)
	seedLaneReplica(t, cfg)
	switchedLaneState(t, cfg, nil)

	var mu sync.Mutex
	var events []reporting.WorkerEventPayload
	var verifications []reporting.VerificationPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/verifications"):
			var payload reporting.VerificationPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Error(err)
			}
			verifications = append(verifications, payload)
		case strings.HasSuffix(r.URL.Path, "/events"):
			var payload reporting.WorkerEventPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Error(err)
			}
			events = append(events, payload)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	cfg.ControlBaseURL = server.URL
	reported := func() bool {
		state, err := readUpgradeLaneState(cfg.DataDir)
		return err == nil && state.BlockedEventDeliveredAt != nil && state.BaseProofFailure != nil && state.BaseProofFailure.DeliveredAt != nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls []string
	run := func(runCtx context.Context, phase Config) error {
		calls = append(calls, "run "+phase.upgradePhase)
		if phase.upgradePhase != UpgradePhasePre {
			return nil
		}
		if len(calls) > 1 {
			deadline := time.Now().Add(10 * time.Second)
			for !reported() && time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
			}
			cancel()
		}
		<-runCtx.Done()
		return nil
	}
	purger := chainLanePurger(&calls, func() error {
		return fmt.Errorf("%w: restore failed: %s txid 0000000000000444: exit status 1: Error: decode ltx: checksum mismatch", errBaseProofFailed, reporting.PreservedChainRestoreSubject)
	})
	if err := runUpgradeLane(ctx, cfg, time.Now, purger, run); err != nil {
		t.Fatalf("runUpgradeLane() error = %v, want the lane left running on the base build", err)
	}
	want := []string{"run pre-upgrade", "base-restore 0000000000000444", "base-restore 0000000000000444", "base-restore 0000000000000444", "run pre-upgrade"}
	if strings.Join(calls, "; ") != strings.Join(want, "; ") {
		t.Fatalf("calls = %v\nwant    %v: the base build is retried, then kept running, and the candidate never starts", calls, want)
	}
	state, err := readUpgradeLaneState(cfg.DataDir)
	if err != nil || state.Phase != UpgradePhasePre || state.SwitchedAt != nil || state.SwitchBlockedAt == nil || !strings.Contains(state.SwitchBlockedReason, "checksum mismatch") {
		t.Fatalf("state = %+v, %v; want a blocked switch that never left the pre-upgrade phase", state, err)
	}
	if state.BaseProofFailure == nil || state.BaseProofFailure.DeliveredAt == nil || state.BlockedEventDeliveredAt == nil {
		t.Fatalf("state = %+v; want the failed proof and the blocked event both delivered", state)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 || events[0].EventType != upgradeSwitchBlockedEvent {
		t.Fatalf("events = %+v, want one %s and no worker_failed", events, upgradeSwitchBlockedEvent)
	}
	if len(verifications) != 1 {
		t.Fatalf("verifications = %+v, want the failed proof reported once", verifications)
	}
	got := verifications[0]
	if got.UpgradePhase != UpgradePhasePre || got.ActiveLitestreamSHA != testUpgradeBaseSHA || got.Status != "failed" || got.Passed || got.RestoreMode != restoreModeTXID || got.CheckType == preUpgradeRestoreCheckType || !strings.Contains(got.ErrorMessage, "checksum mismatch") {
		t.Fatalf("verification = %+v, want a failed pre-upgrade txid restore of the base build", got)
	}
	if reporting.PinnedRestoreTargetUnavailable(got.RestoreMode, got.ErrorMessage) {
		t.Fatalf("error %q would be normalized to inconclusive", got.ErrorMessage)
	}
}

func TestUpgradeLaneSwitchedWithoutAChainKeepsTheSavedTXID(t *testing.T) {
	cfg, _ := fileLane(t)
	seedLaneReplica(t, cfg)
	switched := time.Now().UTC().Add(-time.Hour)
	switchedLaneState(t, cfg, func(state *upgradeLaneState) {
		state.Phase = UpgradePhasePost
		state.SwitchedAt = &switched
	})

	var calls []string
	var post Config
	if err := runUpgradeLane(context.Background(), cfg, time.Now, chainLanePurger(&calls, func() error { return nil }), func(_ context.Context, phase Config) error {
		post = phase
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 || post.upgradePreTXID != 0x433 || post.upgradePreservedChain != "" {
		t.Fatalf("calls = %v, post txid %x chain %q; a replica the candidate already wrote to must not be copied as the base build's chain", calls, post.upgradePreTXID, post.upgradePreservedChain)
	}
	if _, err := os.Stat(preservedChainConfig(cfg).ReplicaPath); !os.IsNotExist(err) {
		t.Fatalf("preserved prefix created after the switch: %v", err)
	}
}

func TestPreservedChainRestoreUsesTheCopyAndFailsForReal(t *testing.T) {
	const merged = "Error: cannot calc restore plan: txid 0000000000000433 unavailable (nearest covered TXIDs: before=000000000000042f, after=0000000000000444): transaction not available"
	script := `
out=""
txid=""
config=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift ;;
    -txid) txid="$2"; shift ;;
    -config) config="$2"; shift ;;
  esac
  shift
done
if [ "$txid" = "0000000000000433" ]; then
  cp "$config" "$CONFIG_COPY"
  case "${PRE_UPGRADE_OUTCOME:-ok}" in
    ok) cp "$SOURCE_PATH" "$out"; exit 0 ;;
    *) echo '` + merged + `' >&2; exit 1 ;;
  esac
fi
cp "$SOURCE_PATH" "$out"
echo 'level=DEBUG msg="restore plan" n=1 txid=0000000000000042'
`
	tests := []struct {
		name, outcome string
		preserved     bool
		wantStatus    string
		wantText      string
	}{
		{name: "live replica target merged by compaction is inconclusive", outcome: "merged", wantStatus: "aborted", wantText: "verification inconclusive"},
		{name: "preserved chain restores", outcome: "ok", preserved: true, wantStatus: "passed", wantText: "preserved pre-upgrade chain txid 0000000000000433 restored"},
		{name: "preserved chain target unavailable is a failure", outcome: "merged", preserved: true, wantStatus: "failed", wantText: "restore failed: preserved pre-upgrade chain txid 0000000000000433 (exit 1): " + merged},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _ := restoreModeFixture(t, script)
			configCopy := filepath.Join(cfg.DataDir, "config-copy")
			t.Setenv("SOURCE_PATH", cfg.DBPath)
			t.Setenv("PRE_UPGRADE_OUTCOME", tt.outcome)
			t.Setenv("CONFIG_COPY", configCopy)
			cfg.ReplicaType = "file"
			cfg.ReplicaPath = filepath.Join(cfg.DataDir, "replica")
			cfg.upgradePreTXID = 0x433
			if tt.preserved {
				cfg.upgradePreservedChain = cfg.ReplicaPath + reporting.PreservedChainPrefixSuffix
			}
			cfg.VerifyRestoreRotation = false
			startBoundarySyncFixture(t, &cfg, 0x42)
			v := NewVerifier(cfg, &fakePauser{})
			if main, err := v.RunCycle(context.Background()); err != nil || !main.Passed {
				t.Fatalf("main cycle = %+v, %v", main, err)
			}
			extras := v.takeExtraResults()
			if len(extras) != 1 || extras[0].CheckType != preUpgradeRestoreCheckType {
				t.Fatalf("extra results = %+v", extras)
			}
			historical := extras[0]
			if historical.Status != tt.wantStatus || !strings.Contains(historical.Summary+" "+historical.ErrorMessage, tt.wantText) {
				t.Fatalf("historical = status %q summary %q error %q, want %q with %q", historical.Status, historical.Summary, historical.ErrorMessage, tt.wantStatus, tt.wantText)
			}
			used, err := os.ReadFile(configCopy)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Contains(string(used), "path: "+cfg.ReplicaPath+reporting.PreservedChainPrefixSuffix+"\n"); got != tt.preserved {
				t.Fatalf("restore config points at the preserved copy = %v, want %v:\n%s", got, tt.preserved, used)
			}
			leftovers, err := filepath.Glob(filepath.Join(cfg.DataDir, "litestream-preserved-*.yml"))
			if err != nil || len(leftovers) != 0 {
				t.Fatalf("preserved chain config left behind: %v, %v", leftovers, err)
			}
		})
	}
}

func baseRestoreFixture(t *testing.T) Config {
	t.Helper()
	cfg, _ := restoreModeFixture(t, "exit 0\n")
	cfg.ReplicaType = "file"
	cfg.ReplicaPath = filepath.Join(cfg.DataDir, "replica")
	baseBinary := filepath.Join(cfg.DataDir, "litestream-base")
	script := `#!/bin/sh
printf '%s\n' "$@" > "$BASE_ARGS"
out=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift ;;
    -config) cp "$2" "$BASE_CONFIG"; shift ;;
  esac
  shift
done
case "$BASE_OUTCOME" in
  fail) echo 'Error: decode ltx: checksum mismatch' >&2; exit 1 ;;
  enospc) echo "Error: decode database: write page 7: write $out.tmp: no space left on device" >&2; exit 1 ;;
  hang) exec sleep 30 ;;
  corrupt) printf 'not a database' > "$out"; exit 0 ;;
esac
cp "$SOURCE_PATH" "$out"
`
	if err := os.WriteFile(baseBinary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BASE_ARGS", filepath.Join(cfg.DataDir, "base-args"))
	t.Setenv("BASE_CONFIG", filepath.Join(cfg.DataDir, "base-config"))
	t.Setenv("SOURCE_PATH", cfg.DBPath)
	cfg.litestreamBinary = baseBinary
	return cfg
}

func TestRestorePreservedChainRunsTheBaseBuildAgainstTheCopy(t *testing.T) {
	base := baseRestoreFixture(t)
	if err := restorePreservedChain(context.Background(), base, preservedChainConfig(base), 0x444); err != nil {
		t.Fatalf("restorePreservedChain() error = %v", err)
	}
	args := readLines(t, filepath.Join(base.DataDir, "base-args"))
	assertContains(t, args, "-txid")
	assertContains(t, args, "0000000000000444")
	assertContains(t, args, base.DBPath+".restored.pre")
	used, err := os.ReadFile(filepath.Join(base.DataDir, "base-config"))
	if err != nil || !strings.Contains(string(used), "path: "+base.ReplicaPath+reporting.PreservedChainPrefixSuffix+"\n") {
		t.Fatalf("base restore config = %q, %v; want the preserved copy as its replica", used, err)
	}
	if _, err := os.Stat(base.DBPath + ".restored.pre"); !os.IsNotExist(err) {
		t.Fatalf("restore artifact left behind: %v", err)
	}
}

func TestRestorePreservedChainSeparatesBuildFailuresFromHarnessLimits(t *testing.T) {
	tests := []struct {
		name, outcome string
		restoreBudget time.Duration
		wantProof     bool
		wantText      string
	}{
		{name: "restore exits with an error", outcome: "fail", wantProof: true, wantText: "restore failed: preserved pre-upgrade chain txid 0000000000000444: exit status 1: Error: decode ltx: checksum mismatch"},
		{name: "restored database fails the integrity check", outcome: "corrupt", wantProof: true, wantText: "integrity check"},
		{name: "restore output ran out of disk", outcome: "enospc", wantText: "no space left on device"},
		{name: "restore ran past the verifier's restore budget", outcome: "hang", restoreBudget: 50 * time.Millisecond, wantText: "restore did not finish inside its 50ms budget"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := baseRestoreFixture(t)
			t.Setenv("BASE_OUTCOME", tt.outcome)
			base.VerifyRestoreBudget = tt.restoreBudget
			err := restorePreservedChain(context.Background(), base, preservedChainConfig(base), 0x444)
			if err == nil || errors.Is(err, errBaseProofFailed) != tt.wantProof || !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("restorePreservedChain() error = %v; want base build failure %v containing %q", err, tt.wantProof, tt.wantText)
			}
		})
	}
}

const laneS3Prefix = "soak/worker-a/vol_1"

func s3Lane(t *testing.T) (Config, Config, *s3fake.Server) {
	t.Helper()
	quickenChainPreservation(t)
	store := s3fake.New("bkt")
	t.Cleanup(store.Close)
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bkt"
	cfg.S3Path = laneS3Prefix
	cfg.S3Endpoint = store.URL
	cfg.S3AccessKey = "access"
	cfg.S3SecretKey = "secret"
	store.Put(laneS3Prefix+"/0000/0000000000000444-0000000000000444.ltx", []byte("l0"), "")
	store.Put(laneS3Prefix+"/0009/0000000000000001-000000000000042f.ltx", []byte("snapshot"), "bbbb-3")
	store.Put(laneS3Prefix+"/profiles/2026-11-17T06:28:01Z_verification-failed heap+cpu 'q'.pprof.json", []byte("{}"), "")
	store.Put(laneS3Prefix+"/ltx-not-the-tree.txt", []byte("x"), "")
	store.Put(laneS3Prefix+"/ltx/0/0000000000000444-0000000000000444.ltx", []byte("file replica layout, not what the S3 client writes"), "")
	store.Put(laneS3Prefix+"/0000/notes.txt", []byte("x"), "")
	store.Put(laneS3Prefix+"-pre-upgrade/0000/0000000000000999-0000000000000999.ltx", []byte("left by an earlier lane"), "")
	store.Put(laneS3Prefix+"-pre-upgrade/profiles/partial.pprof.json", []byte("left by a failed copy"), "")
	base, err := cfg.ForUpgradePhase(UpgradePhasePre)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, base, store
}

func TestUpgradeChainPreserveCopiesOnlyTheLTXTreeOfAnS3Replica(t *testing.T) {
	cfg, base, store := s3Lane(t)
	var restoredFrom string
	preserver := &upgradeChainPreserver{restore: func(_ context.Context, _, preserved Config, _ uint64) error {
		restoredFrom = preserved.ReplicaURL()
		return nil
	}}
	record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
	if err != nil || !record.succeeded() || record.TXID != "0000000000000444" || record.Objects != 2 || record.Bytes != 10 || record.Prefix != laneS3Prefix+"-pre-upgrade" {
		t.Fatalf("record = %+v, %v", record, err)
	}
	if restoredFrom != "s3://bkt/"+laneS3Prefix+"-pre-upgrade" {
		t.Fatalf("base restore replica = %q", restoredFrom)
	}
	want := []string{
		laneS3Prefix + "-pre-upgrade/0000/0000000000000444-0000000000000444.ltx",
		laneS3Prefix + "-pre-upgrade/0009/0000000000000001-000000000000042f.ltx",
	}
	if got := store.Keys(laneS3Prefix + "-pre-upgrade/"); !slices.Equal(got, want) {
		t.Fatalf("preserved objects = %v\nwant %v: the LTX tree only, with stale and non-LTX objects gone", got, want)
	}
	for _, call := range store.Calls() {
		if strings.HasPrefix(call, "copy ") && (strings.Contains(call, "pprof.json") || strings.Contains(call, "ltx-not-the-tree") || strings.Contains(call, "/ltx/0/") || strings.Contains(call, "notes.txt")) {
			t.Fatalf("copied a non-LTX object: %s", call)
		}
	}
	if len(store.Keys(laneS3Prefix+"/")) != 6 {
		t.Fatalf("source objects = %v, want the replica untouched", store.Keys(laneS3Prefix+"/"))
	}

	before := len(store.Calls())
	if record, err := preserver.preserve(context.Background(), cfg, base, time.Now); err != nil || !record.succeeded() {
		t.Fatalf("second preserve: record = %+v, %v", record, err)
	}
	if after := store.Calls(); len(after) != before {
		t.Fatalf("second preserve repeated work: %v", after[before:])
	}
}

func TestUpgradeChainPreserveChecksAnS3CopyBySizeAndETag(t *testing.T) {
	l0 := "0000/0000000000000444-0000000000000444.ltx"
	tests := []struct {
		name    string
		onCopy  func(store *s3fake.Server) func(source, destination string) error
		after   func(store *s3fake.Server)
		wantErr string
	}{
		{name: "copy fails", onCopy: func(*s3fake.Server) func(string, string) error {
			return func(source, _ string) error {
				if strings.HasSuffix(source, l0) {
					return errors.New("we encountered an internal error")
				}
				return nil
			}
		}, wantErr: "copy replica to " + laneS3Prefix + "-pre-upgrade: copy object failed: HTTP 500"},
		{name: "replica changed during the copy", onCopy: func(store *s3fake.Server) func(string, string) error {
			return func(string, string) error {
				store.Put(laneS3Prefix+"/0000/0000000000000445-0000000000000445.ltx", []byte("late upload"), "")
				return nil
			}
		}, wantErr: "changed while it was being copied: 3 object(s), want 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, base, store := s3Lane(t)
			store.OnCopy = tt.onCopy(store)
			restores := 0
			preserver := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error {
				restores++
				return nil
			}}
			record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
			if err == nil || record.succeeded() || record.BaseProofFailed || !strings.Contains(err.Error(), tt.wantErr) || restores != 0 {
				t.Fatalf("record = %+v, %v, %d restore(s); want a lane fault containing %q and no base restore", record, err, restores, tt.wantErr)
			}
		})
	}

	t.Run("copied object has different content", func(t *testing.T) {
		cfg, _, store := s3Lane(t)
		source, err := replicaObjectStats(context.Background(), cfg, replicaLTXKeys(cfg))
		if err != nil {
			t.Fatal(err)
		}
		if err := syncReplicaObjects(context.Background(), cfg, preservedChainConfig(cfg), source); err != nil {
			t.Fatal(err)
		}
		store.Put(laneS3Prefix+"-pre-upgrade/"+l0, []byte("zz"), "")
		copied, err := replicaObjectStats(context.Background(), preservedChainConfig(cfg), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := matchReplicaObjects(source, copied, true); err == nil || !strings.Contains(err.Error(), l0+" has ETag") {
			t.Fatalf("matchReplicaObjects() = %v, want the same-size object with different content caught", err)
		}
		if err := syncReplicaObjects(context.Background(), cfg, preservedChainConfig(cfg), source); err != nil {
			t.Fatal(err)
		}
		if body, _ := store.Get(laneS3Prefix + "-pre-upgrade/" + l0); string(body) != "l0" {
			t.Fatalf("mismatched object = %q, want it copied again", body)
		}
	})
}

func TestUpgradeChainCopyBudgetScalesWithTheReplica(t *testing.T) {
	for _, tt := range []struct {
		name          string
		snapshotBytes int64
		wantBudget    string
	}{
		{name: "small replica", snapshotBytes: 900, wantBudget: "timed out after 60ms"},
		{name: "three gigabyte replica", snapshotBytes: 3 << 30, wantBudget: "timed out after 240ms"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, base, store := s3Lane(t)
			cfg.LogicalTimeout = 60 * time.Millisecond
			store.SetSize(laneS3Prefix+"/0009/0000000000000001-000000000000042f.ltx", tt.snapshotBytes)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			store.OnCopy = func(string, string) error {
				select {
				case <-release:
				case <-time.After(5 * time.Second):
				}
				return nil
			}
			preserver := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error { return nil }}
			record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
			if err == nil || record.BaseProofFailed || !strings.Contains(err.Error(), tt.wantBudget) || !strings.Contains(err.Error(), "resumes from the objects already copied") {
				t.Fatalf("record = %+v, %v; want %q", record, err, tt.wantBudget)
			}
		})
	}
}

func TestFileReplicaPreservationLeavesNonLTXObjectsBehind(t *testing.T) {
	cfg, base := fileLane(t)
	seedLaneReplica(t, cfg)
	writeLaneFile(t, filepath.Join(cfg.ReplicaPath, "profiles", "cpu.pprof.json"), "{}")
	writeLaneFile(t, preservedLaneFile(cfg, "profiles/partial.pprof.json"), "left by a failed copy")
	preserver := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error { return nil }}
	record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
	if err != nil || record.Objects != len(laneReplicaKeys) {
		t.Fatalf("record = %+v, %v", record, err)
	}
	copied, err := listFileReplicaObjects(preservedChainConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(copied)
	if !slices.Equal(copied, laneReplicaKeys) {
		t.Fatalf("preserved files = %v, want only %v", copied, laneReplicaKeys)
	}
}

func TestUpgradeLaneFaultIsTerminalAcrossRestarts(t *testing.T) {
	shortenUpgradeEvidenceWait(t, time.Second)
	cfg, _ := fileLane(t)
	seedLaneReplica(t, cfg)
	switchedLaneState(t, cfg, nil)
	var mu sync.Mutex
	var events []reporting.WorkerEventPayload
	failing := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event reporting.WorkerEventPayload
		_ = json.NewDecoder(r.Body).Decode(&event)
		mu.Lock()
		defer mu.Unlock()
		if failing {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		events = append(events, event)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	cfg.ControlBaseURL = server.URL

	var calls []string
	run := func(ctx context.Context, phase Config) error {
		calls = append(calls, "run "+phase.upgradePhase)
		if phase.upgradePhase == UpgradePhasePre {
			<-ctx.Done()
		}
		return nil
	}
	purger := chainLanePurger(&calls, func() error { return errors.New("copy object failed: HTTP 403 SignatureDoesNotMatch") })
	err := runUpgradeLane(context.Background(), cfg, time.Now, purger, run)
	if !errors.Is(err, errUpgradeLaneHalted) || !strings.Contains(err.Error(), "3 attempt(s) failed") {
		t.Fatalf("runUpgradeLane() error = %v, want the lane halted after its attempts", err)
	}
	first := slices.Clone(calls)
	want := []string{"run pre-upgrade", "base-restore 0000000000000444", "base-restore 0000000000000444", "base-restore 0000000000000444"}
	if !slices.Equal(first, want) {
		t.Fatalf("calls = %v, want %v", first, want)
	}

	mu.Lock()
	failing = false
	mu.Unlock()
	for range 3 {
		if err := runUpgradeLane(context.Background(), cfg, time.Now, purger, run); !errors.Is(err, errUpgradeLaneHalted) {
			t.Fatalf("restarted runUpgradeLane() error = %v, want the lane still halted", err)
		}
	}
	if !slices.Equal(calls, first) {
		t.Fatalf("calls after restarts = %v, want no base build start and no further copy attempt beyond %v", calls, first)
	}
	state, err := readUpgradeLaneState(cfg.DataDir)
	if err != nil || state.LaneFault == nil || state.LaneFault.DeliveredAt == nil || state.Phase != UpgradePhasePre || state.PreservedChain.Attempt != replicaL9PurgeAttempts {
		t.Fatalf("state = %+v, %v; want a delivered, terminal lane fault", state, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 || events[0].EventType != "worker_failed" || !strings.Contains(events[0].Message, "SignatureDoesNotMatch") {
		t.Fatalf("events = %+v, want the fault reported exactly once", events)
	}
}

func TestRunUpgradeLaneIdlesOnAHaltedLaneInsteadOfExiting(t *testing.T) {
	cfg, _ := fileLane(t)
	switchedLaneState(t, cfg, func(state *upgradeLaneState) {
		state.LaneFault = &laneFault{At: time.Now().UTC(), Error: "upgrade lane pre-upgrade chain preservation: 3 attempt(s) failed"}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunUpgradeLane(ctx, cfg, func(context.Context, Config) error {
			t.Error("a halted lane started a build")
			return nil
		})
	}()
	select {
	case err := <-done:
		t.Fatalf("RunUpgradeLane() returned %v; an exit would have the platform restart the machine", err)
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunUpgradeLane() error = %v after shutdown, want a clean exit", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunUpgradeLane() did not return after shutdown")
	}
}

func TestUpgradeChainAttemptsAreCountedAcrossRestarts(t *testing.T) {
	cfg, _ := fileLane(t)
	seedLaneReplica(t, cfg)
	switchedLaneState(t, cfg, func(state *upgradeLaneState) {
		state.PreservedChain = &preservedChain{Prefix: "p", Attempt: 2, Error: "attempt did not finish"}
	})
	restores := 0
	preserver := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error {
		restores++
		if state, err := readUpgradeLaneState(cfg.DataDir); err != nil || state.PreservedChain == nil || state.PreservedChain.Attempt != 3 || state.PreservedChain.Error == "" {
			t.Errorf("state during the attempt = %+v, %v; want attempt 3 on record before it finishes", state.PreservedChain, err)
		}
		return errors.New("restore capacity: no room")
	}}
	record, err := preserveUntilVerified(context.Background(), cfg, time.Now, preserver)
	if err == nil || restores != 1 || record.Attempt != 3 {
		t.Fatalf("record = %+v, %v, %d restore(s); want only the one attempt the lane had left", record, err, restores)
	}
	record, err = preserveUntilVerified(context.Background(), cfg, time.Now, preserver)
	if err == nil || restores != 1 || !strings.Contains(err.Error(), "restore capacity: no room") {
		t.Fatalf("record = %+v, %v, %d restore(s); want no attempt once the lane has used them all", record, err, restores)
	}
}

func TestPhaseEndDuringStartupIsNotAWorkerFailure(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.ControlBaseURL = server.URL
	runner := NewRunner(cfg)
	runner.reporter = NewReporter(cfg)
	runner.sendPreStartFailureEvent(fmt.Errorf("wait for first sync: %w", errUpgradeWarmupEnded))
	if got := requests.Load(); got != 0 {
		t.Fatalf("events = %d, want none when the warm-up ends while the base build is starting", got)
	}
	runner.sendPreStartFailureEvent(errors.New("wait for first sync: litestream exited"))
	if got := requests.Load(); got != 1 {
		t.Fatalf("events = %d, want a real startup failure still reported", got)
	}
}

func TestStopLitestreamThenCancelLetsTheProcessExitOnItsOwn(t *testing.T) {
	for _, tt := range []struct {
		name, trap string
		grace      time.Duration
		wantClean  bool
	}{
		{name: "signalled process finishes its work before the context is canceled", trap: `trap 'sleep 0.3; echo drained > "$STOP_MARKER"; exit 0' INT`, grace: 30 * time.Second, wantClean: true},
		{name: "process that ignores the signal is killed after the grace", trap: `trap '' INT`, grace: 100 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			marker := filepath.Join(dir, "marker")
			ready := filepath.Join(dir, "ready")
			binary := filepath.Join(dir, "litestream")
			script := "#!/bin/sh\n" + tt.trap + "\necho ready > \"$STOP_READY\"\nwhile :; do sleep 0.05; done\n"
			if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("STOP_MARKER", marker)
			t.Setenv("STOP_READY", ready)
			cfg := DefaultConfig()
			cfg.DataDir = dir
			cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
			cfg.litestreamBinary = binary
			manager := newLitestreamManager(&cfg)
			ctx, cancel := context.WithCancel(context.Background())
			if err := manager.startLitestream(ctx); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("fake litestream never started")
				}
				time.Sleep(5 * time.Millisecond)
			}
			started := time.Now()
			manager.stopLitestreamThenCancel(cancel, tt.grace)
			if manager.litestreamRunning() || ctx.Err() == nil {
				t.Fatal("process still running or context still live after the stop")
			}
			_, err := os.Stat(marker)
			if clean := err == nil; clean != tt.wantClean {
				t.Fatalf("process finished its shutdown work = %v, want %v (stop took %s)", clean, tt.wantClean, time.Since(started))
			}
		})
	}
}

func TestLitestreamStopWaitFitsThePlatformKillGrace(t *testing.T) {
	finalReports := 5*time.Second + 5*time.Second
	for _, tt := range []struct {
		name  string
		cause error
		want  time.Duration
	}{
		{name: "upgrade warm-up ended, the worker keeps running", cause: errUpgradeWarmupEnded, want: litestreamStopGrace},
		{name: "stress scenario slot ended", cause: errStressScenarioPhaseEnded, want: litestreamStopGrace},
		{name: "worker told to stop", cause: context.Canceled, want: litestreamShutdownGrace},
		{name: "run failed on its own", cause: nil, want: litestreamShutdownGrace},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := litestreamStopGraceFor(tt.cause); got != tt.want {
				t.Fatalf("litestreamStopGraceFor(%v) = %s, want %s", tt.cause, got, tt.want)
			}
		})
	}
	if litestreamShutdownGrace >= 5*time.Second {
		t.Fatalf("shutdown wait %s does not fit inside Fly's default five-second kill grace", litestreamShutdownGrace)
	}
	if room := flyapi.WorkerStopTimeout - litestreamStopGrace - finalReports; room < time.Minute {
		t.Fatalf("worker stop timeout %s leaves %s after the longest Litestream wait and the final reports, want at least a minute", flyapi.WorkerStopTimeout, room)
	}
}

func TestPreservationReportsWhatAReplicaWithoutLTXObjectsHolds(t *testing.T) {
	cfg, base, store := s3Lane(t)
	for _, key := range store.Keys(laneS3Prefix + "/") {
		if s3LTXKeyPattern.MatchString(strings.TrimPrefix(key, laneS3Prefix+"/")) {
			store.Delete(key)
		}
	}
	preserver := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error { return nil }}
	record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
	if err == nil || record.succeeded() {
		t.Fatalf("record = %+v, %v", record, err)
	}
	for _, want := range []string{"4 object(s) listed, none shaped like", "0000/notes.txt", "ltx/0/0000000000000444-0000000000000444.ltx"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want it to say what the prefix does hold (%q)", err, want)
		}
	}
}

func TestReplicaLTXKeyLayoutMatchesLitestream(t *testing.T) {
	for _, tt := range []struct {
		backend, key string
		want         bool
		txid         uint64
	}{
		{backend: "s3", key: "0000/0000000000000443-0000000000000444.ltx", want: true, txid: 0x444},
		{backend: "s3", key: "0009/0000000000000001-000000000000042f.ltx", want: true, txid: 0x42f},
		{backend: "s3", key: "ltx/0/0000000000000443-0000000000000444.ltx"},
		{backend: "s3", key: "ltx/0000/0000000000000443-0000000000000444.ltx"},
		{backend: "s3", key: "profiles/x.pprof.json"},
		{backend: "s3", key: "0000/0000000000000443-0000000000000444.ltx.tmp"},
		{backend: "file", key: "ltx/0/0000000000000443-0000000000000444.ltx", want: true, txid: 0x444},
		{backend: "file", key: "ltx/9/0000000000000001-000000000000042f.ltx", want: true, txid: 0x42f},
		{backend: "file", key: "0000/0000000000000443-0000000000000444.ltx"},
	} {
		cfg := Config{ReplicaType: tt.backend}
		if got := replicaLTXKeys(cfg)(tt.key); got != tt.want {
			t.Errorf("%s key %q is LTX = %v, want %v", tt.backend, tt.key, got, tt.want)
		}
		if got := maxLTXObjectTXID(cfg, []string{tt.key}); got != tt.txid {
			t.Errorf("%s key %q txid = %x, want %x", tt.backend, tt.key, got, tt.txid)
		}
	}
}
