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

func seedLaneReplica(t *testing.T, cfg Config) {
	t.Helper()
	for _, key := range []string{
		"ltx/0000/0000000000000443-0000000000000443.ltx",
		"ltx/0000/0000000000000444-0000000000000444.ltx",
		"ltx/0001/0000000000000430-0000000000000442.ltx",
		"ltx/0009/0000000000000001-000000000000042f.ltx",
	} {
		path := filepath.Join(cfg.ReplicaPath, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(key), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func preservedLaneFile(cfg Config, key string) string {
	return filepath.Join(preservedChainConfig(cfg).ReplicaPath, filepath.FromSlash(key))
}

func TestUpgradeChainPreserveCopiesTheReplicaAndProvesItWithTheBaseBuild(t *testing.T) {
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.ReplicaPath = filepath.Join(dir, "replica")
	seedLaneReplica(t, cfg)
	stale := preservedLaneFile(cfg, "ltx/0000/0000000000000999-0000000000000999.ltx")
	if err := os.MkdirAll(filepath.Dir(stale), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, []byte("left by an earlier lane"), 0o644); err != nil {
		t.Fatal(err)
	}
	base, err := cfg.ForUpgradePhase(UpgradePhasePre)
	if err != nil {
		t.Fatal(err)
	}
	var restored []uint64
	preserver := &upgradeChainPreserver{restore: func(_ context.Context, build, preserved Config, txid uint64) error {
		if build.litestreamCommand() != cfg.LitestreamBaseBinary {
			t.Errorf("restore ran %s, want the base build", build.litestreamCommand())
		}
		if _, err := os.Stat(filepath.Join(preserved.ReplicaPath, "ltx", "0009", "0000000000000001-000000000000042f.ltx")); err != nil {
			t.Errorf("preserved copy is incomplete when the base build restores it: %v", err)
		}
		restored = append(restored, txid)
		return nil
	}}

	record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
	if err != nil {
		t.Fatalf("preserve() error = %v", err)
	}
	if !record.succeeded() || record.TXID != "0000000000000444" || record.Objects != 4 || record.Prefix != cfg.ReplicaPath+preservedChainSuffix {
		t.Fatalf("record = %+v, want the newest replicated txid proven on a four-object copy", record)
	}
	if len(restored) != 1 || restored[0] != 0x444 {
		t.Fatalf("base restores = %x, want one restore of txid 444", restored)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("object from an earlier lane survived in the preserved prefix: %v", err)
	}
	if err := os.Remove(filepath.Join(cfg.ReplicaPath, "ltx", "0000", "0000000000000443-0000000000000443.ltx")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(preservedLaneFile(cfg, "ltx/0000/0000000000000443-0000000000000443.ltx")); err != nil {
		t.Fatalf("preserved copy lost a file once the live replica dropped it: %v", err)
	}
}

func TestUpgradeChainPreserveFailsWithoutABaseRestore(t *testing.T) {
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.ReplicaPath = filepath.Join(dir, "replica")
	base, err := cfg.ForUpgradePhase(UpgradePhasePre)
	if err != nil {
		t.Fatal(err)
	}
	preserver := &upgradeChainPreserver{restore: func(context.Context, Config, Config, uint64) error {
		return errors.New("integrity check: database disk image is malformed")
	}}
	if record, err := preserver.preserve(context.Background(), cfg, base, time.Now); err == nil || record.succeeded() || !strings.Contains(record.Error, "no LTX objects") {
		t.Fatalf("empty replica: record = %+v, %v; want a refused preservation", record, err)
	}
	seedLaneReplica(t, cfg)
	record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
	if err == nil || record.succeeded() || !strings.Contains(record.Error, "could not restore txid 0000000000000444 from the preserved copy") {
		t.Fatalf("record = %+v, %v; want the base build's failed restore on record", record, err)
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
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.ReplicaPath = filepath.Join(dir, "replica")
	cfg.UpgradeDeleteL9AtSwitch = true
	seedLaneReplica(t, cfg)
	switchedLaneState(t, cfg, nil)

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
	purger := chainLanePurger(&calls, func() error { return nil })
	for range 2 {
		if err := runUpgradeLane(context.Background(), cfg, time.Now, purger, run); err != nil {
			t.Fatalf("runUpgradeLane() error = %v", err)
		}
	}
	want := []string{"run pre-upgrade", "base-restore 0000000000000444", "purge ltx/0009/0000000000000001-000000000000042f.ltx", "run post-upgrade", "run post-upgrade"}
	if strings.Join(calls, "; ") != strings.Join(want, "; ") {
		t.Fatalf("calls = %v\nwant    %v: the chain is copied and proven once, before the purge and before the candidate", calls, want)
	}
	for _, phase := range post {
		if phase.upgradePreTXID != 0x444 || phase.upgradePreservedChain != cfg.ReplicaPath+preservedChainSuffix {
			t.Fatalf("post-upgrade config txid %x chain %q, want the preserved chain's proven txid, not the saved boundary 433", phase.upgradePreTXID, phase.upgradePreservedChain)
		}
	}
	if _, err := os.Stat(preservedLaneFile(cfg, "ltx/0009/0000000000000001-000000000000042f.ltx")); err != nil {
		t.Fatalf("preserved chain lost the base build's snapshot to the L9 purge: %v", err)
	}
	state, err := readUpgradeLaneState(dir)
	if err != nil || !state.PreservedChain.succeeded() || state.PreservedChain.Attempt != 1 {
		t.Fatalf("state = %+v, %v; want one successful preservation on record", state.PreservedChain, err)
	}
}

func TestUpgradeLaneDoesNotStartTheCandidateWithoutAProvenChain(t *testing.T) {
	shortenUpgradeEvidenceWait(t, time.Second)
	delay := replicaL9PurgeRetryDelay
	replicaL9PurgeRetryDelay = time.Millisecond
	t.Cleanup(func() { replicaL9PurgeRetryDelay = delay })
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.ReplicaPath = filepath.Join(dir, "replica")
	seedLaneReplica(t, cfg)
	switchedLaneState(t, cfg, nil)

	var calls []string
	run := func(ctx context.Context, phase Config) error {
		calls = append(calls, "run "+phase.upgradePhase)
		if phase.upgradePhase == UpgradePhasePre {
			<-ctx.Done()
		}
		return nil
	}
	err := runUpgradeLane(context.Background(), cfg, time.Now, chainLanePurger(&calls, func() error { return errors.New("restore: exit status 1") }), run)
	if err == nil || !strings.Contains(err.Error(), "pre-upgrade chain preservation") {
		t.Fatalf("runUpgradeLane() error = %v, want the failed preservation", err)
	}
	for _, call := range calls {
		if call == "run post-upgrade" {
			t.Fatalf("calls = %v, want the candidate never started", calls)
		}
	}
	state, err := readUpgradeLaneState(dir)
	if err != nil || state.PreservedChain == nil || state.PreservedChain.succeeded() || state.PreservedChain.Attempt != replicaL9PurgeAttempts {
		t.Fatalf("state = %+v, %v; want every failed attempt on record", state.PreservedChain, err)
	}
}

func TestUpgradeLaneSwitchedWithoutAChainKeepsTheSavedTXID(t *testing.T) {
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.ReplicaPath = filepath.Join(dir, "replica")
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
				cfg.upgradePreservedChain = cfg.ReplicaPath + preservedChainSuffix
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
			if got := strings.Contains(string(used), "path: "+cfg.ReplicaPath+preservedChainSuffix+"\n"); got != tt.preserved {
				t.Fatalf("restore config points at the preserved copy = %v, want %v:\n%s", got, tt.preserved, used)
			}
			leftovers, err := filepath.Glob(filepath.Join(cfg.DataDir, "litestream-preserved-*.yml"))
			if err != nil || len(leftovers) != 0 {
				t.Fatalf("preserved chain config left behind: %v, %v", leftovers, err)
			}
		})
	}
}

func TestRestorePreservedChainRunsTheBaseBuildAgainstTheCopy(t *testing.T) {
	cfg, _ := restoreModeFixture(t, "exit 0\n")
	cfg.ReplicaType = "file"
	cfg.ReplicaPath = filepath.Join(cfg.DataDir, "replica")
	argsPath := filepath.Join(cfg.DataDir, "base-args")
	configCopy := filepath.Join(cfg.DataDir, "base-config")
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
if [ "$BASE_OUTCOME" = fail ]; then
  echo 'Error: decode ltx: checksum mismatch' >&2
  exit 1
fi
cp "$SOURCE_PATH" "$out"
`
	if err := os.WriteFile(baseBinary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BASE_ARGS", argsPath)
	t.Setenv("BASE_CONFIG", configCopy)
	t.Setenv("SOURCE_PATH", cfg.DBPath)
	base := cfg
	base.litestreamBinary = baseBinary

	if err := restorePreservedChain(context.Background(), base, preservedChainConfig(base), 0x444); err != nil {
		t.Fatalf("restorePreservedChain() error = %v", err)
	}
	args := readLines(t, argsPath)
	assertContains(t, args, "-txid")
	assertContains(t, args, "0000000000000444")
	assertContains(t, args, cfg.DBPath+".restored.chain")
	used, err := os.ReadFile(configCopy)
	if err != nil || !strings.Contains(string(used), "path: "+cfg.ReplicaPath+preservedChainSuffix+"\n") {
		t.Fatalf("base restore config = %q, %v; want the preserved copy as its replica", used, err)
	}
	if _, err := os.Stat(cfg.DBPath + ".restored.chain"); !os.IsNotExist(err) {
		t.Fatalf("restore artifact left behind: %v", err)
	}

	t.Setenv("BASE_OUTCOME", "fail")
	if err := restorePreservedChain(context.Background(), base, preservedChainConfig(base), 0x444); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("restorePreservedChain() error = %v, want the base build's restore failure", err)
	}
}

func TestUpgradeChainPreserveCopiesAnS3PrefixServerSide(t *testing.T) {
	dir := t.TempDir()
	cfg := upgradeLaneConfig(t, dir)
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bkt"
	cfg.S3Path = "soak/worker-a"
	cfg.S3Endpoint = "http://minio:9000"
	bin := t.TempDir()
	log := filepath.Join(dir, "s3cmd.log")
	source := filepath.Join(dir, "source.list")
	copied := filepath.Join(dir, "copied.list")
	script := `#!/bin/sh
verb=""
for arg in "$@"; do
  case "$arg" in
    ls|cp|del) verb="$arg" ;;
  esac
  last="$arg"
done
printf '%s\n' "$*" >> "$S3CMD_LOG"
case "$verb" in
  ls)
    case "$last" in
      *worker-a-pre-upgrade/) cat "$S3CMD_COPIED" ;;
      *) cat "$S3CMD_SOURCE" ;;
    esac ;;
  cp) sed 's#soak/worker-a/#soak/worker-a-pre-upgrade/#' "$S3CMD_SOURCE" > "$S3CMD_COPIED" ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "s3cmd"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("S3CMD_LOG", log)
	t.Setenv("S3CMD_SOURCE", source)
	t.Setenv("S3CMD_COPIED", copied)
	listing := "2026-11-17 03:00 100 s3://bkt/soak/worker-a/ltx/0000/0000000000000444-0000000000000444.ltx\n" +
		"2026-11-17 03:00 900 s3://bkt/soak/worker-a/ltx/0009/0000000000000001-000000000000042f.ltx\n"
	if err := os.WriteFile(source, []byte(listing), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(copied, []byte("2026-11-10 01:00 5 s3://bkt/soak/worker-a-pre-upgrade/ltx/0000/0000000000000999-0000000000000999.ltx\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	base, err := cfg.ForUpgradePhase(UpgradePhasePre)
	if err != nil {
		t.Fatal(err)
	}
	var restoredFrom string
	preserver := &upgradeChainPreserver{restore: func(_ context.Context, _, preserved Config, _ uint64) error {
		restoredFrom = preserved.ReplicaURL()
		return nil
	}}
	record, err := preserver.preserve(context.Background(), cfg, base, time.Now)
	if err != nil || !record.succeeded() || record.TXID != "0000000000000444" || record.Objects != 2 || record.Prefix != "soak/worker-a-pre-upgrade" {
		t.Fatalf("record = %+v, %v", record, err)
	}
	if restoredFrom != "s3://bkt/soak/worker-a-pre-upgrade" {
		t.Fatalf("base restore replica = %q", restoredFrom)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"del s3://bkt/soak/worker-a-pre-upgrade/ltx/0000/0000000000000999-0000000000000999.ltx",
		"cp --recursive s3://bkt/soak/worker-a/ s3://bkt/soak/worker-a-pre-upgrade/",
	} {
		if !strings.Contains(string(calls), want) {
			t.Fatalf("s3cmd calls missing %q:\n%s", want, calls)
		}
	}
	if strings.Index(string(calls), "del s3://") > strings.Index(string(calls), "cp --recursive") {
		t.Fatalf("stale objects were cleared after the copy:\n%s", calls)
	}
}
