package worker

import (
	"context"
	"os"
	"strings"
	"testing"
)

// fakePreUpgradeRestoreScript restores the current boundary normally and
// handles a pinned pre-upgrade txid according to PRE_UPGRADE_OUTCOME:
// "ok" copies the source, "lost" reports the target as unavailable, anything
// else fails with a generic error.
const fakePreUpgradeRestoreScript = `
out=""
txid=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift ;;
    -txid) txid="$2"; shift ;;
  esac
  shift
done
if [ "$txid" = "0000000000000010" ]; then
  case "${PRE_UPGRADE_OUTCOME:-ok}" in
    ok) cp "$SOURCE_PATH" "$out"; exit 0 ;;
    lost) echo '` + pinnedRestoreTargetUnavailableError + `' >&2; exit 1 ;;
    *) echo 'Error: replica unreachable' >&2; exit 1 ;;
  esac
fi
cp "$SOURCE_PATH" "$out"
echo 'level=DEBUG msg="restore plan" n=1 txid=0000000000000042'
`

func TestPreUpgradeRestoreIsItsOwnResult(t *testing.T) {
	tests := []struct {
		name        string
		outcome     string
		wantStatus  string
		wantSummary string
	}{
		{name: "pre-upgrade txid restores", outcome: "ok", wantStatus: "passed", wantSummary: "pre-upgrade txid 0000000000000010 restored"},
		{name: "retention removed the target", outcome: "lost", wantStatus: "aborted", wantSummary: "verification inconclusive"},
		{name: "restore error", outcome: "error", wantStatus: "failed", wantSummary: "replica unreachable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _ := restoreModeFixture(t, fakePreUpgradeRestoreScript)
			t.Setenv("SOURCE_PATH", cfg.DBPath)
			t.Setenv("PRE_UPGRADE_OUTCOME", tt.outcome)
			cfg.upgradePreTXID = 0x10
			cfg.VerifyRestoreRotation = false
			startBoundarySyncFixture(t, &cfg, 0x42)
			v := NewVerifier(cfg, &fakePauser{})

			main, err := v.RunCycle(context.Background())
			if err != nil || !main.Passed || main.Status != "passed" {
				t.Fatalf("main cycle = %+v, %v; the historical restore must not touch it", main, err)
			}
			for _, step := range main.Steps {
				if strings.HasPrefix(step.Name, "restore_pre_upgrade") || strings.HasPrefix(step.Name, "integrity_check_pre_upgrade") {
					t.Fatalf("historical restore leaked into the main cycle: %+v", step)
				}
			}
			extras := v.takeExtraResults()
			if len(extras) != 1 {
				t.Fatalf("extra results = %+v, want one pre-upgrade restore", extras)
			}
			historical := extras[0]
			if historical.CheckType != preUpgradeRestoreCheckType || historical.RestoreMode != "txid" || historical.BoundaryTXID != 0x10 {
				t.Fatalf("historical result identity = %+v", historical)
			}
			if historical.Status != tt.wantStatus || historical.Passed != (tt.wantStatus == "passed") || !strings.Contains(historical.Summary, tt.wantSummary) {
				t.Fatalf("historical result = status %q passed %v summary %q error %q", historical.Status, historical.Passed, historical.Summary, historical.ErrorMessage)
			}
			if stepStatus(historical, "restore_pre_upgrade_txid") == "missing" {
				t.Fatalf("historical result steps = %+v", historical.Steps)
			}
			if tt.wantStatus == "passed" && stepStatus(historical, "integrity_check_pre_upgrade") != "ok" {
				t.Fatalf("integrity step = %+v", historical.Steps)
			}
			if _, err := os.Stat(cfg.DBPath + ".restored.pre"); !os.IsNotExist(err) {
				t.Fatalf("pre-upgrade restore artifact left behind: %v", err)
			}
			if again := v.takeExtraResults(); len(again) != 0 {
				t.Fatalf("extra results not cleared: %+v", again)
			}
		})
	}
}

func TestUpgradeLaneStateIsWrittenDurably(t *testing.T) {
	dir := t.TempDir()
	state := upgradeLaneState{Phase: UpgradePhasePost, BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA}
	if err := writeUpgradeLaneState(dir, state); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(upgradeLaneStatePath(dir) + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary marker left behind: %v", err)
	}
	stored, err := readUpgradeLaneState(dir)
	if err != nil || stored.Phase != UpgradePhasePost || stored.BaseSHA != testUpgradeBaseSHA {
		t.Fatalf("state = %+v, %v", stored, err)
	}
	if err := writeFileDurable(upgradeLaneStatePath(dir)+".tmp", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := writeUpgradeLaneState(dir, state); err != nil {
		t.Fatalf("rewrite over a stale temp file: %v", err)
	}
}
