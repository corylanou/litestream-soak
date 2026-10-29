package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

// fakeFollowScript stands in for `litestream restore -f`: it rejects a pinned
// txid the way current Litestream does unless FOLLOW_PINNED_OK=1, copies the
// source into the output path, writes the txid sidecar, then idles until it is
// interrupted (or exits at once when FOLLOW_EXIT_AFTER_RESTORE=1).
const fakeFollowScript = `
shift
out=""
pinned=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift ;;
    -txid) pinned=1 ;;
  esac
  shift
done
if [ "$pinned" = 1 ] && [ "${FOLLOW_PINNED_OK:-0}" != 1 ]; then
  echo "${FOLLOW_PINNED_MESSAGE:-cannot use follow mode with -txid}" >&2
  exit 1
fi
cp "$FOLLOW_SOURCE" "$out"
printf '%s\n' "$FOLLOW_TXID" > "$out-txid"
if [ "${FOLLOW_EXIT_AFTER_RESTORE:-0}" = 1 ]; then
  exit 0
fi
trap 'kill "$child" 2>/dev/null; exit 0' INT TERM
while :; do
  sleep 0.2 &
  child=$!
  wait "$child"
done
`

type followFixture struct {
	cfg Config
	v   *Verifier
}

func newFollowFixture(t *testing.T) *followFixture {
	t.Helper()
	cfg, _ := restoreModeFixture(t, fakeFollowScript)
	cfg.FollowCheck = true
	logicalTestDB(t, cfg.DBPath, "INSERT INTO t VALUES (1), (2)")
	t.Setenv("FOLLOW_SOURCE", cfg.DBPath)
	t.Setenv("FOLLOW_TXID", "0000000000000042")
	v := NewVerifier(cfg)
	t.Cleanup(v.stopFollow)
	return &followFixture{cfg: cfg, v: v}
}

func (f *followFixture) cycle(t *testing.T, l9MaxTXID uint64) VerificationResult {
	t.Helper()
	f.v.replicaLevels = func(Config, uint64) *reporting.ReplicaLevelReport {
		return levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 3, MaxTXIDDecimal: 0x42}, 9: {ObjectCount: 1, MaxTXIDDecimal: l9MaxTXID}})
	}
	snapshot, err := f.v.logicalValidation(context.Background(), f.cfg.DBPath, 0x42)
	if err != nil {
		t.Fatalf("logicalValidation() error = %v", err)
	}
	return f.v.runFollowCheck(context.Background(), verificationBoundary{logicalSnapshot: snapshot, txid: 0x42})
}

func stepStatus(result VerificationResult, name string) string {
	for _, step := range result.Steps {
		if step.Name == name {
			return step.Status
		}
	}
	return "missing"
}

func TestFollowCheckAcrossL9Boundary(t *testing.T) {
	type cycle struct {
		l9           uint64
		wantStatus   string
		wantSummary  string
		wantSteps    map[string]string
		wantFallback string
	}
	tests := []struct {
		name   string
		env    map[string]string
		cycles []cycle
	}{
		{
			name: "pinned unsupported, passes once L9 advances",
			cycles: []cycle{
				{l9: 100, wantStatus: "pending", wantSummary: "awaiting a new L9 snapshot", wantFallback: followPinnedUnsupportedFallback,
					wantSteps: map[string]string{"follow_pinned": followStepUnsupported, "follow_start": "ok", "follow_initial": "ok", "follow_compare": "ok"}},
				{l9: 100, wantStatus: "pending", wantSummary: "no new L9 snapshot since follow start",
					wantSteps: map[string]string{"follow_catch_up": "ok", "follow_compare": "ok", "follow_l9": "pending"}},
				{l9: 200, wantStatus: "passed", wantSummary: "after L9 snapshot advanced from 0000000000000064 to 00000000000000c8",
					wantSteps: map[string]string{"follow_catch_up": "ok", "follow_compare": "ok", "follow_l9": "ok"}},
				{l9: 200, wantStatus: "pending", wantSummary: "no new L9 snapshot since last pass"},
				{l9: 260, wantStatus: "passed", wantSummary: "advanced from 00000000000000c8 to 0000000000000104"},
			},
		},
		{
			name: "pinned follow supported",
			env:  map[string]string{"FOLLOW_PINNED_OK": "1"},
			cycles: []cycle{
				{l9: 100, wantStatus: "pending", wantSummary: "pinned-txid follow matched source at txid 0000000000000042", wantSteps: map[string]string{"follow_pinned": "ok"}},
			},
		},
		{
			name: "pinned follow rejected for another reason fails",
			env:  map[string]string{"FOLLOW_PINNED_MESSAGE": "replica unavailable"},
			cycles: []cycle{
				{l9: 100, wantStatus: "failed", wantSummary: "pinned follow restore", wantSteps: map[string]string{"follow_pinned": "error"}},
			},
		},
		{
			name: "follow process exit fails the next cycle",
			env:  map[string]string{"FOLLOW_EXIT_AFTER_RESTORE": "1"},
			cycles: []cycle{
				{l9: 100, wantStatus: "pending", wantFallback: followPinnedUnsupportedFallback},
				{l9: 200, wantStatus: "failed", wantSummary: "follow restore exited", wantSteps: map[string]string{"follow_process": "error"}},
				{l9: 200, wantStatus: "pending", wantSummary: "awaiting a new L9 snapshot", wantFallback: followPinnedUnsupportedFallback},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newFollowFixture(t)
			for key, value := range tt.env {
				t.Setenv(key, value)
			}
			for i, want := range tt.cycles {
				result := fixture.cycle(t, want.l9)
				if result.Status != want.wantStatus || result.CheckType != followCheckType || result.RestoreMode != followRestoreMode || result.BoundaryTXID != 0x42 {
					t.Fatalf("cycle %d = status %q type %q mode %q txid %#x summary %q error %q", i, result.Status, result.CheckType, result.RestoreMode, result.BoundaryTXID, result.Summary, result.ErrorMessage)
				}
				if result.Passed != (want.wantStatus == "passed") {
					t.Fatalf("cycle %d passed = %v for status %s", i, result.Passed, result.Status)
				}
				if want.wantSummary != "" && !strings.Contains(result.Summary, want.wantSummary) {
					t.Fatalf("cycle %d summary = %q, want %q", i, result.Summary, want.wantSummary)
				}
				if result.RestoreModeFallback != want.wantFallback {
					t.Fatalf("cycle %d fallback = %q, want %q", i, result.RestoreModeFallback, want.wantFallback)
				}
				for step, status := range want.wantSteps {
					if got := stepStatus(result, step); got != status {
						t.Fatalf("cycle %d step %s = %s, want %s (steps %+v)", i, step, got, status, result.Steps)
					}
				}
				if result.Status == "failed" && fixture.v.follow != nil {
					t.Fatalf("cycle %d failed but the follow session is still running", i)
				}
				if result.Status != "failed" && fixture.v.follow == nil {
					t.Fatalf("cycle %d left no follow session", i)
				}
			}
		})
	}
}

func TestFollowCheckDivergenceFailsAndRestarts(t *testing.T) {
	fixture := newFollowFixture(t)
	if result := fixture.cycle(t, 100); result.Status != "pending" {
		t.Fatalf("first cycle = %+v", result)
	}
	logicalTestDB(t, fixture.cfg.DBPath, "INSERT INTO t VALUES (3)")
	result := fixture.cycle(t, 200)
	if result.Status != "failed" || result.Passed || !strings.Contains(result.ErrorMessage, "follow restore diverged from source at txid 0000000000000042") {
		t.Fatalf("diverged cycle = %+v", result)
	}
	if fixture.v.follow != nil {
		t.Fatal("diverged follow session kept running")
	}
	if _, err := os.Stat(followDBPath(fixture.cfg.DBPath)); !os.IsNotExist(err) {
		t.Fatalf("follow artifacts not removed: %v", err)
	}
	if result := fixture.cycle(t, 200); result.Status != "pending" || stepStatus(result, "follow_start") != "ok" {
		t.Fatalf("restart cycle = %+v", result)
	}
}

func TestFollowArgsAndSidecar(t *testing.T) {
	tests := []struct {
		txid uint64
		want string
	}{
		{txid: 0, want: "restore -config c.yml -f -follow-interval 1s -o out.db src.db"},
		{txid: 0x42, want: "restore -config c.yml -f -follow-interval 1s -txid 0000000000000042 -o out.db src.db"},
	}
	for _, tt := range tests {
		if got := strings.Join(followArgs("c.yml", tt.txid, "out.db", "src.db"), " "); got != tt.want {
			t.Fatalf("followArgs(%#x) = %q, want %q", tt.txid, got, tt.want)
		}
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "follow.db")
	if txid, err := readFollowTXID(path); err != nil || txid != 0 {
		t.Fatalf("missing sidecar = %#x, %v", txid, err)
	}
	if err := os.WriteFile(followSidecarPath(path), []byte("00000000000000ff\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if txid, err := readFollowTXID(path); err != nil || txid != 0xff {
		t.Fatalf("sidecar = %#x, %v", txid, err)
	}
	if err := os.WriteFile(followSidecarPath(path), []byte("nope\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readFollowTXID(path); err == nil {
		t.Fatal("malformed sidecar accepted")
	}
	for _, tt := range []struct {
		before, after reporting.ReplicaLevelSummary
		want          bool
	}{
		{reporting.ReplicaLevelSummary{MaxTXIDDecimal: 10, ObjectCount: 1}, reporting.ReplicaLevelSummary{MaxTXIDDecimal: 10, ObjectCount: 1}, false},
		{reporting.ReplicaLevelSummary{MaxTXIDDecimal: 10, ObjectCount: 1}, reporting.ReplicaLevelSummary{MaxTXIDDecimal: 11, ObjectCount: 1}, true},
		{reporting.ReplicaLevelSummary{MaxTXIDDecimal: 10, ObjectCount: 1}, reporting.ReplicaLevelSummary{MaxTXIDDecimal: 10, ObjectCount: 2}, true},
		{reporting.ReplicaLevelSummary{MaxTXIDDecimal: 10, ObjectCount: 2}, reporting.ReplicaLevelSummary{MaxTXIDDecimal: 9, ObjectCount: 3}, false},
	} {
		if got := l9Advanced(tt.before, tt.after); got != tt.want {
			t.Fatalf("l9Advanced(%+v, %+v) = %v, want %v", tt.before, tt.after, got, tt.want)
		}
	}
}

func TestFollowSessionStopsWithinGrace(t *testing.T) {
	fixture := newFollowFixture(t)
	if result := fixture.cycle(t, 100); result.Status != "pending" {
		t.Fatalf("first cycle = %+v", result)
	}
	start := time.Now()
	fixture.v.stopFollow()
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("stopFollow took %s", elapsed)
	}
	if fixture.v.follow != nil {
		t.Fatal("follow session not cleared")
	}
}
