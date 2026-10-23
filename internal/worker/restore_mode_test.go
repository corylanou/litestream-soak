package worker

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestVerifierRestoreModeRotation(t *testing.T) {
	starts := make(map[string]bool)
	for _, profile := range []string{"low-volume", "high-volume", "many-dbs-100-dir", "gharchive", "fts"} {
		var first []string
		for _, source := range []string{"worker-main-" + profile, "worker-pr-1585-" + profile} {
			cfg := DefaultConfig()
			cfg.ProfileName = profile
			cfg.WorkerID = source
			cfg.WorkerName = source
			v := NewVerifier(cfg)
			var got []string
			for range 6 {
				got = append(got, v.nextRestoreMode())
			}
			for i, mode := range got {
				if want := restoreModeRotation[(restoreModeOffset(profile)+i)%3]; mode != want {
					t.Fatalf("%s cycle %d mode = %q, want %q (%v)", source, i, mode, want, got)
				}
			}
			if first == nil {
				first = got
			} else if !slices.Equal(got, first) {
				t.Fatalf("%s modes %v differ from paired source %v", source, got, first)
			}
		}
		starts[first[0]] = true
	}
	if len(starts) < 2 {
		t.Fatalf("every profile starts on the same mode: %v", starts)
	}
}

func writeLocalL0(t *testing.T, sourcePath string, minTXID, maxTXID uint64, timestamp time.Time) {
	t.Helper()
	dir := filepath.Join(litestreamStateDir(sourcePath), "ltx", "0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 100)
	copy(header, "LTX1")
	binary.BigEndian.PutUint32(header[8:], 4096)
	binary.BigEndian.PutUint64(header[16:], minTXID)
	binary.BigEndian.PutUint64(header[24:], maxTXID)
	binary.BigEndian.PutUint64(header[32:], uint64(timestamp.UnixMilli()))
	name := fmt.Sprintf("%016x-%016x.ltx", minTXID, maxTXID)
	if err := os.WriteFile(filepath.Join(dir, name), header, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestBoundaryLTXTimestamp(t *testing.T) {
	base := time.Date(2026, 10, 23, 12, 0, 0, 123_000_000, time.UTC)
	tests := []struct {
		name   string
		files  [][2]uint64
		times  []time.Time
		txid   uint64
		want   time.Time
		wantOK bool
	}{
		{name: "exact boundary", files: [][2]uint64{{1, 9}, {10, 10}, {11, 12}}, times: []time.Time{base.Add(-time.Second), base, base.Add(time.Second)}, txid: 10, want: base, wantOK: true},
		{name: "no file ends at boundary", files: [][2]uint64{{1, 9}, {10, 12}}, times: []time.Time{base.Add(-time.Second), base}, txid: 10},
		{name: "later file shares boundary millisecond", files: [][2]uint64{{10, 10}, {11, 11}}, times: []time.Time{base, base}, txid: 10},
		{name: "missing local state", txid: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourcePath := filepath.Join(t.TempDir(), "test.db")
			for i, file := range tt.files {
				writeLocalL0(t, sourcePath, file[0], file[1], tt.times[i])
			}
			got, ok := boundaryLTXTimestamp(sourcePath, tt.txid)
			if ok != tt.wantOK || !got.Equal(tt.want) {
				t.Fatalf("boundaryLTXTimestamp() = %v, %v; want %v, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestBoundaryLTXTimestampRejectsMismatchedHeader(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "test.db")
	writeLocalL0(t, sourcePath, 1, 9, time.Now())
	dir := filepath.Join(litestreamStateDir(sourcePath), "ltx", "0")
	if err := os.Rename(filepath.Join(dir, "0000000000000001-0000000000000009.ltx"), filepath.Join(dir, "0000000000000001-000000000000000a.ltx")); err != nil {
		t.Fatal(err)
	}
	if _, ok := boundaryLTXTimestamp(sourcePath, 10); ok {
		t.Fatal("accepted header that does not match the filename")
	}
}

func TestRestoreArgsPerMode(t *testing.T) {
	timestamp := time.Date(2026, 10, 23, 12, 0, 0, 123_000_001, time.UTC)
	tests := []struct {
		mode string
		want []string
	}{
		{mode: "txid", want: []string{"restore", "-config", "c.yml", "-txid", "000000000000002a", "-o", "out.db", "src.db"}},
		{mode: "timestamp", want: []string{"restore", "-config", "c.yml", "-timestamp", "2026-10-23T12:00:00.123000001Z", "-o", "out.db", "src.db"}},
		{mode: "latest", want: []string{"restore", "-config", "c.yml", "-o", "out.db", "src.db"}},
	}
	for _, tt := range tests {
		if got := restoreArgs("c.yml", tt.mode, 42, timestamp, "out.db", "src.db"); !slices.Equal(got, tt.want) {
			t.Fatalf("%s args = %v, want %v", tt.mode, got, tt.want)
		}
	}
	parsed, err := time.Parse(time.RFC3339, restoreArgs("c.yml", "timestamp", 42, timestamp, "out.db", "src.db")[4])
	if err != nil || !parsed.Equal(timestamp) {
		t.Fatalf("Litestream RFC3339 parse = %v, %v; want %v", parsed, err, timestamp)
	}
}

func TestRestoreOutputTracksLastPlanTXID(t *testing.T) {
	var output restoreOutput
	for _, line := range []string{
		`time=2026-10-23T12:00:00Z level=DEBUG msg="restore plan" db=test.db n=3 txid=0000000000000010 timestamp=2026-10-23T12:00:00Z`,
		`time=2026-10-23T12:00:01Z level=DEBUG msg="restore file disappeared, recalculating plan" target=0000000000000010 attempt=1`,
		`{"time":"2026-10-23T12:00:02Z","level":"DEBUG","msg":"restore plan","n":2,"txid":"0000000000000012"}`,
		`time=2026-10-23T12:00:03Z level=ERROR msg="restore plan retries exhausted for txid 0000000000000099"`,
	} {
		_, _ = output.Write([]byte(line + "\n"))
	}
	if output.planTXID != 0x12 {
		t.Fatalf("planTXID = %016x, want 0000000000000012", output.planTXID)
	}
}

func restoreModeFixture(t *testing.T, script string) (Config, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake binary requires Unix")
	}
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args")
	writeFakePinnedRestore(t, dir, `printf '%s\n' "$@" > "$LITESTREAM_TEST_ARGS"
`+script)
	t.Setenv("LITESTREAM_TEST_ARGS", argsPath)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	logicalTestDB(t, cfg.DBPath, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	logicalTestDB(t, cfg.DBPath+".restored", "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	if err := os.WriteFile(cfg.ConfigPath, []byte("dbs: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return cfg, argsPath
}

func TestVerifierTimestampModeRestoresAtBoundaryHeaderTime(t *testing.T) {
	cfg, argsPath := restoreModeFixture(t, "exit 0\n")
	boundary := time.Date(2026, 10, 23, 12, 0, 0, 123_000_000, time.UTC)
	writeLocalL0(t, cfg.DBPath, 0x40, 0x42, boundary)
	startBoundarySyncFixture(t, &cfg, 0x42)
	v := NewVerifier(cfg)
	result := VerificationResult{SyncTXID: 0x42, RestoreMode: "timestamp"}
	passed, err := v.validateDBResult(context.Background(), cfg.DBPath, cfg.DBPath+".restored", &result)
	if err != nil || !passed {
		t.Fatalf("validateDBResult() = %v, %v", passed, err)
	}
	args := readLines(t, argsPath)
	if slices.Contains(args, "-txid") {
		t.Fatalf("timestamp restore passed -txid: %v", args)
	}
	assertContains(t, args, "-timestamp")
	assertContains(t, args, boundary.Add(time.Nanosecond).Format(time.RFC3339Nano))
	if result.RestoreMode != "timestamp" || result.RestoreModeFallback != "" {
		t.Fatalf("restore mode = %q fallback %q", result.RestoreMode, result.RestoreModeFallback)
	}
	if !strings.Contains(v.logicalEvidence, "restore_mode=timestamp") {
		t.Fatalf("evidence missing restore mode: %s", v.logicalEvidence)
	}
}

func TestVerifierTimestampModeFallsBackToTXIDWithoutLTXBoundary(t *testing.T) {
	cfg, argsPath := restoreModeFixture(t, "exit 0\n")
	writeLocalL0(t, cfg.DBPath, 0x40, 0x43, time.Now())
	startBoundarySyncFixture(t, &cfg, 0x42)
	v := NewVerifier(cfg)
	result := VerificationResult{SyncTXID: 0x42, RestoreMode: "timestamp"}
	passed, err := v.validateDBResult(context.Background(), cfg.DBPath, cfg.DBPath+".restored", &result)
	if err != nil || !passed {
		t.Fatalf("validateDBResult() = %v, %v", passed, err)
	}
	args := readLines(t, argsPath)
	assertContains(t, args, "-txid")
	assertContains(t, args, "0000000000000042")
	if slices.Contains(args, "-timestamp") {
		t.Fatalf("fallback restore passed -timestamp: %v", args)
	}
	if result.RestoreMode != "txid" || result.RestoreModeFallback != restoreModeFallbackNoLTXBoundary {
		t.Fatalf("restore mode = %q fallback %q", result.RestoreMode, result.RestoreModeFallback)
	}
	if !strings.Contains(v.logicalEvidence, "restore_mode_fallback="+restoreModeFallbackNoLTXBoundary) {
		t.Fatalf("evidence missing fallback: %s", v.logicalEvidence)
	}
}

func TestVerifierLatestMode(t *testing.T) {
	plan := func(txid string) string {
		return fmt.Sprintf("echo 'time=2026-10-23T12:00:00Z level=DEBUG msg=\"restore plan\" n=2 txid=%s'\nexit 0\n", txid)
	}
	tests := []struct {
		name      string
		script    string
		corrupt   bool
		wantPass  bool
		signature string
	}{
		{name: "restored at boundary", script: plan("0000000000000042"), wantPass: true},
		{name: "restored past boundary", script: plan("0000000000000050"), wantPass: true},
		{name: "restored behind boundary", script: plan("0000000000000041"), signature: "latest_restore_behind_boundary"},
		{name: "restored txid unknown", script: "exit 0\n", signature: "latest_restore_txid_unavailable"},
		{name: "integrity failure", script: plan("0000000000000050"), corrupt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, argsPath := restoreModeFixture(t, tt.script)
			if tt.corrupt {
				if err := os.WriteFile(cfg.DBPath+".restored", []byte("not a database"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			logicalTestDB(t, cfg.DBPath, "INSERT INTO t VALUES (1)")
			startBoundarySyncFixture(t, &cfg, 0x42)
			v := NewVerifier(cfg)
			result := VerificationResult{SyncTXID: 0x42, RestoreMode: "latest"}
			passed, err := v.validateDBResult(context.Background(), cfg.DBPath, cfg.DBPath+".restored", &result)
			args := readLines(t, argsPath)
			if slices.Contains(args, "-txid") || slices.Contains(args, "-timestamp") {
				t.Fatalf("latest restore passed a target: %v", args)
			}
			if result.RestoreMode != "latest" {
				t.Fatalf("restore mode = %q", result.RestoreMode)
			}
			if tt.wantPass {
				if err != nil || !passed {
					t.Fatalf("validateDBResult() = %v, %v", passed, err)
				}
				return
			}
			if err == nil || passed {
				t.Fatalf("validateDBResult() = %v, %v; want failure", passed, err)
			}
			if tt.signature != "" {
				if got := reporting.ClassifyVerificationFailure("restore_validate", err.Error()); got.Signature != tt.signature || got.Stage != "restore" {
					t.Fatalf("classification = %+v, want restore/%s", got, tt.signature)
				}
			}
		})
	}
}

func TestRunCycleRecordsRestoreMode(t *testing.T) {
	cfg, _ := restoreModeFixture(t, `
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    cp "$SOURCE_PATH" "$2"
  fi
  shift
done
echo 'level=DEBUG msg="restore plan" n=1 txid=0000000000000042'
`)
	t.Setenv("SOURCE_PATH", cfg.DBPath)
	startBoundarySyncFixture(t, &cfg, 0x42)
	v := NewVerifier(cfg, &fakePauser{})
	for i := range restoreModeRotation {
		want := restoreModeRotation[(restoreModeOffset(cfg.ProfileName)+i)%len(restoreModeRotation)]
		result, err := v.RunCycle(context.Background())
		if err != nil || !result.Passed {
			t.Fatalf("%s cycle: %+v %v", want, result, err)
		}
		wantMode, wantFallback := want, ""
		if want == "timestamp" {
			wantMode, wantFallback = "txid", restoreModeFallbackNoLTXBoundary
		}
		if result.RestoreMode != wantMode || result.RestoreModeFallback != wantFallback {
			t.Fatalf("%s cycle mode = %q fallback %q", want, result.RestoreMode, result.RestoreModeFallback)
		}
	}
}

func TestRunnerFailureClassificationIncludesRestoreMode(t *testing.T) {
	runner := NewRunner(DefaultConfig())
	classification := runner.failureClassification(VerificationResult{
		Status:       "failed",
		CheckType:    "restore_validate",
		ErrorMessage: "restore failed: latest restore behind boundary: restored=0000000000000041 boundary=0000000000000042",
		RestoreMode:  "latest",
	})
	if classification == nil || classification.RestoreMode != "latest" || classification.Signature != "latest_restore_behind_boundary" {
		t.Fatalf("classification = %+v", classification)
	}
}
