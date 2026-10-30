package worker

import (
	"context"
	"encoding/binary"
	"errors"
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
	interval := 30 * time.Minute
	base := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	starts := make(map[string]bool)
	for _, profile := range []string{"low-volume", "high-volume", "gate-stress", "gate-large-db", "gate-remote"} {
		var first []string
		for _, source := range []string{"worker-rc-aaaaaaaa-" + profile, "worker-rcbase-bbbbbbbb-" + profile} {
			cfg := DefaultConfig()
			cfg.ProfileName = profile
			cfg.WorkerID = source
			cfg.WorkerName = source
			cfg.VerifyInterval = interval
			cfg.VerifyRestoreRotation = true
			v := NewVerifier(cfg)
			var got []string
			for i := range 6 {
				at := base.Add(time.Duration(i)*interval + time.Duration(i+1)*time.Minute)
				got = append(got, v.restoreModeAt(at))
			}
			if first == nil {
				first = got
			} else if !slices.Equal(got, first) {
				t.Fatalf("%s modes %v differ from paired source %v", source, got, first)
			}
		}
		for i := 1; i < len(first); i++ {
			if slices.Index(restoreModeRotation, first[i]) != (slices.Index(restoreModeRotation, first[i-1])+1)%len(restoreModeRotation) {
				t.Fatalf("%s modes %v do not rotate per verify interval", profile, first)
			}
		}
		starts[first[0]] = true
	}
	if len(starts) < 2 {
		t.Fatalf("every profile starts on the same mode: %v", starts)
	}

	cfg := DefaultConfig()
	if cfg.VerifyRestoreRotation {
		t.Fatal("DefaultConfig enables restore rotation")
	}
	v := NewVerifier(cfg)
	for i := range 6 {
		if mode := v.restoreModeAt(base.Add(time.Duration(i) * cfg.VerifyInterval)); mode != "txid" {
			t.Fatalf("rotation disabled mode = %q", mode)
		}
	}
	env, err := configFromLookup(func(string) string { return "" })
	if err != nil || !env.VerifyRestoreRotation {
		t.Fatalf("worker environment rotation = %v, %v", env.VerifyRestoreRotation, err)
	}
	env, err = configFromLookup(func(key string) string {
		if key == "SOAK_VERIFY_RESTORE_ROTATION" {
			return "false"
		}
		return ""
	})
	if err != nil || env.VerifyRestoreRotation {
		t.Fatalf("disabled worker environment rotation = %v, %v", env.VerifyRestoreRotation, err)
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

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	data, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

const restoreFromScript = `
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    cp "$RESTORE_FROM" "$2"
  fi
  shift
done
`

func TestVerifierLatestMode(t *testing.T) {
	plan := func(txid string) string {
		return restoreFromScript + fmt.Sprintf("echo 'time=2026-10-23T12:00:00Z level=DEBUG msg=\"restore plan\" n=2 txid=%s'\nexit 0\n", txid)
	}
	tests := []struct {
		name         string
		script       string
		stale        bool
		corrupt      bool
		wantPass     bool
		inconclusive bool
		signature    string
	}{
		{name: "restored at boundary", script: plan("0000000000000042"), wantPass: true},
		{name: "restored past boundary", script: plan("0000000000000050"), wantPass: true},
		{name: "stale data with new plan", script: plan("0000000000000050"), stale: true, signature: "latest_restore_behind_boundary"},
		{name: "planned behind boundary", script: plan("0000000000000041"), signature: "latest_restore_behind_boundary"},
		{name: "plan line missing", script: restoreFromScript + "exit 0\n", inconclusive: true},
		{name: "stale data without plan line", script: restoreFromScript + "exit 0\n", stale: true, signature: "latest_restore_behind_boundary"},
		{name: "integrity failure", script: plan("0000000000000050"), corrupt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, argsPath := restoreModeFixture(t, tt.script)
			logicalTestDB(t, cfg.DBPath, "INSERT INTO t VALUES (1)")
			restoreFrom := cfg.DBPath
			if tt.stale {
				restoreFrom = filepath.Join(cfg.DataDir, "stale.db")
				copyFile(t, cfg.DBPath, restoreFrom)
			}
			if tt.corrupt {
				restoreFrom = filepath.Join(cfg.DataDir, "corrupt.db")
				if err := os.WriteFile(restoreFrom, []byte("not a database"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("RESTORE_FROM", restoreFrom)
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
			if !strings.Contains(v.logicalEvidence, "restore_marker=") {
				t.Fatalf("evidence missing marker: %s", v.logicalEvidence)
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
			v.failValidationResult(context.Background(), &result, err)
			if tt.inconclusive {
				if result.Status != "aborted" || !strings.HasPrefix(result.Summary, "verification inconclusive:") {
					t.Fatalf("status = %q summary %q, want inconclusive", result.Status, result.Summary)
				}
				return
			}
			if result.Status != "failed" {
				t.Fatalf("status = %q, want failed", result.Status)
			}
			if tt.signature != "" {
				if got := reporting.ClassifyVerificationFailure("restore_validate", err.Error()); got.Signature != tt.signature || got.Stage != "restore" {
					t.Fatalf("classification = %+v, want restore/%s", got, tt.signature)
				}
			}
		})
	}
}

func TestVerifierLatestModeFallsBackWhenMarkerUnwritable(t *testing.T) {
	cfg, argsPath := restoreModeFixture(t, "exit 0\n")
	logicalTestDB(t, cfg.DBPath, "CREATE TABLE _soak_restore_marker (x)")
	logicalTestDB(t, cfg.DBPath+".restored", "CREATE TABLE _soak_restore_marker (x)")
	startBoundarySyncFixture(t, &cfg, 0x42)
	v := NewVerifier(cfg)
	result := VerificationResult{SyncTXID: 0x42, RestoreMode: "latest"}
	passed, err := v.validateDBResult(context.Background(), cfg.DBPath, cfg.DBPath+".restored", &result)
	if err != nil || !passed {
		t.Fatalf("validateDBResult() = %v, %v", passed, err)
	}
	assertContains(t, readLines(t, argsPath), "-txid")
	if result.RestoreMode != "txid" || result.RestoreModeFallback != restoreModeFallbackNoMarker {
		t.Fatalf("restore mode = %q fallback %q", result.RestoreMode, result.RestoreModeFallback)
	}
}

func TestRestoreMarkerExcludedFromLogicalCompare(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	restored := filepath.Join(dir, "restored.db")
	logicalTestDB(t, source, "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1)")
	logicalTestDB(t, restored, "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1)")
	if err := writeRestoreMarker(context.Background(), source, 7, 0x42); err != nil {
		t.Fatal(err)
	}
	if err := CompareLogicalDatabases(context.Background(), source, restored); err != nil {
		t.Fatalf("marker table changed the logical compare: %v", err)
	}
	if err := checkRestoreMarker(context.Background(), source, 7); err != nil {
		t.Fatalf("checkRestoreMarker(source) = %v", err)
	}
	if err := checkRestoreMarker(context.Background(), restored, 7); err == nil {
		t.Fatal("checkRestoreMarker(restored) accepted a database without the marker")
	}
}

func TestRunCycleRecordsRestoreMode(t *testing.T) {
	cfg, _ := restoreModeFixture(t, `
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    cp "$SOURCE_PATH" "$2"
    if [ -f "$SOURCE_PATH-wal" ]; then
      cp "$SOURCE_PATH-wal" "$2-wal"
    fi
  fi
  shift
done
echo 'level=DEBUG msg="restore plan" n=1 txid=0000000000000042'
`)
	t.Setenv("SOURCE_PATH", cfg.DBPath)
	cfg.VerifyRestoreRotation = true
	startBoundarySyncFixture(t, &cfg, 0x42)
	v := NewVerifier(cfg, &fakePauser{})
	base := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	for i := range restoreModeRotation {
		at := base.Add(time.Duration(i) * cfg.VerifyInterval)
		v.clock = func() time.Time { return at }
		want := v.restoreModeAt(at)
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

func TestRunCycleRecordsRotationDisabled(t *testing.T) {
	cfg, args := restoreModeFixture(t, restoreFromScript)
	t.Setenv("RESTORE_FROM", cfg.DBPath)
	startBoundarySyncFixture(t, &cfg, 0x42)
	v := NewVerifier(cfg, &fakePauser{})
	result, err := v.RunCycle(context.Background())
	if err != nil || !result.Passed {
		t.Fatalf("cycle: %+v %v", result, err)
	}
	assertContains(t, readLines(t, args), "-txid")
	if result.RestoreMode != "txid" || result.RestoreModeFallback != restoreModeFallbackRotationDisabled {
		t.Fatalf("mode = %q fallback %q", result.RestoreMode, result.RestoreModeFallback)
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

const pinnedRestoreTargetUnavailableError = "Error: no matching backup files available: restore target txid 0000000000003d9d is no longer available: txid 0000000000003d9d unavailable (nearest covered TXIDs: before=0000000000003c48, after=0000000000003dcf): transaction not available"

func TestVerifierPinnedRestoreTargetUnavailableIsInconclusive(t *testing.T) {
	tests := []struct {
		name         string
		mode         string
		final        string
		inconclusive bool
	}{
		{name: "txid", mode: "txid", final: pinnedRestoreTargetUnavailableError, inconclusive: true},
		{name: "timestamp", mode: "timestamp", final: pinnedRestoreTargetUnavailableError, inconclusive: true},
		{name: "latest", mode: "latest", final: pinnedRestoreTargetUnavailableError},
		{name: "txid missing backup files", mode: "txid", final: "Error: no matching backup files available"},
		{name: "timestamp recalculate", mode: "timestamp", final: "Error: cannot recalculate restore plan"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := "echo 'time=2026-10-23T12:00:00Z level=WARN msg=\"restore file disappeared, recalculating plan\"'\necho '" + tt.final + "' >&2\nexit 1\n"
			cfg, _ := restoreModeFixture(t, script)
			writeLocalL0(t, cfg.DBPath, 0x40, 0x42, time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
			startBoundarySyncFixture(t, &cfg, 0x42)
			v := NewVerifier(cfg)
			result := VerificationResult{SyncTXID: 0x42, RestoreMode: tt.mode}
			passed, err := v.validateDBResult(context.Background(), cfg.DBPath, cfg.DBPath+".restored", &result)
			if err == nil || passed {
				t.Fatalf("validateDBResult() = %v, %v; want failure", passed, err)
			}
			if result.RestoreMode != tt.mode {
				t.Fatalf("restore mode = %q, want %q", result.RestoreMode, tt.mode)
			}
			v.failValidationResult(context.Background(), &result, err)
			if !strings.Contains(result.ErrorMessage, strings.TrimPrefix(tt.final, "Error: ")) {
				t.Fatalf("error message = %q", result.ErrorMessage)
			}
			if !tt.inconclusive {
				if result.Status != "failed" {
					t.Fatalf("status = %q, want failed", result.Status)
				}
				return
			}
			if result.Status != "aborted" || !strings.HasPrefix(result.Summary, "verification inconclusive:") {
				t.Fatalf("status = %q summary %q, want inconclusive", result.Status, result.Summary)
			}
			if !strings.Contains(v.logicalEvidence, "restore_target=unavailable") {
				t.Fatalf("evidence missing unavailable target: %s", v.logicalEvidence)
			}
			if classification := NewRunner(DefaultConfig()).failureClassification(result); classification != nil {
				t.Fatalf("inconclusive result classified as failure: %+v", classification)
			}
		})
	}
}

func TestVerifierRestoreCapacityIsHarnessInconclusive(t *testing.T) {
	const (
		restoreTmpENOSPC = "Error: decode database: write page 501132: write /data/scenarios/overload-truncate0/test.db.restored.tmp: no space left on device"
		walENOSPC        = "Error: write /data/scenarios/overload-truncate0/test.db-wal: no space left on device"
		stagingENOSPC    = "Error: disk full while staging ltx file: write /data/.test.db-litestream/ltx/0/0000000000000042.ltx: no space left on device"
	)
	tests := []struct {
		name         string
		freeBytes    uint64
		script       string
		wantStatus   string
		wantHarness  bool
		wantEvidence string
		wantRan      bool
	}{
		{name: "preflight skips the restore when space is short", freeBytes: 1 << 20, script: "exit 0\n", wantStatus: "aborted", wantHarness: true, wantEvidence: "restore_capacity=insufficient"},
		{name: "preflight passes with enough space", freeBytes: 1 << 40, script: "exit 0\n", wantStatus: "passed", wantRan: true},
		{name: "restore tmp enospc mid-write is harness inconclusive", freeBytes: 1 << 40, script: "echo '" + restoreTmpENOSPC + "' >&2\nexit 1\n", wantStatus: "aborted", wantHarness: true, wantEvidence: "restore_capacity=exhausted", wantRan: true},
		{name: "wal enospc stays a failure", freeBytes: 1 << 40, script: "echo '" + walENOSPC + "' >&2\nexit 1\n", wantStatus: "failed", wantRan: true},
		{name: "litestream staging enospc stays a failure", freeBytes: 1 << 40, script: "echo '" + stagingENOSPC + "' >&2\nexit 1\n", wantStatus: "failed", wantRan: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previous := restoreFreeBytes
			restoreFreeBytes = func(string) (uint64, error) { return tt.freeBytes, nil }
			t.Cleanup(func() { restoreFreeBytes = previous })
			cfg, argsPath := restoreModeFixture(t, tt.script)
			writeLocalL0(t, cfg.DBPath, 0x40, 0x42, time.Date(2026, 10, 30, 2, 36, 0, 0, time.UTC))
			startBoundarySyncFixture(t, &cfg, 0x42)
			v := NewVerifier(cfg)
			result := VerificationResult{SyncTXID: 0x42, RestoreMode: "txid"}
			passed, err := v.validateDBResult(context.Background(), cfg.DBPath, cfg.DBPath+".restored", &result)
			if _, statErr := os.Stat(argsPath); (statErr == nil) != tt.wantRan {
				t.Fatalf("restore ran = %v, want %v", statErr == nil, tt.wantRan)
			}
			if tt.wantStatus == "passed" {
				if err != nil || !passed {
					t.Fatalf("validateDBResult() = %v, %v; want pass", passed, err)
				}
				return
			}
			if err == nil || passed {
				t.Fatalf("validateDBResult() = %v, %v; want failure", passed, err)
			}
			v.failValidationResult(context.Background(), &result, err)
			if result.Status != tt.wantStatus {
				t.Fatalf("status = %q, want %q (%s)", result.Status, tt.wantStatus, result.ErrorMessage)
			}
			classification := NewRunner(DefaultConfig()).failureClassification(result)
			if !tt.wantHarness {
				if classification == nil || classification.Signature != "disk_capacity_full" {
					t.Fatalf("classification = %+v, want disk_capacity_full failure", classification)
				}
				return
			}
			if !strings.HasPrefix(result.Summary, "verification inconclusive:") || !strings.Contains(v.logicalEvidence, tt.wantEvidence) {
				t.Fatalf("summary %q evidence %q, want inconclusive with %s", result.Summary, v.logicalEvidence, tt.wantEvidence)
			}
			if classification == nil || classification.Stage != reporting.StageHarnessCapacity || classification.Signature != reporting.SignatureHarnessCapacityInconclusive || classification.RestoreMode != "txid" {
				t.Fatalf("classification = %+v, want harness capacity", classification)
			}
			var capacity *restoreCapacityInconclusiveError
			if !errors.As(err, &capacity) || capacity.neededBytes == 0 {
				t.Fatalf("error %v does not carry the capacity estimate", err)
			}
			if tt.wantEvidence == "restore_capacity=insufficient" && (capacity.freeBytes != tt.freeBytes || !strings.Contains(result.ErrorMessage, "bytes are free")) {
				t.Fatalf("preflight error = %q free=%d", result.ErrorMessage, capacity.freeBytes)
			}
		})
	}
}

func TestVerifierRestoreRequestCanceledIsInconclusive(t *testing.T) {
	tests := []struct {
		name         string
		mode         string
		final        string
		inconclusive bool
	}{
		{name: "list request canceled", mode: "txid", final: "Error: cannot calc restore plan: operation error S3: ListObjectsV2, exceeded maximum number of attempts, 10, https response error StatusCode: 408, RequestID: 1761761212849403, HostID: , api error RequestCanceled: Request is canceled.", inconclusive: true},
		{name: "get request canceled latest", mode: "latest", final: "Error: operation error S3: GetObject, exceeded maximum number of attempts, 10, https response error StatusCode: 408, RequestID: 1761761212849404, HostID: , api error RequestCanceled: Request is canceled.", inconclusive: true},
		{name: "access denied", mode: "txid", final: "Error: cannot calc restore plan: operation error S3: ListObjectsV2, https response error StatusCode: 403, RequestID: 2, HostID: , api error AccessDenied: Access Denied"},
		{name: "server error", mode: "txid", final: "Error: cannot calc restore plan: operation error S3: ListObjectsV2, https response error StatusCode: 500, RequestID: 3, HostID: , api error InternalError: We encountered an internal error."},
		{name: "decode error", mode: "timestamp", final: "Error: read page header: unexpected EOF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := "echo 'time=2026-10-29T18:00:00Z level=WARN msg=\"retrying request\"'\necho '" + tt.final + "' >&2\nexit 1\n"
			cfg, _ := restoreModeFixture(t, script)
			writeLocalL0(t, cfg.DBPath, 0x40, 0x42, time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC))
			startBoundarySyncFixture(t, &cfg, 0x42)
			v := NewVerifier(cfg)
			result := VerificationResult{SyncTXID: 0x42, RestoreMode: tt.mode}
			passed, err := v.validateDBResult(context.Background(), cfg.DBPath, cfg.DBPath+".restored", &result)
			if err == nil || passed {
				t.Fatalf("validateDBResult() = %v, %v; want failure", passed, err)
			}
			v.failValidationResult(context.Background(), &result, err)
			if !strings.Contains(result.ErrorMessage, strings.TrimPrefix(tt.final, "Error: ")) {
				t.Fatalf("error message = %q", result.ErrorMessage)
			}
			if !tt.inconclusive {
				if result.Status != "failed" {
					t.Fatalf("status = %q, want failed", result.Status)
				}
				if classification := NewRunner(DefaultConfig()).failureClassification(result); classification == nil {
					t.Fatal("failed result lost its classification")
				}
				return
			}
			if result.Status != "aborted" || !strings.HasPrefix(result.Summary, "verification inconclusive:") {
				t.Fatalf("status = %q summary %q, want inconclusive", result.Status, result.Summary)
			}
			if !strings.Contains(v.logicalEvidence, "object_store=request_canceled") {
				t.Fatalf("evidence missing canceled request: %s", v.logicalEvidence)
			}
			if classification := NewRunner(DefaultConfig()).failureClassification(result); classification != nil {
				t.Fatalf("inconclusive result classified as failure: %+v", classification)
			}
		})
	}
}
