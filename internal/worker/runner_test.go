package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestPollDBStatsMarksSnapshotHealthy(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-%d.sock", time.Now().UnixNano()))

	if err := os.WriteFile(cfg.DBPath, []byte("1234567890"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.DBPath+"-wal", []byte("12345"), 0o644); err != nil {
		t.Fatal(err)
	}

	SetWorkerInfo(cfg)
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/txid":
			_, _ = w.Write([]byte(`{"txid":42}`))
		case "/info":
			_, _ = w.Write([]byte(`{"uptime_seconds":99}`))
		case "/list":
			lastSyncAt := time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
			_, _ = w.Write([]byte(`{"databases":[{"status":"replicating","last_sync_at":"` + lastSyncAt + `"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))

	runner := NewRunner(cfg)
	runner.pollDBStats()

	snapshot := runner.currentSnapshot()
	if !snapshot.LitestreamSnapshotHealthy {
		t.Fatalf("expected healthy snapshot, got error %q", snapshot.LitestreamSnapshotError)
	}
	if snapshot.DBTXID != 42 {
		t.Fatalf("db txid=%d want 42", snapshot.DBTXID)
	}
	if snapshot.DBStatus != "replicating" {
		t.Fatalf("db status=%q want %q", snapshot.DBStatus, "replicating")
	}
	if snapshot.LitestreamUptimeSeconds != 99 {
		t.Fatalf("litestream uptime=%v want 99", snapshot.LitestreamUptimeSeconds)
	}
	if snapshot.LastSyncAgeSeconds <= 0 {
		t.Fatalf("last sync age=%v want > 0", snapshot.LastSyncAgeSeconds)
	}
	if snapshot.DBSizeBytes != 10 {
		t.Fatalf("db size=%d want 10", snapshot.DBSizeBytes)
	}
	if snapshot.WALSizeBytes != 5 {
		t.Fatalf("wal size=%d want 5", snapshot.WALSizeBytes)
	}
	if snapshot.DataDiskTotalBytes == 0 {
		t.Fatal("expected data disk total bytes")
	}
	if snapshot.DataDiskUsedBytes == 0 {
		t.Fatal("expected data disk used bytes")
	}
	if snapshot.DataDiskUsedPercent == 0 {
		t.Fatal("expected data disk used percent")
	}
	if snapshot.SnapshotCollectedAt.IsZero() {
		t.Fatal("expected snapshot collected timestamp")
	}
	if snapshot.LitestreamSnapshotError != "" {
		t.Fatalf("unexpected snapshot error %q", snapshot.LitestreamSnapshotError)
	}
}

func TestPollDBStatsClosesIPCConnections(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-ipc-cleanup"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-%d.sock", time.Now().UnixNano()))

	if err := os.WriteFile(cfg.DBPath, []byte("1234567890"), 0o644); err != nil {
		t.Fatal(err)
	}

	tracker := startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/txid":
			_, _ = w.Write([]byte(`{"txid":42}`))
		case "/info":
			_, _ = w.Write([]byte(`{"uptime_seconds":99}`))
		case "/list":
			lastSyncAt := time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
			_, _ = w.Write([]byte(`{"databases":[{"status":"replicating","last_sync_at":"` + lastSyncAt + `"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))

	runner := NewRunner(cfg)
	runner.pollDBStats()

	if !waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
		return tracker.Active() == 0
	}) {
		t.Fatalf("active IPC connections=%d want 0", tracker.Active())
	}
}

func TestVerifierWaitForSyncClosesIPCConnection(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-sync-%d.sock", time.Now().UnixNano()))

	tracker := startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			_, _ = w.Write([]byte(`{"status":"synced","txid":11,"replicated_txid":11}`))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	result := VerificationResult{}
	if err := verifier.waitForSync(context.Background(), &result); err != nil {
		t.Fatalf("waitForSync() error = %v", err)
	}
	if result.SyncStatusBeforeSync == nil {
		t.Fatal("expected sync status before sync")
	}
	if result.SyncTXID != 11 {
		t.Fatalf("sync txid=%d want 11", result.SyncTXID)
	}
	if result.SyncReplicatedTXID != 11 {
		t.Fatalf("sync replicated txid=%d want 11", result.SyncReplicatedTXID)
	}

	if !waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
		return tracker.Active() == 0
	}) {
		t.Fatalf("active IPC connections=%d want 0", tracker.Active())
	}
}

func TestVerifierWaitForSyncUsesConfiguredTimeout(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-sync-timeout-%d.sock", time.Now().UnixNano()))
	cfg.VerifySyncTimeout = 7*time.Minute + 500*time.Millisecond

	var timeoutValue float64
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode request: %v", err)
			}
			if value, ok := body["timeout"].(float64); ok {
				timeoutValue = value
			}
			_, _ = w.Write([]byte(`{"status":"synced","txid":1,"replicated_txid":1}`))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	if err := verifier.waitForSync(context.Background(), &VerificationResult{}); err != nil {
		t.Fatalf("waitForSync() error = %v", err)
	}
	if timeoutValue != 421 {
		t.Fatalf("sync timeout=%v want 421", timeoutValue)
	}
}

func TestVerifierValidatePassesPinnedTXID(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake binary requires Unix")
	}

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args")
	restorePath := filepath.Join(dir, "test.db.restored")
	writeFakeLitestreamTest(t, dir, `
if [ "$1" = "validate" ]; then
  shift
fi
printf '%s\n' "$@" > "$LITESTREAM_TEST_ARGS"
exit 0
`)
	t.Setenv("LITESTREAM_TEST_ARGS", argsPath)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	if err := os.WriteFile(cfg.DBPath, []byte("db"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.ConfigPath, []byte("dbs: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	verifier := NewVerifier(cfg)
	passed, err := verifier.validate(context.Background(), 0x224b6)
	if err != nil {
		t.Fatalf("validate() error = %v", err)
	}
	if !passed {
		t.Fatal("validate() passed=false")
	}
	args := readLines(t, argsPath)
	assertContains(t, args, "-txid")
	assertContains(t, args, "00000000000224b6")
	assertContains(t, args, "-restored-db")
	assertContains(t, args, restorePath)
}

func TestVerifierValidateRetriesWithoutPinnedTXIDWhenUnsupported(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake binary requires Unix")
	}

	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	argsPath := filepath.Join(dir, "args")
	writeFakeLitestreamTest(t, dir, `
count=0
if [ -f "$LITESTREAM_TEST_COUNT" ]; then
  count=$(cat "$LITESTREAM_TEST_COUNT")
fi
count=$((count + 1))
printf '%s' "$count" > "$LITESTREAM_TEST_COUNT"
if [ "$count" = "1" ]; then
  echo "flag provided but not defined: -txid" >&2
  exit 2
fi
if [ "$1" = "validate" ]; then
  shift
fi
printf '%s\n' "$@" > "$LITESTREAM_TEST_ARGS"
exit 0
`)
	t.Setenv("LITESTREAM_TEST_COUNT", countPath)
	t.Setenv("LITESTREAM_TEST_ARGS", argsPath)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	if err := os.WriteFile(cfg.DBPath, []byte("db"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.ConfigPath, []byte("dbs: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	verifier := NewVerifier(cfg)
	passed, err := verifier.validate(context.Background(), 0x224b6)
	if err != nil {
		t.Fatalf("validate() error = %v", err)
	}
	if !passed {
		t.Fatal("validate() passed=false")
	}
	if got := strings.TrimSpace(readFile(t, countPath)); got != "2" {
		t.Fatalf("validate executions=%q want 2", got)
	}
	args := readLines(t, argsPath)
	for _, arg := range args {
		if arg == "-txid" {
			t.Fatalf("fallback args unexpectedly include -txid: %v", args)
		}
	}
}

func TestVerifierWaitForSyncCapturesSyncFailureDiagnostics(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-sync-fail-%d.sock", time.Now().UnixNano()))
	cfg.WorkerID = "worker-test"
	cfg.WorkerName = "worker-test"
	cfg.ProfileName = "high-volume"
	if err := os.WriteFile(cfg.DBPath, []byte("db"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.ConfigPath, []byte("dbs: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	paths := make([]string, 0, 4)
	syncStatusCalls := 0
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var syncStatusCall int
		mu.Lock()
		paths = append(paths, r.URL.RequestURI())
		if r.URL.Path == "/debug/sync-status" {
			syncStatusCalls++
			syncStatusCall = syncStatusCalls
		}
		mu.Unlock()

		switch r.URL.Path {
		case "/debug/sync-status":
			if syncStatusCall == 1 {
				_, _ = w.Write([]byte(`{"active":true,"operation":"replica_sync","phase":"upload","elapsed_seconds":12.5,"executor_waiter_count":0}`))
				return
			}
			_, _ = w.Write([]byte(`{"active":true,"operation":"db_sync","phase":"checkpoint","elapsed_seconds":72.25,"executor_waiter_count":1,"executor_wait_started_at":"2026-05-04T12:00:00Z","executor_wait_seconds":61.5}`))
		case "/sync":
			http.Error(w, "sync database: db sync: wait for db sync executor: context deadline exceeded", http.StatusInternalServerError)
		case "/debug/pprof/goroutine":
			_, _ = w.Write([]byte("goroutine 42 [sync.Mutex.Lock]:\ngithub.com/benbjohnson/litestream.(*DB).Sync\n"))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	result := VerificationResult{}
	err := verifier.waitForSync(context.Background(), &result)
	if err == nil {
		t.Fatal("expected sync error")
	}
	if !strings.Contains(err.Error(), "wait for db sync executor") {
		t.Fatalf("sync error=%q want db sync executor", err.Error())
	}

	if result.SyncStatusBeforeSync == nil {
		t.Fatal("expected sync status before sync")
	}
	if active := result.SyncStatusBeforeSync.Active; active == nil || !*active {
		t.Fatalf("before active=%v want true", active)
	}
	if result.SyncStatusBeforeSync.Operation != "replica_sync" {
		t.Fatalf("before operation=%q want replica_sync", result.SyncStatusBeforeSync.Operation)
	}
	if result.SyncStatusAfterSyncFailure == nil {
		t.Fatal("expected sync status after sync failure")
	}
	if result.SyncStatusAfterSyncFailure.Phase != "checkpoint" {
		t.Fatalf("after phase=%q want checkpoint", result.SyncStatusAfterSyncFailure.Phase)
	}
	if result.SyncStatusAfterSyncFailure.ExecutorWaiterCount == nil || *result.SyncStatusAfterSyncFailure.ExecutorWaiterCount != 1 {
		t.Fatalf("after executor_waiter_count=%v want 1", result.SyncStatusAfterSyncFailure.ExecutorWaiterCount)
	}
	if result.LitestreamGoroutinesOnSyncFailure == nil {
		t.Fatal("expected goroutine snapshot on sync timeout")
	}
	if !strings.Contains(result.LitestreamGoroutinesOnSyncFailure.Output, "litestream.(*DB).Sync") {
		t.Fatalf("goroutine output=%q want DB sync stack", result.LitestreamGoroutinesOnSyncFailure.Output)
	}

	mu.Lock()
	gotPaths := append([]string(nil), paths...)
	mu.Unlock()
	wantPrefix := []string{"/debug/sync-status", "/sync", "/debug/sync-status", "/debug/pprof/goroutine?debug=2"}
	if len(gotPaths) < len(wantPrefix) {
		t.Fatalf("paths=%v want prefix %v", gotPaths, wantPrefix)
	}
	for i, want := range wantPrefix {
		if gotPaths[i] != want {
			t.Fatalf("paths=%v want prefix %v", gotPaths, wantPrefix)
		}
	}

	runner := NewRunner(cfg)
	result.Status = "failed"
	result.Summary = summarizeVerificationMessage("wait for sync: " + err.Error())
	result.ErrorMessage = "wait for sync: " + err.Error()
	snapshot := runner.captureFailureDebugSnapshotIfDue(result)
	if snapshot == nil {
		t.Fatal("expected failure debug snapshot")
	}
	if snapshot.SyncStatusBeforeSync == nil {
		t.Fatal("expected snapshot sync_status_before_sync")
	}
	if snapshot.SyncStatusAfterSyncFailure == nil {
		t.Fatal("expected snapshot sync_status_after_sync_failure")
	}
	if snapshot.LitestreamGoroutinesOnSyncFailure == nil {
		t.Fatal("expected snapshot litestream_goroutines_on_sync_failure")
	}
}

func TestRecordVerificationStepCapturesTimingAndError(t *testing.T) {
	result := VerificationResult{}
	stepErr := errors.New("sync failed")

	err := recordVerificationStep(&result, "sync", func() error {
		return stepErr
	})

	if !errors.Is(err, stepErr) {
		t.Fatalf("recordVerificationStep() error = %v want %v", err, stepErr)
	}
	if len(result.Steps) != 1 {
		t.Fatalf("steps=%d want 1", len(result.Steps))
	}
	step := result.Steps[0]
	if step.Name != "sync" {
		t.Fatalf("step name=%q want sync", step.Name)
	}
	if step.Status != "error" {
		t.Fatalf("step status=%q want error", step.Status)
	}
	if step.Error != "sync failed" {
		t.Fatalf("step error=%q want sync failed", step.Error)
	}
	if step.StartedAt.IsZero() || step.CompletedAt.IsZero() {
		t.Fatal("expected step timestamps")
	}
}

func TestFailureDebugSnapshotIsRateLimitedForRepeatedFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.SocketPath = filepath.Join(dir, "litestream.sock")
	cfg.WorkerID = "worker-test"
	cfg.WorkerName = "worker-test"
	cfg.ProfileName = "low-volume"
	if err := os.WriteFile(cfg.DBPath, []byte("db"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.ConfigPath, []byte("dbs: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(cfg)
	result := VerificationResult{
		Status:       "failed",
		Summary:      "wait for sync: connection refused",
		ErrorMessage: "wait for sync: connection refused",
	}

	first := runner.captureFailureDebugSnapshotIfDue(result)
	if first == nil {
		t.Fatal("expected first failure to capture debug snapshot")
	}
	second := runner.captureFailureDebugSnapshotIfDue(result)
	if second != nil {
		t.Fatal("expected repeated same failure to skip debug snapshot")
	}
	runner.resetFailureDebugState()
	third := runner.captureFailureDebugSnapshotIfDue(result)
	if third == nil {
		t.Fatal("expected reset failure state to capture debug snapshot")
	}

	runner.resetFailureDebugState()
	validationA := VerificationResult{
		Status:       "failed",
		CheckType:    "integrity",
		Summary:      "validation failed (exit 1): time=2026-04-21T10:00:00Z",
		ErrorMessage: "validation failed (exit 1): time=2026-04-21T10:00:00Z",
	}
	validationB := VerificationResult{
		Status:       "failed",
		CheckType:    "integrity",
		Summary:      "validation failed (exit 1): time=2026-04-21T10:30:00Z",
		ErrorMessage: "validation failed (exit 1): time=2026-04-21T10:30:00Z",
	}
	if runner.captureFailureDebugSnapshotIfDue(validationA) == nil {
		t.Fatal("expected first validation failure to capture debug snapshot")
	}
	if runner.captureFailureDebugSnapshotIfDue(validationB) != nil {
		t.Fatal("expected repeated validation failure with a new timestamp to skip debug snapshot")
	}
}

func TestPollDBStatsClearsStaleLitestreamStateOnFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-fail"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "missing.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-%d.sock", time.Now().UnixNano()))

	SetWorkerInfo(cfg)

	runner := NewRunner(cfg)
	runner.setDBSize(10)
	runner.setWALSize(5)
	runner.setLitestreamSnapshot(reporting.RuntimePayload{
		DBTXID:                    99,
		DBStatus:                  "replicating",
		LastSyncAgeSeconds:        1,
		LitestreamUptimeSeconds:   123,
		SnapshotCollectedAt:       time.Now().Add(-1 * time.Minute).UTC(),
		LitestreamSnapshotHealthy: true,
	})

	runner.pollDBStats()

	snapshot := runner.currentSnapshot()
	if snapshot.LitestreamSnapshotHealthy {
		t.Fatal("expected unhealthy snapshot")
	}
	if snapshot.DBTXID != 0 {
		t.Fatalf("db txid=%d want 0", snapshot.DBTXID)
	}
	if snapshot.DBStatus != "unknown" {
		t.Fatalf("db status=%q want %q", snapshot.DBStatus, "unknown")
	}
	if snapshot.LastSyncAgeSeconds != 0 {
		t.Fatalf("last sync age=%v want 0", snapshot.LastSyncAgeSeconds)
	}
	if snapshot.LitestreamUptimeSeconds != 0 {
		t.Fatalf("litestream uptime=%v want 0", snapshot.LitestreamUptimeSeconds)
	}
	if snapshot.DBSizeBytes != 10 {
		t.Fatalf("db size=%d want 10", snapshot.DBSizeBytes)
	}
	if snapshot.WALSizeBytes != 5 {
		t.Fatalf("wal size=%d want 5", snapshot.WALSizeBytes)
	}
	if snapshot.DataDiskTotalBytes == 0 {
		t.Fatal("expected data disk total bytes")
	}
	if !strings.Contains(snapshot.LitestreamSnapshotError, "read txid") {
		t.Fatalf("snapshot error=%q", snapshot.LitestreamSnapshotError)
	}
	if snapshot.SnapshotCollectedAt.IsZero() {
		t.Fatal("expected snapshot collected timestamp")
	}
}

func TestPollLitestreamLocalState(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, ".test.db-litestream")
	if err := os.MkdirAll(filepath.Join(stateDir, "ltx"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "meta"), []byte("123"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "ltx", "0001.ltx"), []byte("12345"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-local-state"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")

	SetWorkerInfo(cfg)
	runner := NewRunner(cfg)
	runner.pollLitestreamLocalState()

	snapshot := runner.currentSnapshot()
	if snapshot.LitestreamDirSizeBytes != 8 {
		t.Fatalf("litestream dir size=%d want 8", snapshot.LitestreamDirSizeBytes)
	}
	if snapshot.LitestreamLTXSizeBytes != 5 {
		t.Fatalf("litestream ltx size=%d want 5", snapshot.LitestreamLTXSizeBytes)
	}
}

func TestMonitorLitestreamCancelsRunContextOnUnexpectedExit(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	runner := NewRunner(DefaultConfig())
	done := make(chan struct{})
	runner.litestreamDone = done

	runner.monitorLitestream(ctx, cancel)
	close(done)

	if !waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
		return ctx.Err() != nil
	}) {
		t.Fatal("context was not canceled after Litestream exit")
	}

	if got := context.Cause(ctx); got == nil || got.Error() != "litestream exited unexpectedly" {
		t.Fatalf("context cause=%v want litestream exited unexpectedly", got)
	}
}

func startTestUnixServer(t *testing.T, socketPath string, handler http.Handler) {
	t.Helper()

	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}

	server := &http.Server{Handler: handler}
	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(socketPath)
	})
}

type unixServerTracker struct {
	active atomic.Int64
}

func (s *unixServerTracker) Active() int64 {
	return s.active.Load()
}

func startTrackedUnixServer(t *testing.T, socketPath string, handler http.Handler) *unixServerTracker {
	t.Helper()

	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}

	tracker := &unixServerTracker{}
	server := &http.Server{Handler: handler}
	go func() {
		_ = server.Serve(&trackedListener{
			Listener: listener,
			active:   &tracker.active,
		})
	}()

	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(socketPath)
	})

	return tracker
}

type trackedListener struct {
	net.Listener
	active *atomic.Int64
}

func (l *trackedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.active.Add(1)
	return &trackedConn{Conn: conn, active: l.active}, nil
}

type trackedConn struct {
	net.Conn
	active *atomic.Int64
	once   sync.Once
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		c.active.Add(-1)
	})
	return err
}

func waitUntil(timeout, interval time.Duration, condition func() bool) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if condition() {
			return true
		}
		select {
		case <-deadline.C:
			return condition()
		case <-ticker.C:
		}
	}
}

func writeFakeLitestreamTest(t *testing.T, dir, body string) {
	t.Helper()

	path := filepath.Join(dir, "litestream-test")
	script := "#!/bin/sh\n" + body
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func readLines(t *testing.T, path string) []string {
	t.Helper()

	text := strings.TrimSpace(readFile(t, path))
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

func assertContains(t *testing.T, values []string, want string) {
	t.Helper()

	if !slices.Contains(values, want) {
		t.Fatalf("values=%v want %q", values, want)
	}
}
