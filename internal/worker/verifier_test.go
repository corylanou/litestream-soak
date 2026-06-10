package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus/testutil"
	_ "modernc.org/sqlite"
)

type fakePauser struct {
	pauseCalls  int
	resumeCalls int
	pauseErr    error
}

func (f *fakePauser) Pause(ctx context.Context) error {
	f.pauseCalls++
	return f.pauseErr
}

func (f *fakePauser) Resume() {
	f.resumeCalls++
}

func walDSN(dbPath string) string {
	return dbPath + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
}

func createWALDatabaseWithPendingFrames(t *testing.T, dbPath string) {
	t.Helper()

	writer, err := sql.Open("sqlite", walDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(1)
	if _, err := writer.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := writer.Exec("INSERT INTO t (body) VALUES (?)", strings.Repeat("x", 512)); err != nil {
			t.Fatal(err)
		}
	}

	holder, err := sql.Open("sqlite", walDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Close() })

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(dbPath + "-wal")
	if err != nil {
		t.Fatalf("expected WAL file to exist before checkpoint: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected non-empty WAL file before checkpoint")
	}
}

func holdWriteLock(t *testing.T, dbPath string) {
	t.Helper()

	lockDB, err := sql.Open("sqlite", walDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := lockDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), "INSERT INTO t (body) VALUES ('locked')"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = lockDB.Close()
	})
}

func TestCheckpointTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	createWALDatabaseWithPendingFrames(t, dbPath)

	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = dbPath

	verifier := NewVerifier(cfg)
	if err := verifier.checkpoint(context.Background()); err != nil {
		t.Fatalf("checkpoint() error = %v", err)
	}

	info, err := os.Stat(dbPath + "-wal")
	if err == nil && info.Size() != 0 {
		t.Fatalf("WAL size after checkpoint = %d, want 0 (or removed)", info.Size())
	}
}

func TestCheckpointReportsBusy(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	createWALDatabaseWithPendingFrames(t, dbPath)
	holdWriteLock(t, dbPath)

	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = dbPath

	verifier := NewVerifier(cfg)
	verifier.checkpointAttempts = 2
	verifier.checkpointRetryDelay = 10 * time.Millisecond
	verifier.checkpointBusyTimeout = 50 * time.Millisecond

	err := verifier.checkpoint(context.Background())
	if err == nil {
		t.Fatal("checkpoint() error = nil, want busy error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "busy") {
		t.Fatalf("checkpoint() error = %q, want it to mention busy", err.Error())
	}
}

func TestRunCycleFailsWhenCheckpointFails(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	createWALDatabaseWithPendingFrames(t, dbPath)
	holdWriteLock(t, dbPath)

	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = dbPath
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-ckpt-%d.sock", time.Now().UnixNano()))

	var syncHits atomic.Int64
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			syncHits.Add(1)
			_, _ = w.Write([]byte(`{"status":"ok","txid":1,"replicated_txid":1}`))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	verifier.checkpointAttempts = 1
	verifier.checkpointRetryDelay = 10 * time.Millisecond
	verifier.checkpointBusyTimeout = 50 * time.Millisecond

	result, err := verifier.RunCycle(context.Background())
	if err == nil {
		t.Fatal("RunCycle() error = nil, want checkpoint error")
	}
	if !strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("RunCycle() error = %q, want checkpoint error", err.Error())
	}
	if result.Status != "failed" {
		t.Fatalf("result status = %q, want failed", result.Status)
	}
	checkpointStep := findStep(t, result.Steps, "checkpoint")
	if checkpointStep.Status != "error" {
		t.Fatalf("checkpoint step status = %q, want error", checkpointStep.Status)
	}
	if got := syncHits.Load(); got != 0 {
		t.Fatalf("/sync requests = %d, want 0", got)
	}
}

func findStep(t *testing.T, steps []reporting.VerificationStep, name string) reporting.VerificationStep {
	t.Helper()

	for _, step := range steps {
		if step.Name == name {
			return step
		}
	}
	t.Fatalf("step %q not found in %+v", name, steps)
	return reporting.VerificationStep{}
}

func TestWaitForSyncRejectsEmptyBody(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-empty-%d.sock", time.Now().UnixNano()))

	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	err := verifier.waitForSync(context.Background(), &VerificationResult{})
	if err == nil {
		t.Fatal("waitForSync() error = nil, want empty-body error")
	}
	if !strings.Contains(err.Error(), "sync response was empty") {
		t.Fatalf("waitForSync() error = %q, want sync response was empty", err.Error())
	}
}

func TestWaitForSyncRetriesUntilReplicatedCatchesUp(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-lag-%d.sock", time.Now().UnixNano()))

	var syncHits atomic.Int64
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			if syncHits.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"status":"ok","txid":12,"replicated_txid":10}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"ok","txid":12,"replicated_txid":12}`))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	verifier.syncRetryDelay = 10 * time.Millisecond

	result := VerificationResult{}
	if err := verifier.waitForSync(context.Background(), &result); err != nil {
		t.Fatalf("waitForSync() error = %v", err)
	}
	if got := syncHits.Load(); got != 2 {
		t.Fatalf("/sync requests = %d, want 2", got)
	}
	if result.SyncTXID != 12 {
		t.Fatalf("sync txid = %d, want 12", result.SyncTXID)
	}
	if result.SyncReplicatedTXID != 12 {
		t.Fatalf("sync replicated txid = %d, want 12", result.SyncReplicatedTXID)
	}
}

func TestWaitForSyncFailsWhenLagPersists(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-stuck-%d.sock", time.Now().UnixNano()))
	cfg.VerifySyncTimeout = 100 * time.Millisecond

	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":true,"operation":"replica_sync","phase":"upload"}`))
		case "/sync":
			_, _ = w.Write([]byte(`{"status":"ok","txid":12,"replicated_txid":10}`))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	verifier.syncRetryDelay = 10 * time.Millisecond

	result := VerificationResult{}
	err := verifier.waitForSync(context.Background(), &result)
	if err == nil {
		t.Fatal("waitForSync() error = nil, want lag error")
	}
	if !strings.Contains(err.Error(), "sync lag not resolved") {
		t.Fatalf("waitForSync() error = %q, want sync lag not resolved", err.Error())
	}
	if result.SyncStatusAfterSyncFailure == nil {
		t.Fatal("expected sync status after sync failure")
	}
	if result.SyncTXID != 12 {
		t.Fatalf("sync txid = %d, want 12", result.SyncTXID)
	}
	if result.SyncReplicatedTXID != 10 {
		t.Fatalf("sync replicated txid = %d, want 10", result.SyncReplicatedTXID)
	}
}

func TestWaitForSyncSetsReplicationMetrics(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-sync-metrics"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-met-%d.sock", time.Now().UnixNano()))

	SetWorkerInfo(cfg)
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			_, _ = w.Write([]byte(`{"status":"ok","txid":42,"replicated_txid":42}`))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	if err := verifier.waitForSync(context.Background(), &VerificationResult{}); err != nil {
		t.Fatalf("waitForSync() error = %v", err)
	}

	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source}
	if got := testutil.ToFloat64(replicatedTXID.WithLabelValues(labels...)); got != 42 {
		t.Fatalf("soak_replicated_txid = %v, want 42", got)
	}
	if got := testutil.ToFloat64(replicationLag.WithLabelValues(labels...)); got != 0 {
		t.Fatalf("soak_replication_lag_txids = %v, want 0", got)
	}
}

func TestPollDBStatsSetsReplicationMetrics(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-poll-metrics"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-poll-%d.sock", time.Now().UnixNano()))

	if err := os.WriteFile(cfg.DBPath, []byte("1234567890"), 0o644); err != nil {
		t.Fatal(err)
	}

	SetWorkerInfo(cfg)
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/txid":
			_, _ = w.Write([]byte(`{"txid":42}`))
		case "/info":
			_, _ = w.Write([]byte(`{"uptime_seconds":99}`))
		case "/list":
			lastSyncAt := time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
			_, _ = w.Write([]byte(`{"databases":[{"status":"replicating","txid":42,"replicated_txid":40,"last_sync_at":"` + lastSyncAt + `"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))

	runner := NewRunner(cfg)
	runner.pollDBStats()

	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source}
	if got := testutil.ToFloat64(replicatedTXID.WithLabelValues(labels...)); got != 40 {
		t.Fatalf("soak_replicated_txid = %v, want 40", got)
	}
	if got := testutil.ToFloat64(replicationLag.WithLabelValues(labels...)); got != 2 {
		t.Fatalf("soak_replication_lag_txids = %v, want 2", got)
	}
}

func TestRunCycleRemovesRestoredFileOnFailedSync(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-rst-%d.sock", time.Now().UnixNano()))

	restoredPath := cfg.DBPath + ".restored"
	if err := os.WriteFile(restoredPath, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			http.Error(w, "sync exploded", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	if _, err := verifier.RunCycle(context.Background()); err == nil {
		t.Fatal("RunCycle() error = nil, want sync error")
	}

	if _, err := os.Stat(restoredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %s to be removed after failed cycle, stat err = %v", restoredPath, err)
	}
}

func TestRunCycleCleansStaleRestoredFileBeforeValidate(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-stale-%d.sock", time.Now().UnixNano()))
	if err := os.WriteFile(cfg.ConfigPath, []byte("dbs: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	restoredPath := cfg.DBPath + ".restored"
	markerPath := filepath.Join(dir, "marker")
	if err := os.WriteFile(restoredPath, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	writeFakeLitestreamTest(t, dir, `
if [ -f "$RESTORED_PATH" ]; then
  printf present > "$MARKER_PATH"
else
  printf absent > "$MARKER_PATH"
fi
exit 0
`)
	t.Setenv("RESTORED_PATH", restoredPath)
	t.Setenv("MARKER_PATH", markerPath)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			_, _ = w.Write([]byte(`{"status":"ok","txid":7,"replicated_txid":7}`))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	result, err := verifier.RunCycle(context.Background())
	if err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}
	if !result.Passed {
		t.Fatalf("result passed = false, status = %q", result.Status)
	}

	if got := strings.TrimSpace(readFile(t, markerPath)); got != "absent" {
		t.Fatalf("stale .restored visibility at validate time = %q, want absent", got)
	}
	if _, err := os.Stat(restoredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %s to be removed after cycle, stat err = %v", restoredPath, err)
	}
}

func TestRecordVerificationOutcomeAborted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-aborted-metric"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	SetWorkerInfo(cfg)

	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source}
	RecordVerificationOutcome("passed", 0)
	if got := testutil.ToFloat64(verificationLastResult.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("last result after passed = %v, want 1", got)
	}

	before := testutil.ToFloat64(verificationTotal.WithLabelValues(append(labels, "aborted")...))
	RecordVerificationOutcome("aborted", 1.5)

	if got := testutil.ToFloat64(verificationTotal.WithLabelValues(append(labels, "aborted")...)); got != before+1 {
		t.Fatalf("aborted counter = %v, want %v", got, before+1)
	}
	if got := testutil.ToFloat64(verificationLastResult.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("last result after aborted = %v, want unchanged 1", got)
	}
}

func TestRunCycleRecordsFailedMetricOnEarlyFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-early-failure"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	SetWorkerInfo(cfg)

	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source}
	before := testutil.ToFloat64(verificationTotal.WithLabelValues(append(labels, "failed")...))

	verifier := NewVerifier(cfg, &fakePauser{pauseErr: errors.New("pause exploded")})
	result, err := verifier.RunCycle(context.Background())
	if err == nil {
		t.Fatal("RunCycle() error = nil, want pause error")
	}
	if result.Status != "failed" {
		t.Fatalf("result status = %q, want failed", result.Status)
	}

	if got := testutil.ToFloat64(verificationTotal.WithLabelValues(append(labels, "failed")...)); got != before+1 {
		t.Fatalf("failed counter = %v, want %v", got, before+1)
	}
}

func TestRunCycleMarksAbortedOnContextCancellation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-aborted-cycle"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-abort-%d.sock", time.Now().UnixNano()))
	SetWorkerInfo(cfg)

	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source}
	RecordVerificationOutcome("passed", 0)
	abortedBefore := testutil.ToFloat64(verificationTotal.WithLabelValues(append(labels, "aborted")...))

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			cancel(errors.New("litestream exited unexpectedly"))
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	result, err := verifier.RunCycle(ctx)
	if err == nil {
		t.Fatal("RunCycle() error = nil, want abort error")
	}
	if result.Status != "aborted" {
		t.Fatalf("result status = %q, want aborted", result.Status)
	}
	if result.Passed {
		t.Fatal("result passed = true, want false")
	}
	if !strings.HasPrefix(result.Summary, "verification aborted: ") {
		t.Fatalf("result summary = %q, want verification aborted prefix", result.Summary)
	}

	if got := testutil.ToFloat64(verificationTotal.WithLabelValues(append(labels, "aborted")...)); got != abortedBefore+1 {
		t.Fatalf("aborted counter = %v, want %v", got, abortedBefore+1)
	}
	if got := testutil.ToFloat64(verificationLastResult.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("last result after aborted cycle = %v, want unchanged 1", got)
	}
}

func TestRunVerifyLoopSendsVerificationAfterContextCancellation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-detached-send"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-send-%d.sock", time.Now().UnixNano()))
	cfg.VerifyInterval = 50 * time.Millisecond

	var verificationPosts atomic.Int64
	var lastStatus atomic.Value
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/verifications") {
			var payload struct {
				Status string `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode verification payload: %v", err)
			}
			lastStatus.Store(payload.Status)
			verificationPosts.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(control.Close)
	cfg.ControlBaseURL = control.URL

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			cancel(errors.New("litestream exited unexpectedly"))
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))

	runner := NewRunner(cfg)
	runner.reporter = NewReporter(cfg)
	runner.verifier = NewVerifier(cfg)

	err := runner.runVerifyLoop(ctx)
	if err == nil || err.Error() != "litestream exited unexpectedly" {
		t.Fatalf("runVerifyLoop() error = %v, want litestream exited unexpectedly", err)
	}
	if got := verificationPosts.Load(); got < 1 {
		t.Fatalf("verification posts = %d, want >= 1 (terminal report must survive cancellation)", got)
	}
	if got, _ := lastStatus.Load().(string); got != "aborted" {
		t.Fatalf("posted verification status = %q, want aborted", got)
	}
}

func TestVerifierPausesAllPausersAndResumesOnFailure(t *testing.T) {
	first := &fakePauser{}
	second := &fakePauser{pauseErr: errors.New("pause exploded")}

	verifier := NewVerifier(DefaultConfig(), first, second)

	err := verifier.pauseLoad(context.Background())
	if err == nil {
		t.Fatal("pauseLoad() error = nil, want error from second pauser")
	}
	if !strings.Contains(err.Error(), "pause exploded") {
		t.Fatalf("pauseLoad() error = %q, want it to wrap %q", err.Error(), "pause exploded")
	}
	if first.pauseCalls != 1 {
		t.Fatalf("first pauser pause calls = %d, want 1", first.pauseCalls)
	}
	if second.pauseCalls != 1 {
		t.Fatalf("second pauser pause calls = %d, want 1", second.pauseCalls)
	}
	if first.resumeCalls != 1 {
		t.Fatalf("first pauser resume calls = %d, want 1 (rollback)", first.resumeCalls)
	}
	if second.resumeCalls != 1 {
		t.Fatalf("second pauser resume calls = %d, want 1 (failing pauser must be unparked)", second.resumeCalls)
	}
}

func TestVerifierResumeLoadResumesAllPausers(t *testing.T) {
	first := &fakePauser{}
	second := &fakePauser{}

	verifier := NewVerifier(DefaultConfig(), first, second)
	if err := verifier.pauseLoad(context.Background()); err != nil {
		t.Fatalf("pauseLoad() error = %v", err)
	}

	verifier.resumeLoad()

	if first.resumeCalls != 1 {
		t.Fatalf("first pauser resume calls = %d, want 1", first.resumeCalls)
	}
	if second.resumeCalls != 1 {
		t.Fatalf("second pauser resume calls = %d, want 1", second.resumeCalls)
	}
}

func TestWaitForSyncRejectsMissingTXIDFields(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-notxid-%d.sock", time.Now().UnixNano()))

	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/sync-status":
			_, _ = w.Write([]byte(`{"active":false}`))
		case "/sync":
			_, _ = w.Write([]byte(`{"status":"synced"}`))
		default:
			http.NotFound(w, r)
		}
	}))

	verifier := NewVerifier(cfg)
	err := verifier.waitForSync(context.Background(), &VerificationResult{})
	if err == nil {
		t.Fatal("waitForSync() error = nil, want missing-txid error")
	}
	if !strings.Contains(err.Error(), "missing txid") {
		t.Fatalf("waitForSync() error = %q, want missing txid error", err.Error())
	}
}

func TestRunCycleFailsWhenStaleRestoredArtifactCannotBeRemoved(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-dirty-%d.sock", time.Now().UnixNano()))

	restoredPath := cfg.DBPath + ".restored"
	if err := os.MkdirAll(filepath.Join(restoredPath, "blocker"), 0o755); err != nil {
		t.Fatal(err)
	}

	var syncRequests atomic.Int32
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sync" {
			syncRequests.Add(1)
		}
		_, _ = w.Write([]byte(`{}`))
	}))

	verifier := NewVerifier(cfg)
	result, err := verifier.RunCycle(context.Background())
	if err == nil {
		t.Fatal("RunCycle() error = nil, want clean-restored error")
	}
	if result.Status != "failed" {
		t.Fatalf("result status = %q, want failed", result.Status)
	}
	if syncRequests.Load() != 0 {
		t.Fatalf("sync requests = %d, want 0 after clean-restored failure", syncRequests.Load())
	}
}

func TestPollDBStatsZeroTXIDsResetReplicationMetrics(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-poll-zero"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-pollzero-%d.sock", time.Now().UnixNano()))

	if err := os.WriteFile(cfg.DBPath, []byte("1234567890"), 0o644); err != nil {
		t.Fatal(err)
	}

	SetWorkerInfo(cfg)
	var listBody atomic.Value
	listBody.Store(`{"databases":[{"status":"replicating","txid":42,"replicated_txid":40}]}`)
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/txid":
			_, _ = w.Write([]byte(`{"txid":42}`))
		case "/info":
			_, _ = w.Write([]byte(`{"uptime_seconds":99}`))
		case "/list":
			_, _ = w.Write([]byte(listBody.Load().(string)))
		default:
			http.NotFound(w, r)
		}
	}))

	runner := NewRunner(cfg)
	runner.pollDBStats()

	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source}
	if got := testutil.ToFloat64(replicatedTXID.WithLabelValues(labels...)); got != 40 {
		t.Fatalf("soak_replicated_txid = %v, want 40", got)
	}

	listBody.Store(`{"databases":[{"status":"replicating","txid":0,"replicated_txid":0}]}`)
	runner.pollDBStats()
	if got := testutil.ToFloat64(replicatedTXID.WithLabelValues(labels...)); got != 0 {
		t.Fatalf("soak_replicated_txid after zero report = %v, want 0", got)
	}
	if got := testutil.ToFloat64(replicationLag.WithLabelValues(labels...)); got != 0 {
		t.Fatalf("soak_replication_lag_txids after zero report = %v, want 0", got)
	}

	listBody.Store(`{"databases":[{"status":"replicating"}]}`)
	if got := testutil.ToFloat64(replicatedTXID.WithLabelValues(labels...)); got != 0 {
		t.Fatalf("soak_replicated_txid = %v, want unchanged 0", got)
	}
}
