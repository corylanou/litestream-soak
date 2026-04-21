package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
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
		if r.URL.Path != "/sync" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	verifier := NewVerifier(cfg, nil)
	if err := verifier.waitForSync(context.Background()); err != nil {
		t.Fatalf("waitForSync() error = %v", err)
	}

	if !waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
		return tracker.Active() == 0
	}) {
		t.Fatalf("active IPC connections=%d want 0", tracker.Active())
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
