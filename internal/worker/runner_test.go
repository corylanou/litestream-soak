package worker

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
