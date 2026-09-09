package worker

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func writeProcessFixture(t *testing.T, root string, pid int, user, system, pages, start string) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(filepath.Join(dir, "fd"), 0o755); err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields("S 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0")
	fields[11], fields[12], fields[19], fields[21] = user, system, start, pages
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(fmt.Sprintf("%d (name with ) spaces) %s", pid, strings.Join(fields, " "))), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestProcessObservationsLifecycle(t *testing.T) {
	root := t.TempDir()
	pid := 12345
	poller := newStatsPoller(&Config{})
	poller.litestreamPID = func() int { return pid }
	at := time.Now().UTC()
	poller.collectProcessObservations(root, false, at)
	if poller.snapshot.LitestreamProcess.Status != "unsupported" || !math.IsNaN(testutil.ToFloat64(litestreamRSSBytes.WithLabelValues(currentMetricLabels()...))) {
		t.Fatal("unsupported collection presented as zero")
	}
	poller.collectProcessObservations(root, true, at)
	if poller.snapshot.LitestreamProcess.Status != "unavailable" {
		t.Fatal("missing process must be unavailable")
	}
	writeProcessFixture(t, root, pid, "125", "25", "2", "10")
	writeProcessFixture(t, root, os.Getpid(), "0", "0", "1", "20")
	poller.collectProcessObservations(root, true, at)
	if poller.snapshot.LitestreamProcess.Status != "fresh" || poller.snapshot.LitestreamCPUSecondsTotal != 1.5 || poller.snapshot.LitestreamRSSBytes != int64(2*os.Getpagesize()) {
		t.Fatalf("incorrect units: %+v", poller.snapshot)
	}
	writeProcessFixture(t, root, pid, "bad", "25", "2", "10")
	poller.collectProcessObservations(root, true, at.Add(time.Second))
	if poller.snapshot.LitestreamProcess.Status != "stale" || !poller.snapshot.LitestreamProcess.CollectedAt.Equal(at) || poller.snapshot.LitestreamCPUSecondsTotal != 1.5 {
		t.Fatal("failed collection lost last observation")
	}
	writeProcessFixture(t, root, pid, "0", "0", "0", "30")
	poller.collectProcessObservations(root, true, at.Add(2*time.Second))
	if poller.snapshot.LitestreamProcess.Status != "fresh" || poller.snapshot.LitestreamCPUSecondsTotal != 0 || poller.snapshot.LitestreamProcess.StartTicks != "30" || testutil.ToFloat64(litestreamRSSBytes.WithLabelValues(currentMetricLabels()...)) != 0 {
		t.Fatal("restart or actual zero not represented")
	}
	pid++
	poller.collectProcessObservations(root, true, at.Add(3*time.Second))
	if poller.snapshot.LitestreamProcess.Status != "unavailable" || !poller.snapshot.LitestreamProcess.CollectedAt.IsZero() {
		t.Fatal("new PID inherited old sample")
	}
}

func TestProcessCollectionRunsWithoutIPC(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), SocketPath: filepath.Join(t.TempDir(), "missing.sock")}
	poller := newStatsPoller(&cfg)
	poller.litestreamPID = os.Getpid
	poller.pollDBStats()
	snapshot := poller.currentSnapshot()
	if snapshot.LitestreamSnapshotHealthy {
		t.Fatal("IPC unexpectedly healthy")
	}
	want := "unsupported"
	if processCollectionSupported() {
		want = "fresh"
	}
	if snapshot.LitestreamProcess.Status != want || snapshot.WorkerProcess.Status != want {
		t.Fatalf("process collection did not run independently: %+v", snapshot)
	}
}

func TestReadProcStatsInvalidSamples(t *testing.T) {
	for _, tc := range []struct{ name, user, system, pages string }{
		{"user", "bad", "0", "0"}, {"system", "0", "bad", "0"}, {"rss", "0", "0", "-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeProcessFixture(t, root, 42, tc.user, tc.system, tc.pages, "1")
			if _, _, _, _, err := readProcStatsAt(root, 42); err == nil {
				t.Fatal("invalid sample accepted")
			}
		})
	}
	if _, _, _, _, err := readProcStatsAt(t.TempDir(), 0); err == nil {
		t.Fatal("invalid PID accepted")
	}
}

func TestLocalStateScanBudgets(t *testing.T) {
	db := filepath.Join(t.TempDir(), "test.db")
	root := litestreamStateDir(db)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "meta"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		limit    int
		deadline time.Time
	}{
		{"entries", 1, time.Now().Add(time.Hour)}, {"time", 100, time.Now().Add(-time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			total, ltx, err := localStateSizes([]string{db}, tc.limit, tc.deadline)
			if err == nil || total != 0 || ltx != 0 {
				t.Fatal("partial scan published a total")
			}
		})
	}
	total, ltx, err := localStateSizes([]string{db, filepath.Join(t.TempDir(), "absent.db")}, 100, time.Now().Add(time.Hour))
	if err != nil || total != 3 || ltx != 0 {
		t.Fatalf("total=%d ltx=%d err=%v", total, ltx, err)
	}
}

func TestPIDReuseDoesNotRetainPreviousProcess(t *testing.T) {
	root := t.TempDir()
	poller := newStatsPoller(&Config{})
	poller.litestreamPID = func() int { return 42 }
	writeProcessFixture(t, root, 42, "100", "0", "1", "10")
	poller.collectProcessObservations(root, true, time.Now())
	writeProcessFixture(t, root, 42, "bad", "0", "1", "20")
	poller.collectProcessObservations(root, true, time.Now())
	if poller.snapshot.LitestreamProcess.Status != "unavailable" || poller.snapshot.LitestreamRSSBytes != 0 {
		t.Fatal("reused PID retained previous process sample")
	}
}

func TestLocalStateMetricValidity(t *testing.T) {
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "test.db")}
	root := litestreamStateDir(cfg.DBPath)
	if err := os.WriteFile(root, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	poller := newStatsPoller(&cfg)
	poller.pollLitestreamLocalState()
	if poller.snapshot.LocalStateStatus != "unavailable" || !math.IsNaN(testutil.ToFloat64(litestreamDirSize.WithLabelValues(currentMetricLabels()...))) {
		t.Fatal("first failure appears as measured zero")
	}
}

func TestLocalStateScanResumes(t *testing.T) {
	db := filepath.Join(t.TempDir(), "test.db")
	root := litestreamStateDir(db)
	if err := os.MkdirAll(filepath.Join(root, "ltx"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if err := os.WriteFile(filepath.Join(root, "ltx", strconv.Itoa(i)), []byte("abc"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	scanner := localStateScanner{paths: []string{db}}
	defer scanner.close()
	for i := range 100 {
		complete, err := scanner.step(2, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			if i == 0 || scanner.total != 60 || scanner.ltx != 60 {
				t.Fatalf("invalid resumed totals: %+v", scanner)
			}
			return
		}
	}
	t.Fatal("bounded scan never completed")
}

func TestLocalStateStaleAndZeroMetrics(t *testing.T) {
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "test.db")}
	root := litestreamStateDir(cfg.DBPath)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	poller := newStatsPoller(&cfg)
	poller.pollLitestreamLocalState()
	if poller.snapshot.LocalStateStatus != "fresh" || testutil.ToFloat64(litestreamDirSize.WithLabelValues(currentMetricLabels()...)) != 0 {
		t.Fatal("empty state is not a measured zero")
	}
	at := poller.snapshot.LocalStateCollectedAt
	cfg.DBPath = filepath.Join(t.TempDir(), "invalid.db")
	if err := os.WriteFile(litestreamStateDir(cfg.DBPath), []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	poller.pollLitestreamLocalState()
	if poller.snapshot.LocalStateStatus != "stale" || !poller.snapshot.LocalStateCollectedAt.Equal(at) || !math.IsNaN(testutil.ToFloat64(litestreamDirSize.WithLabelValues(currentMetricLabels()...))) {
		t.Fatal("stale sample appears fresh")
	}
	if testutil.ToFloat64(localStateObservationStatus.WithLabelValues(currentMetricLabels()...)) != 2 || testutil.ToFloat64(localStateObservationTime.WithLabelValues(currentMetricLabels()...)) != float64(at.Unix()) {
		t.Fatal("stale status or timestamp missing")
	}
}
