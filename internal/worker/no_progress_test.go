package worker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestDiskPressureNoProgressDetectorFiresAfterBoundedWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DiskFullNoProgressWindow = time.Minute
	runner := NewRunner(cfg)
	base := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)

	samples := []struct {
		at   time.Time
		txid uint64
		want bool
	}{
		{at: base, txid: 10},
		{at: base.Add(30 * time.Second), txid: 11},
		{at: base.Add(61 * time.Second), txid: 12, want: true},
	}

	for _, sample := range samples {
		snapshot := runtimeSnapshot{RuntimePayload: reporting.RuntimePayload{
			DataDiskTotalBytes:        1024,
			DataDiskAvailableBytes:    100,
			DBSizeBytes:               600,
			DBTXID:                    sample.txid,
			ReplicatedTXID:            8,
			ReplicationLagMax:         sample.txid - 8,
			LitestreamSnapshotHealthy: true,
		}}

		got := runner.observeDiskPressureNoProgress(sample.at, snapshot)
		if got.Detected != sample.want {
			t.Fatalf("at %s Detected = %v, want %v", sample.at.Sub(base), got.Detected, sample.want)
		}
		if sample.want {
			if !got.ShouldReport {
				t.Fatal("ShouldReport = false, want true")
			}
			if got.EventType != reporting.WorkerEventDiskFullNoProgress {
				t.Fatalf("EventType = %q, want %q", got.EventType, reporting.WorkerEventDiskFullNoProgress)
			}
			if !got.Runtime.DiskPressureNoProgress {
				t.Fatal("runtime DiskPressureNoProgress = false, want true")
			}
			if got.Runtime.DiskFullSignalObserved {
				t.Fatal("runtime DiskFullSignalObserved = true, want false")
			}
			if got.Runtime.DiskPressureNoProgressSeconds < 60 {
				t.Fatalf("runtime DiskPressureNoProgressSeconds = %v, want >= 60", got.Runtime.DiskPressureNoProgressSeconds)
			}
		}
	}
}

func TestDiskPressureNoProgressDetectorResetsWhenReplicaAdvances(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DiskFullNoProgressWindow = time.Minute
	runner := NewRunner(cfg)
	base := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)

	pressure := reporting.RuntimePayload{
		DataDiskTotalBytes:        1024,
		DataDiskAvailableBytes:    100,
		DBSizeBytes:               600,
		DBTXID:                    10,
		ReplicatedTXID:            8,
		ReplicationLagMax:         2,
		LitestreamSnapshotHealthy: true,
	}
	if got := runner.observeDiskPressureNoProgress(base, runtimeSnapshot{RuntimePayload: pressure}); got.Detected {
		t.Fatal("first pressure sample detected no progress")
	}

	pressure.DBTXID = 11
	if got := runner.observeDiskPressureNoProgress(base.Add(30*time.Second), runtimeSnapshot{RuntimePayload: pressure}); got.Detected {
		t.Fatal("second pressure sample detected before window")
	}

	pressure.DBTXID = 12
	pressure.ReplicatedTXID = 12
	pressure.ReplicationLagMax = 0
	reset := runner.observeDiskPressureNoProgress(base.Add(45*time.Second), runtimeSnapshot{RuntimePayload: pressure})
	if reset.Runtime.DiskPressureNoProgress {
		t.Fatal("DiskPressureNoProgress remained true after replica caught up")
	}

	pressure.DBTXID = 13
	pressure.ReplicatedTXID = 12
	pressure.ReplicationLagMax = 1
	got := runner.observeDiskPressureNoProgress(base.Add(106*time.Second), runtimeSnapshot{RuntimePayload: pressure})
	if got.Detected {
		t.Fatal("detector fired without a fresh full window after reset")
	}
}

func TestDiskPressureGenericENOSPCWithoutMetricReportsNoProgress(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DiskFullNoProgressWindow = time.Minute
	runner := NewRunner(cfg)
	runner.statsPoller.litestreamPID = func() int { return 321 }
	_, _ = runner.litestreamLog.Write([]byte("level=ERROR msg=\"sync error\" error=\"no space left on device\"\n"))

	base := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	pressure := reporting.RuntimePayload{
		DataDiskTotalBytes:        1024,
		DataDiskAvailableBytes:    100,
		DBSizeBytes:               600,
		DBTXID:                    10,
		ReplicatedTXID:            8,
		ReplicationLagMax:         2,
		LitestreamSnapshotHealthy: true,
	}
	if got := runner.observeDiskPressureNoProgress(base, runtimeSnapshot{RuntimePayload: pressure}); got.Detected {
		t.Fatal("first pressure sample detected no progress")
	}

	pressure.DBTXID = 12
	pressure.ReplicationLagMax = 4
	got := runner.observeDiskPressureNoProgress(base.Add(61*time.Second), runtimeSnapshot{RuntimePayload: pressure})
	if !got.ShouldReport {
		t.Fatalf("ShouldReport = false, want no-progress event; runtime=%+v", got.Runtime)
	}
	if got.EventType != reporting.WorkerEventDiskFullNoProgress {
		t.Fatalf("EventType = %q, want %q", got.EventType, reporting.WorkerEventDiskFullNoProgress)
	}
	if got.Runtime.DiskFullSignalObserved {
		t.Fatal("DiskFullSignalObserved = true, want false for generic ENOSPC")
	}
}

func TestDiskPressureSignalFreesReserveAndReportsRecovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.DBPath = filepath.Join(cfg.DataDir, "test.db")
	cfg.DiskFullNoProgressWindow = time.Minute
	cfg.DiskFullRecoveryReserve = 16
	cfg.DiskFullRecoveryTimeout = 2 * time.Minute
	runner := NewRunner(cfg)
	runner.statsPoller.litestreamPID = func() int { return 321 }
	if err := os.WriteFile(runner.diskFullRecoveryReservePath(), []byte("reserved"), 0644); err != nil {
		t.Fatalf("write reserve: %v", err)
	}
	_, _ = runner.litestreamLog.Write([]byte("level=ERROR msg=\"disk full while staging LTX file /data/.test.db-litestream/ltx/0/000000000001.ltx.tmp: replication paused, will resume automatically when space is freed\"\n"))

	base := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	pressure := reporting.RuntimePayload{
		DataDiskTotalBytes:              1024,
		DataDiskAvailableBytes:          100,
		DBSizeBytes:                     600,
		DBTXID:                          10,
		ReplicatedTXID:                  8,
		ReplicationLagMax:               2,
		LitestreamDiskFullMetricPresent: true,
		LitestreamDiskFull:              true,
		LitestreamSnapshotHealthy:       true,
	}
	first := runner.observeDiskPressureNoProgress(base, runtimeSnapshot{RuntimePayload: pressure})
	if first.ShouldReport {
		t.Fatalf("first observation reported %q, want no event", first.EventType)
	}
	if !first.Runtime.DiskFullSignalObserved {
		t.Fatal("DiskFullSignalObserved = false, want true")
	}
	if !first.Runtime.DiskFullRecoveryAttempted {
		t.Fatal("DiskFullRecoveryAttempted = false, want true")
	}
	if _, err := os.Stat(runner.diskFullRecoveryReservePath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reserve file stat error = %v, want not exist", err)
	}

	pressure.DBTXID = 11
	pressure.ReplicatedTXID = 9
	pressure.ReplicationLagMax = 2
	pressure.LitestreamDiskFull = false
	recovered := runner.observeDiskPressureNoProgress(base.Add(15*time.Second), runtimeSnapshot{RuntimePayload: pressure})
	if !recovered.ShouldReport {
		t.Fatal("ShouldReport = false, want recovery event")
	}
	if recovered.EventType != reporting.WorkerEventDiskFullRecovered {
		t.Fatalf("EventType = %q, want %q", recovered.EventType, reporting.WorkerEventDiskFullRecovered)
	}
	if !recovered.Runtime.DiskFullRecovered {
		t.Fatal("DiskFullRecovered = false, want true")
	}
	if !recovered.Runtime.DiskFullRecoveryWithoutRestart {
		t.Fatal("DiskFullRecoveryWithoutRestart = false, want true")
	}
	if recovered.Runtime.DiskPressureNoProgress {
		t.Fatal("DiskPressureNoProgress = true, want false after signal")
	}
}

func TestDiskPressureRecoveryRequiresDiskFullMetricCleared(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.DBPath = filepath.Join(cfg.DataDir, "test.db")
	cfg.DiskFullNoProgressWindow = time.Minute
	cfg.DiskFullRecoveryReserve = 16
	cfg.DiskFullRecoveryTimeout = 2 * time.Minute
	runner := NewRunner(cfg)
	runner.statsPoller.litestreamPID = func() int { return 321 }
	if err := os.WriteFile(runner.diskFullRecoveryReservePath(), []byte("reserved"), 0644); err != nil {
		t.Fatalf("write reserve: %v", err)
	}
	_, _ = runner.litestreamLog.Write([]byte("level=ERROR msg=\"disk full while staging LTX file /data/.test.db-litestream/ltx/0/000000000001.ltx.tmp: replication paused, will resume automatically when space is freed\"\n"))

	base := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	pressure := reporting.RuntimePayload{
		DataDiskTotalBytes:              1024,
		DataDiskAvailableBytes:          100,
		DBSizeBytes:                     600,
		DBTXID:                          10,
		ReplicatedTXID:                  8,
		ReplicationLagMax:               2,
		LitestreamDiskFullMetricPresent: true,
		LitestreamDiskFull:              true,
		LitestreamSnapshotHealthy:       true,
	}
	first := runner.observeDiskPressureNoProgress(base, runtimeSnapshot{RuntimePayload: pressure})
	if first.ShouldReport {
		t.Fatalf("first observation reported %q, want no event", first.EventType)
	}

	pressure.DBTXID = 11
	pressure.ReplicatedTXID = 9
	stillFull := runner.observeDiskPressureNoProgress(base.Add(15*time.Second), runtimeSnapshot{RuntimePayload: pressure})
	if stillFull.ShouldReport {
		t.Fatalf("ShouldReport = true, want no recovery while litestream_disk_full is still 1")
	}
	if stillFull.Runtime.DiskFullRecovered {
		t.Fatal("DiskFullRecovered = true, want false while litestream_disk_full is still 1")
	}
}

func TestDiskPressureSignalReportsRecoveryTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.DBPath = filepath.Join(cfg.DataDir, "test.db")
	cfg.DiskFullNoProgressWindow = time.Minute
	cfg.DiskFullRecoveryReserve = 16
	cfg.DiskFullRecoveryTimeout = time.Minute
	runner := NewRunner(cfg)
	runner.statsPoller.litestreamPID = func() int { return 321 }
	if err := os.WriteFile(runner.diskFullRecoveryReservePath(), []byte("reserved"), 0644); err != nil {
		t.Fatalf("write reserve: %v", err)
	}
	_, _ = runner.litestreamLog.Write([]byte("level=ERROR msg=\"disk full while staging LTX file /data/.test.db-litestream/ltx/0/000000000001.ltx.tmp: replication paused, will resume automatically when space is freed\"\n"))

	base := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	pressure := reporting.RuntimePayload{
		DataDiskTotalBytes:              1024,
		DataDiskAvailableBytes:          100,
		DBSizeBytes:                     600,
		DBTXID:                          10,
		ReplicatedTXID:                  8,
		ReplicationLagMax:               2,
		LitestreamDiskFullMetricPresent: true,
		LitestreamDiskFull:              true,
		LitestreamSnapshotHealthy:       true,
	}
	first := runner.observeDiskPressureNoProgress(base, runtimeSnapshot{RuntimePayload: pressure})
	if first.ShouldReport {
		t.Fatalf("first observation reported %q, want no event", first.EventType)
	}

	pressure.DBTXID = 12
	failed := runner.observeDiskPressureNoProgress(base.Add(61*time.Second), runtimeSnapshot{RuntimePayload: pressure})
	if !failed.ShouldReport {
		t.Fatal("ShouldReport = false, want recovery failure event")
	}
	if failed.EventType != reporting.WorkerEventDiskFullRecoveryFailed {
		t.Fatalf("EventType = %q, want %q", failed.EventType, reporting.WorkerEventDiskFullRecoveryFailed)
	}
	if failed.Runtime.DiskFullRecovered {
		t.Fatal("DiskFullRecovered = true, want false")
	}
	if !failed.Runtime.DiskFullSignalObserved {
		t.Fatal("DiskFullSignalObserved = false, want true")
	}
}

func TestParseLitestreamDiskFullMetric(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		dbPath      string
		wantPresent bool
		wantFull    bool
	}{
		{
			name: "matching db is full",
			body: `
# HELP litestream_disk_full Whether replication is paused because the local disk is full
# TYPE litestream_disk_full gauge
litestream_disk_full{db="/data/test.db"} 1
`,
			dbPath:      "/data/test.db",
			wantPresent: true,
			wantFull:    true,
		},
		{
			name: "matching db recovered",
			body: `
# HELP litestream_disk_full Whether replication is paused because the local disk is full
# TYPE litestream_disk_full gauge
litestream_disk_full{db="/data/test.db"} 0
`,
			dbPath:      "/data/test.db",
			wantPresent: true,
		},
		{
			name: "metric absent",
			body: `
# HELP litestream_sync_count Total sync count
# TYPE litestream_sync_count counter
litestream_sync_count{db="/data/test.db"} 4
`,
			dbPath: "/data/test.db",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotFull, gotPresent, err := parseLitestreamDiskFullMetric(strings.NewReader(test.body), test.dbPath)
			if err != nil {
				t.Fatalf("parseLitestreamDiskFullMetric() error = %v", err)
			}
			if gotPresent != test.wantPresent {
				t.Fatalf("present = %v, want %v", gotPresent, test.wantPresent)
			}
			if gotFull != test.wantFull {
				t.Fatalf("full = %v, want %v", gotFull, test.wantFull)
			}
		})
	}
}

func TestLitestreamDiskFullSignalClassifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
		want bool
	}{
		{name: "distinct staging signal", line: `level=ERROR msg="disk full while staging LTX file /data/.test.db-litestream/ltx/0/000000000001.ltx.tmp: replication paused, will resume automatically when space is freed"`, want: true},
		{name: "no space left", line: `level=ERROR msg="sync" error="no space left on device"`},
		{name: "sqlite full", line: `level=ERROR msg="sync" error="SQLITE_FULL"`},
		{name: "generic sync", line: `level=ERROR msg="sync" error="context deadline exceeded"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, _ := litestreamDiskFullSignal([]string{test.line})
			if got != test.want {
				t.Fatalf("litestreamDiskFullSignal() = %v, want %v", got, test.want)
			}
		})
	}
}
