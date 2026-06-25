package worker

import (
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
			if !got.Runtime.DiskPressureNoProgress {
				t.Fatal("runtime DiskPressureNoProgress = false, want true")
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
