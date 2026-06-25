package worker

import (
	"context"
	"log/slog"
	"syscall"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const diskPressureNoProgressEventType = "platform_disk_full_no_progress"

type diskPressureNoProgressState struct {
	activeSince     time.Time
	lastDBTXID      uint64
	lastReplicaTXID uint64
	reported        bool
}

type diskPressureNoProgressObservation struct {
	Runtime      reporting.RuntimePayload
	Detected     bool
	ShouldReport bool
}

func (r *Runner) observeDiskPressureNoProgress(now time.Time, snapshot runtimeSnapshot) diskPressureNoProgressObservation {
	runtime := snapshot.RuntimePayload
	window := r.cfg.diskFullNoProgressWindow()
	if window <= 0 {
		r.resetDiskPressureNoProgress()
		r.setDiskPressureNoProgress(false, 0)
		return diskPressureNoProgressObservation{Runtime: runtime}
	}

	if !snapshotUnderDiskPressure(runtime) || runtime.ReplicationLagMax == 0 || runtime.DBTXID <= runtime.ReplicatedTXID {
		r.resetDiskPressureNoProgress()
		r.setDiskPressureNoProgress(false, 0)
		return diskPressureNoProgressObservation{Runtime: runtime}
	}

	if r.noProgress.activeSince.IsZero() {
		r.noProgress.activeSince = now
		r.noProgress.lastDBTXID = runtime.DBTXID
		r.noProgress.lastReplicaTXID = runtime.ReplicatedTXID
		r.setDiskPressureNoProgress(false, 0)
		return diskPressureNoProgressObservation{Runtime: runtime}
	}

	localAdvanced := runtime.DBTXID > r.noProgress.lastDBTXID
	replicaFrozen := runtime.ReplicatedTXID <= r.noProgress.lastReplicaTXID
	if !localAdvanced || !replicaFrozen {
		r.noProgress.activeSince = now
		r.noProgress.lastDBTXID = runtime.DBTXID
		r.noProgress.lastReplicaTXID = runtime.ReplicatedTXID
		r.noProgress.reported = false
		r.setDiskPressureNoProgress(false, 0)
		return diskPressureNoProgressObservation{Runtime: runtime}
	}

	elapsed := now.Sub(r.noProgress.activeSince).Seconds()
	r.noProgress.lastDBTXID = runtime.DBTXID
	r.noProgress.lastReplicaTXID = runtime.ReplicatedTXID
	if elapsed < window.Seconds() {
		r.setDiskPressureNoProgress(false, elapsed)
		return diskPressureNoProgressObservation{Runtime: runtime}
	}

	runtime.DiskPressureNoProgress = true
	runtime.DiskPressureNoProgressSeconds = elapsed
	updated := r.setDiskPressureNoProgress(true, elapsed)
	shouldReport := !r.noProgress.reported
	r.noProgress.reported = true
	return diskPressureNoProgressObservation{
		Runtime:      updated.RuntimePayload,
		Detected:     true,
		ShouldReport: shouldReport,
	}
}

func (r *Runner) resetDiskPressureNoProgress() {
	r.noProgress = diskPressureNoProgressState{}
}

func snapshotUnderDiskPressure(runtime reporting.RuntimePayload) bool {
	if runtime.DataDiskTotalBytes == 0 {
		return false
	}
	if runtime.DataDiskUsedPercent >= 95 {
		return true
	}
	estimatedSnapshotBytes := runtime.DBTotalSizeBytes
	if estimatedSnapshotBytes <= 0 {
		estimatedSnapshotBytes = runtime.DBSizeBytes
	}
	if estimatedSnapshotBytes <= 0 {
		return false
	}
	return runtime.DataDiskAvailableBytes < uint64(estimatedSnapshotBytes)
}

func collectDiskPressureRuntime(cfg Config) reporting.RuntimePayload {
	runtime := reporting.RuntimePayload{}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(cfg.DataDir, &stat); err == nil {
		total := stat.Blocks * uint64(stat.Bsize)
		free := stat.Bfree * uint64(stat.Bsize)
		runtime.DataDiskTotalBytes = total
		runtime.DataDiskFreeBytes = free
		runtime.DataDiskAvailableBytes = stat.Bavail * uint64(stat.Bsize)
		runtime.DataDiskUsedBytes = total - free
		if total > 0 {
			runtime.DataDiskUsedPercent = float64(runtime.DataDiskUsedBytes) / float64(total) * 100
		}
	}
	if cfg.ManyDBEnabled() {
		for _, dbPath := range cfg.ManyDBPaths() {
			runtime.DBTotalSizeBytes += fileSize(dbPath)
			runtime.WALTotalSizeBytes += fileSize(dbPath + "-wal")
		}
		return runtime
	}
	runtime.DBSizeBytes = fileSize(cfg.DBPath)
	runtime.WALSizeBytes = fileSize(cfg.DBPath + "-wal")
	return runtime
}

func (p *statsPoller) setDiskPressureNoProgress(active bool, seconds float64) runtimeSnapshot {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.DiskPressureNoProgress = active
	if active || seconds > 0 {
		p.snapshot.DiskPressureNoProgressSeconds = seconds
	} else {
		p.snapshot.DiskPressureNoProgressSeconds = 0
	}
	return p.snapshot
}

func (r *Runner) sendDiskPressureNoProgressEvent(ctx context.Context, runtime reporting.RuntimePayload) {
	if r.reporter == nil || !r.reporter.Enabled() {
		return
	}

	reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.reporter.SendEvent(reportCtx, reporting.WorkerEventPayload{
		EventType:      diskPressureNoProgressEventType,
		Message:        "harness detected no replication progress while the data disk cannot stage the current database snapshot",
		SentAt:         time.Now().UTC(),
		RuntimePayload: runtime,
	}); err != nil {
		slog.Warn("Failed to send disk pressure no-progress event", "error", err)
	}
}

func (c Config) diskFullNoProgressWindow() time.Duration {
	if c.DiskFullNoProgressWindow > 0 {
		return c.DiskFullNoProgressWindow
	}
	return DefaultConfig().DiskFullNoProgressWindow
}
