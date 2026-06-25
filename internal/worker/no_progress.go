package worker

import (
	"context"
	"log/slog"
	"syscall"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

type diskPressureNoProgressState struct {
	activeSince        time.Time
	lastDBTXID         uint64
	lastReplicaTXID    uint64
	reported           bool
	signalObserved     bool
	signalMessage      string
	recoveryStartedAt  time.Time
	recoveryStartTXID  uint64
	recoveryStartPID   int
	recoveryFreedBytes int64
	recovered          bool
	recoveryFailed     bool
	recoverySeconds    float64
	withoutRestart     bool
}

type diskPressureNoProgressObservation struct {
	Runtime      reporting.RuntimePayload
	Detected     bool
	ShouldReport bool
	EventType    string
	Message      string
}

func (r *Runner) observeDiskPressureNoProgress(now time.Time, snapshot runtimeSnapshot) diskPressureNoProgressObservation {
	runtime := snapshot.RuntimePayload
	window := r.cfg.diskFullNoProgressWindow()
	if window <= 0 {
		r.resetDiskPressureNoProgress()
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, 0))
		return diskPressureNoProgressObservation{Runtime: updated.RuntimePayload}
	}

	if runtime.LitestreamDiskFullMetricPresent && runtime.LitestreamDiskFull {
		ok, message := litestreamDiskFullSignal(r.litestreamLog.Lines())
		if ok {
			r.markDiskFullSignal(message)
		}
	}

	if observation, ok := r.observeDiskFullRecovery(now, runtime); ok {
		return observation
	}

	if !snapshotUnderDiskPressure(runtime) || runtime.ReplicationLagMax == 0 || runtime.DBTXID <= runtime.ReplicatedTXID {
		r.resetDiskPressureNoProgressWindow()
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, 0))
		return diskPressureNoProgressObservation{Runtime: updated.RuntimePayload}
	}

	if r.noProgress.activeSince.IsZero() {
		r.noProgress.activeSince = now
		r.noProgress.lastDBTXID = runtime.DBTXID
		r.noProgress.lastReplicaTXID = runtime.ReplicatedTXID
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, 0))
		return diskPressureNoProgressObservation{Runtime: updated.RuntimePayload}
	}

	localAdvanced := runtime.DBTXID > r.noProgress.lastDBTXID
	replicaFrozen := runtime.ReplicatedTXID <= r.noProgress.lastReplicaTXID
	if !localAdvanced || !replicaFrozen {
		r.noProgress.activeSince = now
		r.noProgress.lastDBTXID = runtime.DBTXID
		r.noProgress.lastReplicaTXID = runtime.ReplicatedTXID
		r.noProgress.reported = false
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, 0))
		return diskPressureNoProgressObservation{Runtime: updated.RuntimePayload}
	}

	elapsed := now.Sub(r.noProgress.activeSince).Seconds()
	r.noProgress.lastDBTXID = runtime.DBTXID
	r.noProgress.lastReplicaTXID = runtime.ReplicatedTXID
	if elapsed < window.Seconds() {
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, elapsed))
		return diskPressureNoProgressObservation{Runtime: updated.RuntimePayload}
	}

	if r.noProgress.signalObserved {
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, elapsed))
		return diskPressureNoProgressObservation{Runtime: updated.RuntimePayload}
	}

	updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, true, elapsed))
	shouldReport := !r.noProgress.reported
	r.noProgress.reported = true
	return diskPressureNoProgressObservation{
		Runtime:      updated.RuntimePayload,
		Detected:     true,
		ShouldReport: shouldReport,
		EventType:    reporting.WorkerEventDiskFullNoProgress,
		Message:      "harness detected no replication progress under disk pressure before Litestream emitted a distinct disk-full signal",
	}
}

func (r *Runner) markDiskFullSignal(message string) {
	if r.noProgress.signalObserved {
		if r.noProgress.signalMessage == "" {
			r.noProgress.signalMessage = message
		}
		return
	}
	r.noProgress.signalObserved = true
	r.noProgress.signalMessage = message
}

func (r *Runner) observeDiskFullRecovery(now time.Time, runtime reporting.RuntimePayload) (diskPressureNoProgressObservation, bool) {
	if !r.noProgress.signalObserved {
		return diskPressureNoProgressObservation{}, false
	}

	if r.noProgress.recoveryStartedAt.IsZero() && r.cfg.DiskFullRecoveryReserve > 0 {
		freed, err := r.freeDiskFullRecoveryReserve()
		if err != nil {
			slog.Warn("Failed to free disk-full recovery reserve", "error", err)
		} else {
			r.noProgress.recoveryStartedAt = now
			r.noProgress.recoveryStartTXID = runtime.ReplicatedTXID
			r.noProgress.recoveryStartPID = r.currentLitestreamPID()
			r.noProgress.recoveryFreedBytes = freed
		}
	}

	if r.noProgress.recoveryStartedAt.IsZero() || r.noProgress.recovered || r.noProgress.recoveryFailed {
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, 0))
		return diskPressureNoProgressObservation{Runtime: updated.RuntimePayload}, false
	}

	currentPID := r.currentLitestreamPID()
	sameProcess := r.noProgress.recoveryStartPID == 0 || currentPID == 0 || currentPID == r.noProgress.recoveryStartPID
	metricCleared := runtime.LitestreamDiskFullMetricPresent && !runtime.LitestreamDiskFull
	if metricCleared && runtime.ReplicatedTXID > r.noProgress.recoveryStartTXID && sameProcess {
		r.noProgress.recovered = true
		r.noProgress.withoutRestart = true
		r.noProgress.recoverySeconds = now.Sub(r.noProgress.recoveryStartedAt).Seconds()
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, 0))
		return diskPressureNoProgressObservation{
			Runtime:      updated.RuntimePayload,
			ShouldReport: true,
			EventType:    reporting.WorkerEventDiskFullRecovered,
			Message:      "Litestream emitted a distinct disk-full signal and replicated again after the harness freed reserved disk space without restart",
		}, true
	}

	timeout := r.cfg.diskFullRecoveryTimeout()
	if (!sameProcess || (timeout > 0 && now.Sub(r.noProgress.recoveryStartedAt) >= timeout)) && !r.noProgress.recoveryFailed {
		r.noProgress.recoveryFailed = true
		r.noProgress.recoverySeconds = now.Sub(r.noProgress.recoveryStartedAt).Seconds()
		updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, 0))
		return diskPressureNoProgressObservation{
			Runtime:      updated.RuntimePayload,
			ShouldReport: true,
			EventType:    reporting.WorkerEventDiskFullRecoveryFailed,
			Message:      "Litestream emitted a distinct disk-full signal but did not replicate again after reserved disk space was freed",
		}, true
	}

	updated := r.setDiskFullRuntime(r.diskFullRuntime(now, runtime, false, 0))
	return diskPressureNoProgressObservation{Runtime: updated.RuntimePayload}, false
}

func (r *Runner) diskFullRuntime(now time.Time, runtime reporting.RuntimePayload, noProgress bool, noProgressSeconds float64) reporting.RuntimePayload {
	runtime.DiskPressureNoProgress = noProgress
	if noProgress || noProgressSeconds > 0 {
		runtime.DiskPressureNoProgressSeconds = noProgressSeconds
	} else {
		runtime.DiskPressureNoProgressSeconds = 0
	}
	if r.noProgress.signalObserved {
		runtime.DiskFullSignalObserved = true
		runtime.DiskFullSignalMessage = r.noProgress.signalMessage
	}
	if !r.noProgress.recoveryStartedAt.IsZero() {
		runtime.DiskFullRecoveryAttempted = true
		runtime.DiskFullRecoveryFreedBytes = r.noProgress.recoveryFreedBytes
		if r.noProgress.recoverySeconds > 0 {
			runtime.DiskFullRecoverySeconds = r.noProgress.recoverySeconds
		} else {
			runtime.DiskFullRecoverySeconds = now.Sub(r.noProgress.recoveryStartedAt).Seconds()
		}
	}
	runtime.DiskFullRecovered = r.noProgress.recovered
	runtime.DiskFullRecoveryWithoutRestart = r.noProgress.withoutRestart
	return runtime
}

func (r *Runner) resetDiskPressureNoProgress() {
	r.noProgress = diskPressureNoProgressState{}
}

func (r *Runner) resetDiskPressureNoProgressWindow() {
	r.noProgress.activeSince = time.Time{}
	r.noProgress.lastDBTXID = 0
	r.noProgress.lastReplicaTXID = 0
	r.noProgress.reported = false
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

func (p *statsPoller) setDiskFullRuntime(runtime reporting.RuntimePayload) runtimeSnapshot {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.DiskPressureNoProgress = runtime.DiskPressureNoProgress
	p.snapshot.DiskPressureNoProgressSeconds = runtime.DiskPressureNoProgressSeconds
	p.snapshot.LitestreamDiskFullMetricPresent = runtime.LitestreamDiskFullMetricPresent
	p.snapshot.LitestreamDiskFull = runtime.LitestreamDiskFull
	p.snapshot.DiskFullSignalObserved = runtime.DiskFullSignalObserved
	p.snapshot.DiskFullSignalMessage = runtime.DiskFullSignalMessage
	p.snapshot.DiskFullRecoveryAttempted = runtime.DiskFullRecoveryAttempted
	p.snapshot.DiskFullRecoveryFreedBytes = runtime.DiskFullRecoveryFreedBytes
	p.snapshot.DiskFullRecovered = runtime.DiskFullRecovered
	p.snapshot.DiskFullRecoverySeconds = runtime.DiskFullRecoverySeconds
	p.snapshot.DiskFullRecoveryWithoutRestart = runtime.DiskFullRecoveryWithoutRestart
	return p.snapshot
}

func (r *Runner) sendDiskFullEvent(ctx context.Context, observation diskPressureNoProgressObservation) {
	if r.reporter == nil || !r.reporter.Enabled() || observation.EventType == "" {
		return
	}

	reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.reporter.SendEvent(reportCtx, reporting.WorkerEventPayload{
		EventType:      observation.EventType,
		Message:        observation.Message,
		SentAt:         time.Now().UTC(),
		RuntimePayload: observation.Runtime,
	}); err != nil {
		slog.Warn("Failed to send disk-full event", "event_type", observation.EventType, "error", err)
	}
}

func (c Config) diskFullNoProgressWindow() time.Duration {
	if c.DiskFullNoProgressWindow > 0 {
		return c.DiskFullNoProgressWindow
	}
	return DefaultConfig().DiskFullNoProgressWindow
}
