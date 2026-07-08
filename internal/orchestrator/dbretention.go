package orchestrator

import (
	"context"
	"log/slog"
	"time"
)

// minDBRetentionDays keeps retention above every lifecycle window that reads
// verification history (dormancy streaks, success-teardown thresholds, and
// deployment scorecards all operate well inside 7 days).
const minDBRetentionDays = 7

// RunDBRetentionLoop prunes old verification and event history every hour so
// the control-plane database stops growing without bound (it filled its
// volume on 2026-07-02). retentionDays <= 0 disables the loop.
func (m *Manager) RunDBRetentionLoop(ctx context.Context, retentionDays int) {
	if retentionDays <= 0 {
		return
	}
	if retentionDays < minDBRetentionDays {
		slog.Warn("Clamping DB retention to the lifecycle-safe minimum",
			"requested_days", retentionDays, "min_days", minDBRetentionDays)
		retentionDays = minDBRetentionDays
	}

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	m.pruneDBOnce(ctx, retentionDays)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pruneDBOnce(ctx, retentionDays)
		}
	}
}

func (m *Manager) pruneDBOnce(ctx context.Context, retentionDays int) {
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)

	verifications, err := m.db.PruneVerificationsBefore(ctx, cutoff)
	if err != nil {
		slog.Error("Failed to prune old verifications", "cutoff", cutoff, "deleted_before_error", verifications, "error", err)
	}
	events, err := m.db.PruneEventsBefore(ctx, cutoff)
	if err != nil {
		slog.Error("Failed to prune old events", "cutoff", cutoff, "deleted_before_error", events, "error", err)
	}

	if verifications == 0 && events == 0 {
		return
	}
	checkpoint, err := m.db.CheckpointWAL()
	switch {
	case err != nil:
		slog.Warn("Failed to checkpoint WAL after retention prune", "error", err)
	case checkpoint.Busy != 0:
		slog.Warn("WAL checkpoint could not truncate (concurrent readers); will retry next cycle",
			"busy", checkpoint.Busy, "log_frames", checkpoint.LogFrames, "checkpointed", checkpoint.Checkpointed)
	}
	slog.Info("Pruned old database history",
		"cutoff", cutoff,
		"retention_days", retentionDays,
		"verifications_deleted", verifications,
		"events_deleted", events)
}
