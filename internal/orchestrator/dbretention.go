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

func (m *Manager) RunDBRetentionLoop(ctx context.Context, retentionDays int) {
	if retentionDays > 0 && retentionDays < minDBRetentionDays {
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
	compacted, err := m.db.CompactRuntimeEvidence(ctx, 8)
	if err != nil && ctx.Err() == nil {
		slog.Error("Failed to compact runtime evidence", "rows_processed", compacted, "error", err)
	}
	snapshots, err := m.db.CompactProfileSnapshots(ctx, 16)
	if err != nil && ctx.Err() == nil {
		slog.Error("Failed to compact profile snapshots", "rows_processed", snapshots, "error", err)
	}
	compacted += snapshots
	if retentionDays <= 0 {
		space, vacuumed := m.reclaimEvidenceSpace()
		if compacted > 0 || vacuumed {
			if _, err := m.db.CheckpointWAL(); err != nil {
				slog.Warn("Failed to checkpoint WAL after runtime compaction", "error", err)
			}
			slog.Info("Maintained runtime evidence database", "runtime_compacted", compacted, "database_space", space)
		}
		return
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)

	verifications, err := m.db.PruneVerificationsBefore(ctx, cutoff)
	if err != nil {
		slog.Error("Failed to prune old verifications", "cutoff", cutoff, "deleted_before_error", verifications, "error", err)
	}
	events, err := m.db.PruneEventsBefore(ctx, cutoff)
	if err != nil {
		slog.Error("Failed to prune old events", "cutoff", cutoff, "deleted_before_error", events, "error", err)
	}
	evidence, err := m.db.PruneArchivedEvidenceBefore(ctx, cutoff)
	if err != nil && ctx.Err() == nil {
		slog.Error("Failed to prune archived evidence", "cutoff", cutoff, "deleted_before_error", evidence, "error", err)
	}

	space, vacuumed := m.reclaimEvidenceSpace()
	if verifications == 0 && events == 0 && evidence["runtime"] == 0 && evidence["verifications"] == 0 && evidence["events"] == 0 && compacted == 0 && !vacuumed {
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
		"events_deleted", events,
		"evidence_deleted", evidence,
		"runtime_compacted", compacted,
		"database_space", space)
}

func (m *Manager) reclaimEvidenceSpace() (map[string]int64, bool) {
	space, err := m.db.EvidenceSpace()
	if err != nil {
		slog.Warn("Failed to inspect evidence database space", "error", err)
		return nil, false
	}
	if space["auto_vacuum"] != 2 || space["freelist_count"] == 0 {
		return space, false
	}
	if err := m.db.IncrementalVacuum(128); err != nil {
		slog.Warn("Failed to incrementally vacuum evidence database", "error", err)
		return space, false
	}
	return space, true
}
