package orchestrator

import (
	"context"
	"log/slog"
	"time"
)

// RunDBRetentionLoop prunes old verification and event history every hour so
// the control-plane database stops growing without bound (it filled its
// volume on 2026-07-02). retentionDays <= 0 disables the loop.
func (m *Manager) RunDBRetentionLoop(ctx context.Context, retentionDays int) {
	if retentionDays <= 0 {
		return
	}

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	m.pruneDBOnce(retentionDays)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pruneDBOnce(retentionDays)
		}
	}
}

func (m *Manager) pruneDBOnce(retentionDays int) {
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)

	verifications, err := m.db.PruneVerificationsBefore(cutoff)
	if err != nil {
		slog.Error("Failed to prune old verifications", "cutoff", cutoff, "error", err)
	}
	events, err := m.db.PruneEventsBefore(cutoff)
	if err != nil {
		slog.Error("Failed to prune old events", "cutoff", cutoff, "error", err)
	}

	if verifications == 0 && events == 0 {
		return
	}
	if err := m.db.CheckpointWAL(); err != nil {
		slog.Warn("Failed to checkpoint WAL after retention prune", "error", err)
	}
	slog.Info("Pruned old database history",
		"cutoff", cutoff,
		"retention_days", retentionDays,
		"verifications_deleted", verifications,
		"events_deleted", events)
}
