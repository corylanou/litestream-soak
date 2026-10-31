package worker

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func (v *Verifier) runManyDBCycle(ctx context.Context) (result VerificationResult, retErr error) {
	start := time.Now()
	result = VerificationResult{
		StartedAt:   start.UTC(),
		CheckType:   v.cfg.VerifyType,
		Status:      "running",
		RestoreMode: restoreModeTXID,
	}
	defer func() {
		RecordVerificationOutcome(result.Status, time.Since(start).Seconds())
	}()
	slog.Info("Starting many database verification cycle")
	if v.onStart != nil {
		v.onStart(ctx, result)
	}

	if err := recordVerificationStep(&result, "pause_load", func() error {
		return v.pauseLoad(ctx)
	}); err != nil {
		v.failResult(ctx, &result, fmt.Sprintf("pause load: %v", err))
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("pause load: %w", err)
	}
	defer func() {
		_ = recordVerificationStep(&result, "resume_load", func() error {
			v.resumeLoad()
			return nil
		})
	}()

	select {
	case <-ctx.Done():
		v.failResult(ctx, &result, ctx.Err().Error())
		return result, ctx.Err()
	case <-time.After(2 * time.Second):
	}

	var changed []string
	generations := make(map[string]manyDBChange)
	var batch manyDBBatch
	if v.manyDBChanges != nil {
		for _, change := range v.manyDBChanges.pendingManyDBChanges() {
			changed = append(changed, change.path)
			generations[change.path] = change
		}
		defer func() {
			batch.coverage = v.manyDBChanges.manyDBPendingCoverage()
			batch.stats = manyDBBatchStats(result.Steps)
			_ = recordVerificationStep(&result, "pending_coverage", func() error { return nil })
			result.Steps[len(result.Steps)-1].OutputTail = batch.String()
			slog.Info("Many database pending coverage", "pending_databases", batch.coverage.pending, "written_pending", batch.coverage.written, "oldest_pending_age_seconds", batch.coverage.oldestAge)
		}()
	}
	targets, totalChanged := selectManyDBVerificationTargets(v.cfg, changed)
	if len(targets) == 0 {
		v.failResult(ctx, &result, "many database verification target set was empty")
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("many database verification target set was empty")
	}
	batch.targets, batch.truncated = len(targets), totalChanged-len(targets)
	if totalChanged > len(targets) {
		slog.Warn("Many database verification changed set truncated",
			"changed_databases", totalChanged,
			"target_databases", len(targets),
			"limit", v.cfg.manyDBVerifyChangedLimit(),
		)
		if v.onManyDBTruncated != nil {
			v.onManyDBTruncated(totalChanged, len(targets), v.cfg.manyDBVerifyChangedLimit())
		}
	}

	for _, dbPath := range targets {
		if v.manyDBChanges != nil {
			v.manyDBChanges.attemptManyDBChange(manyDBChange{path: dbPath})
		}
		name := filepath.Base(dbPath)
		restoredPath := dbPath + ".restored"
		if err := recordVerificationStep(&result, "clean_restored "+name, func() error {
			return removeRestoredArtifacts(restoredPath)
		}); err != nil {
			v.failResult(ctx, &result, fmt.Sprintf("clean restored artifacts for %s: %v", name, err))
			v.logResult(start, false, result.ErrorMessage)
			return result, fmt.Errorf("clean restored artifacts for %s: %w", name, err)
		}
		defer func(path string) {
			if err := removeRestoredArtifacts(path); err != nil {
				slog.Warn("Failed to remove restored artifacts", "path", path, "error", err)
			}
		}(restoredPath)

		if err := recordVerificationStep(&result, "checkpoint "+name, func() error {
			residualBusy, cpErr := v.checkpointDB(ctx, dbPath, false)
			result.CheckpointResidualBusy = result.CheckpointResidualBusy || residualBusy
			return cpErr
		}); err != nil {
			v.failResult(ctx, &result, fmt.Sprintf("checkpoint %s: %v", name, err))
			v.logResult(start, false, result.ErrorMessage)
			return result, fmt.Errorf("checkpoint %s: %w", name, err)
		}

		if err := recordVerificationStep(&result, "sync "+name, func() error {
			return v.waitForSyncDB(ctx, &result, dbPath)
		}); err != nil {
			v.failResult(ctx, &result, fmt.Sprintf("wait for sync %s: %v", name, err))
			v.logResult(start, false, result.ErrorMessage)
			return result, fmt.Errorf("wait for sync %s: %w", name, err)
		}

		var passed bool
		var err error
		validateErr := recordVerificationStep(&result, "restore_validate "+name, func() error {
			passed, err = v.validateDBResult(ctx, dbPath, restoredPath, &result)
			return err
		})
		result.Steps[len(result.Steps)-1].OutputTail += "\n" + v.logicalEvidence
		for _, step := range v.validationSteps {
			step.Name += " " + name
			result.Steps = append(result.Steps, step)
		}
		if validateErr != nil {
			v.failValidationResult(ctx, &result, fmt.Errorf("restore validate %s: %w", name, validateErr))
			slog.Error("Many database verification failed", "db", dbPath, "error", validateErr, "duration", time.Since(start))
			v.logResult(start, false, result.ErrorMessage)
			return result, validateErr
		}
		if !passed {
			v.failResult(ctx, &result, fmt.Sprintf("validation returned false for %s", name))
			slog.Error("Many database verification FAILED", "db", dbPath, "duration", time.Since(start))
			v.logResult(start, false, result.ErrorMessage)
			return result, nil
		}
		if err := removeRestoredArtifacts(restoredPath); err != nil {
			v.failResult(ctx, &result, fmt.Sprintf("clean restored artifacts for %s: %v", name, err))
			v.logResult(start, false, result.ErrorMessage)
			return result, err
		}
		if change, ok := generations[dbPath]; ok {
			v.manyDBChanges.acknowledgeManyDBChange(change)
		}
	}

	if v.manyDBChanges != nil {
		batch.coverage = v.manyDBChanges.manyDBPendingCoverage()
	}
	batch.stats = manyDBBatchStats(result.Steps)
	result.Status, result.Summary = batch.outcome()
	result.Passed = result.Status == "passed"
	v.finalizeResult(&result)
	if result.Passed {
		slog.Info("Many database verification passed", "verified_databases", len(targets), "changed_databases", totalChanged, "seeded_backlog", batch.coverage.pending, "duration", time.Since(start))
		v.logResult(start, true, "")
	}
	return result, nil
}

// manyDBBatch describes one verification batch for the summary and the
// pending_coverage step: what it verified, what it could not fit, the pending
// set it left behind, and how long sync and restore took per database.
type manyDBBatch struct {
	targets   int
	truncated int
	coverage  manyDBCoverage
	stats     manyDBStepStats
}

// outcome judges a clean batch. Databases the workload wrote to that did not
// fit the batch leave coverage pending; a remaining seeded backlog of
// databases that were never written does not, since everything the workload
// touched since the last cycle restored correctly, and the backlog is
// reported instead.
func (b manyDBBatch) outcome() (string, string) {
	if b.coverage.written > 0 {
		return "pending", fmt.Sprintf("verified batch of %d databases; coverage pending for %d written databases (%d pending in all, oldest %.3fs); %s", b.targets, b.coverage.written, b.coverage.pending, b.coverage.oldestAge, b.stats)
	}
	return "passed", fmt.Sprintf("verification passed (%d databases verified, %d written databases still pending; seeded backlog %d, oldest %.3fs); %s", b.targets, b.coverage.written, b.coverage.pending, b.coverage.oldestAge, b.stats)
}

func (b manyDBBatch) String() string {
	return fmt.Sprintf("targets=%d truncated=%d pending_databases=%d written_pending=%d oldest_pending_age_seconds=%.3f %s", b.targets, b.truncated, b.coverage.pending, b.coverage.written, b.coverage.oldestAge, b.stats)
}

// manyDBStepStats aggregates the per-database sync and restore_validate step
// durations of one batch.
type manyDBStepStats struct {
	sync    manyDBDurationStats
	restore manyDBDurationStats
}

type manyDBDurationStats struct {
	count int
	total time.Duration
	max   time.Duration
}

func (s *manyDBDurationStats) add(d time.Duration) {
	s.count++
	s.total += d
	s.max = max(s.max, d)
}

func (s manyDBDurationStats) mean() time.Duration {
	if s.count == 0 {
		return 0
	}
	return s.total / time.Duration(s.count)
}

func manyDBBatchStats(steps []reporting.VerificationStep) manyDBStepStats {
	var stats manyDBStepStats
	for _, step := range steps {
		duration := time.Duration(step.DurationMS) * time.Millisecond
		switch {
		case strings.HasPrefix(step.Name, "sync "):
			stats.sync.add(duration)
		case strings.HasPrefix(step.Name, "restore_validate "):
			stats.restore.add(duration)
		}
	}
	return stats
}

func (s manyDBStepStats) String() string {
	return fmt.Sprintf("sync_mean_seconds=%.1f sync_max_seconds=%.1f restore_mean_seconds=%.1f restore_max_seconds=%.1f", s.sync.mean().Seconds(), s.sync.max.Seconds(), s.restore.mean().Seconds(), s.restore.max.Seconds())
}
