package worker

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"
)

func (v *Verifier) runManyDBCycle(ctx context.Context) (result VerificationResult, retErr error) {
	start := time.Now()
	result = VerificationResult{
		StartedAt: start.UTC(),
		CheckType: v.cfg.VerifyType,
		Status:    "running",
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

	time.Sleep(2 * time.Second)

	var changed []string
	if v.manyDBChanges != nil {
		changed = v.manyDBChanges.manyDBChangedPathsAndReset()
	}
	targets, totalChanged := selectManyDBVerificationTargets(v.cfg, changed)
	if len(targets) == 0 {
		v.failResult(ctx, &result, "many database verification target set was empty")
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("many database verification target set was empty")
	}
	if totalChanged > len(targets) {
		slog.Warn("Many database verification changed set truncated",
			"changed_databases", totalChanged,
			"target_databases", len(targets),
			"limit", v.cfg.manyDBVerifyChangedLimit(),
		)
	}

	for _, dbPath := range targets {
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
			residualBusy, cpErr := v.checkpointDB(ctx, dbPath)
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
			passed, err = v.validateDB(ctx, dbPath, restoredPath, result.restoreTXID())
			return err
		})
		if validateErr != nil {
			v.failResult(ctx, &result, fmt.Sprintf("restore validate %s: %v", name, validateErr))
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
	}

	result.Status = "passed"
	result.Passed = true
	result.Summary = fmt.Sprintf("verification passed (%d changed databases)", len(targets))
	v.finalizeResult(&result)
	slog.Info("Many database verification passed", "verified_databases", len(targets), "changed_databases", totalChanged, "duration", time.Since(start))
	v.logResult(start, true, "")
	return result, nil
}
