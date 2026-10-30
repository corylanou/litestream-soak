package worker

import (
	"fmt"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const compactionProgressStep = "compaction_progress"

func replicaLevelSummary(report *reporting.ReplicaLevelReport, level int) reporting.ReplicaLevelSummary {
	if report == nil {
		return reporting.ReplicaLevelSummary{Level: level, LevelName: fmt.Sprintf("%04d", level)}
	}
	for _, summary := range report.Levels {
		if summary.Level == level {
			return summary
		}
	}
	return reporting.ReplicaLevelSummary{Level: level, LevelName: fmt.Sprintf("%04d", level)}
}

func usableReplicaLevelReport(report *reporting.ReplicaLevelReport) bool {
	return report != nil && report.Error == "" && len(report.Levels) > 0
}

// compactionProgressError judges one replica level report against the
// previous one: L0 and L1 must stay inside their object bounds, and when L0
// advanced since the previous report at least one compaction level (L1-L8)
// must have advanced too. Reports that failed to list are not judged.
func compactionProgressError(cfg Config, previous, current *reporting.ReplicaLevelReport) error {
	if !usableReplicaLevelReport(current) {
		return nil
	}
	for level, bound := range map[int]int{0: cfg.CompactionL0ObjectBound, 1: cfg.CompactionL1ObjectBound} {
		if count := replicaLevelSummary(current, level).ObjectCount; bound > 0 && count > bound {
			return fmt.Errorf("compaction stalled: L%d holds %d objects, bound %d", level, count, bound)
		}
	}
	if !usableReplicaLevelReport(previous) {
		return nil
	}
	previousL0, currentL0 := replicaLevelSummary(previous, 0), replicaLevelSummary(current, 0)
	if currentL0.MaxTXIDDecimal <= previousL0.MaxTXIDDecimal {
		return nil
	}
	var highest uint64
	for level := 1; level <= 8; level++ {
		before, after := replicaLevelSummary(previous, level), replicaLevelSummary(current, level)
		if after.MaxTXIDDecimal > before.MaxTXIDDecimal {
			return nil
		}
		highest = max(highest, after.MaxTXIDDecimal)
	}
	return fmt.Errorf("compaction stalled: L0 advanced from %s to %s while L1-L8 stayed at %s", formatTXID(previousL0.MaxTXIDDecimal), formatTXID(currentL0.MaxTXIDDecimal), formatTXID(highest))
}

func compactionProgressSummary(report *reporting.ReplicaLevelReport) string {
	if !usableReplicaLevelReport(report) {
		return "replica levels unavailable"
	}
	l0, l1, l9 := replicaLevelSummary(report, 0), replicaLevelSummary(report, 1), replicaLevelSummary(report, 9)
	return fmt.Sprintf("L0 objects=%d max_txid=%s; L1 objects=%d max_txid=%s; L9 objects=%d max_txid=%s", l0.ObjectCount, formatTXID(l0.MaxTXIDDecimal), l1.ObjectCount, formatTXID(l1.MaxTXIDDecimal), l9.ObjectCount, formatTXID(l9.MaxTXIDDecimal))
}

func (r *Runner) candidateL9Note(l9 reporting.ReplicaLevelSummary, now time.Time) string {
	first, err := recordFirstCandidateL9(r.cfg.DataDir, l9, now)
	switch {
	case err != nil:
		return "candidate first L9: unknown (" + err.Error() + ")"
	case first == nil:
		return "candidate first L9: not yet written since the switch purge"
	default:
		return fmt.Sprintf("candidate first L9: observed %s at max_txid %s", first.ObservedAt.Format(time.RFC3339), first.MaxTXID)
	}
}

// applyCompactionProgressGuard records the compaction verdict for this cycle
// as a step and fails an otherwise passing verification when compaction has
// stalled. The report becomes the baseline for the next cycle.
func (r *Runner) applyCompactionProgressGuard(result VerificationResult, levels *reporting.ReplicaLevelReport) VerificationResult {
	if !r.cfg.CompactionProgressCheck || levels == nil {
		return result
	}
	previous := r.lastReplicaLevels
	if usableReplicaLevelReport(levels) {
		r.lastReplicaLevels = levels
	}
	err := compactionProgressError(r.cfg, previous, levels)
	now := time.Now().UTC()
	step := reporting.VerificationStep{Name: compactionProgressStep, StartedAt: now, CompletedAt: now, Status: "ok", OutputTail: compactionProgressSummary(levels)}
	if r.cfg.UpgradeDeleteL9AtSwitch && r.cfg.upgradePhase == UpgradePhasePost && usableReplicaLevelReport(levels) {
		step.OutputTail += "; " + r.candidateL9Note(replicaLevelSummary(levels, 9), now)
	}
	if err != nil {
		step.Status = "error"
		step.Error = err.Error()
	}
	result.Steps = append(result.Steps, step)
	if err == nil || !result.Passed {
		return result
	}
	return failedS3FaultGuardResult(result, "compaction progress check failed", err.Error())
}
