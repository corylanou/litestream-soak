package worker

import (
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func levelReport(levels map[int]reporting.ReplicaLevelSummary) *reporting.ReplicaLevelReport {
	report := &reporting.ReplicaLevelReport{Complete: true}
	for level := 0; level <= 9; level++ {
		summary := levels[level]
		summary.Level = level
		report.Levels = append(report.Levels, summary)
	}
	return report
}

func levelReportWith(base *reporting.ReplicaLevelReport, mutate func(*reporting.ReplicaLevelReport)) *reporting.ReplicaLevelReport {
	report := *base
	report.Levels = append([]reporting.ReplicaLevelSummary(nil), base.Levels...)
	mutate(&report)
	return &report
}

func TestCompactionProgressError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CompactionL0ObjectBound = 100
	cfg.CompactionL1ObjectBound = 500
	previous := levelReport(map[int]reporting.ReplicaLevelSummary{
		0: {ObjectCount: 40, MaxTXIDDecimal: 1000},
		1: {ObjectCount: 20, MaxTXIDDecimal: 990},
		3: {ObjectCount: 2, MaxTXIDDecimal: 500},
	})
	tests := []struct {
		name     string
		previous *reporting.ReplicaLevelReport
		current  *reporting.ReplicaLevelReport
		wantErr  string
	}{
		{
			name:     "L1 advanced with L0",
			previous: previous,
			current:  levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 45, MaxTXIDDecimal: 1200}, 1: {ObjectCount: 22, MaxTXIDDecimal: 1190}, 3: {ObjectCount: 2, MaxTXIDDecimal: 500}}),
		},
		{
			name:     "only L3 advanced",
			previous: previous,
			current:  levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 45, MaxTXIDDecimal: 1200}, 1: {ObjectCount: 20, MaxTXIDDecimal: 990}, 3: {ObjectCount: 3, MaxTXIDDecimal: 1100}}),
		},
		{
			name:     "L0 advanced while L1-L8 stayed",
			previous: previous,
			current:  levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 90, MaxTXIDDecimal: 1200}, 1: {ObjectCount: 20, MaxTXIDDecimal: 990}, 3: {ObjectCount: 2, MaxTXIDDecimal: 500}, 9: {ObjectCount: 1, MaxTXIDDecimal: 1150}}),
			wantErr:  "compaction stalled: L0 advanced from 00000000000003e8 to 00000000000004b0 while L1-L8 stayed at 00000000000003de",
		},
		{
			name:     "no writes since previous report",
			previous: previous,
			current:  previous,
		},
		{
			name:    "L0 over bound without previous report",
			current: levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 101, MaxTXIDDecimal: 1200}}),
			wantErr: "L0 holds 101 objects, bound 100",
		},
		{
			name:    "L1 over bound",
			current: levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 10, MaxTXIDDecimal: 1200}, 1: {ObjectCount: 501, MaxTXIDDecimal: 1190}}),
			wantErr: "L1 holds 501 objects, bound 500",
		},
		{
			name:     "listing failure is not judged",
			previous: previous,
			current:  &reporting.ReplicaLevelReport{Error: "command timed out"},
		},
		{
			name:     "previous listing failure skips the progress rule",
			previous: &reporting.ReplicaLevelReport{Error: "no ltx entries parsed"},
			current:  levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 90, MaxTXIDDecimal: 1200}}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := compactionProgressError(cfg, tt.previous, tt.current)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestCompactionProgressGuardFailsPassingCycleAndKeepsBaseline(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CompactionProgressCheck = true
	cfg.ReplicaLevelReporting = true
	r := &Runner{cfg: cfg}
	first := levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 10, MaxTXIDDecimal: 100}, 1: {ObjectCount: 5, MaxTXIDDecimal: 90}})
	passed := VerificationResult{Status: "passed", Passed: true, Summary: "verification passed"}

	result := r.applyCompactionProgressGuard(passed, first)
	if !result.Passed || len(result.Steps) != 1 || result.Steps[0].Name != compactionProgressStep || result.Steps[0].Status != "ok" {
		t.Fatalf("first cycle = %+v", result)
	}
	for name, broken := range map[string]*reporting.ReplicaLevelReport{
		"failed listing":     {Error: "command timed out"},
		"truncated listing":  levelReportWith(first, func(report *reporting.ReplicaLevelReport) { report.Truncated = true }),
		"incomplete listing": levelReportWith(first, func(report *reporting.ReplicaLevelReport) { report.Complete = false }),
	} {
		result = r.applyCompactionProgressGuard(passed, broken)
		if result.Passed || result.Status != "aborted" || !strings.HasPrefix(result.Summary, "verification inconclusive:") || !strings.Contains(result.ErrorMessage, "replica level report incomplete") {
			t.Fatalf("%s: result = %+v, want inconclusive", name, result)
		}
		if result.InconclusiveClassification == nil || result.InconclusiveClassification.Signature != reporting.SignatureReplicaLevelReportIncomplete || result.InconclusiveClassification.Stage != compactionProgressStep {
			t.Fatalf("%s: classification = %+v", name, result.InconclusiveClassification)
		}
		if strings.Contains(result.ErrorMessage, "stalled") || r.lastReplicaLevels != first {
			t.Fatalf("%s: incomplete report was judged or became the baseline: %+v", name, result)
		}
		if err := compactionProgressError(cfg, first, broken); err != nil {
			t.Fatalf("%s: compactionProgressError = %v, want nil", name, err)
		}
	}
	stalled := levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 30, MaxTXIDDecimal: 300}, 1: {ObjectCount: 5, MaxTXIDDecimal: 90}})
	result = r.applyCompactionProgressGuard(passed, stalled)
	if result.Passed || result.Status != "failed" || result.Summary != "compaction progress check failed" || !strings.Contains(result.ErrorMessage, "compaction stalled") {
		t.Fatalf("stalled cycle = %+v", result)
	}
	if r.lastReplicaLevels != stalled {
		t.Fatal("stalled report did not become the next baseline")
	}
	failed := VerificationResult{Status: "failed", Passed: false, Summary: "restore failed", ErrorMessage: "restore failed"}
	if result = r.applyCompactionProgressGuard(failed, stalled); result.ErrorMessage != "restore failed" {
		t.Fatalf("guard replaced the original failure: %+v", result)
	}
	r.cfg.CompactionProgressCheck = false
	if result = r.applyCompactionProgressGuard(passed, stalled); len(result.Steps) != 0 || !result.Passed {
		t.Fatalf("disabled guard changed the result: %+v", result)
	}
}
