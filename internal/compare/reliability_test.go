package compare

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalControlUsesSharedEligibility(t *testing.T) {
	p, l := localFixture(t)
	runs, err := Schedule(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range runs {
		if r.Litestream {
			continue
		}
		o, err := l.Execute(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		if o.RunEvidence == nil || o.RunEvidence.Eligible || o.RunEvidence.CoverageComplete || o.RunEvidence.VerificationCount != 1 || o.RunEvidence.WorkloadMutations != 8 {
			t.Fatalf("evidence=%+v", o.RunEvidence)
		}
		if o.Reliability != "unavailable" || len(o.RunEvidence.EligibilityReasons) == 0 {
			t.Fatalf("bounded execution received clean-soak credit: %+v", o)
		}
		return
	}
	t.Fatal("no control scheduled")
}

func TestSharedMaintenanceRetainsRecoveryAndUnknownHistory(t *testing.T) {
	dir := t.TempDir()
	log := "level=INFO msg=\"snapshot complete\" size=10\nlevel=INFO msg=\"compaction complete\" size=10\nlevel=INFO msg=\"l0 retention enforced\" deleted_count=1\nlevel=INFO msg=\"retry recovered\"\nunknown format\n"
	if err := os.WriteFile(filepath.Join(dir, "replicate.log"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}
	p := pinnedPlan(t)
	runs, err := Schedule(p)
	if err != nil {
		t.Fatal(err)
	}
	o := Observation{Request: runs[0], Correctness: "pass", StartedAt: time.Now(), FinishedAt: time.Now(), CompletedOperations: 100}
	if err := recordLogEvidence(dir, &o); err != nil {
		t.Fatal(err)
	}
	attachRunEvidence(&o)
	if o.Maintenance == nil || o.Maintenance.Complete || o.Maintenance.Snapshots != 1 || o.Maintenance.Compactions != 1 || o.Maintenance.Retentions != 1 || o.Maintenance.Errors != 1 {
		t.Fatalf("maintenance=%+v", o.Maintenance)
	}
	if o.Reliability != "fail" || o.RunEvidence.UnexpectedFailures != 1 || o.RunEvidence.Eligible || o.RunEvidence.HistoryComplete {
		t.Fatalf("evidence=%+v", o.RunEvidence)
	}
	if !strings.Contains(o.RunEvidence.Incidents[0].Message, "retry recovered") {
		t.Fatal("recovery erased incident")
	}
}

func TestSharedEvidenceKeepsFailedVerification(t *testing.T) {
	o := Observation{Correctness: "fail", Error: "logical oracle mismatch", StartedAt: time.Now(), FinishedAt: time.Now()}
	attachRunEvidence(&o)
	if o.RunEvidence.CurrentHealth != "failed" || o.RunEvidence.VerificationCount != 1 || o.RunEvidence.UnexpectedFailures != 1 || o.Reliability != "fail" || o.RunEvidence.Eligible {
		t.Fatalf("evidence=%+v", o.RunEvidence)
	}
}
