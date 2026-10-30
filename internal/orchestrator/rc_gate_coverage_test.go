package orchestrator

import (
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	workerconfig "github.com/corylanou/litestream-soak/internal/worker"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestGateFleetsRunLTXCoverageCheckOnSingleDBWorkers(t *testing.T) {
	mgr := &Manager{controlBaseURL: "https://litestream-soak-ctl.fly.dev"}
	for _, source := range []string{testGateCandidate, testGateBaseline, testUpgradeCandidate, testUpgradeBaseline, "up-nol9-aaaaaaaa"} {
		for _, desired := range GateFleetForSource(source, testGateSoakSHA, testGateCandidateSHA).Workers {
			want := desired.ProfileName != gateManyDB500Profile
			cfg := normalizeWorkloadConfig(desired.Workload)
			if cfg.LTXCoverageCheck != want {
				t.Fatalf("%s ltx_coverage_check = %v, want %v", desired.WorkerID, cfg.LTXCoverageCheck, want)
			}
			env := mgr.workerEnv(model.Worker{ID: desired.WorkerID, Name: desired.Name, Source: desired.Source}, cfg)
			effective, err := workerconfig.WorkloadFromEnvironment(env)
			if err != nil {
				t.Fatalf("%s WorkloadFromEnvironment() error = %v", desired.WorkerID, err)
			}
			if effective.LTXCoverageCheck != want {
				t.Fatalf("%s worker parsed ltx_coverage_check = %v, want %v", desired.WorkerID, effective.LTXCoverageCheck, want)
			}
		}
	}
	for _, desired := range DefaultFleetForSource("main", testGateSoakSHA, testGateCandidateSHA).Workers {
		if desired.Workload.LTXCoverageCheck {
			t.Fatalf("%s outside a gate runs the ltx coverage check", desired.WorkerID)
		}
	}
}

type gateCoverageCheck struct {
	source   string
	scenario string
	status   string
}

func recordGateCoverageStatus(t *testing.T, db *model.DB, check gateCoverageCheck, at time.Time) {
	t.Helper()
	worker, err := db.GetWorker("worker-" + check.source + "-" + gateStressProfile)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetDeploymentByVersion(check.source, worker.GitSHA, worker.LitestreamSHA)
	if err != nil || deployment == nil {
		t.Fatalf("deployment for %s: %v", check.source, err)
	}
	run := fixtureRun(*worker, *deployment)
	run.Scenario = check.scenario
	verification := model.Verification{
		WorkerID: worker.ID, StartedAt: at, CompletedAt: &at, Status: check.status, Passed: check.status == "passed",
		CheckType: gateCheckCoverage, RestoreMode: gateModeCoverage, Run: run, Attributed: true,
	}
	switch check.status {
	case "failed":
		verification.ErrorMessage = "ltx coverage hole: L0-L8 union missing txids 0000000000007176-000000000000717f (1 holes)"
	case "aborted":
		verification.ErrorMessage = "ltx coverage check skipped: replica level report incomplete: first listing: listing incomplete"
		verification.FailureClassification = &reporting.FailureClassification{Stage: "ltx_coverage", Signature: reporting.SignatureReplicaLevelReportIncomplete}
	}
	mustRecordVerification(t, db, &verification)
}

func TestRCGateScoresLTXCoverageHoles(t *testing.T) {
	const scenario = "overload-truncate0"
	tests := []struct {
		name         string
		checks       []gateCoverageCheck
		wantCoverage string
		wantRestore  string
		wantGate     string
	}{
		{
			name:         "no holes pass",
			checks:       []gateCoverageCheck{{testGateCandidate, scenario, "passed"}, {testGateBaseline, scenario, "passed"}},
			wantCoverage: rcGateVerdictPass,
			wantRestore:  rcGateVerdictPass,
		},
		{
			name:         "candidate-only hole is a regression",
			checks:       []gateCoverageCheck{{testGateCandidate, scenario, "passed"}, {testGateCandidate, scenario, "failed"}, {testGateBaseline, scenario, "passed"}},
			wantCoverage: rcGateVerdictRegression,
			wantRestore:  rcGateVerdictPass,
			wantGate:     rcGateVerdictFail,
		},
		{
			name:         "candidate hole is a regression while the baseline has only inconclusive listings",
			checks:       []gateCoverageCheck{{testGateCandidate, scenario, "failed"}, {testGateBaseline, scenario, "aborted"}},
			wantCoverage: rcGateVerdictRegression,
			wantRestore:  rcGateVerdictPass,
			wantGate:     rcGateVerdictFail,
		},
		{
			name:         "holes on both sides are both_failing",
			checks:       []gateCoverageCheck{{testGateCandidate, scenario, "failed"}, {testGateBaseline, scenario, "failed"}},
			wantCoverage: rcGateVerdictBothFailing,
			wantRestore:  rcGateVerdictPass,
			wantGate:     rcGateVerdictFail,
		},
		{
			name:         "baseline-only hole does not fail the candidate",
			checks:       []gateCoverageCheck{{testGateCandidate, scenario, "passed"}, {testGateBaseline, scenario, "failed"}},
			wantCoverage: rcGateVerdictPass,
			wantRestore:  rcGateVerdictPass,
		},
		{
			name:         "inconclusive listings give no coverage",
			checks:       []gateCoverageCheck{{testGateCandidate, scenario, "aborted"}, {testGateBaseline, scenario, "passed"}},
			wantCoverage: rcGateVerdictIncomplete,
			wantRestore:  rcGateVerdictPass,
		},
		{
			name:         "a hole in another stress phase is scored there",
			checks:       []gateCoverageCheck{{testGateCandidate, scenario, "passed"}, {testGateBaseline, scenario, "passed"}, {testGateCandidate, "pinned-reader", "failed"}},
			wantCoverage: rcGateVerdictPass,
			wantRestore:  rcGateVerdictPass,
			wantGate:     rcGateVerdictFail,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			gate := registerTestGate(t, db, false, time.Now().UTC().Add(-2*time.Hour))
			createTestGateFleet(t, db, gate)
			at := gate.StartedAt.Add(time.Hour)
			recordGateVerification(t, db, testGateCandidate, gateStressProfile, scenario, true, at)
			recordGateVerification(t, db, testGateBaseline, gateStressProfile, scenario, true, at)
			for i, check := range tt.checks {
				recordGateCoverageStatus(t, db, check, at.Add(time.Duration(i+1)*time.Minute))
			}
			stored, err := db.GetRCGate(gate.ID)
			if err != nil {
				t.Fatal(err)
			}
			results, err := buildRCGateResults(db, *stored, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			byName := map[string]RCGateScenarioResult{}
			for _, result := range results.Scenarios {
				byName[result.Scenario] = result
			}
			coverage, ok := byName[scenario+"-ltx-coverage"]
			if !ok {
				t.Fatalf("no coverage scenario in %+v", results.Scenarios)
			}
			if coverage.Verdict != tt.wantCoverage {
				t.Fatalf("coverage verdict = %s, want %s: %+v", coverage.Verdict, tt.wantCoverage, coverage)
			}
			if len(coverage.Modes) != 1 || coverage.Modes[0].Mode != gateModeCoverage || coverage.Modes[0].Verdict != tt.wantCoverage {
				t.Fatalf("coverage modes = %+v", coverage.Modes)
			}
			if restore := byName[scenario]; restore.Verdict != tt.wantRestore || restore.Candidate.Passed != 1 || restore.Candidate.Failed != 0 {
				t.Fatalf("restore scenario counted coverage checks: %+v", restore)
			}
			if tt.wantGate != "" && results.Verdict != tt.wantGate {
				t.Fatalf("gate verdict = %s, want %s", results.Verdict, tt.wantGate)
			}
		})
	}
}

func TestRCGateSkipsLTXCoverageForWorkersWithoutTheCheck(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-2*time.Hour))
	createTestGateFleetWith(t, db, gate, func(cfg *workload.Config) { cfg.LTXCoverageCheck = false })
	stored, err := db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	results, err := buildRCGateResults(db, *stored, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results.Scenarios {
		if gateScenarioByName(t, gateScenariosFor(*stored), result.Scenario).coverage {
			t.Fatalf("coverage scenario %s scored for workers that never ran the check", result.Scenario)
		}
	}
	if len(results.Scenarios) != 5 {
		t.Fatalf("scenarios = %d, want the five restore scenarios", len(results.Scenarios))
	}
}

func gateScenarioByName(t *testing.T, scenarios []gateScenario, name string) gateScenario {
	t.Helper()
	for _, scenario := range scenarios {
		if scenario.name == name {
			return scenario
		}
	}
	t.Fatalf("no scenario %s", name)
	return gateScenario{}
}

func TestLTXCoverageHoleIsCompactionSubsystem(t *testing.T) {
	classification := reporting.ClassifyVerificationFailure(gateCheckCoverage, "ltx coverage hole: L0-L8 union missing txids 0000000000007176-000000000000717f (1 holes)")
	if classification.Signature != reporting.SignatureLTXCoverageHole {
		t.Fatalf("signature = %q", classification.Signature)
	}
	if got := inferProbableSubsystem(classification.Stage, classification.Signature); got != "Litestream compaction / retention" {
		t.Fatalf("subsystem = %q", got)
	}
	if got := failureCategoryForSignature(classification.Signature); got != failureCategoryActionable {
		t.Fatalf("category = %q", got)
	}
}
