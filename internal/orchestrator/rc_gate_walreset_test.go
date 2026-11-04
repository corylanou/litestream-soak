package orchestrator

import (
	"slices"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	workerconfig "github.com/corylanou/litestream-soak/internal/worker"
)

func TestGateWALResetProfileIsOptIn(t *testing.T) {
	now := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	db := openTestDB(t)
	byDefault := registerTestGate(t, db, false, now)
	if gateHasProfile(byDefault, gateWALResetProfile) || gateSelectsProfile(byDefault, gateWALResetProfile) || len(byDefault.Resources) != 8 {
		t.Fatalf("default gate runs gate-walreset: resources %d", len(byDefault.Resources))
	}
	entry, err := buildRCGateLedgerEntry(db, byDefault, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range entry.Scenarios {
		if scenario.Profile == gateWALResetProfile {
			t.Fatalf("default gate scores %s", scenario.Scenario)
		}
	}

	explicit := registerTestGateWithProfiles(t, db, now.Add(time.Hour), "wal1", gateWALResetProfile)
	if !slices.Equal(explicit.Profiles, []string{gateWALResetProfile}) || len(explicit.Resources) != 2 || !gateHasProfile(explicit, gateWALResetProfile) || gateHasProfile(explicit, gateVFSProfile) {
		t.Fatalf("explicit gate = profiles %v resources %+v", explicit.Profiles, explicit.Resources)
	}
	for _, source := range explicit.Sources() {
		spec := gateFleetForSource(explicit, source, testGateSoakSHA, "")
		if len(spec.Workers) != 1 || spec.Workers[0].ProfileName != gateWALResetProfile || spec.Workers[0].Name != "worker-"+source+"-"+gateWALResetProfile {
			t.Fatalf("%s fleet = %+v", source, spec.Workers)
		}
		cfg := normalizeWorkloadConfig(spec.Workers[0].Workload)
		if !cfg.WALResetCheck || cfg.VFSReaderCheck || !cfg.LTXCoverageCheck || cfg.SnapshotInterval != "8760h" || cfg.CPUs != 2 || cfg.MemoryMB != 2048 || spec.Workers[0].VolumeSizeGB != 10 {
			t.Fatalf("%s gate-walreset config = %+v", source, cfg)
		}
		env := (&Manager{}).workerEnv(model.Worker{ID: spec.Workers[0].WorkerID, Name: spec.Workers[0].Name, Source: source}, cfg)
		if env["WAL_RESET_CHECK"] != "true" {
			t.Fatalf("%s env lacks WAL_RESET_CHECK: %v", source, env)
		}
		effective, err := workerconfig.WorkloadFromEnvironment(env)
		if err != nil || !effective.WALResetCheck {
			t.Fatalf("%s worker parsed wal_reset_check = %v, err %v", source, effective.WALResetCheck, err)
		}
	}
	entry, err = buildRCGateLedgerEntry(db, explicit, now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var scenarios []string
	for _, scenario := range entry.Scenarios {
		scenarios = append(scenarios, scenario.Scenario)
	}
	if !slices.Equal(scenarios, []string{"walreset"}) || entry.TotalMachines != 2 || entry.TotalVolumeGB != 20 {
		t.Fatalf("explicit ledger before any worker = %v machines %d volume %d", scenarios, entry.TotalMachines, entry.TotalVolumeGB)
	}
	createTestGateFleet(t, db, explicit)
	stored, err := db.GetRCGate(explicit.ID)
	if err != nil {
		t.Fatal(err)
	}
	results, err := buildRCGateResults(db, *stored, now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	scenarios = scenarios[:0]
	for _, scenario := range results.Scenarios {
		scenarios = append(scenarios, scenario.Scenario)
	}
	if !slices.Equal(scenarios, []string{"walreset", "walreset-ltx-coverage", "walreset-wal-reset"}) {
		t.Fatalf("explicit ledger with workers = %v", scenarios)
	}
	for _, desired := range DefaultFleetForSource("main", testGateSoakSHA, testGateCandidateSHA).Workers {
		if desired.Workload.WALResetCheck {
			t.Fatalf("%s outside a gate runs the wal reset check", desired.WorkerID)
		}
	}
}

type gateWALResetCheck struct {
	source string
	status string
	phase  string
}

func recordGateWALResetCheck(t *testing.T, db *model.DB, check gateWALResetCheck, at time.Time) {
	t.Helper()
	worker, err := db.GetWorker("worker-" + check.source + "-" + gateWALResetProfile)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetDeploymentByVersion(check.source, worker.GitSHA, worker.LitestreamSHA)
	if err != nil || deployment == nil {
		t.Fatalf("deployment for %s: %v", check.source, err)
	}
	verification := model.Verification{
		WorkerID: worker.ID, StartedAt: at, CompletedAt: &at, Status: check.status, Passed: check.status == "passed",
		CheckType: gateCheckWALReset, RestoreMode: gateModeWALReset, Run: fixtureRun(*worker, *deployment), Attributed: true,
	}
	switch {
	case check.status == "failed" && check.phase == "integrity_check":
		verification.ErrorMessage = "wal reset integrity failed: restored integrity: Page 50485: never used"
	case check.status == "failed" && check.phase == "compare":
		verification.ErrorMessage = "wal reset logical mismatch: table wal_reset_burst: 80 rows restored, 201 in source"
	case check.status == "failed":
		verification.ErrorMessage = "wal reset restore failed: restore failed: validation failed (exit 1): page 50485: never used"
	case check.status == "aborted":
		verification.ErrorMessage = "wal reset check skipped: wal reset shape not reached: 2 wal resets while litestream was down, need at least 3"
		verification.FailureClassification = &reporting.FailureClassification{Stage: "wal_reset", Signature: reporting.SignatureWALResetShapeNotReached, RestoreMode: gateModeWALReset}
	}
	mustRecordVerification(t, db, &verification)
}

func TestRCGateScoresWALResetChecks(t *testing.T) {
	tests := []struct {
		name         string
		checks       []gateWALResetCheck
		wantScenario string
		wantRestore  string
		wantGate     string
	}{
		{
			name:         "both sides restore intact is no reproduction, not a pass",
			checks:       []gateWALResetCheck{{testGateCandidate, "passed", ""}, {testGateBaseline, "passed", ""}},
			wantScenario: rcGateVerdictNoReproduction, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictIncomplete,
		},
		{
			name:         "the candidate fixes a corrupt latest restore the baseline still has",
			checks:       []gateWALResetCheck{{testGateCandidate, "passed", ""}, {testGateBaseline, "failed", "integrity_check"}, {testGateBaseline, "passed", ""}},
			wantScenario: rcGateVerdictPass, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictPass,
		},
		{
			name:         "a baseline that lost rows is a reproduction too",
			checks:       []gateWALResetCheck{{testGateCandidate, "passed", ""}, {testGateBaseline, "failed", "compare"}},
			wantScenario: rcGateVerdictPass, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictPass,
		},
		{
			name:         "a candidate-only corrupt restore is a regression",
			checks:       []gateWALResetCheck{{testGateCandidate, "passed", ""}, {testGateCandidate, "failed", "integrity_check"}, {testGateBaseline, "passed", ""}},
			wantScenario: rcGateVerdictRegression, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictFail,
		},
		{
			name:         "a candidate-only logical mismatch is a regression",
			checks:       []gateWALResetCheck{{testGateCandidate, "failed", "compare"}, {testGateBaseline, "passed", ""}},
			wantScenario: rcGateVerdictRegression, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictFail,
		},
		{
			name:         "corrupt restores on both sides are both_failing",
			checks:       []gateWALResetCheck{{testGateCandidate, "failed", ""}, {testGateBaseline, "failed", ""}},
			wantScenario: rcGateVerdictBothFailing, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictFail,
		},
		{
			name:         "a candidate failure against an inconclusive baseline is a regression",
			checks:       []gateWALResetCheck{{testGateCandidate, "failed", ""}, {testGateBaseline, "aborted", ""}},
			wantScenario: rcGateVerdictRegression, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictFail,
		},
		{
			name:         "inconclusive cycles give no coverage",
			checks:       []gateWALResetCheck{{testGateCandidate, "aborted", ""}, {testGateBaseline, "passed", ""}},
			wantScenario: rcGateVerdictIncomplete, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictIncomplete,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			gate := registerTestGateWithProfiles(t, db, time.Now().UTC().Add(-2*time.Hour), "", gateWALResetProfile)
			createTestGateFleet(t, db, gate)
			at := gate.StartedAt.Add(time.Hour)
			recordGateVerification(t, db, testGateCandidate, gateWALResetProfile, "", true, at)
			recordGateVerification(t, db, testGateBaseline, gateWALResetProfile, "", true, at)
			recordGateCoverageCheck(t, db, testGateCandidate, gateWALResetProfile, "", true, at)
			recordGateCoverageCheck(t, db, testGateBaseline, gateWALResetProfile, "", true, at)
			for i, check := range tt.checks {
				recordGateWALResetCheck(t, db, check, at.Add(time.Duration(i+1)*time.Minute))
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
			reset, ok := byName["walreset-wal-reset"]
			if !ok {
				t.Fatalf("no wal reset scenario in %+v", results.Scenarios)
			}
			if reset.Verdict != tt.wantScenario {
				t.Fatalf("wal reset verdict = %s, want %s: %+v", reset.Verdict, tt.wantScenario, reset)
			}
			if len(reset.Modes) != 1 || reset.Modes[0].Mode != gateModeWALReset || reset.Modes[0].Verdict != tt.wantScenario {
				t.Fatalf("wal reset modes = %+v, want one %s mode with verdict %s", reset.Modes, gateModeWALReset, tt.wantScenario)
			}
			wantReproduced := 0
			for _, check := range tt.checks {
				if check.source == testGateBaseline && check.status == "failed" {
					wantReproduced++
				}
			}
			if reset.Baseline.Reproduced != wantReproduced || reset.Modes[0].Baseline.Reproduced != wantReproduced {
				t.Fatalf("baseline reproduced = %d (mode %d), want %d", reset.Baseline.Reproduced, reset.Modes[0].Baseline.Reproduced, wantReproduced)
			}
			if tt.wantScenario == rcGateVerdictPass && reset.Baseline.Reproduced == 0 {
				t.Fatal("wal reset scenario passed without a baseline reproduction")
			}
			if reset.Candidate.RotationEnabled || reset.Baseline.RotationEnabled {
				t.Fatalf("wal reset mode counted as restore rotation: %+v", reset)
			}
			if restore := byName["walreset"]; restore.Verdict != tt.wantRestore || restore.Candidate.Passed != 1 || restore.Candidate.Failed != 0 || restore.Baseline.Passed != 1 {
				t.Fatalf("restore scenario counted wal reset checks: %+v", restore)
			}
			if coverage := byName["walreset-ltx-coverage"]; coverage.Verdict != rcGateVerdictPass || coverage.Candidate.Passed != 1 {
				t.Fatalf("coverage scenario counted wal reset checks: %+v", coverage)
			}
			if results.Verdict != tt.wantGate {
				t.Fatalf("gate verdict = %s, want %s", results.Verdict, tt.wantGate)
			}
		})
	}
}

func TestWALResetFailuresAreTheirOwnSubsystem(t *testing.T) {
	for _, tt := range []struct {
		message   string
		signature string
	}{
		{"wal reset restore failed: restore failed: validation failed (exit 1): page 50485: never used", reporting.SignatureWALResetRestoreFailed},
		{"wal reset integrity failed: restored integrity: Page 50485: never used", reporting.SignatureWALResetIntegrityFailed},
		{"wal reset logical mismatch: table wal_reset_burst: 80 rows restored, 201 in source", reporting.SignatureWALResetLogicalMismatch},
	} {
		classification := reporting.ClassifyVerificationFailure(gateCheckWALReset, tt.message)
		if classification.Signature != tt.signature || classification.Stage != gateCheckWALReset {
			t.Fatalf("classification = %+v, want %s in stage %s", classification, tt.signature, gateCheckWALReset)
		}
		if got := inferProbableSubsystem(classification.Stage, classification.Signature); got != "Litestream WAL reset continuity" {
			t.Fatalf("subsystem = %q", got)
		}
		if got := failureCategoryForSignature(classification.Signature); got != failureCategoryActionable {
			t.Fatalf("category = %q", got)
		}
	}
	for _, signature := range []string{reporting.SignatureWALResetShapeNotReached, reporting.SignatureWALResetLitestreamRestartFailed, reporting.SignatureWALResetCatchUpTimeout, reporting.SignatureWALResetUnavailable} {
		if got := inferProbableSubsystem("wal_reset", signature); got != "Litestream WAL reset continuity" {
			t.Fatalf("subsystem for %s = %q", signature, got)
		}
	}
	plain := reporting.ClassifyVerificationFailure("integrity", "restore failed: validation failed (exit 1): page 50485: never used")
	if plain.Signature == reporting.SignatureWALResetRestoreFailed || plain.Stage != "restore" {
		t.Fatalf("ordinary restore failure classified as a wal reset failure: %+v", plain)
	}
}
