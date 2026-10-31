package orchestrator

import (
	"slices"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	workerconfig "github.com/corylanou/litestream-soak/internal/worker"
)

func registerTestGateWithProfiles(t *testing.T, db *model.DB, started time.Time, suffix string, profiles ...string) model.RCGate {
	t.Helper()
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	gate, _, err := manager.RegisterRCGate(RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateRef: "pull/1611/head",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "24h",
		Profiles:     profiles,
		SourceSuffix: suffix,
	}, started)
	if err != nil {
		t.Fatalf("RegisterRCGate() error = %v", err)
	}
	return gate
}

func TestGateVFSProfileIsOptIn(t *testing.T) {
	now := time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC)
	db := openTestDB(t)
	byDefault := registerTestGate(t, db, false, now)
	if gateHasProfile(byDefault, gateVFSProfile) || gateSelectsProfile(byDefault, gateVFSProfile) || len(byDefault.Resources) != 8 {
		t.Fatalf("default gate runs gate-vfs: resources %d", len(byDefault.Resources))
	}
	for _, resource := range byDefault.Resources {
		if resource.Profile == gateVFSProfile {
			t.Fatalf("default gate reserved %+v", resource)
		}
	}
	entry, err := buildRCGateLedgerEntry(db, byDefault, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range entry.Scenarios {
		if scenario.Profile == gateVFSProfile {
			t.Fatalf("default gate scores %s", scenario.Scenario)
		}
	}

	explicit := registerTestGateWithProfiles(t, db, now.Add(time.Hour), "vfs1", gateVFSProfile)
	if !slices.Equal(explicit.Profiles, []string{gateVFSProfile}) || len(explicit.Resources) != 2 || !gateHasProfile(explicit, gateVFSProfile) || gateHasProfile(explicit, gateStressProfile) {
		t.Fatalf("explicit gate = profiles %v resources %+v", explicit.Profiles, explicit.Resources)
	}
	for _, source := range explicit.Sources() {
		spec := gateFleetForSource(explicit, source, testGateSoakSHA, "")
		if len(spec.Workers) != 1 || spec.Workers[0].ProfileName != gateVFSProfile || spec.Workers[0].Name != "worker-"+source+"-"+gateVFSProfile {
			t.Fatalf("%s fleet = %+v", source, spec.Workers)
		}
		cfg := normalizeWorkloadConfig(spec.Workers[0].Workload)
		if !cfg.VFSReaderCheck || !cfg.LTXCoverageCheck || cfg.CPUs != 2 || cfg.MemoryMB != 2048 || spec.Workers[0].VolumeSizeGB != 10 {
			t.Fatalf("%s gate-vfs config = %+v", source, cfg)
		}
		env := (&Manager{}).workerEnv(model.Worker{ID: spec.Workers[0].WorkerID, Name: spec.Workers[0].Name, Source: source}, cfg)
		if env["VFS_READER_CHECK"] != "true" {
			t.Fatalf("%s env lacks VFS_READER_CHECK: %v", source, env)
		}
		effective, err := workerconfig.WorkloadFromEnvironment(env)
		if err != nil || !effective.VFSReaderCheck {
			t.Fatalf("%s worker parsed vfs_reader_check = %v, err %v", source, effective.VFSReaderCheck, err)
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
	if !slices.Equal(scenarios, []string{"vfs"}) || entry.TotalMachines != 2 || entry.TotalVolumeGB != 20 {
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
	if !slices.Equal(scenarios, []string{"vfs", "vfs-ltx-coverage", "vfs-vfs-reader"}) {
		t.Fatalf("explicit ledger with workers = %v", scenarios)
	}
	for _, desired := range DefaultFleetForSource("main", testGateSoakSHA, testGateCandidateSHA).Workers {
		if desired.Workload.VFSReaderCheck {
			t.Fatalf("%s outside a gate runs the vfs reader check", desired.WorkerID)
		}
	}
}

type gateVFSCheck struct {
	source string
	mode   string
	status string
}

func recordGateVFSCheck(t *testing.T, db *model.DB, check gateVFSCheck, at time.Time) {
	t.Helper()
	worker, err := db.GetWorker("worker-" + check.source + "-" + gateVFSProfile)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetDeploymentByVersion(check.source, worker.GitSHA, worker.LitestreamSHA)
	if err != nil || deployment == nil {
		t.Fatalf("deployment for %s: %v", check.source, err)
	}
	verification := model.Verification{
		WorkerID: worker.ID, StartedAt: at, CompletedAt: &at, Status: check.status, Passed: check.status == "passed",
		CheckType: gateCheckVFS, RestoreMode: check.mode, Run: fixtureRun(*worker, *deployment), Attributed: true,
	}
	switch {
	case check.status == "failed" && check.mode == gateModeVFSSnapshot:
		verification.ErrorMessage = "vfs snapshot drift: repeat 3 of 16 inside one read transaction returned 2048|4|5|8320@0000000000000420, first read returned 2048|4|4|8192@000000000000041a (14 of 16 repeats differ)"
	case check.status == "failed":
		verification.ErrorMessage = `vfs read mismatch: load_test count returned vfs="4999" primary="5000" at txid 0000000000000420 (1 of 3 queries differ)`
	case check.status == "aborted":
		verification.ErrorMessage = "vfs snapshot check skipped: vfs hydration incomplete: 62.5% after 10m0s"
		verification.FailureClassification = &reporting.FailureClassification{Stage: "vfs_snapshot", Signature: reporting.SignatureVFSHydrationIncomplete, RestoreMode: check.mode}
	}
	mustRecordVerification(t, db, &verification)
}

func TestRCGateScoresVFSReaderChecks(t *testing.T) {
	tests := []struct {
		name         string
		checks       []gateVFSCheck
		wantScenario string
		wantSnapshot string
		wantRead     string
		wantRestore  string
		wantGate     string
	}{
		{
			name: "both readers hold their snapshots and match the primary",
			checks: []gateVFSCheck{
				{testGateCandidate, gateModeVFSSnapshot, "passed"}, {testGateBaseline, gateModeVFSSnapshot, "passed"},
				{testGateCandidate, gateModeVFSRead, "passed"}, {testGateBaseline, gateModeVFSRead, "passed"},
			},
			wantScenario: rcGateVerdictPass, wantSnapshot: rcGateVerdictPass, wantRead: rcGateVerdictPass, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictPass,
		},
		{
			name: "the candidate fixes a drift the baseline still has",
			checks: []gateVFSCheck{
				{testGateCandidate, gateModeVFSSnapshot, "passed"}, {testGateBaseline, gateModeVFSSnapshot, "failed"}, {testGateBaseline, gateModeVFSSnapshot, "passed"},
				{testGateCandidate, gateModeVFSRead, "passed"}, {testGateBaseline, gateModeVFSRead, "passed"},
			},
			wantScenario: rcGateVerdictPass, wantSnapshot: rcGateVerdictPass, wantRead: rcGateVerdictPass, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictPass,
		},
		{
			name: "candidate-only drift is a regression",
			checks: []gateVFSCheck{
				{testGateCandidate, gateModeVFSSnapshot, "passed"}, {testGateCandidate, gateModeVFSSnapshot, "failed"}, {testGateBaseline, gateModeVFSSnapshot, "passed"},
				{testGateCandidate, gateModeVFSRead, "passed"}, {testGateBaseline, gateModeVFSRead, "passed"},
			},
			wantScenario: rcGateVerdictRegression, wantSnapshot: rcGateVerdictRegression, wantRead: rcGateVerdictPass, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictFail,
		},
		{
			name: "candidate drift is a regression while the baseline is only inconclusive",
			checks: []gateVFSCheck{
				{testGateCandidate, gateModeVFSSnapshot, "failed"}, {testGateBaseline, gateModeVFSSnapshot, "aborted"},
			},
			wantScenario: rcGateVerdictRegression, wantSnapshot: rcGateVerdictRegression, wantRead: rcGateVerdictIncomplete, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictFail,
		},
		{
			name: "drift on both sides is both_failing",
			checks: []gateVFSCheck{
				{testGateCandidate, gateModeVFSSnapshot, "failed"}, {testGateBaseline, gateModeVFSSnapshot, "failed"},
				{testGateCandidate, gateModeVFSRead, "passed"}, {testGateBaseline, gateModeVFSRead, "passed"},
			},
			wantScenario: rcGateVerdictBothFailing, wantSnapshot: rcGateVerdictBothFailing, wantRead: rcGateVerdictPass, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictFail,
		},
		{
			name: "a candidate read mismatch is scored in its own mode",
			checks: []gateVFSCheck{
				{testGateCandidate, gateModeVFSSnapshot, "passed"}, {testGateBaseline, gateModeVFSSnapshot, "passed"},
				{testGateCandidate, gateModeVFSRead, "failed"}, {testGateBaseline, gateModeVFSRead, "passed"},
			},
			wantScenario: rcGateVerdictRegression, wantSnapshot: rcGateVerdictPass, wantRead: rcGateVerdictRegression, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictFail,
		},
		{
			name: "inconclusive cycles give no coverage",
			checks: []gateVFSCheck{
				{testGateCandidate, gateModeVFSSnapshot, "aborted"}, {testGateBaseline, gateModeVFSSnapshot, "passed"},
				{testGateCandidate, gateModeVFSRead, "passed"}, {testGateBaseline, gateModeVFSRead, "passed"},
			},
			wantScenario: rcGateVerdictIncomplete, wantSnapshot: rcGateVerdictIncomplete, wantRead: rcGateVerdictPass, wantRestore: rcGateVerdictPass, wantGate: rcGateVerdictIncomplete,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			gate := registerTestGateWithProfiles(t, db, time.Now().UTC().Add(-2*time.Hour), "", gateVFSProfile)
			createTestGateFleet(t, db, gate)
			at := gate.StartedAt.Add(time.Hour)
			recordGateVerification(t, db, testGateCandidate, gateVFSProfile, "", true, at)
			recordGateVerification(t, db, testGateBaseline, gateVFSProfile, "", true, at)
			recordGateCoverageCheck(t, db, testGateCandidate, gateVFSProfile, "", true, at)
			recordGateCoverageCheck(t, db, testGateBaseline, gateVFSProfile, "", true, at)
			for i, check := range tt.checks {
				recordGateVFSCheck(t, db, check, at.Add(time.Duration(i+1)*time.Minute))
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
			reader, ok := byName["vfs-vfs-reader"]
			if !ok {
				t.Fatalf("no vfs reader scenario in %+v", results.Scenarios)
			}
			if reader.Verdict != tt.wantScenario {
				t.Fatalf("vfs reader verdict = %s, want %s: %+v", reader.Verdict, tt.wantScenario, reader)
			}
			modes := map[string]string{}
			for _, mode := range reader.Modes {
				modes[mode.Mode] = mode.Verdict
			}
			if len(modes) != 2 || modes[gateModeVFSSnapshot] != tt.wantSnapshot || modes[gateModeVFSRead] != tt.wantRead {
				t.Fatalf("vfs reader modes = %+v, want snapshot %s read %s", reader.Modes, tt.wantSnapshot, tt.wantRead)
			}
			if reader.Candidate.RotationEnabled || reader.Baseline.RotationEnabled {
				t.Fatalf("vfs modes counted as restore rotation: %+v", reader)
			}
			if restore := byName["vfs"]; restore.Verdict != tt.wantRestore || restore.Candidate.Passed != 1 || restore.Candidate.Failed != 0 || restore.Baseline.Passed != 1 {
				t.Fatalf("restore scenario counted vfs checks: %+v", restore)
			}
			if coverage := byName["vfs-ltx-coverage"]; coverage.Verdict != rcGateVerdictPass || coverage.Candidate.Passed != 1 {
				t.Fatalf("coverage scenario counted vfs checks: %+v", coverage)
			}
			if results.Verdict != tt.wantGate {
				t.Fatalf("gate verdict = %s, want %s", results.Verdict, tt.wantGate)
			}
		})
	}
}

func TestVFSFailuresAreTheVFSReaderSubsystem(t *testing.T) {
	for _, tt := range []struct {
		message   string
		signature string
	}{
		{"vfs snapshot drift: repeat 3 of 16 inside one read transaction returned 2048|4|5|8320@0000000000000420, first read returned 2048|4|4|8192@000000000000041a (14 of 16 repeats differ)", reporting.SignatureVFSSnapshotDrift},
		{`vfs read mismatch: load_test count returned vfs="4999" primary="5000" at txid 0000000000000420 (1 of 3 queries differ)`, reporting.SignatureVFSReadMismatch},
	} {
		classification := reporting.ClassifyVerificationFailure(gateCheckVFS, tt.message)
		if classification.Signature != tt.signature || classification.Stage != gateCheckVFS {
			t.Fatalf("classification = %+v, want %s", classification, tt.signature)
		}
		if got := inferProbableSubsystem(classification.Stage, classification.Signature); got != "Litestream VFS reader" {
			t.Fatalf("subsystem = %q", got)
		}
		if got := failureCategoryForSignature(classification.Signature); got != failureCategoryActionable {
			t.Fatalf("category = %q", got)
		}
	}
}
