package orchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestReliabilityKeepsRecoveredRegionalFailure(t *testing.T) {
	db := openTestDB(t)
	deployment := model.Deployment{GitSHA: "soak", LitestreamSHA: "litestream", Source: "main", ImageRef: "image", Status: "ready"}
	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	deployment = *stored
	worker := model.Worker{ID: "regional", Name: "regional", Region: "ams", Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, ProfileName: "high-vol-ams", ProfileConfig: "{}", FlyMachineID: "machine"}
	createTestWorker(t, db, worker)
	for i := 0; i < 600; i++ {
		at := deployment.StartedAt.Add(time.Duration(i+1) * time.Second)
		v := model.Verification{WorkerID: worker.ID, Run: fixtureRun(worker, deployment), Attributed: true, StartedAt: at, CompletedAt: &at, Passed: i > 0, Status: "passed", CheckType: "integrity"}
		v.Run.Region = "ams"
		if i == 0 {
			v.Status = "failed"
			v.ErrorMessage = "sync request: context deadline exceeded"
		}
		if err := db.RecordVerification(&v); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.PruneVerificationsBefore(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteWorker(worker.ID); err != nil {
		t.Fatal(err)
	}
	score, err := buildDeploymentScorecard(db, deployment, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(score.Reliability) != 1 {
		t.Fatalf("reliability=%+v", score.Reliability)
	}
	evidence := score.Reliability[0]
	if evidence.CurrentHealth != "passed" || evidence.UnexpectedFailures != 1 || evidence.VerificationCount != 600 || evidence.Eligible {
		t.Fatalf("evidence=%+v", evidence)
	}
	if evidence.Region != "ams" || len(evidence.EligibilityReasons) == 0 {
		t.Fatalf("regional evidence=%+v", evidence)
	}
}

func TestReliabilityFindingClassifications(t *testing.T) {
	for _, tt := range []struct {
		name       string
		base, head int
		covered    bool
		want       string
	}{
		{"fixed", 1, 0, true, "fixed"}, {"improved", 3, 1, true, "improved"}, {"unchanged", 1, 1, true, "unchanged"}, {"new", 0, 1, true, "new"}, {"regressed", 1, 3, true, "regressed"}, {"unknown", 1, 0, false, "inconclusive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := reliabilityFinding(tt.base, tt.head, tt.covered); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}

func TestCleanSoakRejectsUnmeasuredAge(t *testing.T) {
	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-1228", 1228)
	_, ok, err := successTeardownCandidate(db, deployment, SuccessTeardownPolicy{Threshold: 24 * time.Hour}, worker.CreatedAt.Add(30*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("worker age and latest pass cannot prove workload progress or maintenance exposure")
	}
}

func TestRunEligibilityRequiresMeasuredCoverage(t *testing.T) {
	start := time.Now().UTC().Add(-25 * time.Hour)
	end := start.Add(24 * time.Hour)
	valid := WorkerRunEvidence{CurrentHealth: "passed", HistoryComplete: true, VerificationCount: 25, WorkloadProgress: 100, MaintenanceObservations: 3, FirstVerification: &start, LastVerification: &end, VerifiedSpanSeconds: (24 * time.Hour).Seconds(), MaxVerificationGapSeconds: 3600}
	cases := []struct {
		name   string
		change func(*WorkerRunEvidence)
	}{
		{"failure", func(e *WorkerRunEvidence) { e.UnexpectedFailures = 1 }},
		{"expected injection", func(e *WorkerRunEvidence) { e.ExpectedInjections = 1 }},
		{"unknown history", func(e *WorkerRunEvidence) { e.HistoryComplete = false }},
		{"unattributed", func(e *WorkerRunEvidence) { e.UnattributedObservations = 1 }},
		{"pending", func(e *WorkerRunEvidence) { e.IncompleteObservations = 1 }},
		{"no writes", func(e *WorkerRunEvidence) { e.WorkloadProgress = 0 }},
		{"no maintenance", func(e *WorkerRunEvidence) { e.MaintenanceObservations = 0 }},
		{"long gap", func(e *WorkerRunEvidence) { e.MaxVerificationGapSeconds = 3601 }},
		{"short span", func(e *WorkerRunEvidence) { e.VerifiedSpanSeconds = 100 }},
		{"no current pass", func(e *WorkerRunEvidence) { e.CurrentHealth = "unknown" }},
	}
	evaluateRunEligibility(&valid, start, &end, time.Hour, 24*time.Hour)
	if !valid.Eligible {
		t.Fatalf("measured run rejected: %+v", valid)
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			e := valid
			tt.change(&e)
			evaluateRunEligibility(&e, start, &end, time.Hour, 24*time.Hour)
			if e.Eligible || len(e.EligibilityReasons) == 0 {
				t.Fatalf("accepted: %+v", e)
			}
		})
	}
}

func TestIncidentClassificationNeverExcusesRecoveredFailures(t *testing.T) {
	for _, tt := range []struct{ kind, want string }{
		{"fault_injection_engaged", "expected_injection"},
		{"maintenance_snapshot_completed", "maintenance"},
		{"platform_disk_full_recovered", "unexpected"},
		{"provider_retry", "unexpected"},
		{"litestream_metrics_missing", "unavailable"},
		{"litestream_sync_timeout", "unexpected"},
		{"worker_started", ""},
	} {
		t.Run(tt.kind, func(t *testing.T) {
			if got := incidentEventClass(tt.kind); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestReliabilityDashboardSeparatesCurrentHealth(t *testing.T) {
	rendered := renderHomeBody(t, homePageData{ReleaseComparison: &DeploymentComparisonResponse{Head: DeploymentScorecard{Reliability: []WorkerRunEvidence{{WorkerID: "regional", Profile: "high-vol-ams", Region: "ams", CurrentHealth: "passed", UnexpectedFailures: 2, EligibilityReasons: []string{"maintenance exposure unobserved"}, Incidents: []RunIncident{{Kind: "provider_retry", Classification: "unexpected", Message: "retry recovered"}}}}}}})
	for _, want := range []string{"Complete-run reliability", "Current health: passed", "Unexpected incidents: 2", "maintenance exposure unobserved", "provider_retry", "high-vol-ams", "ams"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q", want)
		}
	}
}

func TestReliabilityVerdictRejectsRecoveredRegression(t *testing.T) {
	comparison := DeploymentComparisonResponse{Verdict: "better", Head: DeploymentScorecard{Reliability: []WorkerRunEvidence{{UnexpectedFailures: 1}}}, ReliabilityFindings: []ReliabilityFinding{{Classification: "regressed"}}}
	applyReliabilityVerdict(&comparison)
	if comparison.Verdict != "worse" {
		t.Fatalf("verdict=%s", comparison.Verdict)
	}
	comparison.ReliabilityFindings[0].Classification = "inconclusive"
	comparison.Verdict = "passed"
	applyReliabilityVerdict(&comparison)
	if comparison.Verdict != "insufficient_data" {
		t.Fatalf("verdict=%s", comparison.Verdict)
	}
}

func TestReliabilityMeasuresProgressAndMaintenanceWithPendingNeutral(t *testing.T) {
	db := openTestDB(t)
	deployment := model.Deployment{GitSHA: "soak", LitestreamSHA: "litestream", WorkloadSHA: "generator", Source: "pr-211", ImageRef: "image", Status: "ready"}
	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetLatestDeployment(deployment.Source)
	if err != nil {
		t.Fatal(err)
	}
	deployment = *stored
	worker := model.Worker{ID: "measured", Name: "measured", Source: deployment.Source, GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, ProfileName: "low-volume", ProfileConfig: "{}", FlyMachineID: "machine", Status: model.WorkerRunning}
	createTestWorker(t, db, worker)
	run := fixtureRun(worker, deployment)
	for i := 0; i <= 24; i++ {
		at := deployment.StartedAt.Add(time.Duration(i) * time.Hour)
		runtime := reporting.RuntimePayload{DBTXID: uint64(i + 1), SnapshotCollectedAt: at, LitestreamSnapshotHealthy: true}
		if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, runtime); err != nil {
			t.Fatal(err)
		}
		v := model.Verification{WorkerID: worker.ID, Run: run, Attributed: true, StartedAt: at, CompletedAt: &at, Passed: true, Status: "passed", CheckType: "integrity"}
		if err := db.RecordVerification(&v); err != nil {
			t.Fatal(err)
		}
	}
	at := deployment.StartedAt.Add(24 * time.Hour)
	pendingAt := at.Add(-time.Minute)
	pending := model.Verification{WorkerID: worker.ID, Run: run, Attributed: true, StartedAt: pendingAt, CompletedAt: &pendingAt, Status: "pending", CheckType: "many_db"}
	if err := db.RecordVerification(&pending); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(struct {
		reporting.WorkerEventPayload
		Attributed bool `json:"attributed"`
	}{reporting.WorkerEventPayload{WorkerIdentity: run, EventType: "maintenance_compaction_completed"}, true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordEventAt(worker.ID, "maintenance_compaction_completed", "observed compaction", string(body), at); err != nil {
		t.Fatal(err)
	}
	evidence, err := buildRunReliability(db, deployment, &at)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
	e := evidence[0]
	if !e.Eligible || e.WorkloadProgress != 24 || e.VerificationCount != 25 || e.PendingObservations != 1 || e.UnexpectedFailures != 0 {
		t.Fatalf("evidence=%+v", e)
	}
	latestPending := pending
	latestPending.StartedAt = at
	latestPending.CompletedAt = &at
	if err := db.RecordVerification(&latestPending); err != nil {
		t.Fatal(err)
	}
	pendingEvidence, err := buildRunReliability(db, deployment, &at)
	if err != nil {
		t.Fatal(err)
	}
	if pendingEvidence[0].CurrentHealth != "pending" || pendingEvidence[0].Eligible || pendingEvidence[0].UnexpectedFailures != 0 || pendingEvidence[0].VerificationCount != 25 {
		t.Fatalf("latest pending evidence=%+v", pendingEvidence)
	}
	before := e
	before.UnexpectedFailures = 2
	findings := compareRunReliability([]WorkerRunEvidence{before}, evidence)
	if len(findings) != 1 || findings[0].Classification != "fixed" {
		t.Fatalf("findings=%+v", findings)
	}
	before.WorkloadSHA = "different"
	if got := compareRunReliability([]WorkerRunEvidence{before}, evidence)[0].Classification; got != "inconclusive" {
		t.Fatalf("mismatched workload=%s", got)
	}
	if err := db.RecordEventAt(worker.ID, "platform_restart", "recovered after restart", "{}", at); err != nil {
		t.Fatal(err)
	}
	evidence, err = buildRunReliability(db, deployment, &at)
	if err != nil {
		t.Fatal(err)
	}
	if evidence[0].Eligible || evidence[0].UnattributedObservations != 1 {
		t.Fatalf("unattributed incident erased: %+v", evidence)
	}
}

func TestRunEligibilityLatestPendingAndTrailingGap(t *testing.T) {
	start := time.Now().UTC().Add(-48 * time.Hour)
	last := start.Add(24 * time.Hour)
	for _, tt := range []struct {
		name, health string
		end          *time.Time
	}{
		{"latest pending", "pending", &last}, {"active stale last pass", "passed", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := WorkerRunEvidence{CurrentHealth: tt.health, HistoryComplete: true, VerificationCount: 25, WorkloadProgress: 100, MaintenanceObservations: 3, FirstVerification: &start, LastVerification: &last, VerifiedSpanSeconds: 86400, MaxVerificationGapSeconds: 3600}
			evaluateRunEligibility(&e, start, tt.end, time.Hour, 24*time.Hour)
			if e.Eligible || e.CoverageComplete {
				t.Fatalf("unproven current coverage accepted: %+v", e)
			}
		})
	}
}

func TestChurnRuntimeEvidenceKeepsErrorsAcrossRecreation(t *testing.T) {
	db := openTestDB(t)
	deployment := model.Deployment{GitSHA: "soak", LitestreamSHA: "litestream", WorkloadSHA: "generator", Source: "main", ImageRef: "image", Status: "ready"}
	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	deployment = *stored
	worker := model.Worker{ID: "churn", Name: "churn", Source: "main", ProfileName: "queue-churn", ProfileConfig: "{}", FlyMachineID: "machine"}
	createTestWorker(t, db, worker)
	run := fixtureRun(worker, deployment)
	for _, sample := range []struct {
		run  string
		body string
	}{
		{"first", `{"litestream_snapshot_healthy":true,"workload_counters_present":true,"workload_attempts_total":10,"workload_mutations_total":7,"workload_busy_total":2,"workload_errors_total":3}`},
		{"first", `{"litestream_snapshot_healthy":true,"workload_counters_present":true,"workload_attempts_total":20,"workload_mutations_total":17,"workload_busy_total":2,"workload_errors_total":3}`},
		{"replacement", `{"litestream_snapshot_healthy":true,"workload_counters_present":true,"workload_attempts_total":5,"workload_mutations_total":5}`},
	} {
		run.RunID = sample.run
		if err := db.RecordRuntimeEvidence(run, json.RawMessage(sample.body), true); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteWorker(worker.ID); err != nil {
		t.Fatal(err)
	}
	evidence, err := buildRunReliability(db, deployment, nil)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
	e := evidence[0]
	if e.WorkloadErrors != 3 || e.WorkloadBusy != 2 || e.WorkloadMutations != 22 || e.WorkloadAttempts != 25 || e.Eligible || e.IncompleteObservations == 0 || len(e.Incidents) == 0 {
		t.Fatalf("evidence=%+v", e)
	}
}

func TestWorkloadCounterEpochReplayUsesMaxima(t *testing.T) {
	db := openTestDB(t)
	deployment := model.Deployment{ID: 3, Source: "main", GitSHA: "soak", LitestreamSHA: "ls", WorkloadSHA: "workload", StartedAt: time.Now().Add(-time.Hour)}
	run := fixtureRun(model.Worker{ID: "w", ProfileName: "queue-churn", FlyMachineID: "machine"}, deployment)
	for _, body := range []string{
		`{"litestream_snapshot_healthy":true,"workload_counters_present":true,"workload_counter_epoch":"epoch1","workload_attempts_total":10,"workload_mutations_total":7,"workload_errors_total":3,"workload_busy_total":2}`,
		`{"litestream_snapshot_healthy":true,"workload_counters_present":true,"workload_counter_epoch":"epoch1","workload_attempts_total":10,"workload_mutations_total":7,"workload_errors_total":3,"workload_busy_total":2}`,
		`{"litestream_snapshot_healthy":true,"workload_counters_present":true,"workload_counter_epoch":"epoch2","workload_attempts_total":5,"workload_mutations_total":4,"workload_errors_total":1}`,
	} {
		if err := db.RecordRuntimeEvidence(run, json.RawMessage(body), true); err != nil {
			t.Fatal(err)
		}
	}
	evidence, err := buildRunReliability(db, deployment, nil)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
	e := evidence[0]
	if e.WorkloadErrors != 4 || e.WorkloadMutations != 11 || e.WorkloadAttempts != 15 || e.WorkloadBusy != 2 {
		t.Fatalf("epoch counters=%+v", e)
	}
}
