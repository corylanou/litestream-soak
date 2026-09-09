package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	workerconfig "github.com/corylanou/litestream-soak/internal/worker"
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
	valid := WorkerRunEvidence{ProfileCapability: "disabled", CurrentHealth: "passed", HistoryComplete: true, VerificationCount: 25, WorkloadProgress: 100, MaintenanceObservations: 3, MaintenanceSnapshots: 1, MaintenanceCompactions: 1, MaintenanceRetentions: 1, FirstVerification: &start, LastVerification: &end, VerifiedSpanSeconds: (24 * time.Hour).Seconds(), MaxVerificationGapSeconds: 3600}
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
		{"no maintenance", func(e *WorkerRunEvidence) { e.MaintenanceObservations = 0; e.MaintenanceSnapshots = 0 }},
		{"long gap", func(e *WorkerRunEvidence) { e.MaxVerificationGapSeconds = 3601 }},
		{"short span", func(e *WorkerRunEvidence) { e.VerifiedSpanSeconds = 100 }},
		{"no current pass", func(e *WorkerRunEvidence) { e.CurrentHealth = "unknown" }},
	}
	observeProfilingEvidence(&valid, model.RuntimeEvidence{Attributed: true}, reporting.ProfilingEvidence{ProfileCapability: "observed", ProfileEpoch: "continuous", ProfileHistoryComplete: true, ProfileStatusCounts: map[string]uint64{"cpu-rate-limited": 80, "block-disabled": 80}})
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
		runtime := reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "disabled", ProfileHistoryComplete: true}, DBTXID: uint64(i + 1), SnapshotCollectedAt: at, LitestreamSnapshotHealthy: true}
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
	for _, kind := range []string{"maintenance_snapshot_completed", "maintenance_compaction_completed", "maintenance_retention_completed"} {
		if err := db.RecordEventAt(worker.ID, kind, "observed maintenance", string(body), at); err != nil {
			t.Fatal(err)
		}
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
			e := WorkerRunEvidence{CurrentHealth: tt.health, HistoryComplete: true, VerificationCount: 25, WorkloadProgress: 100, MaintenanceObservations: 3, MaintenanceSnapshots: 1, MaintenanceCompactions: 1, MaintenanceRetentions: 1, FirstVerification: &start, LastVerification: &last, VerifiedSpanSeconds: 86400, MaxVerificationGapSeconds: 3600}
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

func addMeasuredSuccessEvidence(t *testing.T, db *model.DB, workerID string) {
	t.Helper()
	worker, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetLatestDeployment(worker.Source)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i, at := range []time.Time{deployment.StartedAt, now} {
		if err := db.UpdateWorkerRuntimeSnapshot(workerID, reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "disabled", ProfileHistoryComplete: true}, SnapshotCollectedAt: at, LitestreamSnapshotHealthy: true, DBTXID: uint64(i + 1), DBStatus: "replicating"}); err != nil {
			t.Fatal(err)
		}
		mustRecordAttributedFixture(t, db, &model.Verification{WorkerID: workerID, StartedAt: at, CompletedAt: &at, Status: "passed", Passed: true, CheckType: "integrity"})
	}
	body, err := json.Marshal(struct {
		reporting.WorkerEventPayload
		Attributed bool `json:"attributed"`
	}{reporting.WorkerEventPayload{WorkerIdentity: fixtureRun(*worker, *deployment)}, true})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"maintenance_snapshot_completed", "maintenance_compaction_completed", "maintenance_retention_completed"} {
		if err := db.RecordEventAt(workerID, kind, "observed maintenance", string(body), now); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMaintenanceReporterJournalEligibility(t *testing.T) {
	t.Setenv("SOAK_WORKER_TOKEN", "test")
	db := openTestDB(t)
	deployment := model.Deployment{Source: "pr-211", GitSHA: "soak", LitestreamSHA: "ls", WorkloadSHA: "generator", ImageRef: "image", Status: "ready"}
	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetLatestDeployment(deployment.Source)
	if err != nil {
		t.Fatal(err)
	}
	deployment = *stored
	cfg := workerconfig.Config{WorkerID: "observed", WorkerName: "observed", Source: deployment.Source, GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, WorkloadSHA: deployment.WorkloadSHA, ImageRef: deployment.ImageRef, RunID: "run", DeploymentID: deployment.ID, MachineID: "machine", ProfileName: "low-volume", WorkloadID: "workload"}
	expected := reporting.WorkerIdentity{WorkerID: cfg.WorkerID, Source: cfg.Source, GitSHA: cfg.GitSHA, LitestreamSHA: cfg.LitestreamSHA, WorkloadSHA: cfg.WorkloadSHA, ImageRef: cfg.ImageRef, RunID: cfg.RunID, DeploymentID: cfg.DeploymentID, MachineID: cfg.MachineID, ProfileName: cfg.ProfileName, WorkloadID: cfg.WorkloadID, ProfileConfig: cfg.WorkloadConfig().JSON()}
	if err := db.ExpectWorkerRun(expected); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewAPI(db, nil, nil, nil, nil, nil).RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	cfg.ControlBaseURL = server.URL
	reporter := workerconfig.NewReporter(cfg)
	observed := reporting.MaintenanceEvidence{Epoch: "log-epoch", StartedAt: deployment.StartedAt, Complete: true}
	for _, line := range []string{`level=INFO msg="snapshot complete" size=4096`, `level=INFO msg="compaction complete" size=4096`, `level=INFO msg="l0 retention enforced" deleted_count=2`} {
		observed.ObserveLine(line)
	}
	for i, at := range []time.Time{deployment.StartedAt, time.Now().UTC()} {
		runtime := reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "disabled", ProfileHistoryComplete: true}, MaintenanceEvidence: observed, DBTXID: uint64(i + 1), SnapshotCollectedAt: at, LitestreamSnapshotHealthy: true, DBStatus: "replicating"}
		if err := reporter.SendHeartbeat(context.Background(), reporting.HeartbeatPayload{RuntimePayload: runtime, SentAt: at}); err != nil {
			t.Fatal(err)
		}
		if err := reporter.SendVerification(context.Background(), reporting.VerificationPayload{RuntimePayload: runtime, StartedAt: at, CompletedAt: at, Passed: true, Status: "passed", CheckType: "integrity"}); err != nil {
			t.Fatal(err)
		}
	}
	evidence, err := buildRunReliability(db, deployment, nil)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
	e := evidence[0]
	if e.MaintenanceSnapshots != 1 || e.MaintenanceCompactions != 1 || e.MaintenanceRetentions != 1 {
		t.Fatalf("maintenance=%+v", e)
	}
	_, eligible, err := successTeardownCandidate(db, deployment, SuccessTeardownPolicy{Threshold: time.Nanosecond}, time.Now().UTC())
	if err != nil || !eligible {
		t.Fatalf("positive measured run eligible=%v err=%v evidence=%+v", eligible, err, e)
	}
	assertHTTPOutOfOrderReplay(t, db, deployment, server.URL, cfg.WorkerID, observed)
}

func assertHTTPOutOfOrderReplay(t *testing.T, db *model.DB, deployment model.Deployment, baseURL, workerID string, maintenance reporting.MaintenanceEvidence) {
	t.Helper()
	verifications, err := db.ListVerifications(workerID, 1)
	if err != nil {
		t.Fatal(err)
	}
	run := verifications[0].Run
	post := func(kind, epoch, id string, attempts, mutations, failures uint64, at time.Time) {
		t.Helper()
		payload := struct {
			reporting.WorkerIdentity
			reporting.RuntimePayload
			workloadEvidenceCounters
			EventType string `json:"event_type,omitempty"`
			EventID   string `json:"workload_event_id,omitempty"`
			Message   string `json:"message"`
		}{WorkerIdentity: run, workloadEvidenceCounters: workloadEvidenceCounters{Epoch: epoch, Present: true, Attempts: attempts, Mutations: mutations, Errors: failures}, EventID: id, Message: "immutable workload failure"}
		if kind == "heartbeat" {
			payload.RuntimePayload = reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "disabled", ProfileHistoryComplete: true}, MaintenanceEvidence: maintenance, SnapshotCollectedAt: at, LitestreamSnapshotHealthy: true, DBTXID: 101}
		} else {
			payload.EventType = "workload_error"
		}
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		endpoint := kind
		if kind == "event" {
			endpoint = "events"
		}
		response, err := http.Post(baseURL+"/api/workers/"+workerID+"/"+endpoint, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("status=%d", response.StatusCode)
		}
	}
	now := time.Now().UTC()
	post("heartbeat", "current", "", 10, 8, 2, now)
	post("event", "old", "old:1", 1, 0, 1, now.Add(-time.Hour))
	post("event", "current", "current:3", 3, 2, 1, now.Add(-time.Minute))
	post("event", "old", "old:1", 1, 0, 1, now.Add(-time.Hour))
	evidence, err := buildRunReliability(db, deployment, nil)
	if err != nil {
		t.Fatal(err)
	}
	e := evidence[0]
	if e.IncompleteObservations != 0 || e.WorkloadErrors != 3 || e.WorkloadAttempts != 11 {
		t.Fatalf("late replay changed coverage or counters: %+v", e)
	}
	ids := make(map[string]int)
	for _, incident := range e.Incidents {
		if incident.WorkloadEventID != "" {
			ids[incident.WorkloadEventID]++
		}
	}
	if ids["old:1"] != 1 || ids["current:3"] != 1 {
		t.Fatalf("event IDs=%v", ids)
	}
	worker, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatal(err)
	}
	var current reporting.RuntimePayload
	if err := json.Unmarshal([]byte(worker.LastRuntimeJSON), &current); err != nil {
		t.Fatal(err)
	}
	if current.DBTXID != 101 || !current.LitestreamSnapshotHealthy {
		t.Fatalf("replay replaced current snapshot: %+v", current)
	}
	post("heartbeat", "current", "", 1, 1, 0, now.Add(time.Second))
	evidence, err = buildRunReliability(db, deployment, nil)
	if err != nil {
		t.Fatal(err)
	}
	if evidence[0].IncompleteObservations == 0 || evidence[0].WorkloadErrors != 3 {
		t.Fatalf("heartbeat reset erased evidence: %+v", evidence)
	}
}

func TestLogEventReplayCountsEachIncidentOnce(t *testing.T) {
	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-211", 211)
	run := fixtureRun(worker, deployment)
	run.MachineID = "machine"
	for i := 1; i <= 2; i++ {
		payload := map[string]any{"incident_event_id": fmt.Sprintf("epoch:%d", i), "maintenance_epoch": "epoch", "maintenance_observer_started_at": deployment.StartedAt, "maintenance_observer_complete": true, "litestream_log_errors_total": i, "attributed": true}
		identity, _ := json.Marshal(run)
		_ = json.Unmarshal(identity, &payload)
		raw, _ := json.Marshal(payload)
		for replay := 0; replay < 2; replay++ {
			if err := db.RecordRuntimeEvidence(run, raw, true, "event"); err != nil {
				t.Fatal(err)
			}
			if err := db.RecordEvent(worker.ID, "litestream_log_error", fmt.Sprintf("error %d recovered", i), string(raw)); err != nil {
				t.Fatal(err)
			}
		}
	}
	evidence, err := buildRunReliability(db, deployment, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 1 || evidence[0].UnexpectedFailures != 2 {
		t.Fatalf("evidence=%+v", evidence)
	}
	count := 0
	for _, incident := range evidence[0].Incidents {
		if incident.IncidentEventID != "" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("individual incidents=%d", count)
	}
}

func TestLateRunEvidenceSurvivesComparisonBoundary(t *testing.T) {
	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-211", 211)
	run := fixtureRun(worker, deployment)
	run.MachineID = "machine"
	end := time.Now().UTC().Add(-time.Minute)
	at := time.Now().UTC()
	v := model.Verification{WorkerID: worker.ID, Run: run, Attributed: true, StartedAt: at, CompletedAt: &at, Status: "failed", ErrorMessage: "late restore failed"}
	if err := db.RecordVerification(&v); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(struct {
		reporting.WorkerIdentity
		Attributed bool `json:"attributed"`
		reporting.WorkloadCounters
	}{run, true, reporting.WorkloadCounters{WorkloadCountersPresent: true, WorkloadCounterEpoch: "epoch", WorkloadAttemptsTotal: 1, WorkloadErrorsTotal: 1}})
	if err := db.RecordRuntimeEvidence(run, raw, true, "event"); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordEvent(worker.ID, "provider_retry", "earlier retry delivered late", string(raw)); err != nil {
		t.Fatal(err)
	}
	evidence, err := buildRunReliability(db, deployment, &end)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 1 || evidence[0].UnexpectedFailures != 3 {
		t.Fatalf("late failures hidden: %+v", evidence)
	}
}

func TestEvidenceReportRejectsNonObject(t *testing.T) {
	for _, body := range []string{"null", "[]", "42"} {
		var payload reporting.WorkerEventPayload
		if _, err := decodeEvidenceReport(strings.NewReader(body), &payload); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

func TestCurrentPendingClearsAfterPassWithoutErasingFailure(t *testing.T) {
	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-211", 211)
	run := fixtureRun(worker, deployment)
	run.MachineID = "machine"
	at := time.Now().UTC().Add(time.Hour)
	for i, status := range []string{"failed", "pending", "passed"} {
		timestamp := at.Add(time.Duration(i) * time.Second)
		v := model.Verification{WorkerID: worker.ID, Run: run, Attributed: true, StartedAt: timestamp, CompletedAt: &timestamp, Status: status, Passed: status == "passed", ErrorMessage: "original failure"}
		if err := db.RecordVerification(&v); err != nil {
			t.Fatal(err)
		}
		e, err := buildRunReliability(db, deployment, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(e) != 1 || e[0].CurrentHealth != status || e[0].UnexpectedFailures != 1 {
			t.Fatalf("status=%s evidence=%+v", status, e)
		}
		if status == "pending" && e[0].CoverageComplete {
			t.Fatal("pending credited as complete")
		}
	}
}

func TestMissingVerificationHistoryIsUnknown(t *testing.T) {
	db := openTestDB(t)
	deployment := model.Deployment{Source: "main", GitSHA: "soak", LitestreamSHA: "litestream", Status: "ready"}
	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	createTestWorker(t, db, model.Worker{ID: "unobserved", Name: "unobserved", Source: "main", ProfileConfig: "{}"})
	evidence, err := buildRunReliability(db, *stored, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 1 || evidence[0].HistoryComplete {
		t.Fatalf("missing history claimed complete: %+v", evidence)
	}
}

func TestProfilingUploadRecoveryRetainsFailure(t *testing.T) {
	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-211", 211)
	run := fixtureRun(worker, deployment)
	run.MachineID = "machine"
	record := reporting.ProfileRecordEvidence{DeploymentID: deployment.ID, WorkerID: worker.ID, MachineID: run.MachineID, RunID: run.RunID, Artifact: "sample.pprof", Status: "available", Upload: "pending", UploadFailureCount: 1, UploadFailures: []reporting.ProfileUploadFailureEvidence{{Attempt: 1, Error: "signal: killed", Stage: "artifact", At: time.Now().UTC()}}}
	incident := record.Incidents()[0]
	incident.Run = run
	eventBody, _ := json.Marshal(struct {
		reporting.WorkerEventPayload
		Attributed bool `json:"attributed"`
	}{reporting.WorkerEventPayload{WorkerIdentity: run, EventType: incident.Kind, Message: incident.Message, WorkloadEvent: reporting.WorkloadEvent{WorkloadEventID: incident.ID}}, true})
	for i := 0; i < 2; i++ {
		if err := db.RecordEvent(worker.ID, incident.Kind, incident.Message, string(eventBody)); err != nil {
			t.Fatal(err)
		}
	}
	for _, status := range []string{"pending", "uploaded"} {
		record.Upload = status
		runtime := reporting.RuntimePayload{LitestreamSnapshotHealthy: true, SnapshotCollectedAt: time.Now().UTC(), ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "observed", ProfileEpoch: "epoch", ProfileHistoryComplete: true, ProfileRecords: []reporting.ProfileRecordEvidence{record}, ProfileIncidents: []reporting.ProfileIncident{incident}}}
		raw, _ := json.Marshal(runtime)
		if err := db.RecordRuntimeEvidence(run, raw, true); err != nil {
			t.Fatal(err)
		}
	}
	evidence, err := buildRunReliability(db, deployment, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 1 || evidence[0].UnexpectedFailures != 1 || evidence[0].Eligible {
		t.Fatalf("profiling failure disappeared: %+v", evidence)
	}
}

func TestProfilingCapabilitiesAndLegacyHistory(t *testing.T) {
	for _, tt := range []struct {
		name, capability string
		counts           map[string]uint64
		complete         bool
		unknown          bool
	}{
		{name: "disabled", capability: "disabled", complete: true},
		{name: "rate limited", capability: "observed", counts: map[string]uint64{"cpu-rate-limited": 3, "block-disabled": 1}, complete: true},
		{name: "legacy failed delivery", capability: "disabled", counts: map[string]uint64{"upload-failed": 75}, unknown: true},
		{name: "truncated", capability: "observed", complete: false, unknown: true},
		{name: "unreported", unknown: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := WorkerRunEvidence{}
			observeProfilingEvidence(&e, model.RuntimeEvidence{Attributed: true}, reporting.ProfilingEvidence{ProfileCapability: tt.capability, ProfileHistoryComplete: tt.complete, ProfileStatusCounts: tt.counts})
			if e.UnexpectedFailures != 0 || (e.IncompleteObservations > 0) != tt.unknown {
				t.Fatalf("capability=%+v", e)
			}
		})
	}
}
