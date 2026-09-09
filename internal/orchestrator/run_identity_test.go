package orchestrator

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestDeploymentDoesNotCreditUnattributedLateVerification(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	deployment := model.Deployment{GitSHA: "new-soak", LitestreamSHA: "new-litestream", Source: "main", ImageRef: "new-image", Status: "ready"}
	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	deployment = *stored
	worker := model.Worker{ID: "replacement", Name: "replacement", Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, Status: model.WorkerRunning, ProfileName: "low-volume", ProfileConfig: "{}"}
	createTestWorker(t, db, worker)
	completed := deployment.StartedAt.Add(time.Minute)
	verification := model.Verification{WorkerID: worker.ID, StartedAt: deployment.StartedAt.Add(-time.Minute), CompletedAt: &completed, Status: "passed", Passed: true, CheckType: "integrity"}
	if err := db.RecordVerification(&verification); err != nil {
		t.Fatal(err)
	}
	rollout, err := buildDeploymentRollout(db, deployment)
	if err != nil {
		t.Fatal(err)
	}
	if rollout.VerifiedSinceDeploy != 0 {
		t.Fatalf("late legacy report credited to replacement: %d", rollout.VerifiedSinceDeploy)
	}
	scorecard, err := buildDeploymentScorecard(db, deployment, nil)
	if err != nil {
		t.Fatal(err)
	}
	if scorecard.PassedWorkers != 0 {
		t.Fatalf("legacy report scored as proven: %d", scorecard.PassedWorkers)
	}
	history, err := db.ListVerifications(worker.ID, 10)
	if err != nil || len(history) != 1 {
		t.Fatalf("history = %v, error = %v", history, err)
	}
}

func recordAttributedFixture(t *testing.T, db *model.DB, verification *model.Verification) error {
	t.Helper()
	worker, err := db.GetWorker(verification.WorkerID)
	if err != nil {
		return err
	}
	deployments, err := db.ListDeployments(worker.Source, 100)
	if err != nil {
		return err
	}
	observed, _ := verificationObservedAt(*verification)
	for _, deployment := range deployments {
		if deployment.StartedAt.After(observed) {
			continue
		}
		if worker.FlyMachineID == "" {
			worker.FlyMachineID = "machine-" + worker.ID
			if err := db.UpdateWorkerMachine(worker.ID, worker.FlyMachineID, worker.FlyVolumeID); err != nil {
				return err
			}
		}
		verification.Run = fixtureRun(*worker, deployment)
		verification.Attributed = true
		break
	}
	return db.RecordVerification(verification)
}

func mustRecordAttributedFixture(t *testing.T, db *model.DB, verification *model.Verification) {
	t.Helper()
	if err := recordAttributedFixture(t, db, verification); err != nil {
		t.Fatal(err)
	}
}

func fixtureRun(worker model.Worker, deployment model.Deployment) reporting.WorkerIdentity {
	return reporting.WorkerIdentity{WorkloadSHA: "generator-sha", Name: worker.Name, WorkerID: worker.ID, DeploymentID: deployment.ID, GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, Source: deployment.Source, MachineID: worker.FlyMachineID, RunID: "run-" + worker.ID, ProfileName: worker.ProfileName, ProfileConfig: "{}", ProfileHash: "44136fa355b3678a", WorkloadID: "workload", ValidatorID: "soak-verifier:" + deployment.GitSHA}
}

func TestReportIdentityQuarantine(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(*reporting.WorkerIdentity)
		attributed bool
	}{
		{"current", func(*reporting.WorkerIdentity) {}, true},
		{"old instance", func(r *reporting.WorkerIdentity) { r.MachineID = "old-machine"; r.RunID = "old-run" }, false},
		{"old run same machine", func(r *reporting.WorkerIdentity) { r.RunID = "old-run" }, false},
		{"old deployment same builds", func(r *reporting.WorkerIdentity) { r.DeploymentID++ }, false},
		{"soak mismatch", func(r *reporting.WorkerIdentity) { r.GitSHA = "old" }, false},
		{"litestream mismatch", func(r *reporting.WorkerIdentity) { r.LitestreamSHA = "old" }, false},
		{"workload mismatch", func(r *reporting.WorkerIdentity) { r.WorkloadID = "other" }, false},
		{"profile mismatch", func(r *reporting.WorkerIdentity) { r.ProfileName = "other" }, false},
		{"actual config mismatch", func(r *reporting.WorkerIdentity) {
			r.ProfileConfig = `{"write_rate":999}`
			sum := sha256.Sum256([]byte(r.ProfileConfig))
			r.ProfileHash = fmt.Sprintf("%x", sum[:8])
		}, false},
		{"profile hash mismatch", func(r *reporting.WorkerIdentity) { r.ProfileHash = "other" }, false},
		{"validator mismatch", func(r *reporting.WorkerIdentity) { r.ValidatorID = "other" }, false},
		{"source mismatch", func(r *reporting.WorkerIdentity) { r.Source = "pr-1" }, false},
		{"legacy", func(r *reporting.WorkerIdentity) { *r = reporting.WorkerIdentity{WorkerID: r.WorkerID} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTestDB(t)
			mustUpsertReadyDeployment(t, db, model.Deployment{GitSHA: "soak", LitestreamSHA: "litestream", Source: "main", ImageRef: "image", Status: "ready"})
			deployment := mustLatestDeployment(t, db, "main")
			worker := model.Worker{ID: "worker", Name: "worker", FlyMachineID: "new-machine", Source: "main", GitSHA: "soak", LitestreamSHA: "litestream", ProfileName: "low-volume", ProfileConfig: "{}", Status: model.WorkerRunning}
			createTestWorker(t, db, worker)
			expected := fixtureRun(worker, deployment)
			if err := db.ExpectWorkerRun(expected); err != nil {
				t.Fatal(err)
			}
			reported := expected
			tc.mutate(&reported)
			if reported.Name == "" {
				reported.Name = worker.ID
			}
			api := NewAPI(db, nil, nil, nil, nil, nil)
			completed := deployment.StartedAt.Add(time.Minute)
			payload := reporting.VerificationPayload{WorkerIdentity: reported, StartedAt: deployment.StartedAt.Add(-time.Minute), CompletedAt: completed, Status: "failed", CheckType: "integrity", ErrorMessage: "retained failure"}
			post := func(path string, body any, handler http.HandlerFunc) {
				t.Helper()
				data, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
				req.SetPathValue("id", worker.ID)
				response := httptest.NewRecorder()
				handler(response, req)
				if response.Code != http.StatusAccepted {
					t.Fatalf("status = %d: %s", response.Code, response.Body.String())
				}
			}
			post("/verifications", payload, api.handleVerification)
			history, err := db.ListVerifications(worker.ID, 10)
			if err != nil || len(history) != 1 {
				t.Fatalf("history = %v, %v", history, err)
			}
			if history[0].Attributed != tc.attributed || history[0].Run != reported {
				t.Fatalf("evidence = %+v", history[0])
			}
			latest, err := db.GetLatestFailedVerification(worker.ID)
			if err != nil || latest == nil || latest.Run != reported {
				t.Fatalf("latest failure = %v, %v", latest, err)
			}
			recent, err := db.ListRecentFailedVerifications(10)
			if err != nil || len(recent) != 1 || recent[0].Run != reported {
				t.Fatalf("recent = %v, %v", recent, err)
			}
			post("/heartbeat", reporting.HeartbeatPayload{WorkerIdentity: reported, SentAt: completed}, api.handleHeartbeat)
			post("/events", reporting.WorkerEventPayload{WorkerIdentity: reported, EventType: reporting.WorkerEventDiskFullRecovered, SentAt: completed, Message: "self healing retained"}, api.handleWorkerEvent)
			stored, err := db.GetWorker(worker.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantStatus := model.WorkerRunning
			if tc.attributed {
				wantStatus = model.WorkerDegraded
			}
			if stored.FlyMachineID != worker.FlyMachineID || stored.GitSHA != worker.GitSHA || stored.Status != wantStatus {
				t.Fatalf("current worker mutated: %+v", stored)
			}
			scorecard, err := buildDeploymentScorecard(db, deployment, nil)
			if err != nil {
				t.Fatal(err)
			}
			wantFailed := 0
			if tc.attributed {
				wantFailed = 1
			}
			if scorecard.FailedWorkers != wantFailed {
				t.Fatalf("failed workers = %d, want %d", scorecard.FailedWorkers, wantFailed)
			}
			events, err := db.ListWorkerEvents(worker.ID, 10)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, event := range events {
				if bytes.Contains([]byte(event.Details), []byte("self healing retained")) {
					found = true
				}
			}
			if !found {
				t.Fatal("self-healing incident evidence lost")
			}
		})
	}
}

func TestHistoricalDeploymentSurvivesWorkerReplacement(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	mustUpsertReadyDeployment(t, db, model.Deployment{GitSHA: "old-soak", LitestreamSHA: "old-ls", Source: "main", ImageRef: "old-image"})
	old := mustLatestDeployment(t, db, "main")
	worker := model.Worker{ID: "worker", Name: "worker", FlyMachineID: "old-machine", Source: "main", GitSHA: old.GitSHA, LitestreamSHA: old.LitestreamSHA, Status: model.WorkerRunning, ProfileName: "low-volume", ProfileConfig: "{}"}
	createTestWorker(t, db, worker)
	run := fixtureRun(worker, old)
	completed := old.StartedAt.Add(time.Minute)
	if err := db.RecordVerification(&model.Verification{WorkerID: worker.ID, Run: run, Attributed: true, StartedAt: completed.Add(-time.Second), CompletedAt: &completed, Status: "passed", Passed: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateWorkerMachineVersionAndConfig(worker.ID, "new-machine", "new-soak", "new-ls", worker.ProfileName, worker.ProfileConfig); err != nil {
		t.Fatal(err)
	}
	scorecard, err := buildDeploymentScorecard(db, old, nil)
	if err != nil {
		t.Fatal(err)
	}
	if scorecard.PassedWorkers != 1 {
		t.Fatalf("historical pass lost after replacement: %+v", scorecard)
	}
	rollout, err := buildDeploymentRollout(db, old)
	if err != nil {
		t.Fatal(err)
	}
	if rollout.VerifiedSinceDeploy != 0 {
		t.Fatal("historical pass credited to current replacement")
	}
}

func TestManualRunRemainsOperationalWithoutDeployment(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	worker := model.Worker{ID: "manual", Name: "manual", FlyMachineID: "machine", Source: "manual", Status: model.WorkerPending, ProfileName: "low-volume", ProfileConfig: "{}"}
	createTestWorker(t, db, worker)
	run := fixtureRun(worker, model.Deployment{Source: "manual"})
	if err := db.ExpectWorkerRun(run); err != nil {
		t.Fatal(err)
	}
	api := NewAPI(db, nil, nil, nil, nil, nil)
	body, err := json.Marshal(reporting.HeartbeatPayload{WorkerIdentity: run, SentAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/heartbeat", bytes.NewReader(body))
	req.SetPathValue("id", worker.ID)
	response := httptest.NewRecorder()
	api.handleHeartbeat(response, req)
	if response.Code != http.StatusAccepted {
		t.Fatalf("heartbeat: %d %s", response.Code, response.Body.String())
	}
	stored, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.WorkerRunning || stored.LastHeartbeatAt == nil {
		t.Fatalf("manual heartbeat quarantined: %+v", stored)
	}
	attributed, quarantined, err := db.ReportAttribution(run)
	if err != nil || attributed || quarantined {
		t.Fatalf("manual attribution = %v, quarantine = %v, error = %v", attributed, quarantined, err)
	}
}

type heldReportResponse struct {
	*httptest.ResponseRecorder
	reached chan struct{}
	release chan struct{}
}

func (w *heldReportResponse) WriteHeader(code int) {
	close(w.reached)
	<-w.release
	w.ResponseRecorder.WriteHeader(code)
}

func TestReplacementSerializesWithReportMutation(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	worker := model.Worker{ID: "worker", Name: "worker", FlyMachineID: "old-machine", Source: "main", GitSHA: "soak", LitestreamSHA: "ls", Status: model.WorkerRunning, ProfileName: "low-volume", ProfileConfig: "{}"}
	createTestWorker(t, db, worker)
	old := fixtureRun(worker, model.Deployment{ID: 1, GitSHA: "soak", LitestreamSHA: "ls", Source: "main"})
	if err := db.ExpectWorkerRun(old); err != nil {
		t.Fatal(err)
	}
	api := NewAPI(db, nil, nil, nil, nil, nil)
	body, err := json.Marshal(reporting.HeartbeatPayload{WorkerIdentity: old, SentAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/heartbeat", bytes.NewReader(body))
	req.SetPathValue("id", worker.ID)
	response := &heldReportResponse{ResponseRecorder: httptest.NewRecorder(), reached: make(chan struct{}), release: make(chan struct{})}
	reportDone := make(chan struct{})
	go func() { api.handleHeartbeat(response, req); close(reportDone) }()
	<-response.reached
	replacement := old
	replacement.RunID = "new-run"
	replacement.MachineID = "new-machine"
	replacementStarted := make(chan struct{})
	replacementDone := make(chan error, 1)
	go func() { close(replacementStarted); replacementDone <- db.ExpectWorkerRun(replacement) }()
	<-replacementStarted
	expected, readErr := db.ExpectedWorkerRun(worker.ID)
	replacementFinished := false
	select {
	case err := <-replacementDone:
		replacementFinished = true
		t.Errorf("replacement completed inside report critical section: %v", err)
	default:
	}
	close(response.release)
	<-reportDone
	if !replacementFinished {
		if err := <-replacementDone; err != nil {
			t.Fatal(err)
		}
	}
	if readErr != nil || expected == nil || expected.RunID != old.RunID {
		t.Fatalf("expected run changed during report mutation: %v, %v", expected, readErr)
	}
	if err := db.UpdateWorkerMachine(worker.ID, replacement.MachineID, ""); err != nil {
		t.Fatal(err)
	}
	staleReq := httptest.NewRequest(http.MethodPost, "/heartbeat", bytes.NewReader(body))
	staleReq.SetPathValue("id", worker.ID)
	staleResponse := httptest.NewRecorder()
	api.handleHeartbeat(staleResponse, staleReq)
	stored, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.FlyMachineID != replacement.MachineID {
		t.Fatalf("stale report replaced current machine: %s", stored.FlyMachineID)
	}
}

func TestCurrentRolloutRejectsEarlierRunOnSameMachine(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	worker := model.Worker{ID: "worker", FlyMachineID: "machine"}
	deployment := model.Deployment{ID: 1, Source: "main", GitSHA: "soak", LitestreamSHA: "ls"}
	old := fixtureRun(worker, deployment)
	expected := old
	expected.RunID = "replacement-run"
	if err := db.ExpectWorkerRun(expected); err != nil {
		t.Fatal(err)
	}
	evidence := []model.Verification{{WorkerID: worker.ID, Run: old, Attributed: true, Passed: true, Status: "passed"}}
	current, err := currentDeploymentVerifications(db, worker, deployment, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 0 {
		t.Fatal("previous run credited to current run on same machine")
	}
	if len(deploymentVerifications(worker, deployment, evidence)) != 1 {
		t.Fatal("historical attributed evidence lost")
	}
}
