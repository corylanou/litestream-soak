package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestBuildDeploymentRollout(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-new",
		LitestreamSHA: "litestream-new",
		ImageRef:      "registry.fly.io/litestream-soak:sha-new",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-running",
		Name:          "worker-main-running",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-new",
		LitestreamSHA: "litestream-new",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-probing",
		Name:          "worker-main-probing",
		Status:        model.WorkerProbing,
		Source:        "main",
		GitSHA:        "sha-new",
		LitestreamSHA: "litestream-new",
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-dormant",
		Name:          "worker-main-dormant",
		Status:        model.WorkerDormant,
		Source:        "main",
		GitSHA:        "sha-new",
		LitestreamSHA: "litestream-old",
		ProfileName:   "burst-volume",
		ProfileConfig: "{}",
	})

	passedAt := deployment.StartedAt.Add(1 * time.Minute).UTC()
	if err := db.RecordVerification(&model.Verification{
		WorkerID:    "worker-main-running",
		StartedAt:   passedAt.Add(-15 * time.Second),
		CompletedAt: &passedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	}); err != nil {
		t.Fatalf("RecordVerification() error = %v", err)
	}

	failedAt := deployment.StartedAt.Add(-2 * time.Minute).UTC()
	if err := db.RecordVerification(&model.Verification{
		WorkerID:     "worker-main-probing",
		StartedAt:    failedAt.Add(-15 * time.Second),
		CompletedAt:  &failedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`,
	}); err != nil {
		t.Fatalf("RecordVerification() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	rollout, err := api.buildDeploymentRollout(*deployment)
	if err != nil {
		t.Fatalf("buildDeploymentRollout() error = %v", err)
	}

	if rollout.TotalWorkers != 3 {
		t.Fatalf("TotalWorkers = %d, want 3", rollout.TotalWorkers)
	}
	if rollout.UpdatedWorkers != 2 {
		t.Fatalf("UpdatedWorkers = %d, want 2", rollout.UpdatedWorkers)
	}
	if rollout.OutdatedWorkers != 1 {
		t.Fatalf("OutdatedWorkers = %d, want 1", rollout.OutdatedWorkers)
	}
	if rollout.RunningWorkers != 1 {
		t.Fatalf("RunningWorkers = %d, want 1", rollout.RunningWorkers)
	}
	if rollout.ProbingWorkers != 1 {
		t.Fatalf("ProbingWorkers = %d, want 1", rollout.ProbingWorkers)
	}
	if rollout.DormantWorkers != 1 {
		t.Fatalf("DormantWorkers = %d, want 1", rollout.DormantWorkers)
	}
	if rollout.AttentionWorkers != 2 {
		t.Fatalf("AttentionWorkers = %d, want 2", rollout.AttentionWorkers)
	}
	if rollout.VerifiedSinceDeploy != 1 {
		t.Fatalf("VerifiedSinceDeploy = %d, want 1", rollout.VerifiedSinceDeploy)
	}
	if rollout.AwaitingVerification != 1 {
		t.Fatalf("AwaitingVerification = %d, want 1", rollout.AwaitingVerification)
	}
	if rollout.Status != "rolling_out" {
		t.Fatalf("Status = %q, want rolling_out", rollout.Status)
	}
	if rollout.NextAction == "" {
		t.Fatalf("NextAction should not be empty")
	}
	if len(rollout.NextChecks) == 0 {
		t.Fatalf("NextChecks should not be empty")
	}
	if rollout.GraceWindowExceeded {
		t.Fatalf("GraceWindowExceeded = true, want false for fresh rollout")
	}
	if rollout.Workers[0].WorkerID != "worker-main-dormant" {
		t.Fatalf("first worker = %q, want outdated worker first", rollout.Workers[0].WorkerID)
	}
	if rollout.Workers[0].LitestreamSHA != "litestream-old" {
		t.Fatalf("first worker LitestreamSHA = %q, want litestream-old", rollout.Workers[0].LitestreamSHA)
	}
	if rollout.Workers[1].CurrentFailureSignature != "litestream_sync_socket_refused" {
		t.Fatalf("CurrentFailureSignature = %q, want litestream_sync_socket_refused", rollout.Workers[1].CurrentFailureSignature)
	}
	if !rollout.Workers[2].VerifiedSinceDeploy {
		t.Fatalf("VerifiedSinceDeploy = false, want true for worker-main-running")
	}
}

func TestHandleGetLatestDeployment(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-latest",
		LitestreamSHA: "litestream-latest",
		ImageRef:      "registry.fly.io/litestream-soak:sha-latest",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-one",
		Name:          "worker-main-one",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-latest",
		LitestreamSHA: "litestream-latest",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/deployments/latest", nil)
	recorder := httptest.NewRecorder()

	api.handleGetLatestDeployment(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var rollout DeploymentRolloutResponse
	if err := json.NewDecoder(recorder.Body).Decode(&rollout); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if rollout.Deployment.GitSHA != "sha-latest" {
		t.Fatalf("Deployment.GitSHA = %q, want sha-latest", rollout.Deployment.GitSHA)
	}
	if rollout.Deployment.LitestreamSHA != "litestream-latest" {
		t.Fatalf("Deployment.LitestreamSHA = %q, want litestream-latest", rollout.Deployment.LitestreamSHA)
	}
	if rollout.Status != "stable" {
		t.Fatalf("Status = %q, want stable", rollout.Status)
	}
	if rollout.UpdatedWorkers != 1 || rollout.TotalWorkers != 1 {
		t.Fatalf("updated/total = %d/%d, want 1/1", rollout.UpdatedWorkers, rollout.TotalWorkers)
	}
	if rollout.NextAction == "" {
		t.Fatalf("NextAction should not be empty")
	}
}

func TestBuildLatestDeploymentComparison(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-base",
		LitestreamSHA: "litestream-base",
		ImageRef:      "registry.fly.io/litestream-soak:sha-base",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(base) error = %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-head",
		LitestreamSHA: "litestream-head",
		ImageRef:      "registry.fly.io/litestream-soak:sha-head",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(head) error = %v", err)
	}

	deployments, err := db.ListDeployments("main", 2)
	if err != nil {
		t.Fatalf("ListDeployments() error = %v", err)
	}
	head := deployments[0]
	base := deployments[1]

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-one",
		Name:          "worker-main-one",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        head.GitSHA,
		LitestreamSHA: head.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-two",
		Name:          "worker-main-two",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        head.GitSHA,
		LitestreamSHA: head.LitestreamSHA,
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-three",
		Name:          "worker-main-three",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        head.GitSHA,
		LitestreamSHA: head.LitestreamSHA,
		ProfileName:   "burst-volume",
		ProfileConfig: "{}",
	})

	basePassAt := base.StartedAt.Add(200 * time.Millisecond).UTC()
	baseFailAt := base.StartedAt.Add(400 * time.Millisecond).UTC()
	headPassAt := head.StartedAt.Add(200 * time.Millisecond).UTC()
	headFailAt := head.StartedAt.Add(400 * time.Millisecond).UTC()

	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-one",
		StartedAt:   basePassAt.Add(-15 * time.Second),
		CompletedAt: &basePassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-main-two",
		StartedAt:    baseFailAt.Add(-15 * time.Second),
		CompletedAt:  &baseFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wrong # of entries in index idx_load_test_timestamp`,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-three",
		StartedAt:   basePassAt.Add(-30 * time.Second),
		CompletedAt: &basePassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-one",
		StartedAt:   headPassAt.Add(-15 * time.Second),
		CompletedAt: &headPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-two",
		StartedAt:   headPassAt.Add(-30 * time.Second),
		CompletedAt: &headPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-main-three",
		StartedAt:    headFailAt.Add(-15 * time.Second),
		CompletedAt:  &headFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": context deadline exceeded`,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	comparison, err := api.buildLatestDeploymentComparison("main")
	if err != nil {
		t.Fatalf("buildLatestDeploymentComparison() error = %v", err)
	}
	if comparison == nil {
		t.Fatal("comparison = nil, want non-nil")
	}
	if comparison.Verdict != "mixed" {
		t.Fatalf("Verdict = %q, want mixed", comparison.Verdict)
	}
	if comparison.PassDelta != 0 {
		t.Fatalf("PassDelta = %d, want 0", comparison.PassDelta)
	}
	if comparison.FailDelta != 0 {
		t.Fatalf("FailDelta = %d, want 0", comparison.FailDelta)
	}
	if len(comparison.ImprovedWorkers) != 1 || comparison.ImprovedWorkers[0].WorkerID != "worker-main-two" {
		t.Fatalf("ImprovedWorkers = %+v, want worker-main-two", comparison.ImprovedWorkers)
	}
	if len(comparison.RegressedWorkers) != 1 || comparison.RegressedWorkers[0].WorkerID != "worker-main-three" {
		t.Fatalf("RegressedWorkers = %+v, want worker-main-three", comparison.RegressedWorkers)
	}
	if len(comparison.NewFailures) != 1 || comparison.NewFailures[0].Signature != "litestream_sync_timeout" {
		t.Fatalf("NewFailures = %+v, want litestream_sync_timeout", comparison.NewFailures)
	}
	if len(comparison.ResolvedFailures) != 1 || comparison.ResolvedFailures[0].Signature != "sqlite_index_mismatch" {
		t.Fatalf("ResolvedFailures = %+v, want sqlite_index_mismatch", comparison.ResolvedFailures)
	}
}

func TestHandleListAlertsIncludesDeploymentTriage(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if _, created, err := db.CreateAlert(&model.AlertDelivery{
		AlertType:        "deployment_attention",
		Fingerprint:      "deployment_attention:main:sha-latest:needs_attention",
		Status:           "sent",
		FailureStage:     "deployment",
		FailureSignature: "needs_attention",
		Message:          "Treat this as a failed rollout until the degraded or dormant workers are explained.",
	}); err != nil {
		t.Fatalf("CreateAlert() error = %v", err)
	} else if !created {
		t.Fatalf("CreateAlert() created = false, want true")
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/alerts", nil)
	recorder := httptest.NewRecorder()

	api.handleListAlerts(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var alerts []struct {
		Alert          model.AlertDelivery `json:"alert"`
		TriageCommands []string            `json:"triage_commands"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&alerts); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1", len(alerts))
	}
	if len(alerts[0].TriageCommands) == 0 {
		t.Fatalf("TriageCommands should not be empty")
	}
}

func TestApplyDeploymentRolloutGuidanceGraceWindow(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	rollout := DeploymentRolloutResponse{
		Deployment: model.Deployment{
			GitSHA:    "sha-old",
			StartedAt: now.Add(-1 * time.Hour),
		},
		Status:          "needs_attention",
		TotalWorkers:    9,
		UpdatedWorkers:  9,
		DegradedWorkers: 2,
		DormantWorkers:  1,
	}

	applyDeploymentRolloutGuidance(&rollout, now)

	if !rollout.GraceWindowExceeded {
		t.Fatalf("GraceWindowExceeded = false, want true")
	}
	if rollout.NextAction == "" {
		t.Fatalf("NextAction should not be empty")
	}
	if len(rollout.NextChecks) == 0 {
		t.Fatalf("NextChecks should not be empty")
	}
}

func openTestDB(t *testing.T) *model.DB {
	t.Helper()

	db, err := model.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("model.Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func createTestWorker(t *testing.T, db *model.DB, worker model.Worker) {
	t.Helper()

	if err := db.CreateWorker(&worker); err != nil {
		t.Fatalf("CreateWorker(%s) error = %v", worker.ID, err)
	}
}

func mustRecordVerification(t *testing.T, db *model.DB, verification *model.Verification) {
	t.Helper()

	if err := db.RecordVerification(verification); err != nil {
		t.Fatalf("RecordVerification(%s) error = %v", verification.WorkerID, err)
	}
}
