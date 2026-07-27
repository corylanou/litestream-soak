package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
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
	if rollout.Workers[1].CurrentFailureSignature != "" {
		t.Fatalf("CurrentFailureSignature = %q, want empty for pre-rollout verification", rollout.Workers[1].CurrentFailureSignature)
	}
	if !rollout.Workers[2].VerifiedSinceDeploy {
		t.Fatalf("VerifiedSinceDeploy = false, want true for worker-main-running")
	}
}

func TestBuildDeploymentRolloutCountsRuntimeUnhealthy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-runtime",
		LitestreamSHA: "litestream-runtime",
		ImageRef:      "registry.fly.io/litestream-soak:sha-runtime",
		Source:        "pr-1228",
		PRNumber:      1228,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	deployment, err := db.GetLatestDeployment("pr-1228")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1228-gharchive",
		Name:          "worker-pr-1228-gharchive",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "sha-runtime",
		LitestreamSHA: "litestream-runtime",
		ProfileName:   "gharchive-replay",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerRuntimeSnapshot("worker-pr-1228-gharchive", reporting.RuntimePayload{
		SnapshotCollectedAt:     deployment.StartedAt.Add(2 * time.Minute).UTC(),
		LitestreamSnapshotError: "litestream process not responding",
	}); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
	}
	passedAt := deployment.StartedAt.Add(3 * time.Minute).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-pr-1228-gharchive",
		StartedAt:   passedAt.Add(-15 * time.Second),
		CompletedAt: &passedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	rollout, err := api.buildDeploymentRollout(*deployment)
	if err != nil {
		t.Fatalf("buildDeploymentRollout() error = %v", err)
	}

	if rollout.RuntimeUnhealthyWorkers != 1 {
		t.Fatalf("RuntimeUnhealthyWorkers = %d, want 1", rollout.RuntimeUnhealthyWorkers)
	}
	if rollout.AttentionWorkers != 1 {
		t.Fatalf("AttentionWorkers = %d, want 1", rollout.AttentionWorkers)
	}
	if rollout.Status != "needs_attention" {
		t.Fatalf("Status = %q, want needs_attention", rollout.Status)
	}
	if rollout.Workers[0].RuntimeSnapshotStatus != reporting.RuntimeSnapshotStatusUnhealthy {
		t.Fatalf("RuntimeSnapshotStatus = %q, want unhealthy", rollout.Workers[0].RuntimeSnapshotStatus)
	}
	if !strings.Contains(rollout.Summary, "1 runtime-unhealthy worker") {
		t.Fatalf("Summary = %q, want runtime-unhealthy worker", rollout.Summary)
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
	if rollout.Status != "settling" {
		t.Fatalf("Status = %q, want settling", rollout.Status)
	}
	if rollout.UpdatedWorkers != 1 || rollout.TotalWorkers != 1 {
		t.Fatalf("updated/total = %d/%d, want 1/1", rollout.UpdatedWorkers, rollout.TotalWorkers)
	}
	if rollout.NextAction == "" {
		t.Fatalf("NextAction should not be empty")
	}
}

func TestHandleGetLatestDeploymentPromptUsesHealthyModeForStableRollout(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-latest",
		LitestreamSHA: "litestream-latest",
		ImageRef:      "registry.fly.io/litestream-soak:sha-latest",
		Source:        "pr-1228",
		PRNumber:      1228,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	deployment, err := db.GetLatestDeployment("pr-1228")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1228-low",
		Name:          "worker-pr-1228-low",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        deployment.GitSHA,
		LitestreamSHA: deployment.LitestreamSHA,
		PRNumber:      1228,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerRuntimeSnapshot("worker-pr-1228-low", reporting.RuntimePayload{
		SnapshotCollectedAt:       deployment.StartedAt.Add(2 * time.Minute).UTC(),
		LitestreamSnapshotHealthy: true,
		DBStatus:                  "replicating",
	}); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
	}
	passedAt := deployment.StartedAt.Add(25 * time.Hour).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-pr-1228-low",
		StartedAt:   passedAt.Add(-15 * time.Second),
		CompletedAt: &passedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/deployments/latest/prompt?source=pr-1228", nil)
	recorder := httptest.NewRecorder()

	api.handleGetLatestDeploymentPrompt(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "<mode>\nhealthy\n</mode>") {
		t.Fatalf("body missing healthy mode: %s", body)
	}
	if !strings.Contains(body, "<latest_rollout>") {
		t.Fatalf("body missing latest rollout section: %s", body)
	}
}

func TestBuildWorkerSummaryIgnoresPrecreationVerification(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	worker := model.Worker{
		ID:            "worker-pr-1228-low-vol",
		Name:          "worker-pr-1228-low-vol",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	createTestWorker(t, db, worker)

	storedWorker, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}

	oldFailureAt := storedWorker.CreatedAt.Add(-1 * time.Minute).UTC()
	if err := db.RecordVerification(&model.Verification{
		WorkerID:     worker.ID,
		StartedAt:    oldFailureAt.Add(-10 * time.Second),
		CompletedAt:  &oldFailureAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`,
	}); err != nil {
		t.Fatalf("RecordVerification() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	summary, err := api.buildWorkerSummary(*storedWorker)
	if err != nil {
		t.Fatalf("buildWorkerSummary() error = %v", err)
	}
	if summary.LastVerification != nil {
		t.Fatalf("LastVerification = %#v, want nil for precreation verification", summary.LastVerification)
	}
	if summary.CurrentFailureSignature != "" {
		t.Fatalf("CurrentFailureSignature = %q, want empty", summary.CurrentFailureSignature)
	}
}

func TestHandleGetRunArchivePromptUsesHealthyModeForSuccessArchive(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	archive := &model.RunArchive{
		DeploymentID:  99,
		Source:        "pr-1228",
		ArchiveType:   "success",
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		Status:        "stable",
		Summary:       "PR #1228 completed cleanly.",
		Payload:       `{"rollout":{"status":"stable"}}`,
	}
	if _, err := db.RecordRunArchive(archive); err != nil {
		t.Fatalf("RecordRunArchive() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/run-archives/1/prompt", nil)
	request.SetPathValue("id", strconv.Itoa(archive.ID))
	recorder := httptest.NewRecorder()

	api.handleGetRunArchivePrompt(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "<mode>\nhealthy\n</mode>") {
		t.Fatalf("body missing healthy mode: %s", body)
	}
	if !strings.Contains(body, "<archived_payload>") {
		t.Fatalf("body missing archived payload: %s", body)
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

func TestBuildHomePageDataDefaultsPRSourceToMainComparison(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	api := NewAPI(db, nil, nil, nil, nil, nil)

	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-main",
		LitestreamSHA: "litestream-main",
		ImageRef:      "registry.fly.io/litestream-soak:sha-main",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(main) error = %v", err)
	}
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		ImageRef:      "registry.fly.io/litestream-soak:sha-pr",
		Source:        "pr-1228",
		PRNumber:      1228,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(pr) error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low",
		Name:          "worker-main-low",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-main",
		LitestreamSHA: "litestream-main",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-low",
		Name:          "worker-pr-low",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		PRNumber:      1228,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})

	request := httptest.NewRequest(http.MethodGet, "/ui?source=pr-1228", nil)
	data, err := api.buildHomePageData(request)
	if err != nil {
		t.Fatalf("buildHomePageData() error = %v", err)
	}

	if data.SelectedSource != "pr-1228" {
		t.Fatalf("SelectedSource = %q, want pr-1228", data.SelectedSource)
	}
	if data.ReleaseComparison == nil {
		t.Fatal("ReleaseComparison = nil")
	}
	if data.ReleaseComparison.ComparisonKind != "cross_source" {
		t.Fatalf("ComparisonKind = %q, want cross_source", data.ReleaseComparison.ComparisonKind)
	}
	if data.ReleaseComparison.BaseSource != "main" {
		t.Fatalf("BaseSource = %q, want main", data.ReleaseComparison.BaseSource)
	}
	if data.ReleaseComparison.HeadSource != "pr-1228" {
		t.Fatalf("HeadSource = %q, want pr-1228", data.ReleaseComparison.HeadSource)
	}
	if data.LatestRolloutURL != "/api/deployments/latest?source=pr-1228" {
		t.Fatalf("LatestRolloutURL = %q", data.LatestRolloutURL)
	}
	if data.ComparisonJSONURL != "/api/deployments/compare/latest?base_source=main&head_source=pr-1228" {
		t.Fatalf("ComparisonJSONURL = %q", data.ComparisonJSONURL)
	}
	if len(data.ActiveSources) != 2 {
		t.Fatalf("len(ActiveSources) = %d, want 2", len(data.ActiveSources))
	}
	if data.ActiveSources[0].Source != "main" || data.ActiveSources[0].Selected {
		t.Fatalf("first ActiveSource = %+v, want unselected main (stable ordering: main always first)", data.ActiveSources[0])
	}
	if data.ActiveSources[1].Source != "pr-1228" || !data.ActiveSources[1].Selected {
		t.Fatalf("second ActiveSource = %+v, want selected pr-1228", data.ActiveSources[1])
	}
}

func TestFailureRecoveryMarksNextPass(t *testing.T) {
	failedAt := timeMustParse("2026-04-26T15:00:00Z")
	passedAt := failedAt.Add(15 * time.Minute)
	olderFailedAt := failedAt.Add(-15 * time.Minute)
	verifications := []model.Verification{
		{
			ID:          3,
			WorkerID:    "worker-pr-1228-gharchive-mixed",
			CompletedAt: &passedAt,
			Status:      "passed",
			Passed:      true,
		},
		{
			ID:           2,
			WorkerID:     "worker-pr-1228-gharchive-mixed",
			CompletedAt:  &failedAt,
			Status:       "failed",
			Passed:       false,
			ErrorMessage: `validation failed: restore failed: get LTX time bounds: operation error S3: ListObjectsV2, https response error StatusCode: 408, RequestID: 1777230002707552565, api error RequestCanceled: Request is canceled.`,
		},
		{
			ID:           1,
			WorkerID:     "worker-pr-1228-gharchive-mixed",
			CompletedAt:  &olderFailedAt,
			Status:       "failed",
			Passed:       false,
			ErrorMessage: "validation failed",
		},
	}

	recovery := failureRecovery(verifications, verifications[1])
	if !recovery.FailedThenNextPassed {
		t.Fatal("FailedThenNextPassed = false, want true")
	}
	if recovery.StillFailing {
		t.Fatal("StillFailing = true, want false")
	}
	if recovery.LastPassAfterFailureAt == nil || !recovery.LastPassAfterFailureAt.Equal(passedAt) {
		t.Fatalf("LastPassAfterFailureAt = %v, want %v", recovery.LastPassAfterFailureAt, passedAt)
	}
}

func TestBuildRequestedDeploymentComparisonCrossSource(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-main",
		LitestreamSHA: "litestream-main",
		ImageRef:      "registry.fly.io/litestream-soak:sha-main",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(main) error = %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		ImageRef:      "registry.fly.io/litestream-soak:sha-pr",
		Source:        "pr-1228",
		PRNumber:      1228,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(pr) error = %v", err)
	}

	mainDeployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment(main) error = %v", err)
	}
	prDeployment, err := db.GetLatestDeployment("pr-1228")
	if err != nil {
		t.Fatalf("GetLatestDeployment(pr-1228) error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low",
		Name:          "worker-main-low",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        mainDeployment.GitSHA,
		LitestreamSHA: mainDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-high",
		Name:          "worker-main-high",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        mainDeployment.GitSHA,
		LitestreamSHA: mainDeployment.LitestreamSHA,
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1228-low",
		Name:          "worker-pr-1228-low",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        prDeployment.GitSHA,
		LitestreamSHA: prDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1228-high",
		Name:          "worker-pr-1228-high",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        prDeployment.GitSHA,
		LitestreamSHA: prDeployment.LitestreamSHA,
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})

	mainPassAt := mainDeployment.StartedAt.Add(200 * time.Millisecond).UTC()
	mainFailAt := mainDeployment.StartedAt.Add(400 * time.Millisecond).UTC()
	prPassAt := prDeployment.StartedAt.Add(200 * time.Millisecond).UTC()

	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-low",
		StartedAt:   mainPassAt.Add(-15 * time.Second),
		CompletedAt: &mainPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-main-high",
		StartedAt:    mainFailAt.Add(-15 * time.Second),
		CompletedAt:  &mainFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wrong # of entries in index idx_load_test_timestamp`,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-pr-1228-low",
		StartedAt:   prPassAt.Add(-15 * time.Second),
		CompletedAt: &prPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-pr-1228-high",
		StartedAt:   prPassAt.Add(-30 * time.Second),
		CompletedAt: &prPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	comparison, err := api.buildRequestedDeploymentComparison("", "main", "pr-1228")
	if err != nil {
		t.Fatalf("buildRequestedDeploymentComparison() error = %v", err)
	}
	if comparison == nil {
		t.Fatal("comparison = nil, want non-nil")
	}
	if comparison.ComparisonKind != "cross_source" {
		t.Fatalf("ComparisonKind = %q, want cross_source", comparison.ComparisonKind)
	}
	if comparison.BaseSource != "main" || comparison.HeadSource != "pr-1228" {
		t.Fatalf("sources = %q/%q, want main/pr-1228", comparison.BaseSource, comparison.HeadSource)
	}
	if comparison.Verdict != "better" {
		t.Fatalf("Verdict = %q, want better", comparison.Verdict)
	}
	if comparison.PassDelta != 1 {
		t.Fatalf("PassDelta = %d, want 1", comparison.PassDelta)
	}
	if comparison.FailDelta != -1 {
		t.Fatalf("FailDelta = %d, want -1", comparison.FailDelta)
	}
	if len(comparison.ImprovedWorkers) != 1 || comparison.ImprovedWorkers[0].Profile != "high-volume" {
		t.Fatalf("ImprovedWorkers = %+v, want high-volume", comparison.ImprovedWorkers)
	}
	if len(comparison.RegressedWorkers) != 0 {
		t.Fatalf("RegressedWorkers = %+v, want none", comparison.RegressedWorkers)
	}
}

func TestBuildRequestedDeploymentComparisonCompletedSuccessHeadPassed(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-main",
		LitestreamSHA: "litestream-main",
		ImageRef:      "registry.fly.io/litestream-soak:sha-main",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(main) error = %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		ImageRef:      "registry.fly.io/litestream-soak:sha-pr",
		Source:        "pr-1312",
		PRNumber:      1312,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(pr) error = %v", err)
	}

	mainDeployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment(main) error = %v", err)
	}
	prDeployment, err := db.GetLatestDeployment("pr-1312")
	if err != nil {
		t.Fatalf("GetLatestDeployment(pr-1312) error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low",
		Name:          "worker-main-low",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        mainDeployment.GitSHA,
		LitestreamSHA: mainDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-high",
		Name:          "worker-main-high",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        mainDeployment.GitSHA,
		LitestreamSHA: mainDeployment.LitestreamSHA,
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	for _, workerID := range []string{"worker-pr-1312-low", "worker-pr-1312-high"} {
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerStopped,
			Source:        "pr-1312",
			GitSHA:        prDeployment.GitSHA,
			LitestreamSHA: prDeployment.LitestreamSHA,
			PRNumber:      1312,
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
	}

	mainPassAt := mainDeployment.StartedAt.Add(200 * time.Millisecond).UTC()
	mainFailAt := mainDeployment.StartedAt.Add(400 * time.Millisecond).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-low",
		StartedAt:   mainPassAt.Add(-15 * time.Second),
		CompletedAt: &mainPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-main-high",
		StartedAt:    mainFailAt.Add(-15 * time.Second),
		CompletedAt:  &mainFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wrong # of entries in index idx_load_test_timestamp`,
	})
	if _, err := db.RecordRunArchive(&model.RunArchive{
		DeploymentID:  prDeployment.ID,
		Source:        "pr-1312",
		ArchiveType:   "success",
		GitSHA:        prDeployment.GitSHA,
		LitestreamSHA: prDeployment.LitestreamSHA,
		ImageRef:      prDeployment.ImageRef,
		Status:        "stable",
		Summary:       "PR #1312 completed a clean soak.",
		Payload:       `{"rollout":{"status":"stable"}}`,
		ArchivedAt:    prDeployment.StartedAt.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("RecordRunArchive() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	comparison, err := api.buildRequestedDeploymentComparison("", "main", "pr-1312")
	if err != nil {
		t.Fatalf("buildRequestedDeploymentComparison() error = %v", err)
	}
	if comparison == nil {
		t.Fatal("comparison = nil, want non-nil")
	}
	if comparison.Verdict != "passed" {
		t.Fatalf("Verdict = %q, want passed", comparison.Verdict)
	}
	if comparison.PassDelta != 0 {
		t.Fatalf("PassDelta = %d, want suppressed zero", comparison.PassDelta)
	}
	if comparison.FailDelta != 0 {
		t.Fatalf("FailDelta = %d, want suppressed zero", comparison.FailDelta)
	}
	if !strings.Contains(comparison.Summary, "PR #1312") || !strings.Contains(comparison.Summary, "torn down on success") {
		t.Fatalf("Summary = %q, want completed-success summary", comparison.Summary)
	}
}

func TestBuildRequestedDeploymentComparisonNoArchiveStaysInsufficient(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-main",
		LitestreamSHA: "litestream-main",
		ImageRef:      "registry.fly.io/litestream-soak:sha-main",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(main) error = %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		ImageRef:      "registry.fly.io/litestream-soak:sha-pr",
		Source:        "pr-1313",
		PRNumber:      1313,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(pr) error = %v", err)
	}

	mainDeployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment(main) error = %v", err)
	}
	prDeployment, err := db.GetLatestDeployment("pr-1313")
	if err != nil {
		t.Fatalf("GetLatestDeployment(pr-1313) error = %v", err)
	}
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low",
		Name:          "worker-main-low",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        mainDeployment.GitSHA,
		LitestreamSHA: mainDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1313-low",
		Name:          "worker-pr-1313-low",
		Status:        model.WorkerRunning,
		Source:        "pr-1313",
		GitSHA:        prDeployment.GitSHA,
		LitestreamSHA: prDeployment.LitestreamSHA,
		PRNumber:      1313,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})

	mainPassAt := mainDeployment.StartedAt.Add(200 * time.Millisecond).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-low",
		StartedAt:   mainPassAt.Add(-15 * time.Second),
		CompletedAt: &mainPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	comparison, err := api.buildRequestedDeploymentComparison("", "main", "pr-1313")
	if err != nil {
		t.Fatalf("buildRequestedDeploymentComparison() error = %v", err)
	}
	if comparison == nil {
		t.Fatal("comparison = nil, want non-nil")
	}
	if comparison.Verdict != "insufficient_data" {
		t.Fatalf("Verdict = %q, want insufficient_data", comparison.Verdict)
	}
}

func TestBuildRequestedDeploymentComparisonSuccessArchiveDoesNotHideActiveWorse(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-main",
		LitestreamSHA: "litestream-main",
		ImageRef:      "registry.fly.io/litestream-soak:sha-main",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(main) error = %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		ImageRef:      "registry.fly.io/litestream-soak:sha-pr",
		Source:        "pr-1314",
		PRNumber:      1314,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(pr) error = %v", err)
	}

	mainDeployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment(main) error = %v", err)
	}
	prDeployment, err := db.GetLatestDeployment("pr-1314")
	if err != nil {
		t.Fatalf("GetLatestDeployment(pr-1314) error = %v", err)
	}
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low",
		Name:          "worker-main-low",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        mainDeployment.GitSHA,
		LitestreamSHA: mainDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1314-low",
		Name:          "worker-pr-1314-low",
		Status:        model.WorkerRunning,
		Source:        "pr-1314",
		GitSHA:        prDeployment.GitSHA,
		LitestreamSHA: prDeployment.LitestreamSHA,
		PRNumber:      1314,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})

	mainPassAt := mainDeployment.StartedAt.Add(200 * time.Millisecond).UTC()
	prFailAt := prDeployment.StartedAt.Add(200 * time.Millisecond).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-low",
		StartedAt:   mainPassAt.Add(-15 * time.Second),
		CompletedAt: &mainPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-pr-1314-low",
		StartedAt:    prFailAt.Add(-15 * time.Second),
		CompletedAt:  &prFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": context deadline exceeded`,
	})
	if _, err := db.RecordRunArchive(&model.RunArchive{
		DeploymentID:  prDeployment.ID,
		Source:        "pr-1314",
		ArchiveType:   "success",
		GitSHA:        prDeployment.GitSHA,
		LitestreamSHA: prDeployment.LitestreamSHA,
		ImageRef:      prDeployment.ImageRef,
		Status:        "stable",
		Summary:       "Older PR #1314 run completed a clean soak.",
		Payload:       `{"rollout":{"status":"stable"}}`,
		ArchivedAt:    prDeployment.StartedAt.Add(-24 * time.Hour),
	}); err != nil {
		t.Fatalf("RecordRunArchive() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	comparison, err := api.buildRequestedDeploymentComparison("", "main", "pr-1314")
	if err != nil {
		t.Fatalf("buildRequestedDeploymentComparison() error = %v", err)
	}
	if comparison == nil {
		t.Fatal("comparison = nil, want non-nil")
	}
	if comparison.Verdict != "worse" {
		t.Fatalf("Verdict = %q, want worse", comparison.Verdict)
	}
}

func TestBuildRequestedDeploymentComparisonExcludesRampUpFailuresFromVerdict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-main",
		LitestreamSHA: "litestream-main",
		ImageRef:      "registry.fly.io/litestream-soak:sha-main",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(main) error = %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		ImageRef:      "registry.fly.io/litestream-soak:sha-pr",
		Source:        "pr-1305",
		PRNumber:      1305,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(pr) error = %v", err)
	}

	mainDeployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment(main) error = %v", err)
	}
	prDeployment, err := db.GetLatestDeployment("pr-1305")
	if err != nil {
		t.Fatalf("GetLatestDeployment(pr-1305) error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low",
		Name:          "worker-main-low",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        mainDeployment.GitSHA,
		LitestreamSHA: mainDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1305-low",
		Name:          "worker-pr-1305-low",
		Status:        model.WorkerRunning,
		Source:        "pr-1305",
		GitSHA:        prDeployment.GitSHA,
		LitestreamSHA: prDeployment.LitestreamSHA,
		PRNumber:      1305,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})

	mainPassAt := mainDeployment.StartedAt.Add(3 * time.Minute).UTC()
	prWorker, err := db.GetWorker("worker-pr-1305-low")
	if err != nil {
		t.Fatalf("GetWorker(pr) error = %v", err)
	}
	prFailAt := prWorker.CreatedAt.Add(30 * time.Minute).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-low",
		StartedAt:   mainPassAt.Add(-15 * time.Second),
		CompletedAt: &mainPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-pr-1305-low",
		StartedAt:    prFailAt.Add(-15 * time.Second),
		CompletedAt:  &prFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `validation failed (exit 1): unexpected EOF reading stdout`,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	comparison, err := api.buildRequestedDeploymentComparison("", "main", "pr-1305")
	if err != nil {
		t.Fatalf("buildRequestedDeploymentComparison() error = %v", err)
	}
	if comparison == nil {
		t.Fatal("comparison = nil, want non-nil")
	}
	if comparison.Head.RampUpFailures != 1 {
		t.Fatalf("Head.RampUpFailures = %d, want 1", comparison.Head.RampUpFailures)
	}
	if comparison.Head.ActionableFailedWorkers != 0 {
		t.Fatalf("Head.ActionableFailedWorkers = %d, want 0", comparison.Head.ActionableFailedWorkers)
	}
	if comparison.FailDelta != 0 {
		t.Fatalf("FailDelta = %d, want 0", comparison.FailDelta)
	}
	if comparison.PassDelta != 0 {
		t.Fatalf("PassDelta = %d, want 0", comparison.PassDelta)
	}
	if comparison.Verdict == "worse" {
		t.Fatalf("Verdict = %q, want non-worse for ramp-up failure", comparison.Verdict)
	}
	if len(comparison.RegressedWorkers) != 0 {
		t.Fatalf("RegressedWorkers = %+v, want none", comparison.RegressedWorkers)
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

func TestHandleListAlertsIncludesDormantFleetConditionAndTriage(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	conditionStartedAt := time.Date(2026, 7, 24, 12, 30, 0, 0, time.UTC)
	if _, created, err := db.CreateAlert(&model.AlertDelivery{
		Source:             "pr-155",
		AlertType:          fleetFullyDormantAlertType,
		Fingerprint:        fleetFullyDormantAlertType + ":pr-155:" + conditionStartedAt.Format(time.RFC3339Nano),
		Status:             "sent",
		ConditionStatus:    "active",
		ConditionStartedAt: &conditionStartedAt,
	}); err != nil {
		t.Fatalf("CreateAlert() error = %v", err)
	} else if !created {
		t.Fatal("CreateAlert() created = false, want true")
	}
	if _, err := db.ResolveActiveAlertCondition(fleetFullyDormantAlertType, "pr-155", conditionStartedAt.Add(time.Hour)); err != nil {
		t.Fatalf("ResolveActiveAlertCondition() error = %v", err)
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
	alert := alerts[0].Alert
	if alert.Source != "pr-155" || alert.ConditionStatus != "resolved" {
		t.Fatalf("Alert = %+v, want pr-155 resolved condition", alert)
	}
	if alert.ConditionStartedAt == nil || alert.ConditionResolvedAt == nil {
		t.Fatalf("condition timestamps = %v/%v, want both", alert.ConditionStartedAt, alert.ConditionResolvedAt)
	}
	if len(alerts[0].TriageCommands) == 0 || !strings.Contains(strings.Join(alerts[0].TriageCommands, " "), "source=pr-155") {
		t.Fatalf("TriageCommands = %+v, want source-specific dormant fleet triage", alerts[0].TriageCommands)
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

func TestHandleListWorkerSummariesRespectsSource(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-one",
		Name:          "worker-main-one",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-main",
		LitestreamSHA: "ls-main",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-one",
		Name:          "worker-pr-one",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "sha-pr",
		LitestreamSHA: "ls-pr",
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/worker-summaries?source=pr-1228", nil)
	recorder := httptest.NewRecorder()

	api.handleListWorkerSummaries(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var summaries []WorkerSummaryResponse
	if err := json.NewDecoder(recorder.Body).Decode(&summaries); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(summaries) != 1 {
		t.Fatalf("len(summaries) = %d, want 1", len(summaries))
	}
	if summaries[0].Worker.Source != "pr-1228" {
		t.Fatalf("Worker.Source = %q, want pr-1228", summaries[0].Worker.Source)
	}
}

func TestGetWorkerDebugSnapshotReturnsLatestFailureSnapshot(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	worker := model.Worker{
		ID:            "worker-main-low-volume",
		Name:          "worker-main-low-volume",
		Status:        model.WorkerDegraded,
		Source:        "main",
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	createTestWorker(t, db, worker)

	details, err := json.Marshal(reporting.VerificationPayload{
		WorkerIdentity: reporting.WorkerIdentity{
			WorkerID: worker.ID,
			RunID:    "run-123",
		},
		Status:       "failed",
		Passed:       false,
		ErrorMessage: "wait for sync: connection refused",
		FailureDebug: &reporting.FailureDebugSnapshot{
			Reason: "wait for sync: connection refused",
			Run: reporting.WorkerIdentity{
				WorkerID: worker.ID,
				RunID:    "run-123",
			},
			SocketSummary: reporting.SocketSummary{
				Path:      "/data/litestream.sock",
				LineCount: 42,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal details: %v", err)
	}
	if err := db.RecordEvent(worker.ID, "verification_failed", "sync failed", string(details)); err != nil {
		t.Fatalf("RecordEvent() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/workers/worker-main-low-volume/debug-snapshot", nil)
	request.SetPathValue("id", worker.ID)
	recorder := httptest.NewRecorder()

	api.handleGetWorkerDebugSnapshot(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var snapshot reporting.FailureDebugSnapshot
	if err := json.NewDecoder(recorder.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if snapshot.Run.RunID != "run-123" {
		t.Fatalf("RunID = %q, want run-123", snapshot.Run.RunID)
	}
	if snapshot.SocketSummary.LineCount != 42 {
		t.Fatalf("SocketSummary.LineCount = %d, want 42", snapshot.SocketSummary.LineCount)
	}
}

func TestHandleGetDiagnosisRespectsSource(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-one",
		Name:          "worker-main-one",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-main",
		LitestreamSHA: "ls-main",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-one",
		Name:          "worker-pr-one",
		Status:        model.WorkerDegraded,
		Source:        "pr-1228",
		GitSHA:        "sha-pr",
		LitestreamSHA: "ls-pr",
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})

	failedAt := time.Now().UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-pr-one",
		StartedAt:    failedAt.Add(-15 * time.Second),
		CompletedAt:  &failedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/diagnosis?source=pr-1228", nil)
	recorder := httptest.NewRecorder()

	api.handleGetDiagnosis(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}

	var response struct {
		Diagnosis diagnosisSnapshot `json:"diagnosis"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response.Diagnosis.AffectedWorkers != 1 {
		t.Fatalf("AffectedWorkers = %d, want 1", response.Diagnosis.AffectedWorkers)
	}
	if response.Diagnosis.DominantSignature != "litestream_sync_socket_refused" {
		t.Fatalf("DominantSignature = %q, want litestream_sync_socket_refused", response.Diagnosis.DominantSignature)
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

func TestHandleVerificationDormantWorkerIgnored(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	workerID := "worker-dormant-verify"

	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		Name:          workerID,
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	if err := db.MarkWorkerDormant(workerID, "probe window expired", "probe_expired", "probe"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}

	dormantBefore, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker() before: %v", err)
	}

	payload := reporting.VerificationPayload{
		WorkerIdentity: reporting.WorkerIdentity{
			WorkerID:      workerID,
			Name:          workerID,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		},
		CheckType: "integrity",
		Status:    "passed",
		Passed:    true,
		Summary:   "all checks passed",
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/verifications", bytes.NewReader(body))
	request.SetPathValue("id", workerID)
	recorder := httptest.NewRecorder()

	api.handleVerification(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}

	dormantAfter, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker() after: %v", err)
	}
	if dormantAfter.Status != model.WorkerDormant {
		t.Fatalf("Status = %q, want %q (dormant must be preserved)", dormantAfter.Status, model.WorkerDormant)
	}
	if dormantAfter.DormantAt == nil {
		t.Fatalf("DormantAt = nil, want preserved")
	}
	if dormantAfter.DormantReason != dormantBefore.DormantReason {
		t.Fatalf("DormantReason = %q, want %q", dormantAfter.DormantReason, dormantBefore.DormantReason)
	}
	if dormantAfter.DormantSignature != dormantBefore.DormantSignature {
		t.Fatalf("DormantSignature = %q, want %q", dormantAfter.DormantSignature, dormantBefore.DormantSignature)
	}
	if dormantAfter.ResumeTrigger != dormantBefore.ResumeTrigger {
		t.Fatalf("ResumeTrigger = %q, want %q", dormantAfter.ResumeTrigger, dormantBefore.ResumeTrigger)
	}
}

func TestHandleVerificationAbortedReportIsNeutral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, db *model.DB, workerID string)
	}{
		{
			name: "running",
		},
		{
			name: "degraded",
			setup: func(t *testing.T, db *model.DB, workerID string) {
				t.Helper()
				if err := db.UpdateWorkerVerificationState(workerID, false, "existing verification failure"); err != nil {
					t.Fatalf("UpdateWorkerVerificationState() error = %v", err)
				}
			},
		},
		{
			name: "probing",
			setup: func(t *testing.T, db *model.DB, workerID string) {
				t.Helper()
				if err := db.MarkWorkerProbing(workerID, "manual_probe"); err != nil {
					t.Fatalf("MarkWorkerProbing() error = %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			workerID := "worker-aborted-" + tt.name
			createTestWorker(t, db, model.Worker{
				ID:            workerID,
				Name:          workerID,
				Status:        model.WorkerRunning,
				Source:        "main",
				GitSHA:        "abc123",
				LitestreamSHA: "ls123",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
			})
			if tt.setup != nil {
				tt.setup(t, db, workerID)
			}

			before, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker() before: %v", err)
			}

			payload := reporting.VerificationPayload{
				WorkerIdentity: reporting.WorkerIdentity{
					WorkerID:      workerID,
					Name:          workerID,
					Source:        "main",
					GitSHA:        "abc123",
					LitestreamSHA: "ls123",
					ProfileName:   "low-volume",
					ProfileConfig: "{}",
				},
				StartedAt:    time.Date(2026, 4, 26, 14, 0, 0, 0, time.UTC),
				CompletedAt:  time.Date(2026, 4, 26, 14, 2, 0, 0, time.UTC),
				CheckType:    "integrity",
				Status:       "aborted",
				Passed:       false,
				Summary:      "verification aborted",
				ErrorMessage: "litestream process stopped during verification",
				DurationMS:   120000,
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}

			manager := &Manager{db: db, appName: "litestream-soak"}
			api := NewAPI(db, nil, nil, nil, manager, nil)
			request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/verifications", bytes.NewReader(body))
			request.SetPathValue("id", workerID)
			recorder := httptest.NewRecorder()

			api.handleVerification(recorder, request)

			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
			}

			after, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker() after: %v", err)
			}
			if after.Status != before.Status {
				t.Fatalf("Status = %q, want %q", after.Status, before.Status)
			}
			if after.ErrorMessage != before.ErrorMessage {
				t.Fatalf("ErrorMessage = %q, want %q", after.ErrorMessage, before.ErrorMessage)
			}
			if after.DormantReason != before.DormantReason {
				t.Fatalf("DormantReason = %q, want %q", after.DormantReason, before.DormantReason)
			}
			if after.DormantSignature != before.DormantSignature {
				t.Fatalf("DormantSignature = %q, want %q", after.DormantSignature, before.DormantSignature)
			}
			if after.ResumeTrigger != before.ResumeTrigger {
				t.Fatalf("ResumeTrigger = %q, want %q", after.ResumeTrigger, before.ResumeTrigger)
			}

			verifications, err := db.ListVerifications(workerID, 10)
			if err != nil {
				t.Fatalf("ListVerifications() error = %v", err)
			}
			if len(verifications) != 1 {
				t.Fatalf("len(verifications) = %d, want 1", len(verifications))
			}
			if verifications[0].Status != "aborted" {
				t.Fatalf("verification Status = %q, want aborted", verifications[0].Status)
			}
			if verifications[0].Passed {
				t.Fatalf("verification Passed = true, want false")
			}

			events, err := db.ListWorkerEvents(workerID, 10)
			if err != nil {
				t.Fatalf("ListWorkerEvents() error = %v", err)
			}
			foundAborted := false
			for _, event := range events {
				switch event.EventType {
				case "verification_failed", "first_failure", "worker_probe_failed", "worker_dormant":
					t.Fatalf("unexpected failure event %q recorded: %+v", event.EventType, events)
				case "verification_aborted":
					foundAborted = true
				}
			}
			if !foundAborted {
				t.Fatalf("verification_aborted event not recorded; got: %+v", events)
			}
		})
	}
}

func TestHandleVerificationPersistsRuntimeDiskClassification(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 7, 26, 1, 29, 13, 0, time.UTC)
	tests := []struct {
		name              string
		profileName       string
		errorMessage      string
		reportedSignature string
		runtime           reporting.RuntimePayload
		signature         string
	}{
		{
			name: "fresh fixture-dominant telemetry overrides old worker classification",
			runtime: reporting.RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    950,
				SnapshotCollectedAt: completedAt,
			},
			signature: "soak_fixture_disk_exhausted",
		},
		{
			name: "fresh non-dominant telemetry remains actionable",
			runtime: reporting.RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    949,
				SnapshotCollectedAt: completedAt,
			},
			signature: "disk_capacity_full",
		},
		{
			name: "missing timestamp remains actionable before normalization",
			runtime: reporting.RuntimePayload{
				DataDiskUsedBytes: 1_000,
				DBTotalSizeBytes:  950,
			},
			signature: "disk_capacity_full",
		},
		{
			name: "stale telemetry remains actionable",
			runtime: reporting.RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    950,
				SnapshotCollectedAt: completedAt.Add(-61 * time.Second),
			},
			signature: "disk_capacity_full",
		},
		{
			name:              "unsupported fixture claim remains actionable",
			errorMessage:      "verification failed",
			reportedSignature: "soak_fixture_disk_exhausted",
			signature:         "disk_capacity_full",
		},
		{
			name:         "non many-db source dominance remains actionable",
			profileName:  "overload-truncate0",
			errorMessage: "checkpoint failed: database or disk is full",
			runtime: reporting.RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    950,
				SnapshotCollectedAt: completedAt,
			},
			signature: "disk_capacity_full",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			workerID := "worker-runtime-disk-" + strings.ReplaceAll(tt.name, " ", "-")
			profileName := tt.profileName
			if profileName == "" {
				profileName = "many-dbs-100-dir"
			}
			createTestWorker(t, db, model.Worker{
				ID:            workerID,
				Name:          workerID,
				Status:        model.WorkerRunning,
				Source:        "main",
				GitSHA:        "abc123",
				LitestreamSHA: "ls123",
				ProfileName:   profileName,
				ProfileConfig: "{}",
			})

			errorMessage := tt.errorMessage
			if errorMessage == "" {
				errorMessage = "sync database: db sync: stage-write ltx file: disk full: write header"
			}
			reportedSignature := tt.reportedSignature
			if reportedSignature == "" {
				reportedSignature = "disk_capacity_full"
			}
			payload := reporting.VerificationPayload{
				WorkerIdentity: reporting.WorkerIdentity{
					WorkerID:      workerID,
					Name:          workerID,
					Source:        "main",
					GitSHA:        "abc123",
					LitestreamSHA: "ls123",
					ProfileName:   profileName,
					ProfileConfig: "{}",
				},
				StartedAt:    completedAt.Add(-time.Minute),
				CompletedAt:  completedAt,
				CheckType:    "integrity",
				Status:       "failed",
				Passed:       false,
				Summary:      "verification failed",
				ErrorMessage: errorMessage,
				FailureClassification: &reporting.FailureClassification{
					Stage:     "disk_capacity",
					Signature: reportedSignature,
				},
				RuntimePayload: tt.runtime,
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}

			api := NewAPI(db, nil, nil, nil, nil, nil)
			request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/verifications", bytes.NewReader(body))
			request.SetPathValue("id", workerID)
			recorder := httptest.NewRecorder()
			api.handleVerification(recorder, request)

			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
			}

			verifications, err := db.ListVerifications(workerID, 10)
			if err != nil {
				t.Fatalf("ListVerifications() error = %v", err)
			}
			if len(verifications) != 1 {
				t.Fatalf("len(verifications) = %d, want 1", len(verifications))
			}
			if verifications[0].FailureClassification == nil {
				t.Fatal("FailureClassification = nil")
			}
			if got := verifications[0].FailureClassification.Signature; got != tt.signature {
				t.Fatalf("stored Signature = %q, want %q", got, tt.signature)
			}

			summary, err := api.buildWorkerSummary(model.Worker{
				ID:        workerID,
				CreatedAt: completedAt.Add(-time.Hour),
			})
			if err != nil {
				t.Fatalf("buildWorkerSummary() error = %v", err)
			}
			if summary.CurrentFailureSignature != tt.signature {
				t.Fatalf("CurrentFailureSignature = %q, want %q", summary.CurrentFailureSignature, tt.signature)
			}
			failuresRequest := httptest.NewRequest(http.MethodGet, "/api/failures", nil)
			failuresRecorder := httptest.NewRecorder()
			api.handleListFailures(failuresRecorder, failuresRequest)
			if failuresRecorder.Code != http.StatusOK {
				t.Fatalf("failures status code = %d, want %d", failuresRecorder.Code, http.StatusOK)
			}
			var failures []FailureResponse
			if err := json.Unmarshal(failuresRecorder.Body.Bytes(), &failures); err != nil {
				t.Fatalf("decode failures response: %v", err)
			}
			if len(failures) != 1 {
				t.Fatalf("len(failures) = %d, want 1", len(failures))
			}
			wantCategory := "actionable"
			wantSeverity := "bad"
			wantSubsystem := "Disk capacity / restore scratch headroom"
			if tt.signature == "soak_fixture_disk_exhausted" {
				wantCategory = "soak-fixture"
				wantSeverity = "warn"
				wantSubsystem = "Soak fixture disk consumption"
			}
			if failures[0].FailureCategory != wantCategory {
				t.Fatalf("FailureCategory = %q, want %q", failures[0].FailureCategory, wantCategory)
			}
			if failures[0].FailureSeverity != wantSeverity {
				t.Fatalf("FailureSeverity = %q, want %q", failures[0].FailureSeverity, wantSeverity)
			}
			if failures[0].ProbableSubsystem != wantSubsystem {
				t.Fatalf("ProbableSubsystem = %q, want %q", failures[0].ProbableSubsystem, wantSubsystem)
			}
		})
	}
}

func TestListFailuresRejectsMalformedPersistedClassification(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	workerID := "worker-malformed-classification"
	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		Name:          workerID,
		Status:        model.WorkerDegraded,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "many-dbs-100-dir",
		ProfileConfig: "{}",
	})
	completedAt := time.Date(2026, 7, 26, 1, 29, 13, 0, time.UTC)
	if err := db.RecordVerification(&model.Verification{
		WorkerID:     workerID,
		StartedAt:    completedAt.Add(-time.Minute),
		CompletedAt:  &completedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: "checkpoint failed: database or disk is full",
		FailureClassification: &reporting.FailureClassification{
			Signature: "soak_fixture_disk_exhausted",
		},
	}); err != nil {
		t.Fatalf("RecordVerification() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/api/failures", nil)
	recorder := httptest.NewRecorder()
	api.handleListFailures(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	var failures []FailureResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &failures); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(failures) != 1 {
		t.Fatalf("len(failures) = %d, want 1", len(failures))
	}
	if failures[0].FailureStage != "disk_capacity" {
		t.Fatalf("FailureStage = %q, want disk_capacity", failures[0].FailureStage)
	}
	if failures[0].Verification.FailureClassification != nil {
		t.Fatalf("Verification.FailureClassification = %+v, want nil", failures[0].Verification.FailureClassification)
	}
	if failures[0].FailureSignature != "disk_capacity_full" {
		t.Fatalf("FailureSignature = %q, want disk_capacity_full", failures[0].FailureSignature)
	}
	if failures[0].FailureCategory != "actionable" {
		t.Fatalf("FailureCategory = %q, want actionable", failures[0].FailureCategory)
	}
	if failures[0].FailureSeverity != "bad" {
		t.Fatalf("FailureSeverity = %q, want bad", failures[0].FailureSeverity)
	}
	if failures[0].ProbableSubsystem != "Disk capacity / restore scratch headroom" {
		t.Fatalf("ProbableSubsystem = %q, want Disk capacity / restore scratch headroom", failures[0].ProbableSubsystem)
	}
}

func TestHandleHeartbeatRuntimeAtGating(t *testing.T) {
	t.Parallel()

	workerID := "worker-hb-gating"

	t.Run("healthy payload sets LastRuntimeAt", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "sha-a",
			LitestreamSHA: "ls-a",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})

		collectedAt := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		payload := reporting.HeartbeatPayload{
			WorkerIdentity: reporting.WorkerIdentity{
				WorkerID:      workerID,
				Name:          workerID,
				Source:        "main",
				GitSHA:        "sha-a",
				LitestreamSHA: "ls-a",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
			},
			SentAt: collectedAt,
			RuntimePayload: reporting.RuntimePayload{
				LitestreamSnapshotHealthy: true,
				SnapshotCollectedAt:       collectedAt,
			},
		}
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}

		api := NewAPI(db, nil, nil, nil, nil, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/heartbeat", bytes.NewReader(body))
		request.SetPathValue("id", workerID)
		recorder := httptest.NewRecorder()
		api.handleHeartbeat(recorder, request)

		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
		}

		stored, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.LastRuntimeAt == nil {
			t.Fatalf("LastRuntimeAt = nil, want %v", collectedAt)
		}
		if !stored.LastRuntimeAt.Equal(collectedAt) {
			t.Fatalf("LastRuntimeAt = %v, want %v", stored.LastRuntimeAt, collectedAt)
		}
	})

	t.Run("failing snapshot payload does not set LastRuntimeAt", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "sha-a",
			LitestreamSHA: "ls-a",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})

		sentAt := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		payload := reporting.HeartbeatPayload{
			WorkerIdentity: reporting.WorkerIdentity{
				WorkerID:      workerID,
				Name:          workerID,
				Source:        "main",
				GitSHA:        "sha-a",
				LitestreamSHA: "ls-a",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
			},
			SentAt: sentAt,
			RuntimePayload: reporting.RuntimePayload{
				LitestreamSnapshotHealthy: false,
				LitestreamSnapshotError:   "litestream process not responding",
				SnapshotCollectedAt:       sentAt,
			},
		}
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}

		api := NewAPI(db, nil, nil, nil, nil, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/heartbeat", bytes.NewReader(body))
		request.SetPathValue("id", workerID)
		recorder := httptest.NewRecorder()
		api.handleHeartbeat(recorder, request)

		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
		}

		stored, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.LastRuntimeAt != nil {
			t.Fatalf("LastRuntimeAt = %v, want nil (failing snapshot must not refresh timestamp)", stored.LastRuntimeAt)
		}
	})

	t.Run("legacy payload with no snapshot metadata does not set LastRuntimeAt", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "sha-a",
			LitestreamSHA: "ls-a",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})

		sentAt := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		payload := reporting.HeartbeatPayload{
			WorkerIdentity: reporting.WorkerIdentity{
				WorkerID:      workerID,
				Name:          workerID,
				Source:        "main",
				GitSHA:        "sha-a",
				LitestreamSHA: "ls-a",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
			},
			SentAt:         sentAt,
			RuntimePayload: reporting.RuntimePayload{},
		}
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}

		api := NewAPI(db, nil, nil, nil, nil, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/heartbeat", bytes.NewReader(body))
		request.SetPathValue("id", workerID)
		recorder := httptest.NewRecorder()
		api.handleHeartbeat(recorder, request)

		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
		}

		stored, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.LastRuntimeAt != nil {
			t.Fatalf("LastRuntimeAt = %v, want nil (legacy payload must not refresh timestamp)", stored.LastRuntimeAt)
		}
	})
}

func TestHandleWorkerEventDiskFullNoProgressDegradesWorker(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	workerID := "worker-disk-no-progress"
	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		Name:          workerID,
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "constrained-disk",
		ProfileConfig: "{}",
	})

	message := "harness detected no replication progress under disk pressure before Litestream emitted a distinct disk-full signal"
	payload := reporting.WorkerEventPayload{
		WorkerIdentity: reporting.WorkerIdentity{
			WorkerID:      workerID,
			Name:          workerID,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "constrained-disk",
			ProfileConfig: "{}",
		},
		EventType: reporting.WorkerEventDiskFullNoProgress,
		Message:   message,
		SentAt:    time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC),
		RuntimePayload: reporting.RuntimePayload{
			DataDiskTotalBytes:            1024,
			DataDiskAvailableBytes:        100,
			DBSizeBytes:                   600,
			DBTXID:                        12,
			ReplicatedTXID:                8,
			ReplicationLagMax:             4,
			DiskPressureNoProgress:        true,
			DiskPressureNoProgressSeconds: 121,
			SnapshotCollectedAt:           time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC),
			LitestreamSnapshotHealthy:     true,
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/events", bytes.NewReader(body))
	request.SetPathValue("id", workerID)
	recorder := httptest.NewRecorder()

	api.handleWorkerEvent(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}

	worker, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if worker.Status != model.WorkerDegraded {
		t.Fatalf("Status = %q, want degraded", worker.Status)
	}
	if worker.ErrorMessage != message {
		t.Fatalf("ErrorMessage = %q, want %q", worker.ErrorMessage, message)
	}

	runtime := extractReportedRuntime(*worker, nil)
	if runtime == nil || !runtime.DiskPressureNoProgress {
		t.Fatalf("runtime = %+v, want disk pressure no-progress", runtime)
	}

	events, err := db.ListWorkerEvents(workerID, 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != reporting.WorkerEventDiskFullNoProgress {
		t.Fatalf("events = %+v, want disk no-progress event", events)
	}
}

func TestHandleWorkerEventDiskFullRecoveryFailedDegradesWorker(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	workerID := "worker-disk-recovery-failed"
	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		Name:          workerID,
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "constrained-disk",
		ProfileConfig: "{}",
	})

	message := "Litestream emitted a distinct disk-full signal but did not replicate again after reserved disk space was freed"
	payload := reporting.WorkerEventPayload{
		WorkerIdentity: reporting.WorkerIdentity{
			WorkerID:      workerID,
			Name:          workerID,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "constrained-disk",
			ProfileConfig: "{}",
		},
		EventType: reporting.WorkerEventDiskFullRecoveryFailed,
		Message:   message,
		SentAt:    time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC),
		RuntimePayload: reporting.RuntimePayload{
			DataDiskTotalBytes:         1024,
			DataDiskAvailableBytes:     500,
			DBSizeBytes:                600,
			DBTXID:                     12,
			ReplicatedTXID:             8,
			ReplicationLagMax:          4,
			DiskFullSignalObserved:     true,
			DiskFullRecoveryAttempted:  true,
			DiskFullRecoveryFreedBytes: 314572800,
			SnapshotCollectedAt:        time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC),
			LitestreamSnapshotHealthy:  true,
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/events", bytes.NewReader(body))
	request.SetPathValue("id", workerID)
	recorder := httptest.NewRecorder()

	api.handleWorkerEvent(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}

	worker, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if worker.Status != model.WorkerDegraded {
		t.Fatalf("Status = %q, want degraded", worker.Status)
	}
	if worker.ErrorMessage != message {
		t.Fatalf("ErrorMessage = %q, want %q", worker.ErrorMessage, message)
	}
}

func TestBuildHomePageDataStalestRuntimeAt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	olderAt := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	newerAt := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)

	createTestWorker(t, db, model.Worker{
		ID:            "worker-older",
		Name:          "worker-older",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-a",
		LitestreamSHA: "ls-a",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerRuntimeSnapshot("worker-older", reporting.RuntimePayload{
		LitestreamSnapshotHealthy: true,
		SnapshotCollectedAt:       olderAt,
	}); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot(older) error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-newer",
		Name:          "worker-newer",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-a",
		LitestreamSHA: "ls-a",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerRuntimeSnapshot("worker-newer", reporting.RuntimePayload{
		LitestreamSnapshotHealthy: true,
		SnapshotCollectedAt:       newerAt,
	}); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot(newer) error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/ui", nil)
	data, err := api.buildHomePageData(request)
	if err != nil {
		t.Fatalf("buildHomePageData() error = %v", err)
	}

	if data.Summary.StalestRuntimeAt == nil {
		t.Fatalf("StalestRuntimeAt = nil, want %v", olderAt)
	}
	if !data.Summary.StalestRuntimeAt.Equal(olderAt) {
		t.Fatalf("StalestRuntimeAt = %v, want %v (oldest timestamp)", data.Summary.StalestRuntimeAt, olderAt)
	}
}

func TestHandleDeploymentReady(t *testing.T) {
	t.Parallel()

	t.Run("nil deployer returns 500", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		api := NewAPI(db, nil, nil, nil, nil, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/deployments/ready?sha=abc1234&litestream_sha=ls123", nil)
		recorder := httptest.NewRecorder()

		api.handleDeploymentReady(recorder, request)

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
	})

	t.Run("missing sha returns 400", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		deployer := &Deployer{db: db}
		api := NewAPI(db, nil, nil, nil, nil, deployer)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/deployments/ready", nil)
		recorder := httptest.NewRecorder()

		api.handleDeploymentReady(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("malformed JSON body returns 400", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		deployer := &Deployer{db: db}
		api := NewAPI(db, nil, nil, nil, nil, deployer)
		body := bytes.NewBufferString(`{"sha": not-valid-json}`)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/deployments/ready", body)
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()

		api.handleDeploymentReady(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("valid request accepted with echoed fields", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		deployer := &Deployer{db: db}
		api := NewAPI(db, nil, nil, nil, nil, deployer)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/deployments/ready?sha=not-hex-sha&litestream_sha=ls-abc123&image=registry.fly.io%2Fapp%3Anot-hex-sha", nil)
		recorder := httptest.NewRecorder()

		api.handleDeploymentReady(recorder, request)

		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
		}

		var resp map[string]any
		if err := json.NewDecoder(recorder.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp["sha"] != "not-hex-sha" {
			t.Fatalf("sha = %v, want not-hex-sha", resp["sha"])
		}
		if resp["source"] != "main" {
			t.Fatalf("source = %v, want main", resp["source"])
		}
		if resp["trigger"] != "deploy_ready" {
			t.Fatalf("trigger = %v, want deploy_ready", resp["trigger"])
		}
		if resp["image_ref"] != "registry.fly.io/app:not-hex-sha" {
			t.Fatalf("image_ref = %v, want registry.fly.io/app:not-hex-sha", resp["image_ref"])
		}
		if resp["accepted"] != true {
			t.Fatalf("accepted = %v, want true", resp["accepted"])
		}
	})
}

func TestHandleRollWorker(t *testing.T) {
	t.Parallel()

	t.Run("nil manager returns 500", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		api := NewAPI(db, nil, nil, nil, nil, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/workers/worker-main-low/roll?sha=abc1234&litestream_sha=ls123&image=reg%2Fimg%3Atag", nil)
		request.SetPathValue("id", "worker-main-low")
		recorder := httptest.NewRecorder()

		api.handleRollWorker(recorder, request)

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
	})

	t.Run("empty worker id returns 400", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/workers//roll?sha=abc1234&litestream_sha=ls123&image=reg%2Fimg%3Atag", nil)
		recorder := httptest.NewRecorder()

		api.handleRollWorker(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("missing sha returns 400", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		workerID := "worker-roll-no-sha"
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		request := httptest.NewRequest(http.MethodPost, "/api/admin/workers/"+workerID+"/roll?litestream_sha=ls123&image=reg%2Fimg%3Atag", nil)
		request.SetPathValue("id", workerID)
		recorder := httptest.NewRecorder()

		api.handleRollWorker(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("missing litestream_sha returns 400", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		workerID := "worker-roll-no-lsha"
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		request := httptest.NewRequest(http.MethodPost, "/api/admin/workers/"+workerID+"/roll?sha=abc1234&image=reg%2Fimg%3Atag", nil)
		request.SetPathValue("id", workerID)
		recorder := httptest.NewRecorder()

		api.handleRollWorker(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("unknown worker id returns 404", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/workers/does-not-exist/roll?sha=abc1234&litestream_sha=ls123&image=reg%2Fimg%3Atag", nil)
		request.SetPathValue("id", "does-not-exist")
		recorder := httptest.NewRecorder()

		api.handleRollWorker(recorder, request)

		if recorder.Code != http.StatusNotFound {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusNotFound)
		}
	})

	t.Run("source mismatch returns 400 and no deployment created", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		workerID := "worker-roll-mismatch"
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})

		request := httptest.NewRequest(http.MethodPost, "/api/admin/workers/"+workerID+"/roll?sha=abc1234&litestream_sha=ls123&source=pr-123&image=reg%2Fimg%3Atag", nil)
		request.SetPathValue("id", workerID)
		recorder := httptest.NewRecorder()

		api.handleRollWorker(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
		if _, err := db.GetDeploymentByVersion("pr-123", "abc1234", "ls123"); err == nil {
			t.Fatalf("GetDeploymentByVersion() returned nil error — no deployment should have been created on mismatch")
		}
	})

	t.Run("success returns 202 and records state", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		workerID := "worker-roll-success"
		const sha = "abc1234def56789"
		const litestreamSHA = "ls123abc456def7"
		const imageRef = "registry.fly.io/litestream-soak:abc1234"
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        sha,
			LitestreamSHA: litestreamSHA,
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})

		request := httptest.NewRequest(http.MethodPost, "/api/admin/workers/"+workerID+"/roll?sha="+sha+"&litestream_sha="+litestreamSHA+"&image="+imageRef, nil)
		request.SetPathValue("id", workerID)
		recorder := httptest.NewRecorder()

		api.handleRollWorker(recorder, request)

		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
		}

		dep, err := db.GetDeploymentByVersion("main", sha, litestreamSHA)
		if err != nil {
			t.Fatalf("GetDeploymentByVersion() error = %v", err)
		}
		if dep.Status != "ready" {
			t.Fatalf("deployment Status = %q, want ready", dep.Status)
		}
		if dep.ImageRef != imageRef {
			t.Fatalf("deployment ImageRef = %q, want %q", dep.ImageRef, imageRef)
		}

		events, err := db.ListWorkerEvents(workerID, 10)
		if err != nil {
			t.Fatalf("ListWorkerEvents() error = %v", err)
		}
		found := false
		for _, e := range events {
			if e.EventType == "targeted_rollout_requested" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("targeted_rollout_requested event not recorded; got: %+v", events)
		}
	})
}

func TestHandleResumeDormantWorkers(t *testing.T) {
	t.Parallel()

	t.Run("nil manager returns 500", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		api := NewAPI(db, nil, nil, nil, nil, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/resume-dormant?image=reg%2Fimg%3Atag", nil)
		recorder := httptest.NewRecorder()

		api.handleResumeDormantWorkers(recorder, request)

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
	})

	t.Run("no dormant workers returns 200 with zero count and event", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/resume-dormant?image=reg%2Fimg%3Atag", nil)
		recorder := httptest.NewRecorder()

		api.handleResumeDormantWorkers(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
		}

		var resp map[string]any
		if err := json.NewDecoder(recorder.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp["resumed_workers"] != float64(0) {
			t.Fatalf("resumed_workers = %v, want 0", resp["resumed_workers"])
		}

		events, err := db.ListEvents(10)
		if err != nil {
			t.Fatalf("ListEvents() error = %v", err)
		}
		found := false
		for _, e := range events {
			if e.EventType == "manual_resume_requested" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("manual_resume_requested event not recorded; got: %+v", events)
		}
	})

	t.Run("dormant worker with no machine or volume returns 500 and worker stays dormant", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		workerID := "worker-resume-no-vol"
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		if err := db.MarkWorkerDormant(workerID, "probe window expired", "probe_expired", "probe"); err != nil {
			t.Fatalf("MarkWorkerDormant() error = %v", err)
		}

		request := httptest.NewRequest(http.MethodPost, "/api/admin/resume-dormant?image=reg%2Fimg%3Atag", nil)
		recorder := httptest.NewRecorder()

		api.handleResumeDormantWorkers(recorder, request)

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusInternalServerError, recorder.Body.String())
		}

		worker, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if worker.Status != model.WorkerDormant {
			t.Fatalf("Status = %q, want %q (worker must remain dormant)", worker.Status, model.WorkerDormant)
		}

		events, err := db.ListWorkerEvents(workerID, 10)
		if err != nil {
			t.Fatalf("ListWorkerEvents() error = %v", err)
		}
		found := false
		for _, e := range events {
			if e.EventType == "worker_probe_start_failed" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("worker_probe_start_failed event not recorded; got: %+v", events)
		}
	})

	t.Run("dormant worker on different source leaves it dormant", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		workerID := "worker-pr7-dormant"
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "pr-7",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
			PRNumber:      7,
		})
		if err := db.MarkWorkerDormant(workerID, "probe window expired", "probe_expired", "probe"); err != nil {
			t.Fatalf("MarkWorkerDormant() error = %v", err)
		}

		request := httptest.NewRequest(http.MethodPost, "/api/admin/resume-dormant?source=main&image=reg%2Fimg%3Atag", nil)
		recorder := httptest.NewRecorder()

		api.handleResumeDormantWorkers(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
		}

		var resp map[string]any
		if err := json.NewDecoder(recorder.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp["resumed_workers"] != float64(0) {
			t.Fatalf("resumed_workers = %v, want 0", resp["resumed_workers"])
		}

		worker, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if worker.Status != model.WorkerDormant {
			t.Fatalf("Status = %q, want %q (pr-7 worker must remain dormant)", worker.Status, model.WorkerDormant)
		}
	})
}

func TestHandlePauseSourceWorkers(t *testing.T) {
	t.Parallel()

	t.Run("nil manager returns 500", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		api := NewAPI(db, nil, nil, nil, nil, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/pause-source", nil)
		recorder := httptest.NewRecorder()

		api.handlePauseSourceWorkers(recorder, request)

		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusInternalServerError)
		}
	})

	t.Run("only stopped and dormant workers returns 200 with zero paused", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)

		createTestWorker(t, db, model.Worker{
			ID:            "worker-pause-stopped",
			Name:          "worker-pause-stopped",
			Status:        model.WorkerStopped,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		createTestWorker(t, db, model.Worker{
			ID:            "worker-pause-dormant",
			Name:          "worker-pause-dormant",
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "high-volume",
			ProfileConfig: "{}",
		})
		if err := db.MarkWorkerDormant("worker-pause-dormant", "probe expired", "probe_expired", "probe"); err != nil {
			t.Fatalf("MarkWorkerDormant() error = %v", err)
		}

		request := httptest.NewRequest(http.MethodPost, "/api/admin/pause-source?source=main", nil)
		recorder := httptest.NewRecorder()

		api.handlePauseSourceWorkers(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
		}

		var resp map[string]any
		if err := json.NewDecoder(recorder.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp["paused_workers"] != float64(0) {
			t.Fatalf("paused_workers = %v, want 0", resp["paused_workers"])
		}
	})

	t.Run("running worker gets dormanted with correct fields", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)
		workerID := "worker-pause-running"
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})

		request := httptest.NewRequest(http.MethodPost, "/api/admin/pause-source?source=main&reason=manual+pause+test&signature=manual_pause_test", nil)
		recorder := httptest.NewRecorder()

		api.handlePauseSourceWorkers(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
		}

		worker, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if worker.Status != model.WorkerDormant {
			t.Fatalf("Status = %q, want %q", worker.Status, model.WorkerDormant)
		}
		if worker.DormantAt == nil {
			t.Fatalf("DormantAt = nil, want non-nil")
		}
		if worker.DormantReason != "manual pause test" {
			t.Fatalf("DormantReason = %q, want %q", worker.DormantReason, "manual pause test")
		}
		if worker.DormantSignature != "manual_pause_test" {
			t.Fatalf("DormantSignature = %q, want %q", worker.DormantSignature, "manual_pause_test")
		}

		events, err := db.ListWorkerEvents(workerID, 10)
		if err != nil {
			t.Fatalf("ListWorkerEvents() error = %v", err)
		}
		workerDormantFound := false
		for _, e := range events {
			if e.EventType == "worker_dormant" {
				workerDormantFound = true
				break
			}
		}
		if !workerDormantFound {
			t.Fatalf("worker_dormant event not recorded; got: %+v", events)
		}

		globalEvents, err := db.ListEvents(10)
		if err != nil {
			t.Fatalf("ListEvents() error = %v", err)
		}
		pauseEventFound := false
		for _, e := range globalEvents {
			if e.EventType == "manual_source_pause_requested" {
				pauseEventFound = true
				break
			}
		}
		if !pauseEventFound {
			t.Fatalf("manual_source_pause_requested event not recorded; got: %+v", globalEvents)
		}
	})

	t.Run("mixed statuses pauses only running worker", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := &Manager{db: db, appName: "litestream-soak"}
		api := NewAPI(db, nil, nil, nil, manager, nil)

		createTestWorker(t, db, model.Worker{
			ID:            "worker-mixed-running",
			Name:          "worker-mixed-running",
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		createTestWorker(t, db, model.Worker{
			ID:            "worker-mixed-dormant",
			Name:          "worker-mixed-dormant",
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "high-volume",
			ProfileConfig: "{}",
		})
		if err := db.MarkWorkerDormant("worker-mixed-dormant", "probe expired", "probe_expired", "probe"); err != nil {
			t.Fatalf("MarkWorkerDormant() error = %v", err)
		}
		createTestWorker(t, db, model.Worker{
			ID:            "worker-mixed-stopped",
			Name:          "worker-mixed-stopped",
			Status:        model.WorkerStopped,
			Source:        "main",
			GitSHA:        "abc1234",
			LitestreamSHA: "ls123",
			ProfileName:   "burst-volume",
			ProfileConfig: "{}",
		})

		request := httptest.NewRequest(http.MethodPost, "/api/admin/pause-source?source=main", nil)
		recorder := httptest.NewRecorder()

		api.handlePauseSourceWorkers(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
		}

		var resp map[string]any
		if err := json.NewDecoder(recorder.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp["paused_workers"] != float64(1) {
			t.Fatalf("paused_workers = %v, want 1", resp["paused_workers"])
		}

		workerIDs, ok := resp["worker_ids"].([]any)
		if !ok || len(workerIDs) != 1 {
			t.Fatalf("worker_ids = %v, want slice of 1", resp["worker_ids"])
		}
		if workerIDs[0] != "worker-mixed-running" {
			t.Fatalf("worker_ids[0] = %v, want worker-mixed-running", workerIDs[0])
		}

		dormantWorker, err := db.GetWorker("worker-mixed-dormant")
		if err != nil {
			t.Fatalf("GetWorker(dormant) error = %v", err)
		}
		if dormantWorker.Status != model.WorkerDormant {
			t.Fatalf("dormant worker Status = %q, want %q", dormantWorker.Status, model.WorkerDormant)
		}

		stoppedWorker, err := db.GetWorker("worker-mixed-stopped")
		if err != nil {
			t.Fatalf("GetWorker(stopped) error = %v", err)
		}
		if stoppedWorker.Status != model.WorkerStopped {
			t.Fatalf("stopped worker Status = %q, want %q", stoppedWorker.Status, model.WorkerStopped)
		}
	})
}

func TestHandleTeardownSourceValidation(t *testing.T) {
	t.Parallel()

	t.Run("requires source", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
		api := NewAPI(db, nil, nil, nil, manager, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source", nil)
		recorder := httptest.NewRecorder()

		api.handleTeardownSource(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("requires main confirmation", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		createTeardownTestDeployment(t, db, "main")
		createTestWorker(t, db, model.Worker{
			ID:            "worker-main-low-vol",
			Name:          "worker-main-low-vol",
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "soak-sha",
			LitestreamSHA: "litestream-sha",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
		api := NewAPI(db, nil, nil, nil, manager, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source=main", nil)
		recorder := httptest.NewRecorder()

		api.handleTeardownSource(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
		worker := mustWorker(t, db, "worker-main-low-vol")
		if worker.Status != model.WorkerRunning {
			t.Fatalf("Status = %q, want %q", worker.Status, model.WorkerRunning)
		}
	})

	t.Run("accepts explicit main confirmation", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		createTeardownTestDeployment(t, db, "main")
		createTestWorker(t, db, model.Worker{
			ID:            "worker-main-confirmed",
			Name:          "worker-main-confirmed",
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "soak-sha",
			LitestreamSHA: "litestream-sha",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
		api := NewAPI(db, nil, nil, nil, manager, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source=main&confirm_main=true", nil)
		recorder := httptest.NewRecorder()

		api.handleTeardownSource(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
		}
		worker := mustWorker(t, db, "worker-main-confirmed")
		if worker.Status != model.WorkerStopped {
			t.Fatalf("Status = %q, want %q", worker.Status, model.WorkerStopped)
		}
	})

	t.Run("unknown source returns not found", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
		api := NewAPI(db, nil, nil, nil, manager, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source=pr-404", nil)
		recorder := httptest.NewRecorder()

		api.handleTeardownSource(recorder, request)

		if recorder.Code != http.StatusNotFound {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusNotFound)
		}
	})

	t.Run("source without deployment returns conflict", func(t *testing.T) {
		t.Parallel()

		db := openTestDB(t)
		createTestWorker(t, db, model.Worker{
			ID:            "worker-pr-no-deployment",
			Name:          "worker-pr-no-deployment",
			Status:        model.WorkerRunning,
			Source:        "pr-no-deployment",
			GitSHA:        "soak-sha",
			LitestreamSHA: "litestream-sha",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
		api := NewAPI(db, nil, nil, nil, manager, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source=pr-no-deployment", nil)
		recorder := httptest.NewRecorder()

		api.handleTeardownSource(recorder, request)

		if recorder.Code != http.StatusConflict {
			t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusConflict)
		}
		worker := mustWorker(t, db, "worker-pr-no-deployment")
		if worker.Status != model.WorkerRunning {
			t.Fatalf("Status = %q, want %q", worker.Status, model.WorkerRunning)
		}
	})
}

func TestHandleTeardownSourceArchivesAndDestroysRunningAndDormantWorkers(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	source := "pr-158"
	createTeardownTestDeployment(t, db, source)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-158-running",
		AppName:       "litestream-soak",
		Name:          "worker-pr-158-running",
		Status:        model.WorkerRunning,
		Source:        source,
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		FlyMachineID:  "machine-pr-158-running",
		FlyVolumeID:   "volume-pr-158-running",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-158-dormant",
		AppName:       "litestream-soak",
		Name:          "worker-pr-158-dormant",
		Status:        model.WorkerRunning,
		Source:        source,
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
		FlyMachineID:  "machine-pr-158-dormant",
		FlyVolumeID:   "volume-pr-158-dormant",
	})
	if err := db.MarkWorkerDormant("worker-pr-158-dormant", "known bad", "integrity_check_mismatch", "known_bad_source"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}

	fly := newTeardownTestServer(t, nil)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{
		Bucket:    "bucket",
		Endpoint:  fly.server.URL,
		AccessKey: "access",
		SecretKey: "secret",
		Region:    "auto",
	}, "", "")
	api := NewAPI(db, fly.client, nil, nil, manager, nil)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	request := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source="+source, nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response SourceTeardownResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Complete {
		t.Fatalf("Complete = false, want true: %+v", response.Workers)
	}
	if response.ArchiveID == 0 || !response.ArchiveCreated {
		t.Fatalf("archive = (%d, created=%t), want new archive", response.ArchiveID, response.ArchiveCreated)
	}
	if response.ArchiveType != runArchiveTypeTeardown {
		t.Fatalf("ArchiveType = %q, want %q", response.ArchiveType, runArchiveTypeTeardown)
	}
	if response.TotalWorkers != 2 || response.DestroyedWorkers != 2 || response.FailedWorkers != 0 {
		t.Fatalf("worker counts = total:%d destroyed:%d failed:%d, want 2/2/0", response.TotalWorkers, response.DestroyedWorkers, response.FailedWorkers)
	}

	outcomes := teardownOutcomesByWorker(response.Workers)
	if outcomes["worker-pr-158-running"].PreviousStatus != model.WorkerRunning {
		t.Fatalf("running previous status = %q, want %q", outcomes["worker-pr-158-running"].PreviousStatus, model.WorkerRunning)
	}
	if outcomes["worker-pr-158-dormant"].PreviousStatus != model.WorkerDormant {
		t.Fatalf("dormant previous status = %q, want %q", outcomes["worker-pr-158-dormant"].PreviousStatus, model.WorkerDormant)
	}
	for _, workerID := range []string{"worker-pr-158-running", "worker-pr-158-dormant"} {
		if outcomes[workerID].Outcome != sourceTeardownOutcomeDestroyed {
			t.Fatalf("%s outcome = %q, want %q", workerID, outcomes[workerID].Outcome, sourceTeardownOutcomeDestroyed)
		}
		worker := mustWorker(t, db, workerID)
		if worker.Status != model.WorkerStopped {
			t.Fatalf("%s Status = %q, want %q", workerID, worker.Status, model.WorkerStopped)
		}
	}

	for _, machineID := range []string{"machine-pr-158-running", "machine-pr-158-dormant"} {
		if !fly.machineDestroyed(machineID) {
			t.Fatalf("machine %s was not destroyed", machineID)
		}
	}
	for _, volumeID := range []string{"volume-pr-158-running", "volume-pr-158-dormant"} {
		if !fly.volumeDestroyed(volumeID) {
			t.Fatalf("volume %s was not destroyed", volumeID)
		}
	}
	prefixes := make(map[string]bool)
	for _, prefix := range fly.prefixes() {
		prefixes[prefix] = true
	}
	for _, prefix := range []string{
		"soak/worker-pr-158-running/volume-pr-158-running/",
		"soak/worker-pr-158-dormant/volume-pr-158-dormant/",
	} {
		if !prefixes[prefix] {
			t.Fatalf("replica prefix %q was not cleared; got %v", prefix, fly.prefixes())
		}
	}

	archiveRequest := httptest.NewRequest(http.MethodGet, "/api/run-archives?source="+source+"&type="+runArchiveTypeTeardown, nil)
	archiveRecorder := httptest.NewRecorder()
	mux.ServeHTTP(archiveRecorder, archiveRequest)
	if archiveRecorder.Code != http.StatusOK {
		t.Fatalf("archive status code = %d, want %d", archiveRecorder.Code, http.StatusOK)
	}
	var archives []model.RunArchive
	if err := json.NewDecoder(archiveRecorder.Body).Decode(&archives); err != nil {
		t.Fatalf("decode archives: %v", err)
	}
	if len(archives) != 1 || archives[0].ID != response.ArchiveID {
		t.Fatalf("archives = %+v, want archive %d", archives, response.ArchiveID)
	}
	var payload runArchivePayload
	if err := json.Unmarshal([]byte(archives[0].Payload), &payload); err != nil {
		t.Fatalf("decode archive payload: %v", err)
	}
	if payload.Reason != "operator_teardown" {
		t.Fatalf("archive reason = %q, want operator_teardown", payload.Reason)
	}
	archivedStatuses := make(map[string]model.WorkerStatus)
	for _, evidence := range payload.Workers {
		archivedStatuses[evidence.Worker.ID] = evidence.Worker.Status
	}
	if archivedStatuses["worker-pr-158-running"] != model.WorkerRunning {
		t.Fatalf("archived running status = %q, want %q", archivedStatuses["worker-pr-158-running"], model.WorkerRunning)
	}
	if archivedStatuses["worker-pr-158-dormant"] != model.WorkerDormant {
		t.Fatalf("archived dormant status = %q, want %q", archivedStatuses["worker-pr-158-dormant"], model.WorkerDormant)
	}

	retryRequest := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source="+source, nil)
	retryRecorder := httptest.NewRecorder()
	mux.ServeHTTP(retryRecorder, retryRequest)
	if retryRecorder.Code != http.StatusOK {
		t.Fatalf("retry status code = %d, want %d; body: %s", retryRecorder.Code, http.StatusOK, retryRecorder.Body.String())
	}
	var retryResponse SourceTeardownResponse
	if err := json.NewDecoder(retryRecorder.Body).Decode(&retryResponse); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if retryResponse.ArchiveID != response.ArchiveID || retryResponse.ArchiveCreated {
		t.Fatalf("retry archive = (%d, created=%t), want existing %d", retryResponse.ArchiveID, retryResponse.ArchiveCreated, response.ArchiveID)
	}
	if retryResponse.AlreadyStoppedWorkers != 2 || retryResponse.DestroyedWorkers != 0 {
		t.Fatalf("retry counts = already_stopped:%d destroyed:%d, want 2/0", retryResponse.AlreadyStoppedWorkers, retryResponse.DestroyedWorkers)
	}
}

func TestHandleTeardownSourceReportsFailuresAndSupportsRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	source := "pr-159"
	createTeardownTestDeployment(t, db, source)
	for _, worker := range []model.Worker{
		{
			ID: "worker-pr-159-fails", Name: "worker-pr-159-fails",
			FlyMachineID: "machine-pr-159-fails", FlyVolumeID: "volume-pr-159-fails",
		},
		{
			ID: "worker-pr-159-succeeds", Name: "worker-pr-159-succeeds",
			FlyMachineID: "machine-pr-159-succeeds", FlyVolumeID: "volume-pr-159-succeeds",
		},
	} {
		worker.AppName = "litestream-soak"
		worker.Status = model.WorkerRunning
		worker.Source = source
		worker.GitSHA = "soak-sha"
		worker.LitestreamSHA = "litestream-sha"
		worker.ProfileName = "low-volume"
		worker.ProfileConfig = "{}"
		createTestWorker(t, db, worker)
	}

	fly := newTeardownTestServer(t, map[string]int{"volume-pr-159-fails": 1})
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	api := NewAPI(db, fly.client, nil, nil, manager, nil)

	request := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source="+source, nil)
	recorder := httptest.NewRecorder()
	api.handleTeardownSource(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response SourceTeardownResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Complete || response.FailedWorkers != 1 || response.DestroyedWorkers != 1 {
		t.Fatalf("response = complete:%t failed:%d destroyed:%d, want false/1/1", response.Complete, response.FailedWorkers, response.DestroyedWorkers)
	}
	outcomes := teardownOutcomesByWorker(response.Workers)
	failed := outcomes["worker-pr-159-fails"]
	if failed.Outcome != sourceTeardownOutcomeFailed || !strings.Contains(failed.Error, "volume cleanup") {
		t.Fatalf("failed outcome = %+v, want volume cleanup failure", failed)
	}
	if failed.Status == model.WorkerStopped {
		t.Fatalf("failed worker Status = %q, must not be stopped", failed.Status)
	}
	if outcomes["worker-pr-159-succeeds"].Outcome != sourceTeardownOutcomeDestroyed {
		t.Fatalf("successful outcome = %+v, want destroyed", outcomes["worker-pr-159-succeeds"])
	}

	retryRequest := httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source="+source, nil)
	retryRecorder := httptest.NewRecorder()
	api.handleTeardownSource(retryRecorder, retryRequest)
	if retryRecorder.Code != http.StatusOK {
		t.Fatalf("retry status code = %d, want %d; body: %s", retryRecorder.Code, http.StatusOK, retryRecorder.Body.String())
	}
	var retryResponse SourceTeardownResponse
	if err := json.NewDecoder(retryRecorder.Body).Decode(&retryResponse); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if !retryResponse.Complete || retryResponse.FailedWorkers != 0 || retryResponse.DestroyedWorkers != 1 || retryResponse.AlreadyStoppedWorkers != 1 {
		t.Fatalf(
			"retry = complete:%t failed:%d destroyed:%d already_stopped:%d, want true/0/1/1",
			retryResponse.Complete,
			retryResponse.FailedWorkers,
			retryResponse.DestroyedWorkers,
			retryResponse.AlreadyStoppedWorkers,
		)
	}
	if retryResponse.ArchiveCreated {
		t.Fatal("retry ArchiveCreated = true, want existing archive")
	}
	for _, workerID := range []string{"worker-pr-159-fails", "worker-pr-159-succeeds"} {
		worker := mustWorker(t, db, workerID)
		if worker.Status != model.WorkerStopped {
			t.Fatalf("%s Status = %q, want %q", workerID, worker.Status, model.WorkerStopped)
		}
	}
}

func createTeardownTestDeployment(t *testing.T, db *model.DB, source string) *model.Deployment {
	t.Helper()

	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ImageRef:      "registry.fly.io/litestream-soak:soak-sha",
		Source:        source,
		PRNumber:      sourcePRNumber(source),
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	deployment, err := db.GetLatestDeployment(source)
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}
	return deployment
}

func teardownOutcomesByWorker(outcomes []SourceTeardownWorkerOutcome) map[string]SourceTeardownWorkerOutcome {
	byWorker := make(map[string]SourceTeardownWorkerOutcome, len(outcomes))
	for _, outcome := range outcomes {
		byWorker[outcome.WorkerID] = outcome
	}
	return byWorker
}

func TestRespondError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     int
		err        error
		msg        string
		wantBody   string
		wantStatus int
		wantLeak   string
	}{
		{
			name:       "explicit msg with internal error does not leak error text",
			status:     http.StatusInternalServerError,
			err:        errors.New("secret-internal-detail"),
			msg:        "operation failed",
			wantBody:   "operation failed\n",
			wantStatus: http.StatusInternalServerError,
			wantLeak:   "secret-internal-detail",
		},
		{
			name:       "empty msg falls back to status text",
			status:     http.StatusNotFound,
			err:        errors.New("sql: no rows"),
			msg:        "",
			wantBody:   http.StatusText(http.StatusNotFound) + "\n",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "nil error with static msg does not panic",
			status:     http.StatusBadRequest,
			err:        nil,
			msg:        "invalid payload",
			wantBody:   "invalid payload\n",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			respondError(rec, req, tc.status, tc.err, tc.msg)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			body := rec.Body.String()
			if body != tc.wantBody {
				t.Fatalf("body = %q, want %q", body, tc.wantBody)
			}
			if tc.wantLeak != "" && strings.Contains(body, tc.wantLeak) {
				t.Fatalf("body leaks internal error text %q: %s", tc.wantLeak, body)
			}
		})
	}
}

func TestHandleGetWorkerSanitizesNotFound(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	api := NewAPI(db, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/workers/missing", nil)
	req.SetPathValue("id", "missing")
	rec := httptest.NewRecorder()

	api.handleGetWorker(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	want := http.StatusText(http.StatusNotFound) + "\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}
