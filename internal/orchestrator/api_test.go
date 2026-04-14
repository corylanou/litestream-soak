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
		GitSHA:   "sha-new",
		ImageRef: "registry.fly.io/litestream-soak:sha-new",
		Source:   "main",
		Status:   "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-running",
		Name:          "worker-main-running",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-new",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-probing",
		Name:          "worker-main-probing",
		Status:        model.WorkerProbing,
		Source:        "main",
		GitSHA:        "sha-new",
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-dormant",
		Name:          "worker-main-dormant",
		Status:        model.WorkerDormant,
		Source:        "main",
		GitSHA:        "sha-old",
		ProfileName:   "burst-volume",
		ProfileConfig: "{}",
	})

	failedAt := time.Now().UTC().Add(-2 * time.Minute)
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
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}

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
	if rollout.Status != "rolling_out" {
		t.Fatalf("Status = %q, want rolling_out", rollout.Status)
	}
	if rollout.Workers[0].WorkerID != "worker-main-dormant" {
		t.Fatalf("first worker = %q, want outdated worker first", rollout.Workers[0].WorkerID)
	}
	if rollout.Workers[1].CurrentFailureSignature != "litestream_sync_socket_refused" {
		t.Fatalf("CurrentFailureSignature = %q, want litestream_sync_socket_refused", rollout.Workers[1].CurrentFailureSignature)
	}
}

func TestHandleGetLatestDeployment(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:   "sha-latest",
		ImageRef: "registry.fly.io/litestream-soak:sha-latest",
		Source:   "main",
		Status:   "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-one",
		Name:          "worker-main-one",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-latest",
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
	if rollout.Status != "stable" {
		t.Fatalf("Status = %q, want stable", rollout.Status)
	}
	if rollout.UpdatedWorkers != 1 || rollout.TotalWorkers != 1 {
		t.Fatalf("updated/total = %d/%d, want 1/1", rollout.UpdatedWorkers, rollout.TotalWorkers)
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
