package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestSuccessTeardownCandidateRequiresAllowedSource(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "main", 0)

	_, ok, err := successTeardownCandidate(db, deployment, SuccessTeardownPolicy{}, worker.CreatedAt.Add(30*time.Hour))
	if err != nil {
		t.Fatalf("successTeardownCandidate() error = %v", err)
	}
	if ok {
		t.Fatal("successTeardownCandidate() = true, want false for default main source")
	}
}

func TestSuccessTeardownCandidateRequiresCleanWindow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-1228", 1228)
	now := worker.CreatedAt.Add(30 * time.Hour)

	_, ok, err := successTeardownCandidate(db, deployment, SuccessTeardownPolicy{
		HeartbeatStaleAfter: 48 * time.Hour,
	}, now)
	if err != nil {
		t.Fatalf("successTeardownCandidate() error = %v", err)
	}
	if !ok {
		t.Fatal("successTeardownCandidate() = false, want true")
	}
}

func TestSuccessTeardownCandidateRejectsFailureInWindow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-1228", 1228)
	failedAt := worker.CreatedAt.Add(5 * time.Hour)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     worker.ID,
		StartedAt:    failedAt.Add(-15 * time.Second),
		CompletedAt:  &failedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": context deadline exceeded`,
	})

	_, ok, err := successTeardownCandidate(db, deployment, SuccessTeardownPolicy{
		HeartbeatStaleAfter: 48 * time.Hour,
	}, worker.CreatedAt.Add(30*time.Hour))
	if err != nil {
		t.Fatalf("successTeardownCandidate() error = %v", err)
	}
	if ok {
		t.Fatal("successTeardownCandidate() = true, want false after a failure in the deployment window")
	}
}

func TestNormalizePRMaxAgePolicyDefaults(t *testing.T) {
	t.Parallel()

	policy := normalizePRMaxAgePolicy(PRMaxAgePolicy{})

	if policy.Threshold != 24*time.Hour {
		t.Fatalf("Threshold = %s, want 24h", policy.Threshold)
	}
	if policy.CheckInterval != 10*time.Minute {
		t.Fatalf("CheckInterval = %s, want 10m", policy.CheckInterval)
	}
	if policy.Action != PRMaxAgeActionStop {
		t.Fatalf("Action = %s, want %s", policy.Action, PRMaxAgeActionStop)
	}
	if len(policy.SourceAllowlist) != 1 || policy.SourceAllowlist[0] != "pr-*" {
		t.Fatalf("SourceAllowlist = %#v, want [pr-*]", policy.SourceAllowlist)
	}
}

func TestPRMaxAgeCandidateRequiresAllowedSource(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "main", 0)

	_, ok, err := prMaxAgeCandidate(db, deployment, PRMaxAgePolicy{}, worker.CreatedAt.Add(30*time.Hour))
	if err != nil {
		t.Fatalf("prMaxAgeCandidate() error = %v", err)
	}
	if ok {
		t.Fatal("prMaxAgeCandidate() = true, want false for default main source")
	}
}

func TestPRMaxAgeCandidateTriggersAfterThreshold(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-1228", 1228)

	evaluation, ok, err := prMaxAgeCandidate(db, deployment, PRMaxAgePolicy{}, worker.CreatedAt.Add(30*time.Hour))
	if err != nil {
		t.Fatalf("prMaxAgeCandidate() error = %v", err)
	}
	if !ok {
		t.Fatal("prMaxAgeCandidate() = false, want true")
	}
	if evaluation.Action != PRMaxAgeActionStop {
		t.Fatalf("Action = %s, want %s", evaluation.Action, PRMaxAgeActionStop)
	}
	if !strings.Contains(evaluation.Summary, "preserving volumes and replica data for debugging") {
		t.Fatalf("Summary = %q, want preserve-data wording", evaluation.Summary)
	}
	if len(evaluation.Workers) != 1 || evaluation.Workers[0].ID != worker.ID {
		t.Fatalf("Workers = %#v, want %s", evaluation.Workers, worker.ID)
	}
}

func TestPRMaxAgeCandidateRejectsFreshDeployment(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	deployment, worker := createCleanSuccessCandidate(t, db, "pr-1228", 1228)
	deployment.StartedAt = worker.CreatedAt.Add(-2 * time.Hour)

	_, ok, err := prMaxAgeCandidate(db, deployment, PRMaxAgePolicy{}, worker.CreatedAt.Add(3*time.Hour))
	if err != nil {
		t.Fatalf("prMaxAgeCandidate() error = %v", err)
	}
	if ok {
		t.Fatal("prMaxAgeCandidate() = true, want false for fresh deployment")
	}
}

func createCleanSuccessCandidate(t *testing.T, db *model.DB, source string, prNumber int) (model.Deployment, model.Worker) {
	t.Helper()

	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ImageRef:      "registry.fly.io/litestream-soak:soak-sha",
		Source:        source,
		PRNumber:      prNumber,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	storedDeployment, err := db.GetLatestDeployment(source)
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}

	worker := model.Worker{
		ID:            "worker-" + source + "-low-vol",
		Name:          "worker-" + source + "-low-vol",
		Status:        model.WorkerRunning,
		Source:        source,
		GitSHA:        storedDeployment.GitSHA,
		LitestreamSHA: storedDeployment.LitestreamSHA,
		PRNumber:      prNumber,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	createTestWorker(t, db, worker)
	if err := db.UpdateWorkerHeartbeat(worker.ID); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat() error = %v", err)
	}
	if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, reporting.RuntimePayload{
		SnapshotCollectedAt:       time.Now().UTC(),
		LitestreamSnapshotHealthy: true,
		DBStatus:                  "replicating",
	}); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
	}

	storedWorker, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	deployment := *storedDeployment
	deployment.StartedAt = storedWorker.CreatedAt.Add(-25 * time.Hour)

	passedAt := storedWorker.CreatedAt.Add(10 * time.Minute)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    worker.ID,
		StartedAt:   passedAt.Add(-15 * time.Second),
		CompletedAt: &passedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})

	return deployment, *storedWorker
}
