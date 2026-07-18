package orchestrator

import (
	"context"
	"fmt"
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

func TestFailedSourcePauseCandidatePausesKnownBadMain(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ImageRef:      "registry.fly.io/litestream-soak:soak-sha",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}

	for _, worker := range []model.Worker{
		{
			ID:            "worker-main-low-vol",
			Name:          "worker-main-low-vol",
			Status:        model.WorkerDegraded,
			Source:        "main",
			GitSHA:        deployment.GitSHA,
			LitestreamSHA: deployment.LitestreamSHA,
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		},
		{
			ID:            "worker-main-read-heavy",
			Name:          "worker-main-read-heavy",
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        deployment.GitSHA,
			LitestreamSHA: deployment.LitestreamSHA,
			ProfileName:   "read-heavy",
			ProfileConfig: "{}",
		},
	} {
		createTestWorker(t, db, worker)
	}

	verifiedAt := time.Now().UTC().Add(time.Minute)
	for _, age := range []time.Duration{40 * time.Second, 15 * time.Second} {
		done := verifiedAt.Add(-age).Add(5 * time.Second)
		mustRecordVerification(t, db, &model.Verification{
			WorkerID:     "worker-main-low-vol",
			StartedAt:    verifiedAt.Add(-age),
			CompletedAt:  &done,
			Status:       "failed",
			CheckType:    "integrity",
			Passed:       false,
			ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": context deadline exceeded`,
		})
	}
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-read-heavy",
		StartedAt:   verifiedAt.Add(-10 * time.Second),
		CompletedAt: &verifiedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
	})

	evaluation, ok, err := failedSourcePauseCandidate(db, *deployment, FailedSourcePausePolicy{})
	if err != nil {
		t.Fatalf("failedSourcePauseCandidate() error = %v", err)
	}
	if !ok {
		t.Fatal("failedSourcePauseCandidate() = false, want true")
	}
	if evaluation.Signature != "litestream_sync_timeout" {
		t.Fatalf("Signature = %q, want litestream_sync_timeout", evaluation.Signature)
	}
	if len(evaluation.Workers) != 2 {
		t.Fatalf("len(Workers) = %d, want 2", len(evaluation.Workers))
	}
}

func TestPauseSourceWorkersMarksActiveWorkersDormant(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	for _, worker := range []model.Worker{
		{ID: "worker-main-low-vol", Name: "worker-main-low-vol", Status: model.WorkerRunning, Source: "main", GitSHA: "soak-sha", ProfileName: "low-volume", ProfileConfig: "{}"},
		{ID: "worker-main-high-vol", Name: "worker-main-high-vol", Status: model.WorkerDegraded, Source: "main", GitSHA: "soak-sha", ProfileName: "high-volume", ProfileConfig: "{}"},
		{ID: "worker-main-burst-vol", Name: "worker-main-burst-vol", Status: model.WorkerDormant, Source: "main", GitSHA: "soak-sha", ProfileName: "burst-volume", ProfileConfig: "{}"},
	} {
		createTestWorker(t, db, worker)
	}

	manager := &Manager{db: db, appName: "litestream-soak"}
	paused, err := manager.PauseSourceWorkers(context.Background(), "main", "known bad", "test_signature", "test")
	if err != nil {
		t.Fatalf("PauseSourceWorkers() error = %v", err)
	}
	if len(paused) != 2 {
		t.Fatalf("len(paused) = %d, want 2", len(paused))
	}
	for _, workerID := range paused {
		worker, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker(%s) error = %v", workerID, err)
		}
		if worker.Status != model.WorkerDormant {
			t.Fatalf("%s status = %s, want dormant", workerID, worker.Status)
		}
		if worker.ResumeTrigger != "test" {
			t.Fatalf("%s ResumeTrigger = %q, want test", workerID, worker.ResumeTrigger)
		}
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

func TestFailedSourcePauseCandidateIgnoresSingleEnvironmentalBlip(t *testing.T) {
	configureEnvPolicyForTest(t, EnvironmentalFailurePolicy{Bucket: "litestream-soak-replicas-shared", EscalateAfterConsecutive: 4, EscalateAfterDuration: 30 * time.Minute})

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA: "soak-sha", LitestreamSHA: "litestream-sha",
		ImageRef: "registry.fly.io/litestream-soak:soak-sha", Source: "main", Status: "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}

	createTestWorker(t, db, model.Worker{
		ID: "worker-main-low-vol-syd", Name: "worker-main-low-vol-syd", Status: model.WorkerDegraded,
		Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA,
		ProfileName: "low-vol-syd", ProfileConfig: "{}",
	})
	for i := 0; i < 14; i++ {
		createTestWorker(t, db, model.Worker{
			ID: fmt.Sprintf("worker-main-green-%02d", i), Name: fmt.Sprintf("worker-main-green-%02d", i),
			Status: model.WorkerRunning, Source: "main", GitSHA: deployment.GitSHA,
			LitestreamSHA: deployment.LitestreamSHA, ProfileName: "low-volume", ProfileConfig: "{}",
		})
	}

	now := time.Now().UTC().Add(time.Minute)
	for i := 9; i >= 1; i-- {
		done := now.Add(-time.Duration(i*5) * time.Second).Add(time.Second)
		mustRecordVerification(t, db, &model.Verification{
			WorkerID: "worker-main-low-vol-syd", StartedAt: now.Add(-time.Duration(i*5) * time.Second),
			CompletedAt: &done, Status: "passed", CheckType: "integrity", Passed: true,
		})
	}
	done := now
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: "worker-main-low-vol-syd", StartedAt: now.Add(-2 * time.Second), CompletedAt: &done,
		Status: "failed", CheckType: "integrity", Passed: false,
		ErrorMessage: `restore failed: operation error S3: ListObjectsV2, https response error StatusCode: 408, RequestID: 1783, api error RequestCanceled: Request was canceled`,
	})
	for i := 0; i < 14; i++ {
		mustRecordVerification(t, db, &model.Verification{
			WorkerID: fmt.Sprintf("worker-main-green-%02d", i), StartedAt: now.Add(-2 * time.Second),
			CompletedAt: &done, Status: "passed", CheckType: "integrity", Passed: true,
		})
	}

	_, ok, err := failedSourcePauseCandidate(db, *deployment, FailedSourcePausePolicy{})
	if err != nil {
		t.Fatalf("failedSourcePauseCandidate() error = %v", err)
	}
	if ok {
		t.Fatal("one environmental 408 blip after nine passes must NOT pause a 15-worker fleet (2026-07-18 false alarm)")
	}
}

func TestFailedSourcePauseCandidateSingleWorkerCorroborated(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA: "soak-sha", LitestreamSHA: "litestream-sha",
		ImageRef: "registry.fly.io/litestream-soak:soak-sha", Source: "main", Status: "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}
	createTestWorker(t, db, model.Worker{
		ID: "worker-main-low-vol", Name: "worker-main-low-vol", Status: model.WorkerDegraded,
		Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA,
		ProfileName: "low-volume", ProfileConfig: "{}",
	})

	now := time.Now().UTC().Add(time.Minute)
	for _, age := range []time.Duration{40 * time.Second, 15 * time.Second} {
		done := now.Add(-age).Add(time.Minute)
		mustRecordVerification(t, db, &model.Verification{
			WorkerID: "worker-main-low-vol", StartedAt: now.Add(-age), CompletedAt: &done,
			Status: "failed", CheckType: "integrity", Passed: false,
			ErrorMessage: "validation failed (exit 1): integrity check mismatch",
		})
	}

	_, ok, err := failedSourcePauseCandidate(db, *deployment, FailedSourcePausePolicy{})
	if err != nil {
		t.Fatalf("failedSourcePauseCandidate() error = %v", err)
	}
	if !ok {
		t.Fatal("a lone worker with consecutive actionable failures must still pause the source")
	}
}

func TestFailedSourcePauseCandidateSurvivesAbortStarvedHistory(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA: "soak-sha", LitestreamSHA: "litestream-sha",
		ImageRef: "registry.fly.io/litestream-soak:soak-sha", Source: "main", Status: "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}
	createTestWorker(t, db, model.Worker{
		ID: "worker-main-low-vol", Name: "worker-main-low-vol", Status: model.WorkerDegraded,
		Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA,
		ProfileName: "low-volume", ProfileConfig: "{}",
	})

	now := time.Now().UTC().Add(time.Minute)
	record := func(age time.Duration, status, msg string) {
		t.Helper()
		done := now.Add(-age).Add(time.Second)
		mustRecordVerification(t, db, &model.Verification{
			WorkerID: "worker-main-low-vol", StartedAt: now.Add(-age), CompletedAt: &done,
			Status: status, CheckType: "integrity", Passed: false, ErrorMessage: msg,
		})
	}
	record(50*time.Second, "failed", "validation failed (exit 1): integrity check mismatch")
	for i := 0; i < 30; i++ {
		record(time.Duration(45-i)*time.Second, "aborted", "")
	}
	record(2*time.Second, "failed", "validation failed (exit 1): integrity check mismatch")

	_, ok, err := failedSourcePauseCandidate(db, *deployment, FailedSourcePausePolicy{})
	if err != nil {
		t.Fatalf("failedSourcePauseCandidate() error = %v", err)
	}
	if !ok {
		t.Fatal("30 interleaved aborts must not hide the earlier hard failure from corroboration")
	}
}

func TestFailedSourcePauseCandidateIgnoresDormantWorkersWithoutFailures(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA: "soak-sha", LitestreamSHA: "litestream-sha",
		ImageRef: "registry.fly.io/litestream-soak:soak-sha", Source: "main", Status: "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}
	for _, worker := range []model.Worker{
		{ID: "worker-main-a", Name: "worker-main-a", Status: model.WorkerDormant, Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, ProfileName: "low-volume", ProfileConfig: "{}"},
		{ID: "worker-main-b", Name: "worker-main-b", Status: model.WorkerDormant, Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, ProfileName: "read-heavy", ProfileConfig: "{}"},
		{ID: "worker-main-c", Name: "worker-main-c", Status: model.WorkerRunning, Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, ProfileName: "burst-volume", ProfileConfig: "{}"},
	} {
		createTestWorker(t, db, worker)
	}
	now := time.Now().UTC().Add(time.Minute)
	for _, id := range []string{"worker-main-a", "worker-main-b", "worker-main-c"} {
		done := now
		mustRecordVerification(t, db, &model.Verification{
			WorkerID: id, StartedAt: now.Add(-5 * time.Second), CompletedAt: &done,
			Status: "passed", CheckType: "integrity", Passed: true,
		})
	}

	_, ok, err := failedSourcePauseCandidate(db, *deployment, FailedSourcePausePolicy{})
	if err != nil {
		t.Fatalf("failedSourcePauseCandidate() error = %v", err)
	}
	if ok {
		t.Fatal("two dormant workers with passing verifications must not mark a release known-bad")
	}
}
