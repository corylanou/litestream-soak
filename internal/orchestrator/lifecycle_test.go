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

func TestEvaluateDormantFleetAlertsPersistsDeduplicatesAndResolvesWithoutWebhook(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	for _, workerID := range []string{"worker-main-a", "worker-main-b"} {
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        "soak-sha",
			LitestreamSHA: "litestream-sha",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		if err := db.MarkWorkerDormant(workerID, "known bad", "integrity_check_mismatch", "known_bad_source"); err != nil {
			t.Fatalf("MarkWorkerDormant(%s) error = %v", workerID, err)
		}
	}

	dispatcher := NewAlertDispatcher(db, "https://control.example", "", "")
	manager := NewManager(nil, db, nil, dispatcher, "litestream-soak", ReplicaConfig{}, "", "")
	policy := DormantFleetAlertPolicy{Threshold: time.Nanosecond}

	manager.evaluateDormantFleetAlerts(context.Background(), policy)
	manager.evaluateDormantFleetAlerts(context.Background(), policy)

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts() error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1", len(alerts))
	}
	if alerts[0].AlertType != "fleet_fully_dormant" {
		t.Fatalf("AlertType = %q, want fleet_fully_dormant", alerts[0].AlertType)
	}
	if alerts[0].Source != "main" {
		t.Fatalf("Source = %q, want main", alerts[0].Source)
	}
	if alerts[0].Status != "not_configured" {
		t.Fatalf("Status = %q, want not_configured", alerts[0].Status)
	}
	if alerts[0].ConditionStatus != "active" {
		t.Fatalf("ConditionStatus = %q, want active", alerts[0].ConditionStatus)
	}
	if alerts[0].ConditionStartedAt == nil {
		t.Fatal("ConditionStartedAt = nil, want dormant epoch")
	}

	if err := db.MarkWorkerProbing("worker-main-a", "deployment_ready"); err != nil {
		t.Fatalf("MarkWorkerProbing() error = %v", err)
	}
	manager.evaluateDormantFleetAlerts(context.Background(), policy)

	alerts, err = db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts() after resume error = %v", err)
	}
	if alerts[0].ConditionStatus != "resolved" {
		t.Fatalf("ConditionStatus after resume = %q, want resolved", alerts[0].ConditionStatus)
	}
	if alerts[0].ConditionResolvedAt == nil {
		t.Fatal("ConditionResolvedAt after resume = nil, want resolution time")
	}
}

func TestEvaluateDormantFleetAlertsWaitsForThresholdAndActiveDeployment(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-alert-gate",
		Name:          "worker-main-alert-gate",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	if err := db.MarkWorkerDormant("worker-main-alert-gate", "known bad", "integrity_check_mismatch", "known_bad_source"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}

	if _, err := db.CreateDeployment(&model.Deployment{
		GitSHA:        "soak-building",
		LitestreamSHA: "litestream-building",
		Source:        "main",
		Status:        "building",
	}); err != nil {
		t.Fatalf("CreateDeployment() error = %v", err)
	}

	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	manager.evaluateDormantFleetAlerts(context.Background(), DormantFleetAlertPolicy{Threshold: time.Hour})

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts() error = %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("len(alerts) before threshold = %d, want 0", len(alerts))
	}

	manager.evaluateDormantFleetAlerts(context.Background(), DormantFleetAlertPolicy{Threshold: time.Nanosecond})
	alerts, err = db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts() during deployment error = %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("len(alerts) during deployment = %d, want 0", len(alerts))
	}

	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}
	staleAt := deployment.StartedAt.UTC().Add(deploymentBuildTimeout + time.Second)
	if err := manager.evaluateDormantFleetAlertLocked("main", DormantFleetAlertPolicy{Threshold: time.Nanosecond}, staleAt); err != nil {
		t.Fatalf("evaluateDormantFleetAlertLocked() error = %v", err)
	}

	alerts, err = db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts() after stale deployment error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) after stale deployment = %d, want 1", len(alerts))
	}
}

func TestResumeDormantWorkerImmediatelyResolvesFleetCondition(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "soak-resume",
		LitestreamSHA: "litestream-resume",
		ImageRef:      "registry.fly.io/litestream-soak:soak-resume",
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
		ID:            "worker-main-resume",
		Name:          "worker-main-resume",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        deployment.GitSHA,
		LitestreamSHA: deployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		FlyVolumeID:   "volume-main-resume",
	})
	if err := db.MarkWorkerDormant("worker-main-resume", "known bad", "integrity_check_mismatch", "known_bad_source"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}
	worker, err := db.GetWorker("worker-main-resume")
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if _, created, err := db.CreateAlert(&model.AlertDelivery{
		Source:             "main",
		AlertType:          fleetFullyDormantAlertType,
		Fingerprint:        fleetFullyDormantAlertType + ":main:" + worker.DormantAt.Format(time.RFC3339Nano),
		Status:             "not_configured",
		ConditionStatus:    "active",
		ConditionStartedAt: worker.DormantAt,
	}); err != nil {
		t.Fatalf("CreateAlert() error = %v", err)
	} else if !created {
		t.Fatal("CreateAlert() created = false, want true")
	}

	fly := newCreateWorkerFlyServer(t)
	manager := &Manager{fly: fly.client, db: db, appName: "litestream-soak"}
	if err := manager.ResumeDormantWorkers(
		context.Background(),
		"main",
		deployment.ImageRef,
		deployment.GitSHA,
		deployment.LitestreamSHA,
		"deployment_ready",
	); err != nil {
		t.Fatalf("ResumeDormantWorkers() error = %v", err)
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts() error = %v", err)
	}
	if len(alerts) != 1 || alerts[0].ConditionStatus != "resolved" {
		t.Fatalf("alerts = %+v, want one resolved condition", alerts)
	}
	rollout, err := buildDeploymentRollout(db, *deployment)
	if err != nil {
		t.Fatalf("buildDeploymentRollout() error = %v", err)
	}
	if rollout.DormantFleetAlert {
		t.Fatal("DormantFleetAlert = true after resume, want false")
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

func TestEvaluateFailedSourcePauseReevaluatesKnownBadDormantWorkers(t *testing.T) {
	tests := []struct {
		name                  string
		failures              int
		policy                FailedSourcePausePolicy
		newerDeploymentStatus string
		wantStatus            model.WorkerStatus
		wantProbes            int
	}{
		{name: "current policy clears verdict", failures: 1, wantStatus: model.WorkerProbing, wantProbes: 1},
		{name: "current policy keeps verdict", failures: 2, wantStatus: model.WorkerDormant, wantProbes: 0},
		{name: "source is outside policy", failures: 1, policy: FailedSourcePausePolicy{SourceAllowlist: []string{"pr-*"}}, wantStatus: model.WorkerDormant},
		{name: "newer deployment is building", failures: 1, newerDeploymentStatus: "building", wantStatus: model.WorkerDormant},
		{name: "newer deployment failed", failures: 1, newerDeploymentStatus: "failed", wantStatus: model.WorkerDormant},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			deployment := createFailedSourceReevaluationDeployment(t, db)
			createFailedSourceReevaluationWorker(t, db, deployment, "worker-main-known-bad", "known_bad_source")
			createFailedSourceReevaluationWorker(t, db, deployment, "worker-main-sustained", "sustained_failure")

			now := time.Now().UTC().Add(3 * time.Minute)
			for i := 0; i < test.failures; i++ {
				startedAt := now.Add(time.Duration(i-test.failures) * time.Minute)
				completedAt := startedAt.Add(5 * time.Second)
				mustRecordVerification(t, db, &model.Verification{
					WorkerID:     "worker-main-known-bad",
					StartedAt:    startedAt,
					CompletedAt:  &completedAt,
					Status:       "failed",
					CheckType:    "integrity",
					Passed:       false,
					ErrorMessage: "validation failed (exit 1): integrity check mismatch",
				})
			}
			if test.newerDeploymentStatus != "" {
				if _, err := db.CreateDeployment(&model.Deployment{
					GitSHA:        "newer-soak-sha",
					LitestreamSHA: "newer-litestream-sha",
					Source:        "main",
					Status:        test.newerDeploymentStatus,
				}); err != nil {
					t.Fatalf("CreateDeployment(newer) error = %v", err)
				}
			}

			fly := newCreateWorkerFlyServer(t)
			manager := &Manager{fly: fly.client, db: db, appName: "litestream-soak"}
			manager.evaluateFailedSourcePause(context.Background(), test.policy)

			knownBad, err := db.GetWorker("worker-main-known-bad")
			if err != nil {
				t.Fatalf("GetWorker(worker-main-known-bad) error = %v", err)
			}
			if knownBad.Status != test.wantStatus {
				t.Fatalf("known-bad status = %s, want %s", knownBad.Status, test.wantStatus)
			}
			if test.wantStatus == model.WorkerProbing {
				if knownBad.GitSHA != deployment.GitSHA {
					t.Fatalf("known-bad GitSHA = %q, want %q", knownBad.GitSHA, deployment.GitSHA)
				}
				if knownBad.LitestreamSHA != deployment.LitestreamSHA {
					t.Fatalf("known-bad LitestreamSHA = %q, want %q", knownBad.LitestreamSHA, deployment.LitestreamSHA)
				}
				requireWorkerEvent(t, db, knownBad.ID, "worker_probe_started")
			}

			sustained, err := db.GetWorker("worker-main-sustained")
			if err != nil {
				t.Fatalf("GetWorker(worker-main-sustained) error = %v", err)
			}
			if sustained.Status != model.WorkerDormant {
				t.Fatalf("sustained status = %s, want dormant", sustained.Status)
			}
			if sustained.ResumeTrigger != "sustained_failure" {
				t.Fatalf("sustained ResumeTrigger = %q, want sustained_failure", sustained.ResumeTrigger)
			}

			requests := fly.machineRequests()
			if len(requests) != test.wantProbes {
				t.Fatalf("len(machine requests) = %d, want %d", len(requests), test.wantProbes)
			}
			for _, request := range requests {
				if request.Config.Image != deployment.ImageRef {
					t.Fatalf("probe image = %q, want %q", request.Config.Image, deployment.ImageRef)
				}
			}
		})
	}
}

func createFailedSourceReevaluationDeployment(t *testing.T, db *model.DB) model.Deployment {
	t.Helper()

	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "latest-soak-sha",
		LitestreamSHA: "latest-litestream-sha",
		ImageRef:      "registry.fly.io/litestream-soak:latest",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}
	return *deployment
}

func createFailedSourceReevaluationWorker(t *testing.T, db *model.DB, deployment model.Deployment, workerID, resumeTrigger string) {
	t.Helper()

	createTestWorker(t, db, model.Worker{
		ID:            workerID,
		Name:          workerID,
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        deployment.GitSHA,
		LitestreamSHA: deployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		FlyVolumeID:   "volume-" + workerID,
	})
	if err := db.MarkWorkerDormant(workerID, "known bad", "integrity_check_mismatch", resumeTrigger); err != nil {
		t.Fatalf("MarkWorkerDormant(%s) error = %v", workerID, err)
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
