package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestDeploymentScorecardSourceDefaultsToMain(t *testing.T) {
	t.Parallel()

	if got := deploymentScorecardSource(model.Deployment{}); got != "main" {
		t.Fatalf("deploymentScorecardSource() = %q, want main", got)
	}
	if got := deploymentScorecardSource(model.Deployment{Source: " pr-1228 "}); got != "pr-1228" {
		t.Fatalf("deploymentScorecardSource() = %q, want pr-1228", got)
	}
}

func TestScoreDeploymentWorkerRespectsWindowBounds(t *testing.T) {
	t.Parallel()

	startedAt := timeMustParse("2026-04-26T15:00:00Z")
	windowEnd := startedAt.Add(30 * time.Minute)
	worker := model.Worker{
		ID:          "worker-main-low",
		Name:        "worker-main-low",
		ProfileName: "low-volume",
	}
	deployment := model.Deployment{StartedAt: startedAt}
	tooEarly := startedAt.Add(-time.Minute)
	inWindow := startedAt.Add(10 * time.Minute)
	tooLate := windowEnd

	outcome, verified := scoreDeploymentWorker(worker, deployment, []model.Verification{
		{
			WorkerID:    worker.ID,
			StartedAt:   tooLate.Add(-15 * time.Second),
			CompletedAt: &tooLate,
			Status:      "passed",
			Passed:      true,
		},
		{
			WorkerID:     worker.ID,
			StartedAt:    inWindow.Add(-15 * time.Second),
			CompletedAt:  &inWindow,
			Status:       "failed",
			CheckType:    "integrity",
			Passed:       false,
			ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": context deadline exceeded`,
		},
		{
			WorkerID:    worker.ID,
			StartedAt:   tooEarly.Add(-15 * time.Second),
			CompletedAt: &tooEarly,
			Status:      "passed",
			Passed:      true,
		},
	}, &windowEnd)
	if !verified {
		t.Fatal("verified = false, want true")
	}
	if outcome.Passed {
		t.Fatal("Passed = true, want false")
	}
	if outcome.FailureSignature != "litestream_sync_timeout" {
		t.Fatalf("FailureSignature = %q, want litestream_sync_timeout", outcome.FailureSignature)
	}
	if outcome.VerifiedAt == nil || !outcome.VerifiedAt.Equal(inWindow) {
		t.Fatalf("VerifiedAt = %v, want %v", outcome.VerifiedAt, inWindow)
	}

	_, verified = scoreDeploymentWorker(worker, deployment, []model.Verification{
		{
			WorkerID:    worker.ID,
			StartedAt:   tooEarly.Add(-15 * time.Second),
			CompletedAt: &tooEarly,
			Status:      "passed",
			Passed:      true,
		},
	}, &windowEnd)
	if verified {
		t.Fatal("verified = true, want false")
	}
}

func TestBuildDeploymentRolloutIgnoresAbortedForVerifiedSinceDeploy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-aborted-rollout",
		LitestreamSHA: "litestream-aborted-rollout",
		ImageRef:      "registry.fly.io/litestream-soak:sha-aborted-rollout",
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
		ID:            "worker-aborted-after-pass",
		Name:          "worker-aborted-after-pass",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-aborted-rollout",
		LitestreamSHA: "litestream-aborted-rollout",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})

	passedAt := deployment.StartedAt.Add(5 * time.Minute).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-aborted-after-pass",
		StartedAt:   passedAt.Add(-2 * time.Minute),
		CompletedAt: &passedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  120000,
	})

	abortedAt := deployment.StartedAt.Add(10 * time.Minute).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-aborted-after-pass",
		StartedAt:    abortedAt.Add(-2 * time.Minute),
		CompletedAt:  &abortedAt,
		Status:       "aborted",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: "litestream process stopped during verification",
		DurationMS:   120000,
	})

	rollout, err := buildDeploymentRollout(db, *deployment)
	if err != nil {
		t.Fatalf("buildDeploymentRollout() error = %v", err)
	}

	if rollout.VerifiedSinceDeploy != 1 {
		t.Fatalf("VerifiedSinceDeploy = %d, want 1", rollout.VerifiedSinceDeploy)
	}
	if rollout.AwaitingVerification != 0 {
		t.Fatalf("AwaitingVerification = %d, want 0", rollout.AwaitingVerification)
	}
	if len(rollout.Workers) != 1 {
		t.Fatalf("len(Workers) = %d, want 1", len(rollout.Workers))
	}
	if !rollout.Workers[0].VerifiedSinceDeploy {
		t.Fatalf("worker VerifiedSinceDeploy = false, want true")
	}
	if rollout.Workers[0].LastVerificationAt == nil || !rollout.Workers[0].LastVerificationAt.Equal(abortedAt) {
		t.Fatalf("LastVerificationAt = %v, want aborted report time %v", rollout.Workers[0].LastVerificationAt, abortedAt)
	}
}

func TestBuildDeploymentRolloutSurfacesAndClearsDormantFleetCondition(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "soak-dormant",
		LitestreamSHA: "litestream-dormant",
		ImageRef:      "registry.fly.io/litestream-soak:soak-dormant",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}
	for _, workerID := range []string{"worker-main-a", "worker-main-b"} {
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerRunning,
			Source:        "main",
			GitSHA:        deployment.GitSHA,
			LitestreamSHA: deployment.LitestreamSHA,
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		if err := db.MarkWorkerDormant(workerID, "known bad", "integrity_check_mismatch", "known_bad_source"); err != nil {
			t.Fatalf("MarkWorkerDormant(%s) error = %v", workerID, err)
		}
	}
	workers, err := db.ListWorkersForSource("main")
	if err != nil {
		t.Fatalf("ListWorkersForSource() error = %v", err)
	}
	conditionStartedAt, fullyDormant := fullyDormantFleetEpoch(workers)
	if !fullyDormant {
		t.Fatal("fullyDormantFleetEpoch() = false, want true")
	}
	if _, created, err := db.CreateAlert(&model.AlertDelivery{
		Source:             "main",
		AlertType:          "fleet_fully_dormant",
		Fingerprint:        "fleet_fully_dormant:main:" + conditionStartedAt.Format(time.RFC3339Nano),
		Status:             "not_configured",
		ConditionStatus:    "active",
		ConditionStartedAt: &conditionStartedAt,
	}); err != nil {
		t.Fatalf("CreateAlert() error = %v", err)
	} else if !created {
		t.Fatal("CreateAlert() created = false, want true")
	}

	rollout, err := buildDeploymentRollout(db, *deployment)
	if err != nil {
		t.Fatalf("buildDeploymentRollout() error = %v", err)
	}
	if !rollout.DormantFleetAlert {
		t.Fatal("DormantFleetAlert = false, want true")
	}
	if rollout.DormantFleetSince == nil || !rollout.DormantFleetSince.Equal(conditionStartedAt) {
		t.Fatalf("DormantFleetSince = %v, want %v", rollout.DormantFleetSince, conditionStartedAt)
	}
	for name, value := range map[string]string{
		"Summary":    rollout.Summary,
		"NextAction": rollout.NextAction,
		"NextChecks": strings.Join(rollout.NextChecks, " "),
	} {
		if !strings.Contains(value, "zero soak coverage") {
			t.Fatalf("%s = %q, want zero soak coverage", name, value)
		}
	}
	if !strings.Contains(rollout.Summary, "100% dormant") {
		t.Fatalf("Summary = %q, want 100%% dormant", rollout.Summary)
	}

	if _, err := db.ResolveActiveAlertCondition("fleet_fully_dormant", "main", time.Now().UTC()); err != nil {
		t.Fatalf("ResolveActiveAlertCondition() error = %v", err)
	}
	resolved, err := buildDeploymentRollout(db, *deployment)
	if err != nil {
		t.Fatalf("buildDeploymentRollout() after resolve error = %v", err)
	}
	if resolved.DormantFleetAlert {
		t.Fatal("DormantFleetAlert after resolve = true, want false")
	}
	if resolved.DormantFleetSince != nil {
		t.Fatalf("DormantFleetSince after resolve = %v, want nil", resolved.DormantFleetSince)
	}
	if strings.Contains(resolved.Summary, "zero soak coverage") || strings.Contains(resolved.NextAction, "zero soak coverage") {
		t.Fatalf("resolved rollout retained dormant condition wording: %+v", resolved)
	}
}

func TestBuildDeploymentScorecardExcludesRegionalWorkers(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	startedAt := timeMustParse("2026-04-26T15:00:00Z")
	deployment := model.Deployment{
		GitSHA:        "sha-new",
		LitestreamSHA: "litestream-new",
		Source:        "main",
		StartedAt:     startedAt,
	}

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low-vol",
		Name:          "worker-main-low-vol",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        deployment.GitSHA,
		LitestreamSHA: deployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		Region:        "ord",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low-vol-syd",
		Name:          "worker-main-low-vol-syd",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        deployment.GitSHA,
		LitestreamSHA: deployment.LitestreamSHA,
		ProfileName:   "low-vol-syd",
		ProfileConfig: "{}",
		Region:        "syd",
	})

	passedAt := startedAt.Add(time.Minute)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-low-vol",
		StartedAt:   passedAt.Add(-15 * time.Second),
		CompletedAt: &passedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
	})
	failedAt := startedAt.Add(2 * time.Minute)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-main-low-vol-syd",
		StartedAt:    failedAt.Add(-15 * time.Second),
		CompletedAt:  &failedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": context deadline exceeded`,
	})

	scorecard, err := buildDeploymentScorecard(db, deployment, nil)
	if err != nil {
		t.Fatalf("buildDeploymentScorecard() error = %v", err)
	}
	if scorecard.TotalWorkers != 1 {
		t.Fatalf("TotalWorkers = %d, want 1", scorecard.TotalWorkers)
	}
	if scorecard.PassedWorkers != 1 {
		t.Fatalf("PassedWorkers = %d, want 1", scorecard.PassedWorkers)
	}
	if scorecard.FailedWorkers != 0 {
		t.Fatalf("FailedWorkers = %d, want 0", scorecard.FailedWorkers)
	}
	if len(scorecard.Outcomes) != 1 || scorecard.Outcomes[0].WorkerID != "worker-main-low-vol" {
		t.Fatalf("Outcomes = %+v, want only worker-main-low-vol", scorecard.Outcomes)
	}
}

func TestCountDeploymentScorecardOutcomeCountsFailuresBySignature(t *testing.T) {
	t.Parallel()

	scorecard := DeploymentScorecard{TotalWorkers: 3}
	failureCounts := make(map[string]DeploymentFailureCount)

	countDeploymentScorecardOutcome(&scorecard, failureCounts, DeploymentWorkerOutcome{
		WorkerID:         "worker-b",
		Name:             "worker-b",
		Passed:           false,
		FailureStage:     "sync",
		FailureSignature: "litestream_sync_timeout",
	}, true)
	countDeploymentScorecardOutcome(&scorecard, failureCounts, DeploymentWorkerOutcome{
		WorkerID:         "worker-c",
		Name:             "worker-c",
		Passed:           false,
		FailureStage:     "sync",
		FailureSignature: "litestream_sync_timeout",
	}, true)
	countDeploymentScorecardOutcome(&scorecard, failureCounts, DeploymentWorkerOutcome{
		WorkerID: "worker-a",
		Name:     "worker-a",
		Passed:   true,
	}, true)
	countDeploymentScorecardOutcome(&scorecard, failureCounts, DeploymentWorkerOutcome{}, false)

	if scorecard.VerifiedWorkers != 3 {
		t.Fatalf("VerifiedWorkers = %d, want 3", scorecard.VerifiedWorkers)
	}
	if scorecard.PassedWorkers != 1 {
		t.Fatalf("PassedWorkers = %d, want 1", scorecard.PassedWorkers)
	}
	if scorecard.FailedWorkers != 2 {
		t.Fatalf("FailedWorkers = %d, want 2", scorecard.FailedWorkers)
	}
	if scorecard.AwaitingWorkers != 1 {
		t.Fatalf("AwaitingWorkers = %d, want 1", scorecard.AwaitingWorkers)
	}
	if failureCounts["litestream_sync_timeout"].Count != 2 {
		t.Fatalf("failure count = %d, want 2", failureCounts["litestream_sync_timeout"].Count)
	}
}

func TestFinalizeDeploymentScorecardSortsAndCalculatesRate(t *testing.T) {
	t.Parallel()

	scorecard := DeploymentScorecard{
		TotalWorkers:  4,
		PassedWorkers: 2,
		Outcomes: []DeploymentWorkerOutcome{
			{Name: "worker-c"},
			{Name: "worker-a"},
			{Name: "worker-b"},
		},
	}
	failureCounts := map[string]DeploymentFailureCount{
		"restore_s3_list_request_canceled": {
			Signature: "restore_s3_list_request_canceled",
			Stage:     "restore",
			Count:     1,
		},
		"litestream_sync_timeout": {
			Signature: "litestream_sync_timeout",
			Stage:     "sync",
			Count:     2,
		},
		"litestream_sync_socket_refused": {
			Signature: "litestream_sync_socket_refused",
			Stage:     "sync",
			Count:     2,
		},
	}

	finalizeDeploymentScorecard(&scorecard, failureCounts)

	if scorecard.PassRate != 0.5 {
		t.Fatalf("PassRate = %v, want 0.5", scorecard.PassRate)
	}
	if got := []string{scorecard.Outcomes[0].Name, scorecard.Outcomes[1].Name, scorecard.Outcomes[2].Name}; got[0] != "worker-a" || got[1] != "worker-b" || got[2] != "worker-c" {
		t.Fatalf("outcome order = %v, want worker-a worker-b worker-c", got)
	}
	if got := []string{scorecard.Failures[0].Signature, scorecard.Failures[1].Signature, scorecard.Failures[2].Signature}; got[0] != "litestream_sync_socket_refused" || got[1] != "litestream_sync_timeout" || got[2] != "restore_s3_list_request_canceled" {
		t.Fatalf("failure order = %v", got)
	}
}
