package orchestrator

import (
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
