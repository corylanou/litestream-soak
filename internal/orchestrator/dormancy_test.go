package orchestrator

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestDetectDormancyCandidate(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-12*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-25*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	candidate, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 3)
	if !ok {
		t.Fatal("expected dormancy candidate")
	}
	if candidate.Signature != "litestream_sync_socket_refused" {
		t.Fatalf("signature=%q want %q", candidate.Signature, "litestream_sync_socket_refused")
	}
	if candidate.Count != 3 {
		t.Fatalf("count=%d want 3", candidate.Count)
	}
	if !candidate.Since.Equal(now.Add(-25 * time.Hour)) {
		t.Fatalf("since=%s want %s", candidate.Since, now.Add(-25*time.Hour))
	}
}

func TestDetectDormancyCandidateRequiresConsecutiveFailures(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	passCompletedAt := now.Add(-8 * time.Hour)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		{
			StartedAt:    passCompletedAt.Add(-10 * time.Second),
			CompletedAt:  &passCompletedAt,
			Status:       "completed",
			CheckType:    "integrity",
			Passed:       true,
			ErrorMessage: "",
		},
		failedVerificationAt(now.Add(-30*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	if _, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 2); ok {
		t.Fatal("expected no dormancy candidate when a pass interrupts the run")
	}
}

func TestDetectDormancyCandidateRequiresSameSignature(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-12*time.Hour), `wait for sync: context deadline exceeded`),
		failedVerificationAt(now.Add(-30*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	if _, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 2); ok {
		t.Fatal("expected no dormancy candidate when signatures differ")
	}
}

func TestVerificationsSinceIgnoresFailuresBeforeWorkerRun(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-10 * time.Minute)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-1*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-12*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-25*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	filtered := verificationsSince(verifications, cutoff)
	if len(filtered) != 1 {
		t.Fatalf("len(filtered)=%d want 1", len(filtered))
	}
	if _, ok := detectDormancyCandidate(filtered, now, 24*time.Hour, 3); ok {
		t.Fatal("expected no dormancy candidate from pre-run failures")
	}
}

func TestDormancyEvaluationStartUsesProbeTime(t *testing.T) {
	createdAt := time.Date(2026, 4, 13, 8, 0, 0, 0, time.UTC)
	lastProbeAt := createdAt.Add(4 * time.Hour)
	worker := model.Worker{
		CreatedAt:   createdAt,
		LastProbeAt: &lastProbeAt,
	}

	if got := dormancyEvaluationStart(worker); !got.Equal(lastProbeAt) {
		t.Fatalf("dormancyEvaluationStart()=%s want %s", got, lastProbeAt)
	}
}

func TestInferDeploymentRolloutStatus(t *testing.T) {
	tests := []struct {
		name    string
		rollout DeploymentRolloutResponse
		want    string
	}{
		{name: "no workers", rollout: DeploymentRolloutResponse{}, want: "no_workers"},
		{name: "outdated workers", rollout: DeploymentRolloutResponse{TotalWorkers: 9, OutdatedWorkers: 2}, want: "rolling_out"},
		{name: "probing workers", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, ProbingWorkers: 3}, want: "probing"},
		{name: "attention workers", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, DegradedWorkers: 1}, want: "needs_attention"},
		{name: "runtime unhealthy workers", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, RunningWorkers: 9, RuntimeUnhealthyWorkers: 1}, want: "needs_attention"},
		{name: "awaiting verification", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, RunningWorkers: 9, AwaitingVerification: 9}, want: "settling"},
		{name: "stable fleet", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, RunningWorkers: 9}, want: "stable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := inferDeploymentRolloutStatus(test.rollout); got != test.want {
				t.Fatalf("inferDeploymentRolloutStatus()=%q want %q", got, test.want)
			}
		})
	}
}

func TestSummarizeDeploymentRollout(t *testing.T) {
	rollout := DeploymentRolloutResponse{
		Deployment:           model.Deployment{GitSHA: "0123456789abcdef", LitestreamSHA: "fedcba9876543210", Source: "main"},
		Status:               "probing",
		TotalWorkers:         9,
		UpdatedWorkers:       9,
		ProbingWorkers:       3,
		VerifiedSinceDeploy:  6,
		AwaitingVerification: 3,
	}

	summary := summarizeDeploymentRollout(rollout)
	if summary != "The main branch rollout is still settling. All 9 workers are on the new release, 6 have verified since rollout, and 3 still need a fresh verification." {
		t.Fatalf("summary=%q", summary)
	}
}

func TestSummarizeDeploymentRolloutUsesSingularAttentionGrammar(t *testing.T) {
	rollout := DeploymentRolloutResponse{
		Deployment:       model.Deployment{Source: "pr-1228", PRNumber: 1228},
		Status:           "needs_attention",
		TotalWorkers:     9,
		UpdatedWorkers:   9,
		AttentionWorkers: 1,
		DegradedWorkers:  1,
		DormantWorkers:   0,
	}

	summary := summarizeDeploymentRollout(rollout)
	if summary != "The PR #1228 rollout needs attention. All 9 workers are on the new release, but 1 worker still needs investigation: 1 degraded worker." {
		t.Fatalf("summary=%q", summary)
	}
}

func TestSummarizeDeploymentComparisonUsesPlainEnglish(t *testing.T) {
	comparison := DeploymentComparisonResponse{
		Base: &DeploymentScorecard{
			Deployment:    model.Deployment{Source: "main"},
			PassedWorkers: 4,
			FailedWorkers: 4,
		},
		Head: DeploymentScorecard{
			Deployment:    model.Deployment{Source: "pr-1228", PRNumber: 1228},
			PassedWorkers: 9,
			FailedWorkers: 0,
		},
		Verdict: "better",
	}

	summary := summarizeDeploymentComparison(comparison)
	if summary != "The PR #1228 rollout looks better than the main branch rollout so far: 9 workers passed versus 4, and 0 failed versus 4." {
		t.Fatalf("summary=%q", summary)
	}
}

func TestResumeDormantWorkersReturnsWorkerFailures(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	worker := &model.Worker{
		ID:            "worker-pr-1228-taxi-mixed",
		Name:          "worker-pr-1228-taxi-mixed",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "old-soak",
		LitestreamSHA: "old-litestream",
		ProfileName:   "taxi-mixed",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	if err := db.MarkWorkerDormant(worker.ID, "stale failure", "sqlite_index_mismatch", "test"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}

	manager := &Manager{db: db, appName: "litestream-soak"}
	err = manager.ResumeDormantWorkers(context.Background(), "pr-1228", "image", "new-soak", "new-litestream", "test")
	if err == nil {
		t.Fatal("ResumeDormantWorkers() error = nil, want worker failure")
	}
	if !strings.Contains(err.Error(), worker.ID) {
		t.Fatalf("ResumeDormantWorkers() error = %q, want worker id", err)
	}

	events, err := db.ListWorkerEvents(worker.ID, 5)
	if err != nil {
		t.Fatalf("ListWorkerEvents() error = %v", err)
	}
	found := false
	for _, event := range events {
		if event.EventType == "worker_probe_start_failed" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("worker_probe_start_failed event not recorded: %+v", events)
	}
}

func failedVerificationAt(completedAt time.Time, errorMessage string) model.Verification {
	startedAt := completedAt.Add(-10 * time.Second)
	return model.Verification{
		StartedAt:    startedAt,
		CompletedAt:  &completedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: errorMessage,
	}
}
