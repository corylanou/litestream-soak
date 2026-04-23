package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/workload"
)

const (
	runArchiveTypeSuccess = "success"
	runArchiveTypeFailure = "failure"
)

type SuccessTeardownPolicy struct {
	Threshold           time.Duration
	CheckInterval       time.Duration
	HeartbeatStaleAfter time.Duration
	SourceAllowlist     []string
}

type successTeardownEvaluation struct {
	Deployment model.Deployment
	Rollout    DeploymentRolloutResponse
	Workers    []model.Worker
	Summary    string
}

type runArchivePayload struct {
	GeneratedAt time.Time                     `json:"generated_at"`
	Reason      string                        `json:"reason,omitempty"`
	Deployment  model.Deployment              `json:"deployment"`
	Rollout     *DeploymentRolloutResponse    `json:"rollout,omitempty"`
	Comparison  *DeploymentComparisonResponse `json:"comparison,omitempty"`
	Candidate   *dormancyCandidate            `json:"dormancy_candidate,omitempty"`
	Workers     []workerRunEvidence           `json:"workers,omitempty"`
}

type workerRunEvidence struct {
	Worker                   model.Worker                    `json:"worker"`
	Workload                 workload.Config                 `json:"workload"`
	RuntimeSnapshotStatus    string                          `json:"runtime_snapshot_status,omitempty"`
	ReportedRuntime          *reporting.RuntimePayload       `json:"reported_runtime,omitempty"`
	LatestVerification       *model.Verification             `json:"latest_verification,omitempty"`
	LatestFailure            *model.Verification             `json:"latest_failure,omitempty"`
	CurrentFailureStage      string                          `json:"current_failure_stage,omitempty"`
	CurrentFailureSignature  string                          `json:"current_failure_signature,omitempty"`
	CurrentProbableSubsystem string                          `json:"current_probable_subsystem,omitempty"`
	RecentVerifications      []model.Verification            `json:"recent_verifications,omitempty"`
	RecentEvents             []model.Event                   `json:"recent_events,omitempty"`
	FailureDebug             *reporting.FailureDebugSnapshot `json:"failure_debug,omitempty"`
}

func (m *Manager) RunSuccessTeardownLoop(ctx context.Context, policy SuccessTeardownPolicy) {
	policy = normalizeSuccessTeardownPolicy(policy)

	m.evaluateSuccessTeardown(ctx, policy)

	ticker := time.NewTicker(policy.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evaluateSuccessTeardown(ctx, policy)
		}
	}
}

func normalizeSuccessTeardownPolicy(policy SuccessTeardownPolicy) SuccessTeardownPolicy {
	if policy.CheckInterval <= 0 {
		policy.CheckInterval = 10 * time.Minute
	}
	if policy.Threshold <= 0 {
		policy.Threshold = 24 * time.Hour
	}
	if policy.HeartbeatStaleAfter <= 0 {
		policy.HeartbeatStaleAfter = 15 * time.Minute
	}
	if len(policy.SourceAllowlist) == 0 {
		policy.SourceAllowlist = []string{"pr-*"}
	}
	return policy
}

func (m *Manager) evaluateSuccessTeardown(ctx context.Context, policy SuccessTeardownPolicy) {
	sources, err := m.db.ListActiveWorkerSources()
	if err != nil {
		slog.Error("Failed to list active worker sources for success teardown", "error", err)
		return
	}

	now := time.Now().UTC()
	for _, source := range sources {
		if !sourceAllowedForSuccessTeardown(source, policy.SourceAllowlist) {
			continue
		}

		deployment, err := m.db.GetLatestDeployment(source)
		if err != nil {
			slog.Error("Failed to get latest deployment for success teardown", "source", source, "error", err)
			continue
		}
		if deployment == nil {
			continue
		}

		evaluation, ok, err := successTeardownCandidate(m.db, *deployment, policy, now)
		if err != nil {
			slog.Error("Failed to evaluate success teardown", "source", source, "deployment_id", deployment.ID, "error", err)
			continue
		}
		if !ok {
			continue
		}

		archive, created, err := m.archiveSuccessRun(evaluation, now)
		if err != nil {
			slog.Error("Failed to archive successful run; skipping teardown", "source", source, "deployment_id", deployment.ID, "error", err)
			continue
		}
		if created {
			_ = m.db.RecordEvent("", "run_success_archived", evaluation.Summary, fmt.Sprintf("archive_id=%d source=%s", archive.ID, source))
		}

		for _, worker := range evaluation.Workers {
			slog.Info("Destroying successful soak worker", "worker_id", worker.ID, "source", worker.Source, "deployment_id", deployment.ID)
			if err := m.DestroyWorker(ctx, worker.ID); err != nil {
				slog.Error("Failed to destroy successful soak worker", "worker_id", worker.ID, "error", err)
				continue
			}
			_ = m.db.RecordEvent(worker.ID, "run_success_worker_destroyed", "Destroyed worker after archived successful soak run", fmt.Sprintf("archive_id=%d", archive.ID))
		}
	}
}

func successTeardownCandidate(db *model.DB, deployment model.Deployment, policy SuccessTeardownPolicy, now time.Time) (successTeardownEvaluation, bool, error) {
	policy = normalizeSuccessTeardownPolicy(policy)
	source := firstNonEmpty(strings.TrimSpace(deployment.Source), "main")
	if !sourceAllowedForSuccessTeardown(source, policy.SourceAllowlist) {
		return successTeardownEvaluation{}, false, nil
	}
	if deployment.StartedAt.IsZero() || now.Sub(deployment.StartedAt.UTC()) < policy.Threshold {
		return successTeardownEvaluation{}, false, nil
	}

	rollout, err := buildDeploymentRollout(db, deployment)
	if err != nil {
		return successTeardownEvaluation{}, false, err
	}
	if rollout.TotalWorkers == 0 ||
		rollout.Status != "stable" ||
		rollout.UpdatedWorkers != rollout.TotalWorkers ||
		rollout.VerifiedSinceDeploy != rollout.TotalWorkers ||
		rollout.AwaitingVerification != 0 ||
		rollout.AttentionWorkers != 0 {
		return successTeardownEvaluation{}, false, nil
	}

	workers, err := db.ListWorkersForSource(source)
	if err != nil {
		return successTeardownEvaluation{}, false, err
	}
	for _, worker := range workers {
		ok, err := workerPassedSuccessWindow(db, worker, deployment, policy, now)
		if err != nil {
			return successTeardownEvaluation{}, false, err
		}
		if !ok {
			return successTeardownEvaluation{}, false, nil
		}
	}

	summary := fmt.Sprintf("%s completed a clean %s soak; archiving evidence and destroying worker compute, volumes, and replica prefix data.", sourceHumanLabel(source), policy.Threshold)
	return successTeardownEvaluation{
		Deployment: deployment,
		Rollout:    rollout,
		Workers:    workers,
		Summary:    summary,
	}, true, nil
}

func workerPassedSuccessWindow(db *model.DB, worker model.Worker, deployment model.Deployment, policy SuccessTeardownPolicy, now time.Time) (bool, error) {
	if worker.Status != model.WorkerRunning || !workerMatchesDeployment(worker, deployment) {
		return false, nil
	}
	if worker.LastHeartbeatAt == nil || now.Sub(worker.LastHeartbeatAt.UTC()) > policy.HeartbeatStaleAfter {
		return false, nil
	}
	runtimeStatus := reporting.SnapshotStatus(extractReportedRuntime(worker, nil))
	if runtimeStatus != reporting.RuntimeSnapshotStatusHealthy {
		return false, nil
	}

	verifications, err := db.ListVerifications(worker.ID, 512)
	if err != nil {
		return false, err
	}
	var latestPassAt time.Time
	for _, verification := range verifications {
		observedAt, ok := verificationObservedAt(verification)
		if !ok {
			continue
		}
		if observedAt.Before(deployment.StartedAt.UTC()) {
			break
		}
		if activeFailure(&verification) {
			return false, nil
		}
		if verification.Passed && verification.Status != "failed" && observedAt.After(latestPassAt) {
			latestPassAt = observedAt
		}
	}

	if latestPassAt.IsZero() {
		return false, nil
	}
	return !latestPassAt.Before(deployment.StartedAt.UTC().Add(policy.Threshold)), nil
}

func sourceAllowedForSuccessTeardown(source string, allowlist []string) bool {
	source = firstNonEmpty(strings.TrimSpace(source), "main")
	if len(allowlist) == 0 {
		allowlist = []string{"pr-*"}
	}
	for _, pattern := range allowlist {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if pattern == "*" || pattern == source {
			return true
		}
		if ok, err := path.Match(pattern, source); err == nil && ok {
			return true
		}
	}
	return false
}

func (m *Manager) archiveSuccessRun(evaluation successTeardownEvaluation, now time.Time) (*model.RunArchive, bool, error) {
	payload := runArchivePayload{
		GeneratedAt: now,
		Reason:      "success_teardown",
		Deployment:  evaluation.Deployment,
		Rollout:     &evaluation.Rollout,
		Workers:     make([]workerRunEvidence, 0, len(evaluation.Workers)),
	}
	if comparison, err := buildLatestCrossSourceDeploymentComparison(m.db, "main", evaluation.Deployment.Source); err == nil {
		payload.Comparison = comparison
	}

	for _, worker := range evaluation.Workers {
		evidence, err := m.workerRunEvidence(worker)
		if err != nil {
			return nil, false, err
		}
		payload.Workers = append(payload.Workers, evidence)
	}

	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return nil, false, fmt.Errorf("marshal success archive: %w", err)
	}

	archive := &model.RunArchive{
		DeploymentID:  evaluation.Deployment.ID,
		Source:        evaluation.Deployment.Source,
		ArchiveType:   runArchiveTypeSuccess,
		GitSHA:        evaluation.Deployment.GitSHA,
		LitestreamSHA: evaluation.Deployment.LitestreamSHA,
		ImageRef:      evaluation.Deployment.ImageRef,
		Status:        evaluation.Rollout.Status,
		Summary:       evaluation.Summary,
		Payload:       string(body),
		ArchivedAt:    now,
	}
	created, err := m.db.RecordRunArchive(archive)
	return archive, created, err
}

func (m *Manager) archiveFailureWorker(worker model.Worker, candidate dormancyCandidate, reason string, now time.Time) (*model.RunArchive, bool, error) {
	deployment := m.deploymentForWorker(worker)
	evidence, err := m.workerRunEvidence(worker)
	if err != nil {
		return nil, false, err
	}

	payload := runArchivePayload{
		GeneratedAt: now,
		Reason:      reason,
		Deployment:  deployment,
		Candidate:   &candidate,
		Workers:     []workerRunEvidence{evidence},
	}
	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return nil, false, fmt.Errorf("marshal failure archive: %w", err)
	}

	archive := &model.RunArchive{
		DeploymentID:  deployment.ID,
		Source:        worker.Source,
		WorkerID:      worker.ID,
		ArchiveType:   runArchiveTypeFailure,
		GitSHA:        firstNonEmpty(deployment.GitSHA, worker.GitSHA),
		LitestreamSHA: firstNonEmpty(deployment.LitestreamSHA, worker.LitestreamSHA),
		ImageRef:      deployment.ImageRef,
		Status:        string(worker.Status),
		Summary:       reason,
		Payload:       string(body),
		ArchivedAt:    now,
	}
	created, err := m.db.RecordRunArchive(archive)
	return archive, created, err
}

func (m *Manager) deploymentForWorker(worker model.Worker) model.Deployment {
	deployment, err := m.db.GetDeploymentByVersion(worker.Source, worker.GitSHA, worker.LitestreamSHA)
	if err == nil && deployment != nil {
		return *deployment
	}
	deployment, err = m.db.GetLatestDeployment(worker.Source)
	if err == nil && deployment != nil {
		return *deployment
	}
	return model.Deployment{
		Source:        worker.Source,
		GitSHA:        worker.GitSHA,
		LitestreamSHA: worker.LitestreamSHA,
		PRNumber:      worker.PRNumber,
		StartedAt:     worker.CreatedAt,
	}
}

func (m *Manager) workerRunEvidence(worker model.Worker) (workerRunEvidence, error) {
	verifications, err := m.db.ListVerifications(worker.ID, 20)
	if err != nil {
		return workerRunEvidence{}, err
	}
	events, err := m.db.ListWorkerEvents(worker.ID, 20)
	if err != nil {
		return workerRunEvidence{}, err
	}

	var latestVerification *model.Verification
	var latestFailure *model.Verification
	if len(verifications) > 0 {
		verification := verifications[0]
		latestVerification = &verification
	}
	for _, verification := range verifications {
		if activeFailure(&verification) {
			verificationCopy := verification
			latestFailure = &verificationCopy
			break
		}
	}

	evidence := workerRunEvidence{
		Worker:                worker,
		Workload:              resolveWorkerWorkload(worker),
		RuntimeSnapshotStatus: reporting.SnapshotStatus(extractReportedRuntime(worker, events)),
		ReportedRuntime:       extractReportedRuntime(worker, events),
		LatestVerification:    latestVerification,
		LatestFailure:         latestFailure,
		RecentVerifications:   verifications,
		RecentEvents:          events,
		FailureDebug:          latestFailureDebugSnapshot(events),
	}
	if latestFailure != nil {
		evidence.CurrentFailureStage = inferFailureStage(latestFailure)
		evidence.CurrentFailureSignature = inferFailureSignature(latestFailure)
		evidence.CurrentProbableSubsystem = inferProbableSubsystem(evidence.CurrentFailureStage, evidence.CurrentFailureSignature)
	}
	return evidence, nil
}
