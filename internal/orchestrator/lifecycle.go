package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
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
	runArchiveTypeSuccess      = "success"
	runArchiveTypeFailure      = "failure"
	runArchiveTypeExpired      = "expired"
	fleetFullyDormantAlertType = "fleet_fully_dormant"
)

type SuccessTeardownPolicy struct {
	Threshold           time.Duration
	CheckInterval       time.Duration
	HeartbeatStaleAfter time.Duration
	SourceAllowlist     []string
}

type PRMaxAgeAction string

const (
	PRMaxAgeActionStop    PRMaxAgeAction = "stop"
	PRMaxAgeActionDestroy PRMaxAgeAction = "destroy"
)

type PRMaxAgePolicy struct {
	Threshold       time.Duration
	CheckInterval   time.Duration
	SourceAllowlist []string
	Action          PRMaxAgeAction
}

type FailedSourcePausePolicy struct {
	CheckInterval   time.Duration
	SourceAllowlist []string
	// MinAttentionWorkers pauses immediately when this many workers need
	// attention at once. A single struggling worker instead requires
	// SingleWorkerMinConsecutiveFailures actionable (non-environmental)
	// failed verifications since the deployment started — one provider blip
	// must not park a whole fleet (2026-07-18 false alarm).
	MinAttentionWorkers                int
	SingleWorkerMinConsecutiveFailures int
}

type DormantFleetAlertPolicy struct {
	Threshold     time.Duration
	CheckInterval time.Duration
}

type successTeardownEvaluation struct {
	Deployment model.Deployment
	Rollout    DeploymentRolloutResponse
	Workers    []model.Worker
	Summary    string
}

type prMaxAgeEvaluation struct {
	Deployment model.Deployment
	Rollout    DeploymentRolloutResponse
	Workers    []model.Worker
	Summary    string
	Action     PRMaxAgeAction
}

type failedSourcePauseEvaluation struct {
	Deployment model.Deployment
	Rollout    DeploymentRolloutResponse
	Workers    []model.Worker
	Summary    string
	Signature  string
}

type failedSourcePolicyVerdict int

const (
	failedSourcePolicyInconclusive failedSourcePolicyVerdict = iota
	failedSourcePolicyCleared
	failedSourcePolicyKnownBad
)

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

func (m *Manager) RunDormantFleetAlertLoop(ctx context.Context, policy DormantFleetAlertPolicy) {
	policy = normalizeDormantFleetAlertPolicy(policy)

	m.evaluateDormantFleetAlerts(ctx, policy)

	ticker := time.NewTicker(policy.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evaluateDormantFleetAlerts(ctx, policy)
		}
	}
}

func normalizeDormantFleetAlertPolicy(policy DormantFleetAlertPolicy) DormantFleetAlertPolicy {
	if policy.Threshold <= 0 {
		policy.Threshold = 2 * time.Hour
	}
	if policy.CheckInterval <= 0 {
		policy.CheckInterval = 10 * time.Minute
	}
	return policy
}

func (m *Manager) evaluateDormantFleetAlerts(ctx context.Context, policy DormantFleetAlertPolicy) {
	policy = normalizeDormantFleetAlertPolicy(policy)
	sources, err := m.db.ListActiveWorkerSources()
	if err != nil {
		slog.Error("Failed to list active worker sources for dormant fleet alerts", "error", err)
		return
	}

	activeSources, err := m.db.ListActiveAlertSources(fleetFullyDormantAlertType)
	if err != nil {
		slog.Error("Failed to list active dormant fleet alerts", "error", err)
		return
	}

	sourceSet := make(map[string]struct{}, len(sources)+len(activeSources))
	for _, source := range append(sources, activeSources...) {
		source = firstNonEmpty(strings.TrimSpace(source), "main")
		sourceSet[source] = struct{}{}
	}

	now := time.Now().UTC()
	for source := range sourceSet {
		unlockSource, err := m.lockSource(ctx, source)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Failed to lock source for dormant fleet alert", "source", source, "error", err)
			continue
		}
		err = m.evaluateDormantFleetAlertLocked(source, policy, now)
		unlockSource()
		if err != nil {
			slog.Error("Failed to evaluate dormant fleet alert", "source", source, "error", err)
		}
	}
}

func (m *Manager) evaluateDormantFleetAlertLocked(source string, policy DormantFleetAlertPolicy, now time.Time) error {
	workers, err := m.db.ListWorkersForSource(source)
	if err != nil {
		return fmt.Errorf("list source workers: %w", err)
	}

	epoch, fullyDormant := fullyDormantFleetEpoch(workers)
	active, err := m.db.GetActiveAlertCondition(fleetFullyDormantAlertType, source)
	if err != nil {
		return fmt.Errorf("get active condition: %w", err)
	}

	if active != nil && (!fullyDormant || active.ConditionStartedAt == nil || !active.ConditionStartedAt.Equal(epoch)) {
		if _, err := m.db.ResolveActiveAlertCondition(fleetFullyDormantAlertType, source, now); err != nil {
			return fmt.Errorf("resolve active condition: %w", err)
		}
		active = nil
	}
	if !fullyDormant || active != nil || now.Sub(epoch) < policy.Threshold {
		return nil
	}

	deployment, err := m.db.GetLatestDeployment(source)
	if err != nil {
		return fmt.Errorf("get latest deployment: %w", err)
	}
	if deploymentBuildInFlight(deployment, now) {
		return nil
	}

	var rollout *DeploymentRolloutResponse
	if deployment != nil {
		built, err := buildDeploymentRollout(m.db, *deployment)
		if err != nil {
			return fmt.Errorf("build deployment rollout: %w", err)
		}
		rollout = &built
	}

	dispatcher := m.alerts
	if dispatcher == nil {
		dispatcher = NewAlertDispatcher(m.db, "", "", "")
	}
	if err := dispatcher.NotifyFleetFullyDormant(source, epoch, rollout); err != nil {
		return fmt.Errorf("create dormant fleet alert: %w", err)
	}
	return nil
}

func deploymentBuildInFlight(deployment *model.Deployment, now time.Time) bool {
	if deployment == nil ||
		!strings.EqualFold(strings.TrimSpace(deployment.Status), "building") ||
		deployment.StartedAt.IsZero() {
		return false
	}
	age := now.Sub(deployment.StartedAt.UTC())
	return age >= 0 && age <= deploymentBuildTimeout
}

func fullyDormantFleetEpoch(workers []model.Worker) (time.Time, bool) {
	var latest time.Time
	activeWorkers := 0
	for _, worker := range workers {
		if worker.Status == model.WorkerStopped || worker.Status == model.WorkerFailed {
			continue
		}
		activeWorkers++
		if worker.Status != model.WorkerDormant || worker.DormantAt == nil || worker.DormantAt.IsZero() {
			return time.Time{}, false
		}
		dormantAt := worker.DormantAt.UTC()
		if dormantAt.After(latest) {
			latest = dormantAt
		}
	}
	return latest, activeWorkers > 0
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

func (m *Manager) RunPRMaxAgeLoop(ctx context.Context, policy PRMaxAgePolicy) {
	policy = normalizePRMaxAgePolicy(policy)

	m.evaluatePRMaxAge(ctx, policy)

	ticker := time.NewTicker(policy.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evaluatePRMaxAge(ctx, policy)
		}
	}
}

func (m *Manager) RunFailedSourcePauseLoop(ctx context.Context, policy FailedSourcePausePolicy) {
	policy = normalizeFailedSourcePausePolicy(policy)

	m.evaluateFailedSourcePause(ctx, policy)

	ticker := time.NewTicker(policy.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evaluateFailedSourcePause(ctx, policy)
		}
	}
}

func normalizeFailedSourcePausePolicy(policy FailedSourcePausePolicy) FailedSourcePausePolicy {
	if policy.CheckInterval <= 0 {
		policy.CheckInterval = 10 * time.Minute
	}
	if len(policy.SourceAllowlist) == 0 {
		policy.SourceAllowlist = []string{"main"}
	}
	if policy.MinAttentionWorkers <= 0 {
		policy.MinAttentionWorkers = 2
	}
	if policy.SingleWorkerMinConsecutiveFailures <= 0 {
		policy.SingleWorkerMinConsecutiveFailures = 2
	}
	return policy
}

func normalizePRMaxAgePolicy(policy PRMaxAgePolicy) PRMaxAgePolicy {
	if policy.CheckInterval <= 0 {
		policy.CheckInterval = 10 * time.Minute
	}
	if policy.Threshold <= 0 {
		policy.Threshold = 24 * time.Hour
	}
	if len(policy.SourceAllowlist) == 0 {
		policy.SourceAllowlist = []string{"pr-*"}
	}
	if policy.Action != PRMaxAgeActionDestroy {
		policy.Action = PRMaxAgeActionStop
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
		if !sourceAllowedForPolicy(source, policy.SourceAllowlist) {
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

func (m *Manager) evaluateFailedSourcePause(ctx context.Context, policy FailedSourcePausePolicy) {
	sources, err := m.db.ListActiveWorkerSources()
	if err != nil {
		slog.Error("Failed to list active worker sources for failed-source pause", "error", err)
		return
	}

	for _, source := range sources {
		unlockSource, err := m.lockSource(ctx, source)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Failed to lock source for failed-source pause", "source", source, "error", err)
			continue
		}
		err = m.evaluateFailedSourcePauseLocked(ctx, source, policy)
		unlockSource()
		if err != nil {
			slog.Error("Failed to evaluate failed-source pause", "source", source, "error", err)
		}
	}
}

func (m *Manager) evaluateFailedSourcePauseLocked(ctx context.Context, source string, policy FailedSourcePausePolicy) error {
	deployment, err := m.db.GetLatestDeployment(source)
	if err != nil {
		return fmt.Errorf("get latest deployment: %w", err)
	}
	if deployment == nil {
		return nil
	}

	evaluation, verdict, err := failedSourcePolicyEvaluation(m.db, *deployment, policy)
	if err != nil {
		return fmt.Errorf("evaluate deployment %d: %w", deployment.ID, err)
	}
	if verdict == failedSourcePolicyCleared {
		workers, err := m.db.ListDormantWorkersForResumeTrigger(source, "known_bad_source")
		if err != nil {
			return fmt.Errorf("list known-bad dormant workers: %w", err)
		}
		if len(workers) == 0 {
			return nil
		}

		if err := m.resumeDormantWorkers(
			ctx,
			workers,
			deployment.ImageRef,
			deployment.GitSHA,
			deployment.LitestreamSHA,
			"known_bad_source_policy_recheck",
		); err != nil {
			return fmt.Errorf("resume known-bad dormant workers: %w", err)
		}
		return nil
	}
	if verdict != failedSourcePolicyKnownBad {
		return nil
	}

	workers, err := failedSourcePauseTargets(m.db, *deployment)
	if err != nil {
		return err
	}
	if len(workers) == 0 {
		return nil
	}
	evaluation.Workers = workers

	archive, created, err := m.archiveFailedSourceRun(evaluation, time.Now().UTC())
	if err != nil {
		slog.Warn("Failed to archive failed-source pause evidence", "source", source, "deployment_id", deployment.ID, "error", err)
	} else if created {
		_ = m.db.RecordEvent("", "run_failed_source_archived", evaluation.Summary, fmt.Sprintf("archive_id=%d source=%s", archive.ID, source))
	}

	var pauseErrors []error
	for _, worker := range evaluation.Workers {
		slog.Info("Pausing known-bad source worker", "worker_id", worker.ID, "source", source, "deployment_id", deployment.ID)
		if err := m.DormantWorker(ctx, worker.ID, evaluation.Summary, evaluation.Signature, "known_bad_source"); err != nil {
			slog.Error("Failed to pause known-bad source worker", "worker_id", worker.ID, "error", err)
			pauseErrors = append(pauseErrors, fmt.Errorf("%s: %w", worker.ID, err))
		}
	}
	return errors.Join(pauseErrors...)
}

func (m *Manager) evaluatePRMaxAge(ctx context.Context, policy PRMaxAgePolicy) {
	sources, err := m.db.ListActiveWorkerSources()
	if err != nil {
		slog.Error("Failed to list active worker sources for pr max age", "error", err)
		return
	}

	now := time.Now().UTC()
	for _, source := range sources {
		if !sourceAllowedForPolicy(source, policy.SourceAllowlist) {
			continue
		}

		deployment, err := m.db.GetLatestDeployment(source)
		if err != nil {
			slog.Error("Failed to get latest deployment for pr max age", "source", source, "error", err)
			continue
		}
		if deployment == nil {
			continue
		}

		evaluation, ok, err := prMaxAgeCandidate(m.db, *deployment, policy, now)
		if err != nil {
			slog.Error("Failed to evaluate pr max age", "source", source, "deployment_id", deployment.ID, "error", err)
			continue
		}
		if !ok {
			continue
		}

		archive, created, err := m.archiveExpiredRun(evaluation, now)
		if err != nil {
			slog.Error("Failed to archive max-age soak run", "source", source, "deployment_id", deployment.ID, "error", err)
			continue
		}
		if created {
			_ = m.db.RecordEvent("", "run_expired_archived", evaluation.Summary, fmt.Sprintf("archive_id=%d source=%s", archive.ID, source))
		}

		for _, worker := range evaluation.Workers {
			if !workerActiveForPRMaxAge(worker, evaluation.Deployment) {
				continue
			}

			switch evaluation.Action {
			case PRMaxAgeActionDestroy:
				slog.Info("Destroying max-age soak worker", "worker_id", worker.ID, "source", worker.Source, "deployment_id", evaluation.Deployment.ID)
				if err := m.DestroyWorker(ctx, worker.ID); err != nil {
					slog.Error("Failed to destroy max-age soak worker", "worker_id", worker.ID, "error", err)
					continue
				}
				_ = m.db.RecordEvent(worker.ID, "run_expired_worker_destroyed", "Destroyed worker after PR max-age enforcement", fmt.Sprintf("archive_id=%d", archive.ID))
			default:
				slog.Info("Stopping max-age soak worker", "worker_id", worker.ID, "source", worker.Source, "deployment_id", evaluation.Deployment.ID)
				if err := m.StopWorker(ctx, worker.ID); err != nil {
					slog.Error("Failed to stop max-age soak worker", "worker_id", worker.ID, "error", err)
					continue
				}
				_ = m.db.RecordEvent(worker.ID, "run_expired_worker_stopped", "Stopped worker after PR max-age enforcement", fmt.Sprintf("archive_id=%d", archive.ID))
			}
		}
	}
}

func successTeardownCandidate(db *model.DB, deployment model.Deployment, policy SuccessTeardownPolicy, now time.Time) (successTeardownEvaluation, bool, error) {
	policy = normalizeSuccessTeardownPolicy(policy)
	source := firstNonEmpty(strings.TrimSpace(deployment.Source), "main")
	if !sourceAllowedForPolicy(source, policy.SourceAllowlist) {
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
	environmental := environmentalVerificationIDs(verifications, currentEnvironmentalFailurePolicy())
	for _, verification := range verifications {
		observedAt, ok := verificationObservedAt(verification)
		if !ok {
			continue
		}
		if observedAt.Before(deployment.StartedAt.UTC()) {
			break
		}
		if activeFailure(&verification) {
			if environmental[verification.ID] {
				continue
			}
			return false, nil
		}
		if verification.Succeeded() && observedAt.After(latestPassAt) {
			latestPassAt = observedAt
		}
	}

	if latestPassAt.IsZero() {
		return false, nil
	}
	return !latestPassAt.Before(deployment.StartedAt.UTC().Add(policy.Threshold)), nil
}

func failedSourcePauseCandidate(db *model.DB, deployment model.Deployment, policy FailedSourcePausePolicy) (failedSourcePauseEvaluation, bool, error) {
	evaluation, verdict, err := failedSourcePolicyEvaluation(db, deployment, policy)
	if err != nil || verdict != failedSourcePolicyKnownBad {
		return evaluation, false, err
	}

	workers, err := failedSourcePauseTargets(db, deployment)
	if err != nil {
		return failedSourcePauseEvaluation{}, false, err
	}
	if len(workers) == 0 {
		return failedSourcePauseEvaluation{}, false, nil
	}
	evaluation.Workers = workers
	return evaluation, true, nil
}

func failedSourcePolicyEvaluation(db *model.DB, deployment model.Deployment, policy FailedSourcePausePolicy) (failedSourcePauseEvaluation, failedSourcePolicyVerdict, error) {
	policy = normalizeFailedSourcePausePolicy(policy)
	source := firstNonEmpty(strings.TrimSpace(deployment.Source), "main")
	if !sourceAllowedForPolicy(source, policy.SourceAllowlist) ||
		!strings.EqualFold(strings.TrimSpace(deployment.Status), "ready") ||
		strings.TrimSpace(deployment.ImageRef) == "" {
		return failedSourcePauseEvaluation{}, failedSourcePolicyInconclusive, nil
	}

	rollout, err := buildDeploymentRollout(db, deployment)
	if err != nil {
		return failedSourcePauseEvaluation{}, failedSourcePolicyInconclusive, err
	}
	if rollout.TotalWorkers == 0 ||
		rollout.Status != "needs_attention" ||
		rollout.OutdatedWorkers > 0 ||
		rollout.ProbingWorkers > 0 ||
		rollout.AwaitingVerification > 0 ||
		rollout.UpdatedWorkers != rollout.TotalWorkers ||
		rollout.AttentionWorkers == 0 {
		return failedSourcePauseEvaluation{}, failedSourcePolicyInconclusive, nil
	}

	actionableAttention, err := countActionableAttentionWorkers(db, rollout, deployment)
	if err != nil {
		return failedSourcePauseEvaluation{}, failedSourcePolicyInconclusive, err
	}
	if actionableAttention < policy.MinAttentionWorkers {
		corroborated, err := singleWorkerFailureCorroborated(db, rollout, deployment, policy.SingleWorkerMinConsecutiveFailures)
		if err != nil {
			return failedSourcePauseEvaluation{}, failedSourcePolicyInconclusive, err
		}
		if !corroborated {
			return failedSourcePauseEvaluation{}, failedSourcePolicyCleared, nil
		}
	}

	signature := dominantRolloutFailureSignature(rollout)
	summary := fmt.Sprintf("%s is known-bad for soak %s / litestream %s; pausing active worker compute until the next deployment.", sourceHumanLabel(source), shortVersionValue(deployment.GitSHA), shortVersionValue(deployment.LitestreamSHA))
	return failedSourcePauseEvaluation{
		Deployment: deployment,
		Rollout:    rollout,
		Summary:    summary,
		Signature:  signature,
	}, failedSourcePolicyKnownBad, nil
}

func failedSourcePauseTargets(db *model.DB, deployment model.Deployment) ([]model.Worker, error) {
	source := firstNonEmpty(strings.TrimSpace(deployment.Source), "main")
	workers, err := db.ListWorkersForSource(source)
	if err != nil {
		return nil, fmt.Errorf("list source workers: %w", err)
	}

	activeWorkers := make([]model.Worker, 0, len(workers))
	for _, worker := range workers {
		if workerActiveForSourcePause(worker, deployment) {
			activeWorkers = append(activeWorkers, worker)
		}
	}
	return activeWorkers, nil
}

func prMaxAgeCandidate(db *model.DB, deployment model.Deployment, policy PRMaxAgePolicy, now time.Time) (prMaxAgeEvaluation, bool, error) {
	policy = normalizePRMaxAgePolicy(policy)
	source := firstNonEmpty(strings.TrimSpace(deployment.Source), "main")
	if !sourceAllowedForPolicy(source, policy.SourceAllowlist) {
		return prMaxAgeEvaluation{}, false, nil
	}
	if deployment.StartedAt.IsZero() || now.Sub(deployment.StartedAt.UTC()) < policy.Threshold {
		return prMaxAgeEvaluation{}, false, nil
	}

	rollout, err := buildDeploymentRollout(db, deployment)
	if err != nil {
		return prMaxAgeEvaluation{}, false, err
	}
	workers, err := db.ListWorkersForSource(source)
	if err != nil {
		return prMaxAgeEvaluation{}, false, err
	}

	activeWorkers := make([]model.Worker, 0, len(workers))
	for _, worker := range workers {
		if workerActiveForPRMaxAge(worker, deployment) {
			activeWorkers = append(activeWorkers, worker)
		}
	}
	if len(activeWorkers) == 0 {
		return prMaxAgeEvaluation{}, false, nil
	}

	summary := fmt.Sprintf("%s exceeded max soak age of %s; archiving current evidence and %s.", sourceHumanLabel(source), policy.Threshold, prMaxAgeActionSummary(policy.Action))
	return prMaxAgeEvaluation{
		Deployment: deployment,
		Rollout:    rollout,
		Workers:    activeWorkers,
		Summary:    summary,
		Action:     policy.Action,
	}, true, nil
}

func sourceAllowedForPolicy(source string, allowlist []string) bool {
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
	return m.archiveDeploymentRun(runArchiveTypeSuccess, "success_teardown", evaluation.Deployment, &evaluation.Rollout, evaluation.Workers, evaluation.Summary, now)
}

func (m *Manager) archiveExpiredRun(evaluation prMaxAgeEvaluation, now time.Time) (*model.RunArchive, bool, error) {
	return m.archiveDeploymentRun(runArchiveTypeExpired, "pr_max_age", evaluation.Deployment, &evaluation.Rollout, evaluation.Workers, evaluation.Summary, now)
}

func (m *Manager) archiveFailedSourceRun(evaluation failedSourcePauseEvaluation, now time.Time) (*model.RunArchive, bool, error) {
	return m.archiveDeploymentRun(runArchiveTypeFailure, "failed_source_pause", evaluation.Deployment, &evaluation.Rollout, evaluation.Workers, evaluation.Summary, now)
}

func (m *Manager) archiveDeploymentRun(archiveType, reason string, deployment model.Deployment, rollout *DeploymentRolloutResponse, workers []model.Worker, summary string, now time.Time) (*model.RunArchive, bool, error) {
	payload := runArchivePayload{
		GeneratedAt: now,
		Reason:      reason,
		Deployment:  deployment,
		Rollout:     rollout,
		Workers:     make([]workerRunEvidence, 0, len(workers)),
	}
	if comparison, err := buildLatestCrossSourceDeploymentComparison(m.db, "main", deployment.Source); err == nil {
		payload.Comparison = comparison
	}

	for _, worker := range workers {
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
		DeploymentID:  deployment.ID,
		Source:        deployment.Source,
		ArchiveType:   archiveType,
		GitSHA:        deployment.GitSHA,
		LitestreamSHA: deployment.LitestreamSHA,
		ImageRef:      deployment.ImageRef,
		Status:        archiveDeploymentStatus(deployment, rollout),
		Summary:       summary,
		Payload:       string(body),
		ArchivedAt:    now,
	}
	created, err := m.db.RecordRunArchive(archive)
	return archive, created, err
}

func archiveDeploymentStatus(deployment model.Deployment, rollout *DeploymentRolloutResponse) string {
	if rollout != nil && strings.TrimSpace(rollout.Status) != "" {
		return rollout.Status
	}
	return deployment.Status
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

func workerActiveForPRMaxAge(worker model.Worker, deployment model.Deployment) bool {
	if !workerMatchesDeployment(worker, deployment) {
		return false
	}
	switch worker.Status {
	case model.WorkerRunning, model.WorkerDegraded, model.WorkerProbing:
		return true
	default:
		return false
	}
}

func workerActiveForSourcePause(worker model.Worker, deployment model.Deployment) bool {
	if strings.TrimSpace(deployment.GitSHA) != "" && !workerMatchesDeployment(worker, deployment) {
		return false
	}
	switch worker.Status {
	case model.WorkerRunning, model.WorkerDegraded, model.WorkerProbing:
		return true
	default:
		return false
	}
}

func dominantRolloutFailureSignature(rollout DeploymentRolloutResponse) string {
	counts := make(map[string]int)
	for _, worker := range rollout.Workers {
		if strings.TrimSpace(worker.CurrentFailureSignature) == "" {
			continue
		}
		counts[worker.CurrentFailureSignature]++
	}

	signature := ""
	count := 0
	for candidate, candidateCount := range counts {
		if candidateCount > count || (candidateCount == count && (signature == "" || candidate < signature)) {
			signature = candidate
			count = candidateCount
		}
	}
	return firstNonEmpty(signature, "known_bad_rollout")
}

func prMaxAgeActionSummary(action PRMaxAgeAction) string {
	if action == PRMaxAgeActionDestroy {
		return "destroying worker compute, volumes, and replica prefix data"
	}
	return "stopping worker compute while preserving volumes and replica data for debugging"
}

const sourcePauseHistoryLimitCap = 1000

// pauseEvidenceHistory fetches a worker's verification history with the
// limit growing until the walk can reach a decision boundary (pass, the
// deployment start, or genuinely exhausted history) even when neutral
// aborted rows pad the window — otherwise interleaved aborts could hide
// older failures from corroboration entirely (Codex review finding).
func pauseEvidenceHistory(db *model.DB, workerID string, deployment model.Deployment, initialLimit int) ([]model.Verification, error) {
	limit := initialLimit
	for {
		verifications, err := db.ListVerifications(workerID, limit)
		if err != nil {
			return nil, err
		}
		if len(verifications) < limit || limit >= sourcePauseHistoryLimitCap {
			return verifications, nil
		}
		for _, verification := range verifications {
			if verificationStatusAborted(verification.Status) {
				continue
			}
			return verifications, nil
		}
		oldest := verifications[len(verifications)-1]
		if oldest.StartedAt.Before(deployment.StartedAt.UTC()) {
			return verifications, nil
		}
		limit *= 4
	}
}

// countActionableAttentionWorkers counts attention workers whose most recent
// non-aborted verification since the deployment started is an actionable
// (non-environmental) failure. Dormant or runtime-stale workers without such
// evidence do not corroborate a known-bad release (Codex review finding).
func countActionableAttentionWorkers(db *model.DB, rollout DeploymentRolloutResponse, deployment model.Deployment) (int, error) {
	policy := currentEnvironmentalFailurePolicy()
	count := 0
	for _, progress := range rollout.Workers {
		if !workerNeedsAttention(progress.Status, progress.RuntimeSnapshotStatus) {
			continue
		}
		verifications, err := pauseEvidenceHistory(db, progress.WorkerID, deployment, 20)
		if err != nil {
			return 0, err
		}
		environmental := environmentalVerificationIDs(verifications, policy)
		for _, verification := range verifications {
			if verification.StartedAt.Before(deployment.StartedAt.UTC()) {
				break
			}
			if verificationStatusAborted(verification.Status) {
				continue
			}
			if verification.Failed() && !environmental[verification.ID] {
				count++
			}
			break
		}
	}
	return count, nil
}

// singleWorkerFailureCorroborated decides whether a lone attention worker
// justifies pausing the whole source: it must have accumulated the required
// number of consecutive actionable failed verifications since the deployment
// started. Aborted checks are neutral (skipped); an environmental failure
// resets the count — provider weather interleaving a streak makes the
// attribution ambiguous, and the escalation guard already converts
// persistent weather into actionable failures on its own.
func singleWorkerFailureCorroborated(db *model.DB, rollout DeploymentRolloutResponse, deployment model.Deployment, minConsecutive int) (bool, error) {
	policy := currentEnvironmentalFailurePolicy()
	for _, progress := range rollout.Workers {
		if !workerNeedsAttention(progress.Status, progress.RuntimeSnapshotStatus) {
			continue
		}
		limit := minConsecutive*5 + 10
		for {
			verifications, err := db.ListVerifications(progress.WorkerID, limit)
			if err != nil {
				return false, err
			}
			corroborated, decided := walkConsecutiveActionable(verifications, deployment, policy, minConsecutive)
			if corroborated {
				return true, nil
			}
			// Grow the window only when the walk fell off the end of a full
			// page while skipping neutral aborts — otherwise interleaved
			// aborts could hide older failures from corroboration entirely.
			if decided || len(verifications) < limit || limit >= sourcePauseHistoryLimitCap {
				break
			}
			limit = min(limit*4, sourcePauseHistoryLimitCap)
		}
	}
	return false, nil
}

// walkConsecutiveActionable walks newest-first, skipping aborted checks,
// counting consecutive actionable failures since the deployment started.
// decided is false only when the walk exhausted the slice while the streak
// was still open (more history could change the answer).
func walkConsecutiveActionable(verifications []model.Verification, deployment model.Deployment, policy EnvironmentalFailurePolicy, minConsecutive int) (corroborated, decided bool) {
	environmental := environmentalVerificationIDs(verifications, policy)
	consecutive := 0
	for _, verification := range verifications {
		if verification.StartedAt.Before(deployment.StartedAt.UTC()) {
			return false, true
		}
		if verificationStatusAborted(verification.Status) {
			continue
		}
		if !verification.Failed() || environmental[verification.ID] {
			return false, true
		}
		consecutive++
		if consecutive >= minConsecutive {
			return true, true
		}
	}
	return false, false
}
