package orchestrator

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

const rolloutAttentionGraceWindow = 45 * time.Minute

func (a *API) buildDeploymentRollout(deployment model.Deployment) (DeploymentRolloutResponse, error) {
	return buildDeploymentRollout(a.db, deployment)
}

func (a *API) buildRequestedDeploymentComparison(source, baseSource, headSource string) (*DeploymentComparisonResponse, error) {
	return buildRequestedDeploymentComparison(a.db, source, baseSource, headSource)
}

func (a *API) buildLatestDeploymentComparison(source string) (*DeploymentComparisonResponse, error) {
	return buildLatestDeploymentComparison(a.db, source)
}

func buildLatestDeploymentRollout(db *model.DB, source string) (*DeploymentRolloutResponse, error) {
	deployment, err := db.GetLatestDeployment(strings.TrimSpace(source))
	if err != nil {
		return nil, err
	}
	if deployment == nil {
		return nil, nil
	}

	rollout, err := buildDeploymentRollout(db, *deployment)
	if err != nil {
		return nil, err
	}
	return &rollout, nil
}

func buildLatestDeploymentComparison(db *model.DB, source string) (*DeploymentComparisonResponse, error) {
	deployments, err := db.ListDeployments(strings.TrimSpace(source), 2)
	if err != nil {
		return nil, err
	}
	if len(deployments) == 0 {
		return nil, nil
	}

	head := deployments[0]
	headScorecard, err := buildDeploymentScorecard(db, head, nil)
	if err != nil {
		return nil, err
	}

	comparison := &DeploymentComparisonResponse{
		BaseSource:     head.Source,
		HeadSource:     head.Source,
		ComparisonKind: "source_history",
		Head:           headScorecard,
		Verdict:        "no_baseline",
		Summary:        fmt.Sprintf("Latest rollout %s has no previous deployment to compare against yet.", deploymentVersionSummary(head)),
	}
	if len(deployments) < 2 {
		return comparison, nil
	}

	base := deployments[1]
	baseWindowEnd := head.StartedAt
	baseScorecard, err := buildDeploymentScorecard(db, base, &baseWindowEnd)
	if err != nil {
		return nil, err
	}
	comparison.Base = &baseScorecard
	finalizeDeploymentComparison(comparison)
	return comparison, nil
}

func buildRequestedDeploymentComparison(db *model.DB, source, baseSource, headSource string) (*DeploymentComparisonResponse, error) {
	source = strings.TrimSpace(source)
	baseSource = strings.TrimSpace(baseSource)
	headSource = strings.TrimSpace(headSource)

	if source == "" {
		source = "main"
	}
	if baseSource == "" && headSource == "" {
		return buildLatestDeploymentComparison(db, source)
	}
	if headSource == "" {
		headSource = source
	}
	if baseSource == "" {
		baseSource = "main"
	}
	if headSource == baseSource {
		return buildLatestDeploymentComparison(db, headSource)
	}
	return buildLatestCrossSourceDeploymentComparison(db, baseSource, headSource)
}

func buildLatestCrossSourceDeploymentComparison(db *model.DB, baseSource, headSource string) (*DeploymentComparisonResponse, error) {
	head, err := db.GetLatestDeployment(strings.TrimSpace(headSource))
	if err != nil {
		return nil, err
	}
	if head == nil {
		return nil, nil
	}

	headScorecard, err := buildDeploymentScorecard(db, *head, nil)
	if err != nil {
		return nil, err
	}

	comparison := &DeploymentComparisonResponse{
		BaseSource:     firstNonEmpty(strings.TrimSpace(baseSource), "main"),
		HeadSource:     firstNonEmpty(strings.TrimSpace(headSource), head.Source, "main"),
		ComparisonKind: "cross_source",
		Head:           headScorecard,
		Verdict:        "no_baseline",
	}

	base, err := db.GetLatestDeployment(comparison.BaseSource)
	if err != nil {
		return nil, err
	}
	if base == nil {
		comparison.Summary = fmt.Sprintf("Latest rollout %s has no current %s baseline to compare against yet.", deploymentVersionSummary(*head), comparison.BaseSource)
		return comparison, nil
	}

	baseScorecard, err := buildDeploymentScorecard(db, *base, nil)
	if err != nil {
		return nil, err
	}
	comparison.Base = &baseScorecard

	finalizeDeploymentComparison(comparison)
	return comparison, nil
}

func finalizeDeploymentComparison(comparison *DeploymentComparisonResponse) {
	if comparison == nil || comparison.Base == nil {
		return
	}

	baseScorecard := *comparison.Base
	headScorecard := comparison.Head

	baseByWorker := make(map[string]DeploymentWorkerOutcome, len(baseScorecard.Outcomes))
	for _, outcome := range baseScorecard.Outcomes {
		baseByWorker[comparisonOutcomeKey(outcome)] = outcome
	}
	headByWorker := make(map[string]DeploymentWorkerOutcome, len(headScorecard.Outcomes))
	for _, outcome := range headScorecard.Outcomes {
		headByWorker[comparisonOutcomeKey(outcome)] = outcome
	}

	for outcomeKey, headOutcome := range headByWorker {
		baseOutcome, ok := baseByWorker[outcomeKey]
		if !ok {
			continue
		}
		switch {
		case !baseOutcome.Passed && headOutcome.Passed:
			comparison.ImprovedWorkers = append(comparison.ImprovedWorkers, headOutcome)
		case baseOutcome.Passed && !headOutcome.Passed:
			comparison.RegressedWorkers = append(comparison.RegressedWorkers, headOutcome)
		}
	}
	sort.SliceStable(comparison.ImprovedWorkers, func(i, j int) bool { return comparison.ImprovedWorkers[i].Name < comparison.ImprovedWorkers[j].Name })
	sort.SliceStable(comparison.RegressedWorkers, func(i, j int) bool { return comparison.RegressedWorkers[i].Name < comparison.RegressedWorkers[j].Name })

	headFailures := make(map[string]DeploymentFailureCount, len(headScorecard.Failures))
	for _, failure := range headScorecard.Failures {
		headFailures[failure.Signature] = failure
	}
	baseFailures := make(map[string]DeploymentFailureCount, len(baseScorecard.Failures))
	for _, failure := range baseScorecard.Failures {
		baseFailures[failure.Signature] = failure
	}
	for signature, failure := range headFailures {
		if _, ok := baseFailures[signature]; !ok {
			comparison.NewFailures = append(comparison.NewFailures, failure)
		}
	}
	for signature, failure := range baseFailures {
		if _, ok := headFailures[signature]; !ok {
			comparison.ResolvedFailures = append(comparison.ResolvedFailures, failure)
		}
	}
	sort.SliceStable(comparison.NewFailures, func(i, j int) bool {
		if comparison.NewFailures[i].Count != comparison.NewFailures[j].Count {
			return comparison.NewFailures[i].Count > comparison.NewFailures[j].Count
		}
		return comparison.NewFailures[i].Signature < comparison.NewFailures[j].Signature
	})
	sort.SliceStable(comparison.ResolvedFailures, func(i, j int) bool {
		if comparison.ResolvedFailures[i].Count != comparison.ResolvedFailures[j].Count {
			return comparison.ResolvedFailures[i].Count > comparison.ResolvedFailures[j].Count
		}
		return comparison.ResolvedFailures[i].Signature < comparison.ResolvedFailures[j].Signature
	})

	comparison.PassDelta = headScorecard.PassedWorkers - baseScorecard.PassedWorkers
	comparison.FailDelta = headScorecard.FailedWorkers - baseScorecard.FailedWorkers
	comparison.AwaitingDelta = headScorecard.AwaitingWorkers - baseScorecard.AwaitingWorkers
	comparison.Verdict = inferDeploymentComparisonVerdict(*comparison)
	comparison.Summary = summarizeDeploymentComparison(*comparison)
}

func comparisonOutcomeKey(outcome DeploymentWorkerOutcome) string {
	return firstNonEmpty(strings.TrimSpace(outcome.Profile), strings.TrimSpace(outcome.WorkerID), strings.TrimSpace(outcome.Name))
}

func buildDeploymentScorecard(db *model.DB, deployment model.Deployment, windowEnd *time.Time) (DeploymentScorecard, error) {
	workers, err := deploymentScorecardWorkers(db, deployment)
	if err != nil {
		return DeploymentScorecard{}, err
	}

	scorecard := DeploymentScorecard{
		Deployment:   deployment,
		WindowStart:  deployment.StartedAt,
		WindowEnd:    windowEnd,
		TotalWorkers: len(workers),
		Outcomes:     make([]DeploymentWorkerOutcome, 0, len(workers)),
	}

	failureCounts := make(map[string]DeploymentFailureCount)
	for _, worker := range workers {
		verifications, err := db.ListVerifications(worker.ID, 256)
		if err != nil {
			return DeploymentScorecard{}, err
		}
		outcome, verified := scoreDeploymentWorker(worker, deployment, verifications, windowEnd)
		countDeploymentScorecardOutcome(&scorecard, failureCounts, outcome, verified)
	}

	finalizeDeploymentScorecard(&scorecard, failureCounts)
	return scorecard, nil
}

func deploymentScorecardSource(deployment model.Deployment) string {
	source := strings.TrimSpace(deployment.Source)
	if source == "" {
		return "main"
	}
	return source
}

func deploymentScorecardWorkers(db *model.DB, deployment model.Deployment) ([]model.Worker, error) {
	return db.ListWorkersForSource(deploymentScorecardSource(deployment))
}

func scoreDeploymentWorker(worker model.Worker, deployment model.Deployment, verifications []model.Verification, windowEnd *time.Time) (DeploymentWorkerOutcome, bool) {
	verification := latestVerificationInWindow(verifications, deployment.StartedAt, windowEnd)
	if verification == nil {
		return DeploymentWorkerOutcome{}, false
	}

	outcome := DeploymentWorkerOutcome{
		WorkerID: worker.ID,
		Name:     worker.Name,
		Profile:  worker.ProfileName,
		Passed:   verification.Succeeded(),
	}
	if observedAt, ok := verificationObservedAt(*verification); ok {
		outcome.VerifiedAt = &observedAt
	}
	if verification.Failed() {
		vf := classifyVerification(verification)
		outcome.FailureStage = vf.Stage
		outcome.FailureSignature = vf.Signature
		outcome.ProbableSubsystem = vf.probableSubsystem()
	}
	return outcome, true
}

func countDeploymentScorecardOutcome(scorecard *DeploymentScorecard, failureCounts map[string]DeploymentFailureCount, outcome DeploymentWorkerOutcome, verified bool) {
	if !verified {
		scorecard.AwaitingWorkers++
		return
	}

	scorecard.VerifiedWorkers++
	if outcome.Passed {
		scorecard.PassedWorkers++
	} else {
		scorecard.FailedWorkers++
		failure := failureCounts[outcome.FailureSignature]
		failure.Signature = outcome.FailureSignature
		failure.Stage = outcome.FailureStage
		failure.Count++
		failureCounts[outcome.FailureSignature] = failure
	}
	scorecard.Outcomes = append(scorecard.Outcomes, outcome)
}

func finalizeDeploymentScorecard(scorecard *DeploymentScorecard, failureCounts map[string]DeploymentFailureCount) {
	if scorecard.TotalWorkers > 0 {
		scorecard.PassRate = float64(scorecard.PassedWorkers) / float64(scorecard.TotalWorkers)
	}
	for _, failure := range failureCounts {
		scorecard.Failures = append(scorecard.Failures, failure)
	}
	sort.SliceStable(scorecard.Failures, func(i, j int) bool {
		if scorecard.Failures[i].Count != scorecard.Failures[j].Count {
			return scorecard.Failures[i].Count > scorecard.Failures[j].Count
		}
		return scorecard.Failures[i].Signature < scorecard.Failures[j].Signature
	})
	sort.SliceStable(scorecard.Outcomes, func(i, j int) bool { return scorecard.Outcomes[i].Name < scorecard.Outcomes[j].Name })
}

func buildDeploymentRollout(db *model.DB, deployment model.Deployment) (DeploymentRolloutResponse, error) {
	source := strings.TrimSpace(deployment.Source)
	if source == "" {
		source = "main"
	}

	workers, err := db.ListWorkersForSource(source)
	if err != nil {
		return DeploymentRolloutResponse{}, err
	}

	response := DeploymentRolloutResponse{
		Deployment: deployment,
		Workers:    make([]DeploymentWorkerProgress, 0, len(workers)),
	}

	for _, worker := range workers {
		runtimeStatus := reporting.SnapshotStatus(extractReportedRuntime(worker, nil))
		progress := DeploymentWorkerProgress{
			WorkerID:              worker.ID,
			Name:                  worker.Name,
			Status:                worker.Status,
			GitSHA:                worker.GitSHA,
			LitestreamSHA:         worker.LitestreamSHA,
			RuntimeSnapshotStatus: runtimeStatus,
			Updated:               workerMatchesDeployment(worker, deployment),
			LastHeartbeatAt:       worker.LastHeartbeatAt,
		}

		verifications, err := db.ListVerifications(worker.ID, 20)
		if err == nil && len(verifications) > 0 {
			if observedAt, ok := verificationObservedAt(verifications[0]); ok && !observedAt.Before(worker.CreatedAt.UTC()) {
				progress.LastVerificationAt = &observedAt
			}
			latestConclusive := latestVerificationInWindow(verifications, worker.CreatedAt.UTC(), nil)
			deploymentVerification := latestVerificationInWindow(verifications, deployment.StartedAt, nil)
			progress.VerifiedSinceDeploy = progress.Updated && workerNeedsPostDeployVerification(worker.Status) && !deployment.StartedAt.IsZero() && deploymentVerification != nil
			if activeFailure(latestConclusive) {
				vf := classifyVerification(latestConclusive)
				progress.CurrentFailureStage = vf.Stage
				progress.CurrentFailureSignature = vf.Signature
			}
		}

		response.TotalWorkers++
		if progress.Updated {
			response.UpdatedWorkers++
		} else {
			response.OutdatedWorkers++
		}

		switch worker.Status {
		case model.WorkerRunning:
			response.RunningWorkers++
		case model.WorkerDegraded:
			response.DegradedWorkers++
		case model.WorkerDormant:
			response.DormantWorkers++
		case model.WorkerProbing:
			response.ProbingWorkers++
		}
		if runtimeSnapshotNeedsAttention(runtimeStatus) {
			response.RuntimeUnhealthyWorkers++
		}
		if workerNeedsAttention(worker.Status, runtimeStatus) {
			response.AttentionWorkers++
		}
		if progress.Updated && workerNeedsPostDeployVerification(worker.Status) {
			if progress.VerifiedSinceDeploy {
				response.VerifiedSinceDeploy++
			} else {
				response.AwaitingVerification++
			}
		}

		response.Workers = append(response.Workers, progress)
	}

	sort.SliceStable(response.Workers, func(i, j int) bool {
		left := response.Workers[i]
		right := response.Workers[j]
		if left.Updated != right.Updated {
			return !left.Updated
		}
		if deploymentWorkerRank(left) != deploymentWorkerRank(right) {
			return deploymentWorkerRank(left) < deploymentWorkerRank(right)
		}
		return left.Name < right.Name
	})

	response.Status = inferDeploymentRolloutStatus(response)
	response.Summary = summarizeDeploymentRollout(response)
	applyDeploymentRolloutGuidance(&response, time.Now().UTC())
	return response, nil
}

func (a *API) observeLatestDeploymentState(source string) {
	if a.metrics != nil {
		a.metrics.observeLatestDeployment(a.db)
		a.metrics.observeLatestDeploymentComparison(a.db)
		a.metrics.observeSourceComparisons(a.db)
	}
	if a.alerts == nil {
		return
	}

	rollout, err := buildLatestDeploymentRollout(a.db, source)
	if err != nil || rollout == nil {
		return
	}
	a.alerts.NotifyDeploymentAttention(*rollout)
}

func workerMatchesDeployment(worker model.Worker, deployment model.Deployment) bool {
	if strings.TrimSpace(worker.GitSHA) != strings.TrimSpace(deployment.GitSHA) {
		return false
	}

	deploymentLitestreamSHA := strings.TrimSpace(deployment.LitestreamSHA)
	if deploymentLitestreamSHA == "" {
		return true
	}

	return strings.TrimSpace(worker.LitestreamSHA) == deploymentLitestreamSHA
}

func workerNeedsAttention(status model.WorkerStatus, runtimeStatus string) bool {
	return status != model.WorkerRunning || runtimeSnapshotNeedsAttention(runtimeStatus)
}

func runtimeSnapshotNeedsAttention(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), reporting.RuntimeSnapshotStatusUnhealthy)
}

func deploymentWorkerRank(progress DeploymentWorkerProgress) int {
	if progress.Status == model.WorkerRunning && runtimeSnapshotNeedsAttention(progress.RuntimeSnapshotStatus) {
		return workerRank(model.WorkerDegraded)
	}
	return workerRank(progress.Status)
}

func inferDeploymentRolloutStatus(rollout DeploymentRolloutResponse) string {
	switch {
	case rollout.TotalWorkers == 0:
		return "no_workers"
	case rollout.OutdatedWorkers > 0:
		return "rolling_out"
	case rollout.ProbingWorkers > 0:
		return "probing"
	case rollout.DormantWorkers > 0 || rollout.DegradedWorkers > 0 || rollout.RuntimeUnhealthyWorkers > 0:
		return "needs_attention"
	case rollout.AwaitingVerification > 0:
		return "settling"
	default:
		return "stable"
	}
}

func summarizeDeploymentRollout(rollout DeploymentRolloutResponse) string {
	subject := deploymentHumanLabel(rollout.Deployment)
	switch rollout.Status {
	case "no_workers":
		return fmt.Sprintf("The %s rollout is recorded, but no workers for that source are registered yet.", subject)
	case "rolling_out":
		return fmt.Sprintf("The %s rollout is still in progress. %d of %d workers are updated, %d still need the new release, and %d updated workers still need a fresh verification.", subject, rollout.UpdatedWorkers, rollout.TotalWorkers, rollout.OutdatedWorkers, rollout.AwaitingVerification)
	case "probing":
		return fmt.Sprintf("The %s rollout is still settling. All %d workers are on the new release, %d have verified since rollout, and %d still need a fresh verification.", subject, rollout.TotalWorkers, rollout.VerifiedSinceDeploy, rollout.AwaitingVerification)
	case "settling":
		return fmt.Sprintf("The %s rollout is waiting for verification. All %d workers are on the new release, %d have verified since rollout, and %d still need a fresh verification.", subject, rollout.TotalWorkers, rollout.VerifiedSinceDeploy, rollout.AwaitingVerification)
	case "needs_attention":
		return fmt.Sprintf("The %s rollout needs attention. All %d workers are on the new release, but %s: %s.", subject, rollout.TotalWorkers, workersNeedInvestigation(rollout.AttentionWorkers), rolloutAttentionSignalSummary(rollout))
	default:
		return fmt.Sprintf("The %s rollout is stable. All %d workers are on the new release and %d have verified since rollout.", subject, rollout.TotalWorkers, rollout.VerifiedSinceDeploy)
	}
}

func inferDeploymentComparisonVerdict(comparison DeploymentComparisonResponse) string {
	switch {
	case comparison.Base == nil:
		return "no_baseline"
	case comparison.Base.VerifiedWorkers == 0 || comparison.Head.VerifiedWorkers == 0:
		return "insufficient_data"
	case len(comparison.RegressedWorkers) > 0 && len(comparison.ImprovedWorkers) == 0:
		return "worse"
	case len(comparison.ImprovedWorkers) > 0 && len(comparison.RegressedWorkers) == 0 && comparison.FailDelta <= 0:
		return "better"
	case comparison.PassDelta > 0 && comparison.FailDelta < 0:
		return "better"
	case comparison.PassDelta < 0 || comparison.FailDelta > 0:
		return "worse"
	case len(comparison.ImprovedWorkers) > 0 || len(comparison.RegressedWorkers) > 0 || len(comparison.NewFailures) > 0 || len(comparison.ResolvedFailures) > 0:
		return "mixed"
	default:
		return "unchanged"
	}
}

func summarizeDeploymentComparison(comparison DeploymentComparisonResponse) string {
	includeSources := comparison.Base != nil && comparison.Base.Deployment.Source != comparison.Head.Deployment.Source
	headVersion := comparisonSubjectSummary(comparison.Head.Deployment, includeSources)
	if comparison.Base == nil {
		return fmt.Sprintf("The latest %s rollout has no previous deployment to compare against yet.", headVersion)
	}

	baseVersion := comparisonSubjectSummary(comparison.Base.Deployment, includeSources)
	switch comparison.Verdict {
	case "insufficient_data":
		return fmt.Sprintf("The %s rollout cannot be scored against the %s rollout yet because one of the deployment windows does not have enough post-rollout verification data.", headVersion, baseVersion)
	case "better":
		return fmt.Sprintf("The %s rollout looks better than the %s rollout so far: %d workers passed versus %d, and %d failed versus %d.", headVersion, baseVersion, comparison.Head.PassedWorkers, comparison.Base.PassedWorkers, comparison.Head.FailedWorkers, comparison.Base.FailedWorkers)
	case "worse":
		return fmt.Sprintf("The %s rollout looks worse than the %s rollout so far: %d workers passed versus %d, and %d failed versus %d.", headVersion, baseVersion, comparison.Head.PassedWorkers, comparison.Base.PassedWorkers, comparison.Head.FailedWorkers, comparison.Base.FailedWorkers)
	case "mixed":
		return fmt.Sprintf("The %s rollout is mixed versus the %s rollout: %d workers improved, %d regressed, and %d still await verification.", headVersion, baseVersion, len(comparison.ImprovedWorkers), len(comparison.RegressedWorkers), comparison.Head.AwaitingWorkers)
	default:
		return fmt.Sprintf("The %s rollout is unchanged versus the %s rollout so far: %d passed, %d failed, and %d still await verification.", headVersion, baseVersion, comparison.Head.PassedWorkers, comparison.Head.FailedWorkers, comparison.Head.AwaitingWorkers)
	}
}

func comparisonSubjectSummary(deployment model.Deployment, includeSource bool) string {
	if includeSource {
		return deploymentHumanLabel(deployment)
	}
	if deployment.PRNumber > 0 {
		return deploymentHumanLabel(deployment)
	}
	return sourceHumanLabel(deployment.Source)
}

func deploymentVersionSummary(deployment model.Deployment) string {
	soakSHA := shortVersionValue(deployment.GitSHA)
	litestreamSHA := shortVersionValue(deployment.LitestreamSHA)
	if litestreamSHA == "unknown" {
		return fmt.Sprintf("soak %s", soakSHA)
	}
	return fmt.Sprintf("soak %s / litestream %s", soakSHA, litestreamSHA)
}

func shortVersionValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "unknown"
	}
	return trimSHA(trimmed)
}

func deploymentHumanLabel(deployment model.Deployment) string {
	if deployment.PRNumber > 0 {
		return fmt.Sprintf("PR #%d", deployment.PRNumber)
	}
	return sourceHumanLabel(deployment.Source)
}

func sourceHumanLabel(source string) string {
	source = firstNonEmpty(strings.TrimSpace(source), "main")
	if prNumber := sourcePRNumber(source); prNumber > 0 {
		return fmt.Sprintf("PR #%d", prNumber)
	}
	switch source {
	case "main":
		return "main branch"
	default:
		return fmt.Sprintf("%s branch", source)
	}
}

func countNoun(count int, singular string) string {
	if count == 1 {
		return fmt.Sprintf("1 %s", singular)
	}
	return fmt.Sprintf("%d %ss", count, singular)
}

func workersNeedInvestigation(count int) string {
	if count == 1 {
		return "1 worker still needs investigation"
	}
	return fmt.Sprintf("%d workers still need investigation", count)
}

func rolloutAttentionSignalSummary(rollout DeploymentRolloutResponse) string {
	signals := make([]string, 0, 3)
	if rollout.DegradedWorkers > 0 {
		signals = append(signals, countNoun(rollout.DegradedWorkers, "degraded worker"))
	}
	if rollout.DormantWorkers > 0 {
		signals = append(signals, countNoun(rollout.DormantWorkers, "dormant worker"))
	}
	if rollout.RuntimeUnhealthyWorkers > 0 {
		signals = append(signals, countNoun(rollout.RuntimeUnhealthyWorkers, "runtime-unhealthy worker"))
	}
	if len(signals) == 0 {
		return "attention signal present"
	}
	return joinEnglish(signals)
}

func joinEnglish(values []string) string {
	switch len(values) {
	case 0:
		return ""
	case 1:
		return values[0]
	case 2:
		return values[0] + " and " + values[1]
	default:
		return strings.Join(values[:len(values)-1], ", ") + ", and " + values[len(values)-1]
	}
}

func applyDeploymentRolloutGuidance(rollout *DeploymentRolloutResponse, now time.Time) {
	if rollout == nil {
		return
	}

	rollout.GraceWindowExceeded = deploymentGraceWindowExceeded(*rollout, now)
	rollout.NextAction = inferDeploymentNextAction(*rollout)
	rollout.NextChecks = inferDeploymentNextChecks(*rollout)
}

func deploymentGraceWindowExceeded(rollout DeploymentRolloutResponse, now time.Time) bool {
	if rollout.Deployment.StartedAt.IsZero() {
		return false
	}

	switch rollout.Status {
	case "rolling_out", "probing", "needs_attention":
	default:
		return false
	}

	return now.Sub(rollout.Deployment.StartedAt) >= rolloutAttentionGraceWindow
}

func inferDeploymentNextAction(rollout DeploymentRolloutResponse) string {
	switch rollout.Status {
	case "no_workers":
		return "Create or reconcile the main fleet before trusting this release."
	case "rolling_out":
		if rollout.GraceWindowExceeded {
			return "Open outdated workers now. The rollout is still incomplete beyond the normal probe window."
		}
		return "Wait for the remaining workers to move to the new SHA, then confirm the full fleet is updated."
	case "probing":
		if rollout.GraceWindowExceeded {
			return "Open probing workers now. The rollout has not settled after a full verification cycle."
		}
		return "Wait for the next verification cycle to finish before deciding whether the release helped."
	case "settling":
		if rollout.GraceWindowExceeded {
			return "Open workers that still have no fresh verification. The rollout has not completed a verification cycle."
		}
		return "Wait for the first post-rollout verification cycle to finish before scoring this release."
	case "needs_attention":
		if rollout.GraceWindowExceeded {
			return "Treat this as a failed rollout until every attention worker is explained."
		}
		return "Watch the affected workers through one full verification cycle, then open any that still need attention."
	default:
		return "No immediate action. Spot-check one worker, then keep watching diagnosis for regressions."
	}
}

func inferDeploymentNextChecks(rollout DeploymentRolloutResponse) []string {
	checks := make([]string, 0, 3)
	switch rollout.Status {
	case "no_workers":
		return []string{
			"Confirm the main fleet reconciler is enabled.",
			"Create or re-register the expected main workers.",
			"Check /api/workers to verify the control plane can see the fleet.",
		}
	case "rolling_out":
		checks = append(checks,
			"Check that updated_workers reaches total_workers.",
			"Open any worker still on the previous SHA.",
		)
	case "probing":
		checks = append(checks,
			"Wait for the next verification cycle to finish.",
			"Check that awaiting_verification_workers falls as updated workers report fresh results.",
			"Open workers still marked probing after that cycle.",
		)
	case "settling":
		checks = append(checks,
			"Wait for the first post-rollout verification cycle to finish.",
			"Check that awaiting_verification_workers falls to zero.",
			"Open any worker that still has no fresh verification after the grace window.",
		)
	case "needs_attention":
		checks = append(checks,
			"Open degraded, dormant, and runtime-unhealthy workers first.",
			"Check that awaiting_verification_workers is not masking workers that simply have not rerun yet.",
			"Check failure stage, signature, and probable subsystem on those workers.",
		)
		if rollout.RuntimeUnhealthyWorkers > 0 {
			checks = append(checks, "For runtime-unhealthy workers, open the incident JSON or prompt bundle and treat the current runtime snapshot as stronger evidence than older passing checks.")
		}
	default:
		checks = append(checks,
			"Spot-check one worker incident bundle after the rollout.",
			"Keep diagnosis open for at least one more verification cycle.",
		)
	}

	if rollout.GraceWindowExceeded {
		checks = append(checks, fmt.Sprintf("The rollout has exceeded the %s grace window; escalate if it does not recover.", rolloutAttentionGraceWindow))
	}

	return checks
}

func verificationObservedAt(verification model.Verification) (time.Time, bool) {
	if verification.CompletedAt != nil && !verification.CompletedAt.IsZero() {
		return verification.CompletedAt.UTC(), true
	}
	if !verification.StartedAt.IsZero() {
		return verification.StartedAt.UTC(), true
	}
	return time.Time{}, false
}

func workerNeedsPostDeployVerification(status model.WorkerStatus) bool {
	switch status {
	case model.WorkerRunning, model.WorkerProbing, model.WorkerDegraded:
		return true
	default:
		return false
	}
}
