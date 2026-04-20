package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/workload"
)

type API struct {
	db       *model.DB
	fly      *flyapi.Client
	metrics  *controlMetrics
	alerts   *AlertDispatcher
	manager  *Manager
	deployer *Deployer
}

type WorkerDetailResponse struct {
	Worker                model.Worker              `json:"worker"`
	Workload              workload.Config           `json:"workload"`
	LatestFailure         *model.Verification       `json:"latest_failure,omitempty"`
	LatestPlatformEvent   *model.Event              `json:"latest_platform_event,omitempty"`
	FailureStage          string                    `json:"failure_stage,omitempty"`
	FailureSignature      string                    `json:"failure_signature,omitempty"`
	ProbableSubsystem     string                    `json:"probable_subsystem,omitempty"`
	RuntimeSnapshotStatus string                    `json:"runtime_snapshot_status,omitempty"`
	ReportedRuntime       *reporting.RuntimePayload `json:"reported_runtime,omitempty"`
	TriageCommands        []string                  `json:"triage_commands,omitempty"`
	RecentVerifications   []model.Verification      `json:"recent_verifications"`
	RecentEvents          []model.Event             `json:"recent_events"`
	Machine               *flyapi.Machine           `json:"machine,omitempty"`
	MachineError          string                    `json:"machine_error,omitempty"`
}

type FailureResponse struct {
	Worker            *model.Worker      `json:"worker,omitempty"`
	Verification      model.Verification `json:"verification"`
	FailureStage      string             `json:"failure_stage,omitempty"`
	FailureSignature  string             `json:"failure_signature,omitempty"`
	ProbableSubsystem string             `json:"probable_subsystem,omitempty"`
	TriageCommands    []string           `json:"triage_commands,omitempty"`
}

type WorkerSummaryResponse struct {
	Worker                   model.Worker        `json:"worker"`
	Workload                 workload.Config     `json:"workload"`
	RuntimeSnapshotStatus    string              `json:"runtime_snapshot_status,omitempty"`
	LastVerification         *model.Verification `json:"last_verification,omitempty"`
	LatestFailure            *model.Verification `json:"latest_failure,omitempty"`
	LatestPlatformEvent      *model.Event        `json:"latest_platform_event,omitempty"`
	CurrentFailureStage      string              `json:"current_failure_stage,omitempty"`
	CurrentFailureSignature  string              `json:"current_failure_signature,omitempty"`
	CurrentProbableSubsystem string              `json:"current_probable_subsystem,omitempty"`
	LatestFailureStage       string              `json:"latest_failure_stage,omitempty"`
	LatestFailureSignature   string              `json:"latest_failure_signature,omitempty"`
	LatestProbableSubsystem  string              `json:"latest_probable_subsystem,omitempty"`
	TriageCommands           []string            `json:"triage_commands,omitempty"`
}

type DeploymentWorkerProgress struct {
	WorkerID                string             `json:"worker_id"`
	Name                    string             `json:"name"`
	Status                  model.WorkerStatus `json:"status"`
	GitSHA                  string             `json:"git_sha"`
	LitestreamSHA           string             `json:"litestream_sha,omitempty"`
	RuntimeSnapshotStatus   string             `json:"runtime_snapshot_status,omitempty"`
	Updated                 bool               `json:"updated"`
	VerifiedSinceDeploy     bool               `json:"verified_since_deploy"`
	LastHeartbeatAt         *time.Time         `json:"last_heartbeat_at,omitempty"`
	LastVerificationAt      *time.Time         `json:"last_verification_at,omitempty"`
	CurrentFailureStage     string             `json:"current_failure_stage,omitempty"`
	CurrentFailureSignature string             `json:"current_failure_signature,omitempty"`
}

type DeploymentRolloutResponse struct {
	Deployment           model.Deployment           `json:"deployment"`
	Status               string                     `json:"status"`
	Summary              string                     `json:"summary"`
	NextAction           string                     `json:"next_action,omitempty"`
	NextChecks           []string                   `json:"next_checks,omitempty"`
	GraceWindowExceeded  bool                       `json:"grace_window_exceeded,omitempty"`
	TotalWorkers         int                        `json:"total_workers"`
	UpdatedWorkers       int                        `json:"updated_workers"`
	OutdatedWorkers      int                        `json:"outdated_workers"`
	RunningWorkers       int                        `json:"running_workers"`
	DegradedWorkers      int                        `json:"degraded_workers"`
	DormantWorkers       int                        `json:"dormant_workers"`
	ProbingWorkers       int                        `json:"probing_workers"`
	AttentionWorkers     int                        `json:"attention_workers"`
	VerifiedSinceDeploy  int                        `json:"verified_since_deploy_workers"`
	AwaitingVerification int                        `json:"awaiting_verification_workers"`
	Workers              []DeploymentWorkerProgress `json:"workers,omitempty"`
}

type DeploymentFailureCount struct {
	Signature string `json:"signature"`
	Stage     string `json:"stage,omitempty"`
	Count     int    `json:"count"`
}

type DeploymentWorkerOutcome struct {
	WorkerID          string     `json:"worker_id"`
	Name              string     `json:"name"`
	Profile           string     `json:"profile"`
	Passed            bool       `json:"passed"`
	VerifiedAt        *time.Time `json:"verified_at,omitempty"`
	FailureStage      string     `json:"failure_stage,omitempty"`
	FailureSignature  string     `json:"failure_signature,omitempty"`
	ProbableSubsystem string     `json:"probable_subsystem,omitempty"`
}

type DeploymentScorecard struct {
	Deployment      model.Deployment          `json:"deployment"`
	WindowStart     time.Time                 `json:"window_start"`
	WindowEnd       *time.Time                `json:"window_end,omitempty"`
	TotalWorkers    int                       `json:"total_workers"`
	VerifiedWorkers int                       `json:"verified_workers"`
	PassedWorkers   int                       `json:"passed_workers"`
	FailedWorkers   int                       `json:"failed_workers"`
	AwaitingWorkers int                       `json:"awaiting_workers"`
	PassRate        float64                   `json:"pass_rate"`
	Failures        []DeploymentFailureCount  `json:"failures,omitempty"`
	Outcomes        []DeploymentWorkerOutcome `json:"outcomes,omitempty"`
}

type DeploymentComparisonResponse struct {
	BaseSource       string                    `json:"base_source"`
	HeadSource       string                    `json:"head_source"`
	ComparisonKind   string                    `json:"comparison_kind,omitempty"`
	Base             *DeploymentScorecard      `json:"base,omitempty"`
	Head             DeploymentScorecard       `json:"head"`
	Verdict          string                    `json:"verdict"`
	Summary          string                    `json:"summary"`
	PassDelta        int                       `json:"pass_delta"`
	FailDelta        int                       `json:"fail_delta"`
	AwaitingDelta    int                       `json:"awaiting_delta"`
	ImprovedWorkers  []DeploymentWorkerOutcome `json:"improved_workers,omitempty"`
	RegressedWorkers []DeploymentWorkerOutcome `json:"regressed_workers,omitempty"`
	NewFailures      []DeploymentFailureCount  `json:"new_failures,omitempty"`
	ResolvedFailures []DeploymentFailureCount  `json:"resolved_failures,omitempty"`
}

const rolloutAttentionGraceWindow = 45 * time.Minute

type IncidentBundle struct {
	GeneratedAt           time.Time                 `json:"generated_at"`
	Worker                model.Worker              `json:"worker"`
	Workload              workload.Config           `json:"workload"`
	LatestFailure         *model.Verification       `json:"latest_failure,omitempty"`
	LatestPlatformEvent   *model.Event              `json:"latest_platform_event,omitempty"`
	ActiveFailure         bool                      `json:"active_failure"`
	FailureStage          string                    `json:"failure_stage,omitempty"`
	FailureSignature      string                    `json:"failure_signature,omitempty"`
	ProbableSubsystem     string                    `json:"probable_subsystem,omitempty"`
	RuntimeSnapshotStatus string                    `json:"runtime_snapshot_status,omitempty"`
	ReportedRuntime       *reporting.RuntimePayload `json:"reported_runtime,omitempty"`
	Guide                 incidentGuide             `json:"guide"`
	Diagnosis             diagnosisSnapshot         `json:"diagnosis"`
	RelatedClusters       []diagnosisCluster        `json:"related_clusters,omitempty"`
	PromptModes           []promptModeInfo          `json:"prompt_modes,omitempty"`
	RecentVerifications   []model.Verification      `json:"recent_verifications"`
	RecentEvents          []model.Event             `json:"recent_events"`
	Machine               *flyapi.Machine           `json:"machine,omitempty"`
	MachineError          string                    `json:"machine_error,omitempty"`
	TriageCommands        []string                  `json:"triage_commands,omitempty"`
	Prompt                string                    `json:"prompt"`
}

func NewAPI(db *model.DB, fly *flyapi.Client, metrics *controlMetrics, alerts *AlertDispatcher, manager *Manager, deployer *Deployer) *API {
	if metrics == nil {
		metrics = NewControlMetrics(db)
	}
	return &API{
		db:       db,
		fly:      fly,
		metrics:  metrics,
		alerts:   alerts,
		manager:  manager,
		deployer: deployer,
	}
}

func (a *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /favicon.svg", a.handleFavicon)
	mux.HandleFunc("GET /favicon.ico", a.handleFavicon)
	mux.HandleFunc("GET /", a.handleHome)
	mux.HandleFunc("GET /ui", a.handleHome)
	mux.HandleFunc("GET /ui/partials/home", a.handleHomePartial)
	mux.HandleFunc("GET /ui/help", a.handleHelpPage)
	mux.HandleFunc("GET /ui/workers/{id}", a.handleWorkerPage)
	mux.HandleFunc("GET /api/workers", a.handleListWorkers)
	mux.HandleFunc("GET /api/worker-summaries", a.handleListWorkerSummaries)
	mux.HandleFunc("GET /api/diagnosis", a.handleGetDiagnosis)
	mux.HandleFunc("GET /api/deployments", a.handleListDeployments)
	mux.HandleFunc("GET /api/deployments/latest", a.handleGetLatestDeployment)
	mux.HandleFunc("GET /api/deployments/compare/latest", a.handleGetLatestDeploymentComparison)
	mux.HandleFunc("GET /api/deployments/{sha}", a.handleGetDeployment)
	mux.HandleFunc("POST /api/admin/deployments/ready", a.handleDeploymentReady)
	mux.HandleFunc("POST /api/admin/resume-dormant", a.handleResumeDormantWorkers)
	mux.HandleFunc("GET /api/workers/{id}", a.handleGetWorker)
	mux.HandleFunc("GET /api/workers/{id}/incident", a.handleGetIncident)
	mux.HandleFunc("GET /api/workers/{id}/prompt", a.handleGetPrompt)
	mux.HandleFunc("POST /api/workers/{id}/heartbeat", a.handleHeartbeat)
	mux.HandleFunc("POST /api/workers/{id}/verifications", a.handleVerification)
	mux.HandleFunc("GET /api/events", a.handleListEvents)
	mux.HandleFunc("GET /api/failures", a.handleListFailures)
	mux.HandleFunc("GET /api/alerts", a.handleListAlerts)
}

func (a *API) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := a.db.ListWorkersFiltered(r.URL.Query().Get("status"), strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeAPIJSON(w, workers)
}

func (a *API) handleListWorkerSummaries(w http.ResponseWriter, r *http.Request) {
	summaries, err := a.listWorkerSummaries(r.URL.Query().Get("status"), strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeAPIJSON(w, summaries)
}

func (a *API) handleListDeployments(w http.ResponseWriter, r *http.Request) {
	deployments, err := a.db.ListDeployments(strings.TrimSpace(r.URL.Query().Get("source")), readLimit(r, 10))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rollouts := make([]DeploymentRolloutResponse, 0, len(deployments))
	for _, deployment := range deployments {
		rollout, err := a.buildDeploymentRollout(deployment)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rollouts = append(rollouts, rollout)
	}

	writeAPIJSON(w, rollouts)
}

func (a *API) handleGetLatestDeployment(w http.ResponseWriter, r *http.Request) {
	deployment, err := a.db.GetLatestDeployment(strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if deployment == nil {
		http.Error(w, "deployment not found", http.StatusNotFound)
		return
	}

	rollout, err := a.buildDeploymentRollout(*deployment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeAPIJSON(w, rollout)
}

func (a *API) handleGetLatestDeploymentComparison(w http.ResponseWriter, r *http.Request) {
	comparison, err := a.buildRequestedDeploymentComparison(
		strings.TrimSpace(r.URL.Query().Get("source")),
		strings.TrimSpace(r.URL.Query().Get("base_source")),
		strings.TrimSpace(r.URL.Query().Get("head_source")),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if comparison == nil {
		http.Error(w, "deployment not found", http.StatusNotFound)
		return
	}

	writeAPIJSON(w, comparison)
}

func (a *API) handleGetDeployment(w http.ResponseWriter, r *http.Request) {
	deployment, err := a.db.GetDeploymentBySHA(strings.TrimSpace(r.PathValue("sha")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	rollout, err := a.buildDeploymentRollout(*deployment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeAPIJSON(w, rollout)
}

func (a *API) listWorkerSummaries(status, source string) ([]WorkerSummaryResponse, error) {
	workers, err := a.db.ListWorkersFiltered(status, source)
	if err != nil {
		return nil, err
	}

	summaries := make([]WorkerSummaryResponse, 0, len(workers))
	for _, worker := range workers {
		summary, err := a.buildWorkerSummary(worker)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}

	return summaries, nil
}

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
	source := strings.TrimSpace(deployment.Source)
	if source == "" {
		source = "main"
	}

	workers, err := db.ListWorkersForSource(source)
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
		verification := latestVerificationInWindow(verifications, deployment.StartedAt, windowEnd)
		if verification == nil {
			scorecard.AwaitingWorkers++
			continue
		}

		outcome := DeploymentWorkerOutcome{
			WorkerID: worker.ID,
			Name:     worker.Name,
			Profile:  worker.ProfileName,
			Passed:   verification.Passed,
		}
		if observedAt, ok := verificationObservedAt(*verification); ok {
			outcome.VerifiedAt = &observedAt
		}
		scorecard.VerifiedWorkers++
		if verification.Passed {
			scorecard.PassedWorkers++
		} else {
			scorecard.FailedWorkers++
			outcome.FailureStage = inferFailureStage(verification)
			outcome.FailureSignature = inferFailureSignature(verification)
			outcome.ProbableSubsystem = inferProbableSubsystem(outcome.FailureStage, outcome.FailureSignature)
			failure := failureCounts[outcome.FailureSignature]
			failure.Signature = outcome.FailureSignature
			failure.Stage = outcome.FailureStage
			failure.Count++
			failureCounts[outcome.FailureSignature] = failure
		}
		scorecard.Outcomes = append(scorecard.Outcomes, outcome)
	}

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

	return scorecard, nil
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

		verifications, err := db.ListVerifications(worker.ID, 1)
		if err == nil && len(verifications) > 0 {
			if observedAt, ok := verificationObservedAt(verifications[0]); ok && !observedAt.Before(worker.CreatedAt.UTC()) {
				progress.LastVerificationAt = &observedAt
				progress.VerifiedSinceDeploy = progress.Updated && workerNeedsPostDeployVerification(worker.Status) && !deployment.StartedAt.IsZero() && !observedAt.Before(deployment.StartedAt)
				if activeFailure(&verifications[0]) {
					progress.CurrentFailureStage = inferFailureStage(&verifications[0])
					progress.CurrentFailureSignature = inferFailureSignature(&verifications[0])
				}
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
		if worker.Status != model.WorkerRunning {
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
		if workerRank(left.Status) != workerRank(right.Status) {
			return workerRank(left.Status) < workerRank(right.Status)
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

func (a *API) handleListEvents(w http.ResponseWriter, r *http.Request) {
	limit := readLimit(r, 50)
	workerID := strings.TrimSpace(r.URL.Query().Get("worker_id"))

	var (
		events []model.Event
		err    error
	)
	if workerID != "" {
		events, err = a.db.ListWorkerEvents(workerID, limit)
	} else {
		events, err = a.db.ListEvents(limit)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !readBoolQuery(r, "raw") {
		events = coalesceEventFeed(events)
	}
	writeAPIJSON(w, events)
}

func (a *API) handleListFailures(w http.ResponseWriter, r *http.Request) {
	verifications, err := a.db.ListRecentFailedVerifications(readLimit(r, 20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	failures := make([]FailureResponse, 0, len(verifications))
	for _, verification := range verifications {
		failure := FailureResponse{
			Verification:      verification,
			FailureStage:      inferFailureStage(&verification),
			FailureSignature:  inferFailureSignature(&verification),
			ProbableSubsystem: inferProbableSubsystem(inferFailureStage(&verification), inferFailureSignature(&verification)),
		}
		worker, err := a.db.GetWorker(verification.WorkerID)
		if err == nil {
			failure.Worker = worker
			failure.TriageCommands = buildTriageCommands(*worker, false)
		}
		failures = append(failures, failure)
	}

	writeAPIJSON(w, failures)
}

func (a *API) handleListAlerts(w http.ResponseWriter, r *http.Request) {
	alerts, err := a.db.ListAlerts(readLimit(r, 20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type alertResponse struct {
		Worker         *model.Worker       `json:"worker,omitempty"`
		Alert          model.AlertDelivery `json:"alert"`
		TriageCommands []string            `json:"triage_commands,omitempty"`
	}

	response := make([]alertResponse, 0, len(alerts))
	for _, alert := range alerts {
		item := alertResponse{Alert: alert}
		if alert.WorkerID != "" {
			worker, err := a.db.GetWorker(alert.WorkerID)
			if err == nil {
				item.Worker = worker
				item.TriageCommands = buildTriageCommands(*worker, worker.FlyMachineID != "")
			}
		} else if alert.AlertType == "deployment_attention" {
			item.TriageCommands = buildDeploymentTriageCommands()
		}
		response = append(response, item)
	}

	writeAPIJSON(w, response)
}

func (a *API) handleGetWorker(w http.ResponseWriter, r *http.Request) {
	response, status, err := a.workerDetail(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	writeAPIJSON(w, response)
}

func (a *API) buildWorkerSummary(worker model.Worker) (WorkerSummaryResponse, error) {
	summary := WorkerSummaryResponse{
		Worker:                worker,
		Workload:              resolveWorkerWorkload(worker),
		RuntimeSnapshotStatus: reporting.SnapshotStatus(extractReportedRuntime(worker, nil)),
		TriageCommands:        buildTriageCommands(worker, worker.FlyMachineID != ""),
	}

	verifications, err := a.db.ListVerifications(worker.ID, 1)
	if err != nil {
		return summary, err
	}
	if len(verifications) > 0 {
		verification := verifications[0]
		if observedAt, ok := verificationObservedAt(verification); ok && !observedAt.Before(worker.CreatedAt.UTC()) {
			summary.LastVerification = &verification
			if activeFailure(&verification) {
				summary.CurrentFailureStage = inferFailureStage(&verification)
				summary.CurrentFailureSignature = inferFailureSignature(&verification)
				summary.CurrentProbableSubsystem = inferProbableSubsystem(summary.CurrentFailureStage, summary.CurrentFailureSignature)
			}
		}
	}

	latestFailure, err := a.db.GetLatestFailedVerification(worker.ID)
	if err != nil {
		return summary, err
	}
	if latestFailure != nil {
		summary.LatestFailure = latestFailure
		summary.LatestFailureStage = inferFailureStage(latestFailure)
		summary.LatestFailureSignature = inferFailureSignature(latestFailure)
		summary.LatestProbableSubsystem = inferProbableSubsystem(summary.LatestFailureStage, summary.LatestFailureSignature)
	}

	events, err := a.db.ListWorkerEvents(worker.ID, 10)
	if err == nil {
		summary.LatestPlatformEvent = latestPlatformEvent(coalesceEventFeed(events))
	}

	return summary, nil
}

func (a *API) handleGetIncident(w http.ResponseWriter, r *http.Request) {
	bundle, status, err := a.buildIncidentBundle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	writeAPIJSON(w, bundle)
}

func (a *API) handleGetPrompt(w http.ResponseWriter, r *http.Request) {
	bundle, status, err := a.buildIncidentBundle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	mode := parsePromptMode(r.URL.Query().Get("mode"), bundle.Guide.RecommendedPromptMode)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(buildPrompt(bundle, mode)))
}

func (a *API) handleGetDiagnosis(w http.ResponseWriter, r *http.Request) {
	summaries, err := a.listWorkerSummaries(r.URL.Query().Get("status"), strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeAPIJSON(w, map[string]any{
		"generated_at": time.Now().UTC(),
		"diagnosis":    buildDiagnosisSnapshot(summaries),
		"coverage":     buildCoverageSnapshot(summaries),
	})
}

type deploymentReadyRequest struct {
	SHA           string `json:"sha"`
	LitestreamSHA string `json:"litestream_sha"`
	Source        string `json:"source"`
	ImageRef      string `json:"image_ref"`
	Trigger       string `json:"trigger"`
}

func (a *API) handleDeploymentReady(w http.ResponseWriter, r *http.Request) {
	if a.deployer == nil {
		http.Error(w, "deployer unavailable", http.StatusInternalServerError)
		return
	}

	request, err := readDeploymentReadyRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(request.SHA) == "" {
		http.Error(w, "sha is required", http.StatusBadRequest)
		return
	}

	source := strings.TrimSpace(request.Source)
	if source == "" {
		source = "main"
	}
	trigger := strings.TrimSpace(request.Trigger)
	if trigger == "" {
		trigger = "deploy_ready"
	}

	imageRef, err := a.deployer.NotifyDeploymentReady(r.Context(), source, request.SHA, request.LitestreamSHA, request.ImageRef, trigger)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.observeLatestDeploymentState(source)

	writeAPIJSON(w, map[string]any{
		"sha":            request.SHA,
		"litestream_sha": request.LitestreamSHA,
		"source":         source,
		"image_ref":      imageRef,
		"trigger":        trigger,
	})
}

func (a *API) handleResumeDormantWorkers(w http.ResponseWriter, r *http.Request) {
	if a.manager == nil {
		http.Error(w, "resume manager unavailable", http.StatusInternalServerError)
		return
	}

	source := strings.TrimSpace(r.URL.Query().Get("source"))
	if source == "" {
		source = "main"
	}
	imageRef := strings.TrimSpace(r.URL.Query().Get("image"))
	if imageRef == "" {
		var err error
		imageRef, err = a.manager.currentWorkerImage(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	sha := strings.TrimSpace(r.URL.Query().Get("sha"))
	litestreamSHA := strings.TrimSpace(r.URL.Query().Get("litestream_sha"))
	trigger := strings.TrimSpace(r.URL.Query().Get("trigger"))
	if trigger == "" {
		trigger = "manual_resume"
	}

	dormantWorkers, err := a.db.ListDormantWorkers(source)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := a.manager.ResumeDormantWorkers(r.Context(), source, imageRef, sha, litestreamSHA, trigger); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	workerIDs := make([]string, 0, len(dormantWorkers))
	for _, worker := range dormantWorkers {
		workerIDs = append(workerIDs, worker.ID)
	}
	_ = a.db.RecordEvent("", "manual_resume_requested", fmt.Sprintf("Requested probe resume for %d dormant %s worker(s)", len(dormantWorkers), source), strings.Join(workerIDs, ","))
	a.observeLatestDeploymentState(source)

	writeAPIJSON(w, map[string]any{
		"resumed_workers": len(dormantWorkers),
		"worker_ids":      workerIDs,
		"source":          source,
		"image_ref":       imageRef,
		"git_sha":         sha,
		"litestream_sha":  litestreamSHA,
		"trigger":         trigger,
	})
}

func readDeploymentReadyRequest(r *http.Request) (deploymentReadyRequest, error) {
	var request deploymentReadyRequest
	if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			return deploymentReadyRequest{}, fmt.Errorf("decode request: %w", err)
		}
	}

	if strings.TrimSpace(request.SHA) == "" {
		request.SHA = strings.TrimSpace(r.URL.Query().Get("sha"))
	}
	if strings.TrimSpace(request.Source) == "" {
		request.Source = strings.TrimSpace(r.URL.Query().Get("source"))
	}
	if strings.TrimSpace(request.LitestreamSHA) == "" {
		request.LitestreamSHA = strings.TrimSpace(r.URL.Query().Get("litestream_sha"))
	}
	if strings.TrimSpace(request.ImageRef) == "" {
		request.ImageRef = strings.TrimSpace(r.URL.Query().Get("image"))
	}
	if strings.TrimSpace(request.Trigger) == "" {
		request.Trigger = strings.TrimSpace(r.URL.Query().Get("trigger"))
	}

	return request, nil
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

func (a *API) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	var payload reporting.HeartbeatPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	payload.WorkerID = workerID
	if payload.Name == "" {
		payload.Name = workerID
	}
	payload.RuntimePayload = payload.RuntimePayload.Normalize(payload.SentAt)

	if err := a.db.UpsertReportedWorker(payload.WorkerIdentity); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := a.db.UpdateWorkerRuntimeSnapshot(workerID, payload.RuntimePayload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if worker, err := a.db.GetWorker(workerID); err == nil {
		a.metrics.observeWorker(*worker)
		if events, err := a.db.ListWorkerEvents(workerID, 20); err == nil {
			a.metrics.observePlatformEvent(*worker, latestPlatformEvent(coalesceEventFeed(events)))
		}
		a.observeLatestDeploymentState(worker.Source)
	}

	w.WriteHeader(http.StatusAccepted)
}

func (a *API) handleVerification(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	var payload reporting.VerificationPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	payload.WorkerID = workerID
	if payload.Name == "" {
		payload.Name = workerID
	}
	observedAt := payload.CompletedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	payload.RuntimePayload = payload.RuntimePayload.Normalize(observedAt)

	if err := a.db.UpsertReportedWorker(payload.WorkerIdentity); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := a.db.UpdateWorkerRuntimeSnapshot(workerID, payload.RuntimePayload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	workerBeforeUpdate, _ := a.db.GetWorker(workerID)

	completedAt := payload.CompletedAt
	verification := &model.Verification{
		WorkerID:     workerID,
		StartedAt:    payload.StartedAt,
		Status:       payload.Status,
		CheckType:    payload.CheckType,
		Passed:       payload.Passed,
		DurationMS:   payload.DurationMS,
		ErrorMessage: payload.ErrorMessage,
	}
	if !completedAt.IsZero() {
		verification.CompletedAt = &completedAt
	}

	if err := a.db.RecordVerification(verification); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := a.db.UpdateWorkerVerificationState(workerID, payload.Passed, payload.Summary); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	details, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	eventType := "verification_passed"
	message := payload.Summary
	if message == "" && payload.Passed {
		message = "verification passed"
	}
	if !payload.Passed {
		eventType = "verification_failed"
		if message == "" {
			message = "verification failed"
		}
	}

	if err := a.db.RecordEvent(workerID, eventType, message, string(details)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if workerBeforeUpdate != nil && workerBeforeUpdate.Status == model.WorkerProbing {
		if payload.Passed {
			_ = a.db.RecordEvent(workerID, "worker_probe_passed", "Worker probe verification passed", "")
		} else if a.manager != nil {
			signature := inferFailureSignature(verification)
			reason := fmt.Sprintf("worker probe failed with %s; returning to dormant state", signature)
			if err := a.manager.DormantWorker(r.Context(), workerID, reason, signature, "probe_failed"); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_ = a.db.RecordEvent(workerID, "worker_probe_failed", reason, string(details))
		}
	}

	if worker, err := a.db.GetWorker(workerID); err == nil {
		a.metrics.observeWorker(*worker)
		a.metrics.observeVerification(*worker, *verification)
		if events, err := a.db.ListWorkerEvents(workerID, 20); err == nil {
			a.metrics.observePlatformEvent(*worker, latestPlatformEvent(coalesceEventFeed(events)))
		}
		a.observeLatestDeploymentState(worker.Source)
		if a.alerts != nil {
			a.alerts.NotifyVerificationFailure(*worker, *verification)
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

func (a *API) workerDetail(workerID string) (*WorkerDetailResponse, int, error) {
	worker, err := a.db.GetWorker(workerID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	verifications, err := a.db.ListVerifications(workerID, 10)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	events, err := a.db.ListWorkerEvents(workerID, 20)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	events = coalesceEventFeed(events)

	response := &WorkerDetailResponse{
		Worker:              *worker,
		Workload:            resolveWorkerWorkload(*worker),
		ReportedRuntime:     extractReportedRuntime(*worker, events),
		LatestPlatformEvent: latestPlatformEvent(events),
		RecentVerifications: verifications,
		RecentEvents:        events,
		TriageCommands:      buildTriageCommands(*worker, false),
	}
	response.RuntimeSnapshotStatus = reporting.SnapshotStatus(response.ReportedRuntime)

	for _, verification := range verifications {
		if verification.Passed && verification.Status != "failed" {
			continue
		}
		verificationCopy := verification
		response.LatestFailure = &verificationCopy
		response.FailureStage = inferFailureStage(&verificationCopy)
		response.FailureSignature = inferFailureSignature(&verificationCopy)
		response.ProbableSubsystem = inferProbableSubsystem(response.FailureStage, response.FailureSignature)
		break
	}

	if worker.FlyMachineID != "" {
		flyClient := a.fly
		if appName := strings.TrimSpace(worker.AppName); appName != "" {
			flyClient = a.fly.ForApp(appName)
		}

		machine, err := flyClient.GetMachine(context.Background(), worker.FlyMachineID)
		if err != nil {
			response.MachineError = err.Error()
		} else {
			response.Machine = machine
			response.TriageCommands = buildTriageCommands(*worker, true)
		}
	}

	return response, http.StatusOK, nil
}

func (a *API) buildIncidentBundle(workerID string) (*IncidentBundle, int, error) {
	detail, status, err := a.workerDetail(workerID)
	if err != nil {
		return nil, status, err
	}

	var latestFailure *model.Verification
	activeFailureDetected := false
	for i, verification := range detail.RecentVerifications {
		if i == 0 && activeFailure(&verification) {
			activeFailureDetected = true
		}
		if activeFailure(&verification) {
			verificationCopy := verification
			latestFailure = &verificationCopy
			break
		}
	}

	summaries, err := a.listWorkerSummaries("", detail.Worker.Source)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	diagnosis := buildDiagnosisSnapshot(summaries)
	probableSubsystem := inferProbableSubsystem(inferFailureStage(latestFailure), inferFailureSignature(latestFailure))
	reportedRuntime := extractReportedRuntime(detail.Worker, detail.RecentEvents)

	bundle := &IncidentBundle{
		GeneratedAt:           time.Now().UTC(),
		Worker:                detail.Worker,
		Workload:              detail.Workload,
		LatestFailure:         latestFailure,
		LatestPlatformEvent:   detail.LatestPlatformEvent,
		ActiveFailure:         activeFailureDetected,
		FailureStage:          inferFailureStage(latestFailure),
		FailureSignature:      inferFailureSignature(latestFailure),
		ProbableSubsystem:     probableSubsystem,
		RuntimeSnapshotStatus: reporting.SnapshotStatus(reportedRuntime),
		ReportedRuntime:       reportedRuntime,
		Diagnosis:             diagnosis,
		RelatedClusters:       relatedDiagnosisClusters(diagnosis, detail.Worker.ID, inferFailureSignature(latestFailure), probableSubsystem),
		RecentVerifications:   detail.RecentVerifications,
		RecentEvents:          detail.RecentEvents,
		Machine:               detail.Machine,
		MachineError:          detail.MachineError,
		TriageCommands:        buildTriageCommands(detail.Worker, detail.Machine != nil),
	}
	bundle.Guide = buildIncidentGuide(bundle)
	bundle.PromptModes = buildPromptModes(bundle.Guide.RecommendedPromptMode)
	bundle.Prompt = buildPrompt(bundle, parsePromptMode("", bundle.Guide.RecommendedPromptMode))
	return bundle, http.StatusOK, nil
}

func extractReportedRuntime(worker model.Worker, events []model.Event) *reporting.RuntimePayload {
	var observedAt time.Time
	if worker.LastRuntimeAt != nil {
		observedAt = worker.LastRuntimeAt.UTC()
	}
	if runtime := parseRuntimeJSON(worker.LastRuntimeJSON, observedAt); runtime != nil {
		return runtime
	}

	for _, event := range events {
		if !strings.HasPrefix(event.EventType, "verification_") {
			continue
		}

		var payload reporting.VerificationPayload
		if err := json.Unmarshal([]byte(event.Details), &payload); err != nil {
			continue
		}

		runtime := payload.RuntimePayload.Normalize(event.CreatedAt.UTC())
		return &runtime
	}

	return nil
}

func parseRuntimeJSON(raw string, observedAt time.Time) *reporting.RuntimePayload {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	var runtime reporting.RuntimePayload
	if err := json.Unmarshal([]byte(raw), &runtime); err != nil {
		return nil
	}
	normalized := runtime.Normalize(observedAt)
	return &normalized
}

func mustJSON(v any) string {
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(body)
}

func inferFailureStage(verification *model.Verification) string {
	if verification == nil {
		return ""
	}

	text := strings.ToLower(verification.ErrorMessage)
	switch {
	case strings.Contains(text, "wait for sync") || strings.Contains(text, "sync request") || strings.Contains(text, "litestream.sock"):
		return "sync"
	case strings.Contains(text, "restore failed") || strings.Contains(text, "check_type=restore"):
		return "restore"
	case strings.Contains(text, "integrity check") || strings.Contains(text, "check_type=integrity_check"):
		return "integrity_check"
	case strings.Contains(text, "validation failed"):
		return "validation"
	case verification.CheckType != "":
		return verification.CheckType
	default:
		return ""
	}
}

func inferFailureSignature(verification *model.Verification) string {
	if verification == nil {
		return ""
	}

	text := strings.ToLower(verification.ErrorMessage)
	switch {
	case strings.Contains(text, "litestream.sock") && strings.Contains(text, "too many open files"):
		return "litestream_sync_fd_exhausted"
	case strings.Contains(text, "litestream.sock") && strings.Contains(text, "connect: connection refused"):
		return "litestream_sync_socket_refused"
	case strings.Contains(text, "wait for sync") && (strings.Contains(text, "context deadline exceeded") || strings.Contains(text, "client.timeout exceeded")):
		return "litestream_sync_timeout"
	case strings.Contains(text, "wrong # of entries in index"):
		return "sqlite_index_mismatch"
	case strings.Contains(text, "validation failed"):
		return "validation_failed"
	case strings.Contains(text, "open ltx file: file does not exist"):
		return "replica_ltx_missing"
	case strings.Contains(text, "listobjectsv2") || strings.Contains(text, "requestcanceled"):
		return "replica_s3_timeout"
	case strings.Contains(text, "ltx continuity"):
		return "ltx_continuity"
	default:
		return firstMeaningfulLine(verification.ErrorMessage)
	}
}

func inferDeploymentRolloutStatus(rollout DeploymentRolloutResponse) string {
	switch {
	case rollout.TotalWorkers == 0:
		return "no_workers"
	case rollout.OutdatedWorkers > 0:
		return "rolling_out"
	case rollout.ProbingWorkers > 0:
		return "probing"
	case rollout.DormantWorkers > 0 || rollout.DegradedWorkers > 0:
		return "needs_attention"
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
	case "needs_attention":
		return fmt.Sprintf("The %s rollout needs attention. All %d workers are on the new release, but %s: %d degraded and %d dormant.", subject, rollout.TotalWorkers, workersNeedInvestigation(rollout.AttentionWorkers), rollout.DegradedWorkers, rollout.DormantWorkers)
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
	case "needs_attention":
		if rollout.GraceWindowExceeded {
			return "Treat this as a failed rollout until the degraded or dormant workers are explained."
		}
		return "Watch the affected workers through one full verification cycle, then open any that stay degraded or dormant."
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
	case "needs_attention":
		checks = append(checks,
			"Open degraded and dormant workers first.",
			"Check that awaiting_verification_workers is not masking workers that simply have not rerun yet.",
			"Check failure stage, signature, and probable subsystem on those workers.",
		)
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

func latestVerificationInWindow(verifications []model.Verification, since time.Time, until *time.Time) *model.Verification {
	for i := range verifications {
		observedAt, ok := verificationObservedAt(verifications[i])
		if !ok {
			continue
		}
		if observedAt.Before(since) {
			continue
		}
		if until != nil && !until.IsZero() && !observedAt.Before(*until) {
			continue
		}
		verification := verifications[i]
		return &verification
	}
	return nil
}

func buildTriageCommands(worker model.Worker, hasMachine bool) []string {
	commands := make([]string, 0, 6)
	appName := strings.TrimSpace(worker.AppName)
	if appName == "" {
		appName = "litestream-soak"
	}

	if worker.FlyMachineID != "" {
		commands = append(commands, fmt.Sprintf("fly machine status %s -a %s", worker.FlyMachineID, appName))
		if worker.Status == model.WorkerDormant {
			commands = append(commands, fmt.Sprintf("fly machine start %s -a %s", worker.FlyMachineID, appName))
		}
		commands = append(commands, fmt.Sprintf("fly logs -a %s -i %s", appName, worker.FlyMachineID))
	}
	if hasMachine {
		commands = append(commands, fmt.Sprintf("fly ssh console -a %s", appName))
	}
	commands = append(commands,
		fmt.Sprintf(`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/workers/%s/incident | jq .`, worker.ID),
		`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/diagnosis | jq .`,
	)

	return commands
}

func firstMeaningfulLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}

func formatTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "unknown"
	}
	return t.Format(time.RFC3339)
}

func valueOrUnknown(v string) string {
	if strings.TrimSpace(v) == "" {
		return "unknown"
	}
	return v
}

func readLimit(r *http.Request, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return fallback
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return fallback
	}
	return limit
}

func readBoolQuery(r *http.Request, key string) bool {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func writeAPIJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
