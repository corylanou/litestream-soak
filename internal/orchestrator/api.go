package orchestrator

import (
	"net/http"
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
	Worker                model.Worker                     `json:"worker"`
	Workload              workload.Config                  `json:"workload"`
	LatestFailure         *model.Verification              `json:"latest_failure,omitempty"`
	LatestPlatformEvent   *model.Event                     `json:"latest_platform_event,omitempty"`
	ActiveVerification    *reporting.ActiveVerification    `json:"active_verification,omitempty"`
	FailureStage          string                           `json:"failure_stage,omitempty"`
	FailureSignature      string                           `json:"failure_signature,omitempty"`
	FailureClassification *reporting.FailureClassification `json:"failure_classification,omitempty"`
	ProbableSubsystem     string                           `json:"probable_subsystem,omitempty"`
	RuntimeSnapshotStatus string                           `json:"runtime_snapshot_status,omitempty"`
	ReportedRuntime       *reporting.RuntimePayload        `json:"reported_runtime,omitempty"`
	TriageCommands        []string                         `json:"triage_commands,omitempty"`
	RecentVerifications   []model.Verification             `json:"recent_verifications"`
	RecentEvents          []model.Event                    `json:"recent_events"`
	Machine               *flyapi.Machine                  `json:"machine,omitempty"`
	MachineError          string                           `json:"machine_error,omitempty"`
}

type FailureResponse struct {
	Worker            *model.Worker      `json:"worker,omitempty"`
	Verification      model.Verification `json:"verification"`
	FailureStage      string             `json:"failure_stage,omitempty"`
	FailureSignature  string             `json:"failure_signature,omitempty"`
	FailureCategory   string             `json:"failure_category,omitempty"`
	FailureSeverity   string             `json:"failure_severity,omitempty"`
	ProbableSubsystem string             `json:"probable_subsystem,omitempty"`
	TriageCommands    []string           `json:"triage_commands,omitempty"`
}

type WorkerSummaryResponse struct {
	Worker                       model.Worker                     `json:"worker"`
	Workload                     workload.Config                  `json:"workload"`
	RuntimeSnapshotStatus        string                           `json:"runtime_snapshot_status,omitempty"`
	LastVerification             *model.Verification              `json:"last_verification,omitempty"`
	LatestFailure                *model.Verification              `json:"latest_failure,omitempty"`
	LatestPlatformEvent          *model.Event                     `json:"latest_platform_event,omitempty"`
	ActiveVerification           *reporting.ActiveVerification    `json:"active_verification,omitempty"`
	CurrentFailureStage          string                           `json:"current_failure_stage,omitempty"`
	CurrentFailureSignature      string                           `json:"current_failure_signature,omitempty"`
	CurrentFailureClassification *reporting.FailureClassification `json:"current_failure_classification,omitempty"`
	CurrentProbableSubsystem     string                           `json:"current_probable_subsystem,omitempty"`
	LatestFailureStage           string                           `json:"latest_failure_stage,omitempty"`
	LatestFailureSignature       string                           `json:"latest_failure_signature,omitempty"`
	LatestFailureClassification  *reporting.FailureClassification `json:"latest_failure_classification,omitempty"`
	LatestProbableSubsystem      string                           `json:"latest_probable_subsystem,omitempty"`
	Recovery                     *FailureRecovery                 `json:"recovery,omitempty"`
	TriageCommands               []string                         `json:"triage_commands,omitempty"`
}

type FailureRecovery struct {
	FailedThenNextPassed   bool       `json:"failed_then_next_passed,omitempty"`
	StillFailing           bool       `json:"still_failing,omitempty"`
	LastPassAfterFailureAt *time.Time `json:"last_pass_after_failure_at,omitempty"`
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
	Deployment              model.Deployment           `json:"deployment"`
	Status                  string                     `json:"status"`
	Summary                 string                     `json:"summary"`
	NextAction              string                     `json:"next_action,omitempty"`
	NextChecks              []string                   `json:"next_checks,omitempty"`
	GraceWindowExceeded     bool                       `json:"grace_window_exceeded,omitempty"`
	TotalWorkers            int                        `json:"total_workers"`
	UpdatedWorkers          int                        `json:"updated_workers"`
	OutdatedWorkers         int                        `json:"outdated_workers"`
	RunningWorkers          int                        `json:"running_workers"`
	DegradedWorkers         int                        `json:"degraded_workers"`
	DormantWorkers          int                        `json:"dormant_workers"`
	ProbingWorkers          int                        `json:"probing_workers"`
	RuntimeUnhealthyWorkers int                        `json:"runtime_unhealthy_workers"`
	AttentionWorkers        int                        `json:"attention_workers"`
	VerifiedSinceDeploy     int                        `json:"verified_since_deploy_workers"`
	AwaitingVerification    int                        `json:"awaiting_verification_workers"`
	Workers                 []DeploymentWorkerProgress `json:"workers,omitempty"`
}

type DeploymentFailureCount struct {
	Signature string `json:"signature"`
	Stage     string `json:"stage,omitempty"`
	Category  string `json:"category,omitempty"`
	Severity  string `json:"severity,omitempty"`
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
	FailureCategory   string     `json:"failure_category,omitempty"`
	FailureSeverity   string     `json:"failure_severity,omitempty"`
	ProbableSubsystem string     `json:"probable_subsystem,omitempty"`
}

type DeploymentScorecard struct {
	Deployment              model.Deployment          `json:"deployment"`
	WindowStart             time.Time                 `json:"window_start"`
	WindowEnd               *time.Time                `json:"window_end,omitempty"`
	TotalWorkers            int                       `json:"total_workers"`
	VerifiedWorkers         int                       `json:"verified_workers"`
	PassedWorkers           int                       `json:"passed_workers"`
	FailedWorkers           int                       `json:"failed_workers"`
	ActionableFailedWorkers int                       `json:"actionable_failed_workers"`
	EnvironmentalFailures   int                       `json:"environmental_failures"`
	RampUpFailures          int                       `json:"ramp_up_failures"`
	AwaitingWorkers         int                       `json:"awaiting_workers"`
	PassRate                float64                   `json:"pass_rate"`
	Failures                []DeploymentFailureCount  `json:"failures,omitempty"`
	Outcomes                []DeploymentWorkerOutcome `json:"outcomes,omitempty"`
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

type IncidentBundle struct {
	GeneratedAt           time.Time                        `json:"generated_at"`
	Worker                model.Worker                     `json:"worker"`
	Workload              workload.Config                  `json:"workload"`
	LatestFailure         *model.Verification              `json:"latest_failure,omitempty"`
	LatestPlatformEvent   *model.Event                     `json:"latest_platform_event,omitempty"`
	ActiveVerification    *reporting.ActiveVerification    `json:"active_verification,omitempty"`
	ActiveFailure         bool                             `json:"active_failure"`
	FailureStage          string                           `json:"failure_stage,omitempty"`
	FailureSignature      string                           `json:"failure_signature,omitempty"`
	FailureClassification *reporting.FailureClassification `json:"failure_classification,omitempty"`
	ProbableSubsystem     string                           `json:"probable_subsystem,omitempty"`
	RuntimeSnapshotStatus string                           `json:"runtime_snapshot_status,omitempty"`
	ReportedRuntime       *reporting.RuntimePayload        `json:"reported_runtime,omitempty"`
	FailureDebug          *reporting.FailureDebugSnapshot  `json:"failure_debug,omitempty"`
	Guide                 incidentGuide                    `json:"guide"`
	Diagnosis             diagnosisSnapshot                `json:"diagnosis"`
	RelatedClusters       []diagnosisCluster               `json:"related_clusters,omitempty"`
	PromptModes           []promptModeInfo                 `json:"prompt_modes,omitempty"`
	RecentVerifications   []model.Verification             `json:"recent_verifications"`
	RecentEvents          []model.Event                    `json:"recent_events"`
	Machine               *flyapi.Machine                  `json:"machine,omitempty"`
	MachineError          string                           `json:"machine_error,omitempty"`
	TriageCommands        []string                         `json:"triage_commands,omitempty"`
	Prompt                string                           `json:"prompt"`
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
	mux.Handle("GET /assets/", http.StripPrefix("/assets/", assetsHandler()))
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
	mux.HandleFunc("GET /api/deployments/latest/prompt", a.handleGetLatestDeploymentPrompt)
	mux.HandleFunc("GET /api/deployments/compare/latest", a.handleGetLatestDeploymentComparison)
	mux.HandleFunc("GET /api/deployments/compare/latest/prompt", a.handleGetLatestDeploymentComparisonPrompt)
	mux.HandleFunc("GET /api/deployments/{sha}", a.handleGetDeployment)
	mux.HandleFunc("GET /api/run-archives", a.handleListRunArchives)
	mux.HandleFunc("GET /api/run-archives/{id}", a.handleGetRunArchive)
	mux.HandleFunc("GET /api/run-archives/{id}/prompt", a.handleGetRunArchivePrompt)
	mux.HandleFunc("POST /api/admin/deployments/ready", a.handleDeploymentReady)
	mux.HandleFunc("POST /api/admin/workers/{id}/roll", a.handleRollWorker)
	mux.HandleFunc("POST /api/admin/resume-dormant", a.handleResumeDormantWorkers)
	mux.HandleFunc("POST /api/admin/pause-source", a.handlePauseSourceWorkers)
	mux.HandleFunc("GET /api/workers/{id}", a.handleGetWorker)
	mux.HandleFunc("GET /api/workers/{id}/incident", a.handleGetIncident)
	mux.HandleFunc("GET /api/workers/{id}/prompt", a.handleGetPrompt)
	mux.HandleFunc("GET /api/workers/{id}/debug-snapshot", a.handleGetWorkerDebugSnapshot)
	mux.HandleFunc("POST /api/workers/{id}/heartbeat", a.handleHeartbeat)
	mux.HandleFunc("POST /api/workers/{id}/verifications", a.handleVerification)
	mux.HandleFunc("POST /api/workers/{id}/events", a.handleWorkerEvent)
	mux.HandleFunc("GET /api/events", a.handleListEvents)
	mux.HandleFunc("GET /api/failures", a.handleListFailures)
	mux.HandleFunc("GET /api/alerts", a.handleListAlerts)
}
