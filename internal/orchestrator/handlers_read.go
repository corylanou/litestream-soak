package orchestrator

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func (a *API) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := a.db.ListWorkersFiltered(r.URL.Query().Get("status"), strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to list workers")
		return
	}
	writeAPIJSON(w, workers)
}

func (a *API) handleListWorkerSummaries(w http.ResponseWriter, r *http.Request) {
	summaries, err := a.listWorkerSummaries(r.URL.Query().Get("status"), strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to list worker summaries")
		return
	}

	writeAPIJSON(w, summaries)
}

func (a *API) handleListDeployments(w http.ResponseWriter, r *http.Request) {
	deployments, err := a.db.ListDeployments(strings.TrimSpace(r.URL.Query().Get("source")), readLimit(r, 10))
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to list deployments")
		return
	}

	rollouts := make([]DeploymentRolloutResponse, 0, len(deployments))
	for _, deployment := range deployments {
		rollout, err := a.buildDeploymentRollout(deployment)
		if err != nil {
			respondError(w, r, http.StatusInternalServerError, err, "failed to build deployment rollout")
			return
		}
		rollouts = append(rollouts, rollout)
	}

	writeAPIJSON(w, rollouts)
}

func (a *API) handleGetLatestDeployment(w http.ResponseWriter, r *http.Request) {
	deployment, err := a.db.GetLatestDeployment(strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to load latest deployment")
		return
	}
	if deployment == nil {
		respondError(w, r, http.StatusNotFound, nil, "deployment not found")
		return
	}

	rollout, err := a.buildDeploymentRollout(*deployment)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to build deployment rollout")
		return
	}

	writeAPIJSON(w, rollout)
}

func (a *API) handleGetLatestDeploymentPrompt(w http.ResponseWriter, r *http.Request) {
	deployment, err := a.db.GetLatestDeployment(strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to load latest deployment")
		return
	}
	if deployment == nil {
		respondError(w, r, http.StatusNotFound, nil, "deployment not found")
		return
	}

	rollout, err := a.buildDeploymentRollout(*deployment)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to build deployment rollout")
		return
	}
	mode := parsePromptMode(r.URL.Query().Get("mode"), defaultPromptModeForRollout(rollout))

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(buildRolloutPrompt(rollout, nil, mode)))
}

func (a *API) handleGetLatestDeploymentComparison(w http.ResponseWriter, r *http.Request) {
	comparison, err := a.buildRequestedDeploymentComparison(
		strings.TrimSpace(r.URL.Query().Get("source")),
		strings.TrimSpace(r.URL.Query().Get("base_source")),
		strings.TrimSpace(r.URL.Query().Get("head_source")),
	)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to build deployment comparison")
		return
	}
	if comparison == nil {
		respondError(w, r, http.StatusNotFound, nil, "deployment not found")
		return
	}

	writeAPIJSON(w, comparison)
}

func (a *API) handleGetLatestDeploymentComparisonPrompt(w http.ResponseWriter, r *http.Request) {
	comparison, err := a.buildRequestedDeploymentComparison(
		strings.TrimSpace(r.URL.Query().Get("source")),
		strings.TrimSpace(r.URL.Query().Get("base_source")),
		strings.TrimSpace(r.URL.Query().Get("head_source")),
	)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to build deployment comparison")
		return
	}
	if comparison == nil {
		respondError(w, r, http.StatusNotFound, nil, "deployment not found")
		return
	}

	mode := parsePromptMode(r.URL.Query().Get("mode"), defaultPromptModeForComparison(*comparison))

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(buildComparisonPrompt(*comparison, mode)))
}

func (a *API) handleGetDeployment(w http.ResponseWriter, r *http.Request) {
	deployment, err := a.db.GetDeploymentBySHA(strings.TrimSpace(r.PathValue("sha")))
	if err != nil {
		respondError(w, r, http.StatusNotFound, err, "deployment not found")
		return
	}

	rollout, err := a.buildDeploymentRollout(*deployment)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to build deployment rollout")
		return
	}

	writeAPIJSON(w, rollout)
}

func (a *API) handleListRunArchives(w http.ResponseWriter, r *http.Request) {
	archives, err := a.db.ListRunArchives(
		strings.TrimSpace(r.URL.Query().Get("source")),
		strings.TrimSpace(r.URL.Query().Get("type")),
		readLimit(r, 20),
	)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to list run archives")
		return
	}
	writeAPIJSON(w, archives)
}

func (a *API) handleGetRunArchive(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(strings.TrimSpace(r.PathValue("id")))
	if err != nil || id <= 0 {
		respondError(w, r, http.StatusBadRequest, nil, "invalid archive id")
		return
	}

	archive, err := a.db.GetRunArchive(id)
	if err != nil {
		respondError(w, r, http.StatusNotFound, err, "run archive not found")
		return
	}
	writeAPIJSON(w, archive)
}

func (a *API) handleGetRunArchivePrompt(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(strings.TrimSpace(r.PathValue("id")))
	if err != nil || id <= 0 {
		respondError(w, r, http.StatusBadRequest, nil, "invalid archive id")
		return
	}

	archive, err := a.db.GetRunArchive(id)
	if err != nil {
		respondError(w, r, http.StatusNotFound, err, "run archive not found")
		return
	}
	mode := parsePromptMode(r.URL.Query().Get("mode"), defaultPromptModeForArchive(*archive))

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(buildRunArchivePrompt(*archive, mode)))
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
		respondError(w, r, http.StatusInternalServerError, err, "failed to list events")
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
		respondError(w, r, http.StatusInternalServerError, err, "failed to list failures")
		return
	}

	failures := make([]FailureResponse, 0, len(verifications))
	for _, verification := range verifications {
		vf := classifyVerification(&verification)
		failure := FailureResponse{
			Verification:      verification,
			FailureStage:      vf.Stage,
			FailureSignature:  vf.Signature,
			FailureCategory:   failureCategoryActionable,
			FailureSeverity:   failureSeverityBad,
			ProbableSubsystem: vf.probableSubsystem(),
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
		respondError(w, r, http.StatusInternalServerError, err, "failed to list alerts")
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
		} else if alert.AlertType == fleetFullyDormantAlertType {
			item.TriageCommands = buildDormantFleetTriageCommands(alert.Source)
		}
		response = append(response, item)
	}

	writeAPIJSON(w, response)
}

func (a *API) handleGetWorker(w http.ResponseWriter, r *http.Request) {
	response, status, err := a.workerDetail(r.PathValue("id"))
	if err != nil {
		respondError(w, r, status, err, "")
		return
	}
	writeAPIJSON(w, response)
}

func (a *API) handleGetIncident(w http.ResponseWriter, r *http.Request) {
	bundle, status, err := a.buildIncidentBundle(r.PathValue("id"))
	if err != nil {
		respondError(w, r, status, err, "")
		return
	}
	writeAPIJSON(w, bundle)
}

func (a *API) handleGetPrompt(w http.ResponseWriter, r *http.Request) {
	bundle, status, err := a.buildIncidentBundle(r.PathValue("id"))
	if err != nil {
		respondError(w, r, status, err, "")
		return
	}
	mode := parsePromptMode(r.URL.Query().Get("mode"), bundle.Guide.RecommendedPromptMode)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(buildPrompt(bundle, mode)))
}

func defaultPromptModeForRollout(rollout DeploymentRolloutResponse) string {
	if rollout.Status == "stable" {
		return string(promptModeHealthy)
	}
	return string(promptModeTriage)
}

func defaultPromptModeForComparison(comparison DeploymentComparisonResponse) string {
	if comparison.Head.FailedWorkers == 0 && comparison.Head.AwaitingWorkers == 0 {
		return string(promptModeHealthy)
	}
	return string(promptModeTriage)
}

func defaultPromptModeForArchive(archive model.RunArchive) string {
	if archive.ArchiveType == runArchiveTypeSuccess {
		return string(promptModeHealthy)
	}
	return string(promptModeTriage)
}

func (a *API) handleGetWorkerDebugSnapshot(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	if _, err := a.db.GetWorker(workerID); err != nil {
		respondError(w, r, http.StatusNotFound, err, "worker not found")
		return
	}

	events, err := a.db.ListWorkerEvents(workerID, 40)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to list worker events")
		return
	}

	snapshot := latestFailureDebugSnapshot(events)
	if snapshot == nil {
		respondError(w, r, http.StatusNotFound, nil, "no failure debug snapshot recorded for worker")
		return
	}
	writeAPIJSON(w, snapshot)
}

func (a *API) handleGetDiagnosis(w http.ResponseWriter, r *http.Request) {
	summaries, err := a.listWorkerSummaries(r.URL.Query().Get("status"), strings.TrimSpace(r.URL.Query().Get("source")))
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to list worker summaries")
		return
	}

	writeAPIJSON(w, map[string]any{
		"generated_at":         time.Now().UTC(),
		"diagnosis":            buildDiagnosisSnapshot(summaries),
		"coverage":             buildCoverageSnapshot(summaries),
		"active_verifications": activeVerificationWorkers(summaries),
	})
}
