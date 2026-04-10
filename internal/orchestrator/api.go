package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/workload"
)

type API struct {
	db      *model.DB
	fly     *flyapi.Client
	metrics *controlMetrics
	alerts  *AlertDispatcher
}

type WorkerDetailResponse struct {
	Worker              model.Worker         `json:"worker"`
	Workload            workload.Config      `json:"workload"`
	LatestFailure       *model.Verification  `json:"latest_failure,omitempty"`
	FailureStage        string               `json:"failure_stage,omitempty"`
	FailureSignature    string               `json:"failure_signature,omitempty"`
	TriageCommands      []string             `json:"triage_commands,omitempty"`
	RecentVerifications []model.Verification `json:"recent_verifications"`
	RecentEvents        []model.Event        `json:"recent_events"`
	Machine             *flyapi.Machine      `json:"machine,omitempty"`
	MachineError        string               `json:"machine_error,omitempty"`
}

type FailureResponse struct {
	Worker           *model.Worker      `json:"worker,omitempty"`
	Verification     model.Verification `json:"verification"`
	FailureStage     string             `json:"failure_stage,omitempty"`
	FailureSignature string             `json:"failure_signature,omitempty"`
	TriageCommands   []string           `json:"triage_commands,omitempty"`
}

type WorkerSummaryResponse struct {
	Worker                  model.Worker        `json:"worker"`
	Workload                workload.Config     `json:"workload"`
	LastVerification        *model.Verification `json:"last_verification,omitempty"`
	LatestFailure           *model.Verification `json:"latest_failure,omitempty"`
	CurrentFailureStage     string              `json:"current_failure_stage,omitempty"`
	CurrentFailureSignature string              `json:"current_failure_signature,omitempty"`
	LatestFailureStage      string              `json:"latest_failure_stage,omitempty"`
	LatestFailureSignature  string              `json:"latest_failure_signature,omitempty"`
	TriageCommands          []string            `json:"triage_commands,omitempty"`
}

type IncidentBundle struct {
	GeneratedAt         time.Time            `json:"generated_at"`
	Worker              model.Worker         `json:"worker"`
	Workload            workload.Config      `json:"workload"`
	LatestFailure       *model.Verification  `json:"latest_failure,omitempty"`
	FailureStage        string               `json:"failure_stage,omitempty"`
	FailureSignature    string               `json:"failure_signature,omitempty"`
	RecentVerifications []model.Verification `json:"recent_verifications"`
	RecentEvents        []model.Event        `json:"recent_events"`
	Machine             *flyapi.Machine      `json:"machine,omitempty"`
	MachineError        string               `json:"machine_error,omitempty"`
	TriageCommands      []string             `json:"triage_commands,omitempty"`
	Prompt              string               `json:"prompt"`
}

func NewAPI(db *model.DB, fly *flyapi.Client, metrics *controlMetrics, alerts *AlertDispatcher) *API {
	if metrics == nil {
		metrics = NewControlMetrics(db)
	}
	return &API{
		db:      db,
		fly:     fly,
		metrics: metrics,
		alerts:  alerts,
	}
}

func (a *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.handleHome)
	mux.HandleFunc("GET /ui", a.handleHome)
	mux.HandleFunc("GET /ui/workers/{id}", a.handleWorkerPage)
	mux.HandleFunc("GET /api/workers", a.handleListWorkers)
	mux.HandleFunc("GET /api/worker-summaries", a.handleListWorkerSummaries)
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
	workers, err := a.db.ListWorkers(r.URL.Query().Get("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeAPIJSON(w, workers)
}

func (a *API) handleListWorkerSummaries(w http.ResponseWriter, r *http.Request) {
	workers, err := a.db.ListWorkers(r.URL.Query().Get("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	summaries := make([]WorkerSummaryResponse, 0, len(workers))
	for _, worker := range workers {
		summary, err := a.buildWorkerSummary(worker)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		summaries = append(summaries, summary)
	}

	writeAPIJSON(w, summaries)
}

func (a *API) handleListEvents(w http.ResponseWriter, r *http.Request) {
	events, err := a.db.ListEvents(readLimit(r, 50))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
			Verification:     verification,
			FailureStage:     inferFailureStage(&verification),
			FailureSignature: inferFailureSignature(&verification),
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
		Worker:         worker,
		Workload:       resolveWorkerWorkload(worker),
		TriageCommands: buildTriageCommands(worker, worker.FlyMachineID != ""),
	}

	verifications, err := a.db.ListVerifications(worker.ID, 1)
	if err != nil {
		return summary, err
	}
	if len(verifications) > 0 {
		verification := verifications[0]
		summary.LastVerification = &verification
		if !verification.Passed || verification.Status == "failed" {
			summary.CurrentFailureStage = inferFailureStage(&verification)
			summary.CurrentFailureSignature = inferFailureSignature(&verification)
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

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(bundle.Prompt))
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

	if err := a.db.UpsertReportedWorker(payload.WorkerIdentity); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if worker, err := a.db.GetWorker(workerID); err == nil {
		a.metrics.observeWorker(*worker)
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

	if err := a.db.UpsertReportedWorker(payload.WorkerIdentity); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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
	if worker, err := a.db.GetWorker(workerID); err == nil {
		a.metrics.observeWorker(*worker)
		a.metrics.observeVerification(*worker, *verification)
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

	response := &WorkerDetailResponse{
		Worker:              *worker,
		Workload:            resolveWorkerWorkload(*worker),
		RecentVerifications: verifications,
		RecentEvents:        events,
		TriageCommands:      buildTriageCommands(*worker, false),
	}

	for _, verification := range verifications {
		if verification.Passed && verification.Status != "failed" {
			continue
		}
		verificationCopy := verification
		response.LatestFailure = &verificationCopy
		response.FailureStage = inferFailureStage(&verificationCopy)
		response.FailureSignature = inferFailureSignature(&verificationCopy)
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
	for _, verification := range detail.RecentVerifications {
		if !verification.Passed || verification.Status == "failed" {
			verificationCopy := verification
			latestFailure = &verificationCopy
			break
		}
	}

	bundle := &IncidentBundle{
		GeneratedAt:         time.Now().UTC(),
		Worker:              detail.Worker,
		Workload:            detail.Workload,
		LatestFailure:       latestFailure,
		FailureStage:        inferFailureStage(latestFailure),
		FailureSignature:    inferFailureSignature(latestFailure),
		RecentVerifications: detail.RecentVerifications,
		RecentEvents:        detail.RecentEvents,
		Machine:             detail.Machine,
		MachineError:        detail.MachineError,
		TriageCommands:      buildTriageCommands(detail.Worker, detail.Machine != nil),
	}
	bundle.Prompt = buildPrompt(bundle)
	return bundle, http.StatusOK, nil
}

func buildPrompt(bundle *IncidentBundle) string {
	sections := []string{
		"You are diagnosing a Litestream soak test failure.",
		"Determine whether the failure most likely comes from Litestream, restore/replication behavior, Fly runtime conditions, or the soak harness itself.",
		"Recommend the next commands or log locations to inspect. Do not assume the goal is to fix code immediately; prioritize fast triage.",
		"",
		"Summary:",
		fmt.Sprintf("- generated_at: %s", bundle.GeneratedAt.Format(time.RFC3339)),
		fmt.Sprintf("- worker_id: %s", bundle.Worker.ID),
		fmt.Sprintf("- app_name: %s", valueOrUnknown(bundle.Worker.AppName)),
		fmt.Sprintf("- region: %s", valueOrUnknown(bundle.Worker.Region)),
		fmt.Sprintf("- machine_id: %s", valueOrUnknown(bundle.Worker.FlyMachineID)),
		fmt.Sprintf("- status: %s", bundle.Worker.Status),
		fmt.Sprintf("- last_heartbeat_at: %s", formatTime(bundle.Worker.LastHeartbeatAt)),
		fmt.Sprintf("- load_mode: %s", valueOrUnknown(bundle.Workload.MetricLoadMode())),
		fmt.Sprintf("- replay_dataset: %s", valueOrUnknown(bundle.Workload.MetricReplayDataset())),
		fmt.Sprintf("- load_pattern: %s", valueOrUnknown(bundle.Workload.MetricPattern())),
		fmt.Sprintf("- failure_stage: %s", valueOrUnknown(bundle.FailureStage)),
		fmt.Sprintf("- failure_signature: %s", valueOrUnknown(bundle.FailureSignature)),
		"",
		"Worker:",
		mustJSON(bundle.Worker),
	}

	if bundle.LatestFailure != nil {
		sections = append(
			sections,
			"",
			"Latest failed verification:",
			mustJSON(bundle.LatestFailure),
			"",
			"Immediate triage commands:",
			strings.Join(bundle.TriageCommands, "\n"),
		)
	}

	sections = append(sections, "", "Recent verifications:", mustJSON(bundle.RecentVerifications))
	sections = append(sections, "", "Recent events:", mustJSON(bundle.RecentEvents))
	sections = append(sections, "", "Workload:", mustJSON(bundle.Workload))

	if bundle.Machine != nil {
		sections = append(sections, "", "Current Fly machine:", mustJSON(bundle.Machine))
	}
	if bundle.MachineError != "" {
		sections = append(sections, "", "Machine fetch error:", bundle.MachineError)
	}

	return strings.Join(sections, "\n")
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
	case strings.Contains(text, "wrong # of entries in index"):
		return "sqlite_index_mismatch"
	case strings.Contains(text, "listobjectsv2") || strings.Contains(text, "requestcanceled"):
		return "replica_s3_timeout"
	case strings.Contains(text, "ltx continuity"):
		return "ltx_continuity"
	default:
		return firstMeaningfulLine(verification.ErrorMessage)
	}
}

func buildTriageCommands(worker model.Worker, hasMachine bool) []string {
	commands := make([]string, 0, 4)
	appName := strings.TrimSpace(worker.AppName)
	if appName == "" {
		appName = "litestream-soak"
	}

	if worker.FlyMachineID != "" {
		commands = append(commands, fmt.Sprintf("fly machine status %s -a %s", worker.FlyMachineID, appName))
		commands = append(commands, fmt.Sprintf("fly logs -a %s -i %s", appName, worker.FlyMachineID))
	}
	if hasMachine {
		commands = append(commands, fmt.Sprintf("fly ssh console -a %s", appName))
	}
	commands = append(commands, fmt.Sprintf("curl -sS https://litestream-soak-ctl.fly.dev/api/workers/%s/incident | jq .", worker.ID))

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

func writeAPIJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
