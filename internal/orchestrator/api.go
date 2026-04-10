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
)

type API struct {
	db  *model.DB
	fly *flyapi.Client
}

type WorkerDetailResponse struct {
	Worker              model.Worker         `json:"worker"`
	RecentVerifications []model.Verification `json:"recent_verifications"`
	RecentEvents        []model.Event        `json:"recent_events"`
	Machine             *flyapi.Machine      `json:"machine,omitempty"`
	MachineError        string               `json:"machine_error,omitempty"`
}

type FailureResponse struct {
	Worker       *model.Worker      `json:"worker,omitempty"`
	Verification model.Verification `json:"verification"`
}

type IncidentBundle struct {
	GeneratedAt         time.Time            `json:"generated_at"`
	Worker              model.Worker         `json:"worker"`
	LatestFailure       *model.Verification  `json:"latest_failure,omitempty"`
	RecentVerifications []model.Verification `json:"recent_verifications"`
	RecentEvents        []model.Event        `json:"recent_events"`
	Machine             *flyapi.Machine      `json:"machine,omitempty"`
	MachineError        string               `json:"machine_error,omitempty"`
	Prompt              string               `json:"prompt"`
}

func NewAPI(db *model.DB, fly *flyapi.Client) *API {
	return &API{db: db, fly: fly}
}

func (a *API) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.handleHome)
	mux.HandleFunc("GET /ui", a.handleHome)
	mux.HandleFunc("GET /ui/workers/{id}", a.handleWorkerPage)
	mux.HandleFunc("GET /api/workers", a.handleListWorkers)
	mux.HandleFunc("GET /api/workers/{id}", a.handleGetWorker)
	mux.HandleFunc("GET /api/workers/{id}/incident", a.handleGetIncident)
	mux.HandleFunc("GET /api/workers/{id}/prompt", a.handleGetPrompt)
	mux.HandleFunc("POST /api/workers/{id}/heartbeat", a.handleHeartbeat)
	mux.HandleFunc("POST /api/workers/{id}/verifications", a.handleVerification)
	mux.HandleFunc("GET /api/events", a.handleListEvents)
	mux.HandleFunc("GET /api/failures", a.handleListFailures)
}

func (a *API) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := a.db.ListWorkers(r.URL.Query().Get("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeAPIJSON(w, workers)
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
		failure := FailureResponse{Verification: verification}
		worker, err := a.db.GetWorker(verification.WorkerID)
		if err == nil {
			failure.Worker = worker
		}
		failures = append(failures, failure)
	}

	writeAPIJSON(w, failures)
}

func (a *API) handleGetWorker(w http.ResponseWriter, r *http.Request) {
	response, status, err := a.workerDetail(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	writeAPIJSON(w, response)
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
		RecentVerifications: verifications,
		RecentEvents:        events,
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
		LatestFailure:       latestFailure,
		RecentVerifications: detail.RecentVerifications,
		RecentEvents:        detail.RecentEvents,
		Machine:             detail.Machine,
		MachineError:        detail.MachineError,
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
		"Worker:",
		mustJSON(bundle.Worker),
	}

	if bundle.LatestFailure != nil {
		sections = append(sections, "", "Latest failed verification:", mustJSON(bundle.LatestFailure))
	}

	sections = append(sections, "", "Recent verifications:", mustJSON(bundle.RecentVerifications))
	sections = append(sections, "", "Recent events:", mustJSON(bundle.RecentEvents))

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
