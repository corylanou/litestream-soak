package orchestrator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func (a *API) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	var payload reporting.HeartbeatPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		respondError(w, r, http.StatusBadRequest, nil, "invalid payload")
		return
	}
	payload.WorkerID = workerID
	if payload.Name == "" {
		payload.Name = workerID
	}
	payload.RuntimePayload = payload.Normalize(payload.SentAt)

	if err := a.db.UpsertReportedWorker(payload.WorkerIdentity); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record worker")
		return
	}
	if err := a.db.UpdateWorkerRuntimeSnapshot(workerID, payload.RuntimePayload); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record runtime snapshot")
		return
	}
	if worker, err := a.db.GetWorker(workerID); err == nil {
		a.metrics.observeWorker(*worker)
		if events, err := a.db.ListWorkerEvents(workerID, 20); err == nil {
			a.metrics.observePlatformEvent(*worker, latestPlatformEvent(coalesceEventFeed(events)))
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

func (a *API) handleVerification(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	var payload reporting.VerificationPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		respondError(w, r, http.StatusBadRequest, nil, "invalid payload")
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
	payload.RuntimePayload = payload.Normalize(observedAt)
	var vf verificationFailure
	aborted := verificationStatusAborted(payload.Status)
	failed := !payload.Passed && !aborted
	if failed {
		vf = classifyFailureMessage(payload.CheckType, payload.ErrorMessage)
		if payload.FailureClassification == nil {
			payload.FailureClassification = vf.Classification
		}
	}
	if payload.FailureDebug != nil && payload.FailureDebug.FailureClassification == nil && payload.FailureClassification != nil {
		classification := *payload.FailureClassification
		payload.FailureDebug.FailureClassification = &classification
	}

	if err := a.db.UpsertReportedWorker(payload.WorkerIdentity); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record worker")
		return
	}
	if err := a.db.UpdateWorkerRuntimeSnapshot(workerID, payload.RuntimePayload); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record runtime snapshot")
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
		respondError(w, r, http.StatusInternalServerError, err, "failed to record verification")
		return
	}
	if !aborted {
		if err := a.db.UpdateWorkerVerificationState(workerID, payload.Passed, payload.Summary); err != nil {
			respondError(w, r, http.StatusInternalServerError, err, "failed to update worker state")
			return
		}
	}

	details, err := json.Marshal(payload)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to encode event details")
		return
	}

	eventType := "verification_passed"
	message := payload.Summary
	switch {
	case aborted:
		eventType = "verification_aborted"
		if message == "" {
			message = "verification aborted"
		}
	case failed:
		eventType = "verification_failed"
		if message == "" {
			message = "verification failed"
		}
	case message == "":
		message = "verification passed"
	}

	if err := a.db.RecordEvent(workerID, eventType, message, string(details)); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record event")
		return
	}
	if failed && workerBeforeUpdate != nil && workerBeforeUpdate.Status != model.WorkerDegraded && workerBeforeUpdate.Status != model.WorkerDormant {
		_ = a.db.RecordEvent(workerID, "first_failure", "Worker transitioned from healthy to failing", string(details))
	}

	if workerBeforeUpdate != nil && workerBeforeUpdate.Status == model.WorkerProbing {
		if payload.Passed && !aborted {
			_ = a.db.RecordEvent(workerID, "worker_probe_passed", "Worker probe verification passed", "")
		} else if failed && a.manager != nil {
			signature := vf.Signature
			reason := fmt.Sprintf("worker probe failed with %s; returning to dormant state", signature)
			if err := a.manager.DormantWorker(r.Context(), workerID, reason, signature, "probe_failed"); err != nil {
				respondError(w, r, http.StatusInternalServerError, err, "failed to update worker state")
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

func verificationStatusAborted(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "aborted")
}

func (a *API) handleWorkerEvent(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	var payload reporting.WorkerEventPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		respondError(w, r, http.StatusBadRequest, nil, "invalid payload")
		return
	}
	payload.WorkerID = workerID
	if payload.Name == "" {
		payload.Name = workerID
	}
	if strings.TrimSpace(payload.EventType) == "" {
		respondError(w, r, http.StatusBadRequest, nil, "event_type is required")
		return
	}
	observedAt := payload.SentAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	payload.RuntimePayload = payload.Normalize(observedAt)

	if err := a.db.UpsertReportedWorker(payload.WorkerIdentity); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record worker")
		return
	}
	if err := a.db.UpdateWorkerRuntimeSnapshot(workerID, payload.RuntimePayload); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record runtime snapshot")
		return
	}

	details, err := json.Marshal(payload)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to encode event details")
		return
	}
	message := payload.Message
	if strings.TrimSpace(message) == "" {
		message = payload.EventType
	}
	if err := a.db.RecordEvent(workerID, payload.EventType, message, string(details)); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record event")
		return
	}
	switch strings.TrimSpace(payload.EventType) {
	case reporting.WorkerEventDiskFullNoProgress, reporting.WorkerEventDiskFullRecoveryFailed:
		if err := a.db.UpdateWorkerVerificationState(workerID, false, message); err != nil {
			respondError(w, r, http.StatusInternalServerError, err, "failed to update worker state")
			return
		}
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
