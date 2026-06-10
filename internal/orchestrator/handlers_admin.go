package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

const deploymentReadyRolloutTimeout = 2 * time.Hour

type deploymentReadyRequest struct {
	SHA           string `json:"sha"`
	LitestreamSHA string `json:"litestream_sha"`
	Source        string `json:"source"`
	ImageRef      string `json:"image_ref"`
	Trigger       string `json:"trigger"`
}

func (a *API) handleDeploymentReady(w http.ResponseWriter, r *http.Request) {
	if a.deployer == nil {
		respondError(w, r, http.StatusInternalServerError, nil, "deployer unavailable")
		return
	}

	request, err := readDeploymentReadyRequest(r)
	if err != nil {
		respondError(w, r, http.StatusBadRequest, err, "invalid payload")
		return
	}
	if strings.TrimSpace(request.SHA) == "" {
		respondError(w, r, http.StatusBadRequest, nil, "sha is required")
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

	rolloutCtx, cancel := context.WithTimeout(context.Background(), deploymentReadyRolloutTimeout)
	go func(request deploymentReadyRequest, source, trigger string) {
		defer cancel()

		imageRef, err := a.deployer.NotifyDeploymentReady(rolloutCtx, source, request.SHA, request.LitestreamSHA, request.ImageRef, trigger)
		if err != nil {
			slog.Error("Deployment ready rollout failed", "source", source, "sha", request.SHA, "litestream_sha", request.LitestreamSHA, "error", err)
			_ = a.db.RecordEvent("", "deploy_ready_failed", fmt.Sprintf("Rollout failed for %s / litestream %s: %v", shortVersionValue(request.SHA), shortVersionValue(request.LitestreamSHA), err), imageRef)
			return
		}
		a.observeLatestDeploymentState(source)
		slog.Info("Deployment ready rollout complete", "source", source, "sha", request.SHA, "litestream_sha", request.LitestreamSHA, "image", imageRef, "trigger", trigger)
	}(request, source, trigger)

	w.WriteHeader(http.StatusAccepted)
	writeAPIJSON(w, map[string]any{
		"sha":            request.SHA,
		"litestream_sha": request.LitestreamSHA,
		"source":         source,
		"image_ref":      request.ImageRef,
		"trigger":        trigger,
		"accepted":       true,
	})
}

func (a *API) handleRollWorker(w http.ResponseWriter, r *http.Request) {
	if a.manager == nil {
		respondError(w, r, http.StatusInternalServerError, nil, "roll manager unavailable")
		return
	}

	workerID := strings.TrimSpace(r.PathValue("id"))
	if workerID == "" {
		respondError(w, r, http.StatusBadRequest, nil, "worker id is required")
		return
	}

	request, err := readDeploymentReadyRequest(r)
	if err != nil {
		respondError(w, r, http.StatusBadRequest, err, "invalid payload")
		return
	}
	if strings.TrimSpace(request.SHA) == "" {
		respondError(w, r, http.StatusBadRequest, nil, "sha is required")
		return
	}
	if strings.TrimSpace(request.LitestreamSHA) == "" {
		respondError(w, r, http.StatusBadRequest, nil, "litestream_sha is required")
		return
	}

	worker, err := a.db.GetWorker(workerID)
	if err != nil {
		respondError(w, r, http.StatusNotFound, err, "worker not found")
		return
	}
	source := strings.TrimSpace(request.Source)
	if source == "" {
		source = worker.Source
	}
	if source == "" {
		source = "main"
	}
	if worker.Source != "" && source != worker.Source {
		respondError(w, r, http.StatusBadRequest, nil, fmt.Sprintf("worker %s belongs to source %s, not %s", workerID, worker.Source, source))
		return
	}

	trigger := strings.TrimSpace(request.Trigger)
	if trigger == "" {
		trigger = "manual_worker_roll"
	}
	imageRef := strings.TrimSpace(request.ImageRef)
	if imageRef == "" {
		imageRef, err = a.manager.currentWorkerImage(r.Context())
		if err != nil {
			respondError(w, r, http.StatusInternalServerError, err, "failed to resolve worker image")
			return
		}
	}

	if err := a.db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        strings.TrimSpace(request.SHA),
		LitestreamSHA: strings.TrimSpace(request.LitestreamSHA),
		ImageRef:      imageRef,
		Source:        source,
		PRNumber:      sourcePRNumber(source),
		Status:        "ready",
	}); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to record ready deployment")
		return
	}

	message := fmt.Sprintf("Targeted rollout for %s to soak %s / litestream %s via %s", workerID, shortVersionValue(request.SHA), shortVersionValue(request.LitestreamSHA), trigger)
	_ = a.db.RecordEvent(workerID, "targeted_rollout_requested", message, imageRef)

	rolloutCtx, cancel := context.WithTimeout(context.Background(), deploymentReadyRolloutTimeout)
	go func(request deploymentReadyRequest, imageRef, source, workerID string) {
		defer cancel()
		if _, err := a.manager.RollWorker(rolloutCtx, workerID, imageRef, request.SHA, request.LitestreamSHA); err != nil {
			slog.Error("Targeted worker rollout failed", "worker_id", workerID, "sha", request.SHA, "litestream_sha", request.LitestreamSHA, "error", err)
			_ = a.db.RecordEvent(workerID, "targeted_rollout_failed", fmt.Sprintf("Targeted rollout failed for %s / litestream %s: %v", shortVersionValue(request.SHA), shortVersionValue(request.LitestreamSHA), err), imageRef)
			return
		}
		a.observeLatestDeploymentState(source)
		slog.Info("Targeted worker rollout complete", "worker_id", workerID, "source", source, "sha", request.SHA, "litestream_sha", request.LitestreamSHA, "image", imageRef)
	}(request, imageRef, source, workerID)

	w.WriteHeader(http.StatusAccepted)
	writeAPIJSON(w, map[string]any{
		"worker_id":      workerID,
		"sha":            request.SHA,
		"litestream_sha": request.LitestreamSHA,
		"source":         source,
		"image_ref":      imageRef,
		"trigger":        trigger,
		"accepted":       true,
	})
}

func (a *API) handleResumeDormantWorkers(w http.ResponseWriter, r *http.Request) {
	if a.manager == nil {
		respondError(w, r, http.StatusInternalServerError, nil, "resume manager unavailable")
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
			respondError(w, r, http.StatusInternalServerError, err, "failed to resolve worker image")
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
		respondError(w, r, http.StatusInternalServerError, err, "failed to list dormant workers")
		return
	}

	if err := a.manager.ResumeDormantWorkers(r.Context(), source, imageRef, sha, litestreamSHA, trigger); err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to resume dormant workers")
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

func (a *API) handlePauseSourceWorkers(w http.ResponseWriter, r *http.Request) {
	if a.manager == nil {
		respondError(w, r, http.StatusInternalServerError, nil, "pause manager unavailable")
		return
	}

	source := strings.TrimSpace(r.URL.Query().Get("source"))
	if source == "" {
		source = "main"
	}
	reason := strings.TrimSpace(r.URL.Query().Get("reason"))
	if reason == "" {
		reason = fmt.Sprintf("%s manually paused until the next deployment", sourceHumanLabel(source))
	}
	signature := strings.TrimSpace(r.URL.Query().Get("signature"))
	if signature == "" {
		signature = "manual_source_pause"
	}
	trigger := strings.TrimSpace(r.URL.Query().Get("trigger"))
	if trigger == "" {
		trigger = "manual_pause"
	}

	pausedWorkerIDs, err := a.manager.PauseSourceWorkers(r.Context(), source, reason, signature, trigger)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to pause workers")
		return
	}

	_ = a.db.RecordEvent("", "manual_source_pause_requested", fmt.Sprintf("Paused %d active %s worker(s) until next deploy", len(pausedWorkerIDs), source), strings.Join(pausedWorkerIDs, ","))
	a.observeLatestDeploymentState(source)

	writeAPIJSON(w, map[string]any{
		"paused_workers": len(pausedWorkerIDs),
		"worker_ids":     pausedWorkerIDs,
		"source":         source,
		"reason":         reason,
		"signature":      signature,
		"trigger":        trigger,
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
