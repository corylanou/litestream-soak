package orchestrator

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	maxWebhookBodyBytes              = 1 << 20
	sourceRetirementTimeout          = 10 * time.Minute
	sourceRetirementMaxAttempts      = 3
	defaultPRRetirementRetryInterval = 30 * time.Second
	defaultPRKeepAliveLabel          = "soak:keep-alive"
)

type WebhookHandlerConfig struct {
	Context                   context.Context
	Secret                    string
	DeployEnabled             bool
	PRRepositoryAllowlist     []string
	PRKeepAliveLabel          string
	PRRetirementRetryInterval time.Duration
}

type WebhookHandler struct {
	backgroundContext         context.Context
	secret                    string
	deployer                  *Deployer
	manager                   *Manager
	deployEnabled             bool
	prRepositoryAllowlist     []string
	prKeepAliveLabel          string
	prRetirementRetryInterval time.Duration
	retirementTasks           sync.WaitGroup
}

func NewWebhookHandler(config WebhookHandlerConfig, deployer *Deployer, manager *Manager) *WebhookHandler {
	if config.Secret == "" {
		slog.Warn("GITHUB_WEBHOOK_SECRET is not set; refusing all webhook deliveries")
	}
	keepAliveLabel := strings.TrimSpace(config.PRKeepAliveLabel)
	if keepAliveLabel == "" {
		keepAliveLabel = defaultPRKeepAliveLabel
	}
	backgroundContext := config.Context
	if backgroundContext == nil {
		backgroundContext = context.Background()
	}
	retryInterval := config.PRRetirementRetryInterval
	if retryInterval <= 0 {
		retryInterval = defaultPRRetirementRetryInterval
	}
	return &WebhookHandler{
		backgroundContext:         backgroundContext,
		secret:                    config.Secret,
		deployer:                  deployer,
		manager:                   manager,
		deployEnabled:             config.DeployEnabled,
		prRepositoryAllowlist:     normalizedRepositoryAllowlist(config.PRRepositoryAllowlist),
		prKeepAliveLabel:          keepAliveLabel,
		prRetirementRetryInterval: retryInterval,
	}
}

func (h *WebhookHandler) WaitForRetirements() {
	h.retirementTasks.Wait()
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBodyBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	if h.secret == "" {
		http.Error(w, "webhook secret not configured", http.StatusServiceUnavailable)
		return
	}
	sig := r.Header.Get("X-Hub-Signature-256")
	if !h.verifySignature(body, sig) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	event := r.Header.Get("X-GitHub-Event")
	slog.Info("Received GitHub webhook", "event", event)

	switch event {
	case "push":
		h.handlePush(w, body)
	case "pull_request":
		h.handlePullRequest(w, body)
	case "ping":
		w.WriteHeader(http.StatusOK)
		if _, err := fmt.Fprintln(w, "pong"); err != nil {
			slog.Debug("Failed to write webhook response", "error", err)
		}
	default:
		slog.Info("Ignoring webhook event", "event", event)
		w.WriteHeader(http.StatusOK)
	}
}

type pullRequestPayload struct {
	Action     string `json:"action"`
	Number     int    `json:"number"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	PullRequest struct {
		Merged bool `json:"merged"`
		Labels []struct {
			Name string `json:"name"`
		} `json:"labels"`
	} `json:"pull_request"`
}

func (h *WebhookHandler) handlePullRequest(w http.ResponseWriter, body []byte) {
	var payload pullRequestPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	if payload.Action != "closed" {
		slog.Info("Ignoring non-terminal pull request event", "action", payload.Action)
		writeWebhookMessage(w, http.StatusOK, "ignored: pull request is not closed")
		return
	}

	repository := strings.TrimSpace(payload.Repository.FullName)
	if !repositoryAllowed(repository, h.prRepositoryAllowlist) {
		slog.Warn("Ignoring pull request event from non-allowlisted repository", "repo", repository)
		writeWebhookMessage(w, http.StatusOK, "ignored: repository is not allowlisted")
		return
	}

	if payload.Number <= 0 {
		http.Error(w, "invalid pull request number", http.StatusBadRequest)
		return
	}

	if pullRequestHasLabel(payload, h.prKeepAliveLabel) {
		message := fmt.Sprintf("Kept %s#%d active after the upstream PR closed because it has label %s", repository, payload.Number, h.prKeepAliveLabel)
		slog.Info(message)
		h.recordWebhookEvent("upstream_pr_retirement_skipped", message, "keep_alive_label")
		writeWebhookMessage(w, http.StatusOK, "ignored: pull request has keep-alive label")
		return
	}

	if h.manager == nil {
		http.Error(w, "source retirement unavailable", http.StatusServiceUnavailable)
		return
	}

	source := fmt.Sprintf("pr-%d", payload.Number)
	state := "closed"
	if payload.PullRequest.Merged {
		state = "merged"
	}
	message := fmt.Sprintf("Upstream PR %s#%d %s; retiring %s", repository, payload.Number, state, source)
	h.recordWebhookEvent("upstream_pr_terminal_received", message, source)
	h.retirementTasks.Add(1)
	go func() {
		defer h.retirementTasks.Done()
		h.retirePullRequestSource(payload)
	}()

	writeWebhookMessage(w, http.StatusAccepted, fmt.Sprintf("retiring %s", source))
}

func (h *WebhookHandler) retirePullRequestSource(payload pullRequestPayload) {
	ctx, cancel := context.WithTimeout(h.backgroundContext, sourceRetirementTimeout)
	defer cancel()

	repository := strings.TrimSpace(payload.Repository.FullName)
	var result SourceTeardownResponse
	var err error
	for attempt := 1; attempt <= sourceRetirementMaxAttempts; attempt++ {
		result, err = h.manager.archiveAndDestroyTerminalPRSource(ctx, repository, payload.Number, payload.PullRequest.Merged)
		if errors.Is(err, errSourceNotFound) || errors.Is(err, errSourceDeploymentNotFound) {
			slog.Info("No active soak source found for terminal upstream PR", "repo", repository, "pr_number", payload.Number, "error", err)
			return
		}
		if err == nil && result.Complete {
			return
		}
		if attempt == sourceRetirementMaxAttempts {
			break
		}
		slog.Warn("Retrying incomplete upstream PR source teardown", "repo", repository, "pr_number", payload.Number, "attempt", attempt+1, "error", err, "failed_workers", result.FailedWorkers)
		if !waitForRetirementRetry(ctx, h.prRetirementRetryInterval) {
			break
		}
	}

	if err != nil {
		slog.Error("Failed to retire soak source for terminal upstream PR", "repo", repository, "pr_number", payload.Number, "error", err)
		h.recordWebhookEvent("upstream_pr_source_teardown_failed", fmt.Sprintf("Failed to retire pr-%d after %s#%d closed", payload.Number, repository, payload.Number), err.Error())
		return
	}
	if !result.Complete {
		slog.Error("Upstream PR source teardown incomplete", "repo", repository, "pr_number", payload.Number, "failed_workers", result.FailedWorkers)
		h.recordWebhookEvent("upstream_pr_source_teardown_failed", fmt.Sprintf("Failed to retire pr-%d after %s#%d closed", payload.Number, repository, payload.Number), fmt.Sprintf("failed_workers=%d", result.FailedWorkers))
	}
}

func waitForRetirementRetry(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (h *WebhookHandler) recordWebhookEvent(eventType, message, details string) {
	if h.manager != nil && h.manager.db != nil {
		if err := h.manager.db.RecordEvent("", eventType, message, details); err != nil {
			slog.Warn("Failed to record GitHub webhook event", "event_type", eventType, "error", err)
		}
		return
	}
	if h.deployer != nil && h.deployer.db != nil {
		if err := h.deployer.db.RecordEvent("", eventType, message, details); err != nil {
			slog.Warn("Failed to record GitHub webhook event", "event_type", eventType, "error", err)
		}
	}
}

func pullRequestHasLabel(payload pullRequestPayload, label string) bool {
	for _, candidate := range payload.PullRequest.Labels {
		if strings.EqualFold(strings.TrimSpace(candidate.Name), label) {
			return true
		}
	}
	return false
}

func normalizedRepositoryAllowlist(repositories []string) []string {
	allowlist := make([]string, 0, len(repositories))
	for _, repository := range repositories {
		if repository = strings.TrimSpace(repository); repository != "" {
			allowlist = append(allowlist, repository)
		}
	}
	return allowlist
}

func repositoryAllowed(repository string, allowlist []string) bool {
	for _, allowed := range allowlist {
		if strings.EqualFold(repository, allowed) {
			return true
		}
	}
	return false
}

func writeWebhookMessage(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	if _, err := fmt.Fprintln(w, message); err != nil {
		slog.Debug("Failed to write webhook response", "error", err)
	}
}

type pushPayload struct {
	Ref        string `json:"ref"`
	After      string `json:"after"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	HeadCommit struct {
		Message string `json:"message"`
	} `json:"head_commit"`
}

func (h *WebhookHandler) handlePush(w http.ResponseWriter, body []byte) {
	var payload pushPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	if payload.Ref != "refs/heads/main" {
		slog.Info("Ignoring push to non-main branch", "ref", payload.Ref)
		w.WriteHeader(http.StatusOK)
		if _, err := fmt.Fprintln(w, "ignored: not main branch"); err != nil {
			slog.Debug("Failed to write webhook response", "error", err)
		}
		return
	}

	sha := payload.After
	slog.Info("Push to main detected",
		"repo", payload.Repository.FullName,
		"sha", sha,
		"message", payload.HeadCommit.Message,
	)
	if h.deployer != nil {
		_ = h.deployer.db.RecordEvent("", "github_push_received", fmt.Sprintf("Push received for %s on main", trimSHA(sha)), payload.HeadCommit.Message)
	}

	if !h.deployEnabled {
		slog.Info("Webhook deploy disabled; awaiting external CI", "sha", sha)
		if h.deployer != nil {
			_ = h.deployer.db.RecordEvent("", "github_push_awaiting_ci", "Push acknowledged; awaiting external deploy automation", sha)
		}
		w.WriteHeader(http.StatusAccepted)
		if _, err := fmt.Fprintln(w, "acknowledged: awaiting external deploy automation"); err != nil {
			slog.Debug("Failed to write webhook response", "error", err)
		}
		return
	}

	go func() {
		if err := h.deployer.DeployNewSHA(sha); err != nil {
			slog.Error("Deploy failed", "sha", sha, "error", err)
		}
	}()

	w.WriteHeader(http.StatusAccepted)
	if _, err := fmt.Fprintf(w, "deploying %s\n", sha); err != nil {
		slog.Debug("Failed to write webhook response", "error", err)
	}
}

func (h *WebhookHandler) verifySignature(body []byte, signature string) bool {
	if !strings.HasPrefix(signature, "sha256=") {
		return false
	}

	sig, err := hex.DecodeString(strings.TrimPrefix(signature, "sha256="))
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(h.secret))
	mac.Write(body)
	expected := mac.Sum(nil)

	return hmac.Equal(sig, expected)
}
