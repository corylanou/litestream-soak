package orchestrator

import (
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
)

const maxWebhookBodyBytes = 1 << 20

type WebhookHandler struct {
	secret        string
	deployer      *Deployer
	deployEnabled bool
}

func NewWebhookHandler(secret string, deployer *Deployer, deployEnabled bool) *WebhookHandler {
	if secret == "" {
		slog.Warn("GITHUB_WEBHOOK_SECRET is not set; refusing all webhook deliveries")
	}
	return &WebhookHandler{
		secret:        secret,
		deployer:      deployer,
		deployEnabled: deployEnabled,
	}
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
