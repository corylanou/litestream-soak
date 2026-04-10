package orchestrator

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

type WebhookHandler struct {
	secret  string
	manager *Manager
	deployer *Deployer
}

func NewWebhookHandler(secret string, manager *Manager, deployer *Deployer) *WebhookHandler {
	return &WebhookHandler{
		secret:   secret,
		manager:  manager,
		deployer: deployer,
	}
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	if h.secret != "" {
		sig := r.Header.Get("X-Hub-Signature-256")
		if !h.verifySignature(body, sig) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	event := r.Header.Get("X-GitHub-Event")
	slog.Info("Received GitHub webhook", "event", event)

	switch event {
	case "push":
		h.handlePush(w, body)
	case "ping":
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "pong")
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
		fmt.Fprintln(w, "ignored: not main branch")
		return
	}

	sha := payload.After
	slog.Info("Push to main detected",
		"repo", payload.Repository.FullName,
		"sha", sha,
		"message", payload.HeadCommit.Message,
	)

	go func() {
		if err := h.deployer.DeployNewSHA(sha); err != nil {
			slog.Error("Deploy failed", "sha", sha, "error", err)
		}
	}()

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "deploying %s\n", sha)
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
