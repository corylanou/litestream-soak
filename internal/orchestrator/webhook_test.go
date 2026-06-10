package orchestrator

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebhookHandlerEmptySecretReturns503(t *testing.T) {
	t.Parallel()

	h := NewWebhookHandler("", nil, false)

	body := []byte(`{"ref":"refs/heads/main","after":"abc123","repository":{"full_name":"owner/repo"},"head_commit":{"message":"test"}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "webhook secret not configured") {
		t.Fatalf("body = %q, want to contain 'webhook secret not configured'", rec.Body.String())
	}
}

func TestWebhookHandlerValidSignatureAccepted(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	db := openTestDB(t)
	deployer := &Deployer{db: db}
	h := NewWebhookHandler(secret, deployer, false)

	body := []byte(`{"ref":"refs/heads/main","after":"abc123def456","repository":{"full_name":"owner/repo"},"head_commit":{"message":"test"}}`)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	sig := fmt.Sprintf("sha256=%s", hex.EncodeToString(mac.Sum(nil)))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-Hub-Signature-256", sig)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
}

func signWebhookBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return fmt.Sprintf("sha256=%s", hex.EncodeToString(mac.Sum(nil)))
}

func TestWebhookHandlerMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := NewWebhookHandler("test-secret", nil, false)

	req := httptest.NewRequest(http.MethodGet, "/webhooks/github", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestWebhookHandlerPingEvent(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	h := NewWebhookHandler(secret, nil, false)

	body := []byte(`{"zen":"keep it simple"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "ping")
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "pong") {
		t.Fatalf("body = %q, want to contain 'pong'", rec.Body.String())
	}
}

func TestWebhookHandlerIgnoresUnknownEvent(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	h := NewWebhookHandler(secret, nil, false)

	body := []byte(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "issues")
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestWebhookHandlerIgnoresNonMainPush(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	h := NewWebhookHandler(secret, nil, false)

	body := []byte(`{"ref":"refs/heads/feature","after":"abc123def456","repository":{"full_name":"owner/repo"},"head_commit":{"message":"test"}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "ignored") {
		t.Fatalf("body = %q, want to contain 'ignored'", rec.Body.String())
	}
}

func TestWebhookHandlerInvalidPayloadReturns400(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	h := NewWebhookHandler(secret, nil, false)

	body := []byte(`not json`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestWebhookHandlerMissingSignatureReturns401(t *testing.T) {
	t.Parallel()

	h := NewWebhookHandler("test-secret", nil, false)

	body := []byte(`{"ref":"refs/heads/main","after":"abc123","repository":{"full_name":"owner/repo"},"head_commit":{"message":"test"}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestWebhookHandlerInvalidSignatureReturns401(t *testing.T) {
	t.Parallel()

	h := NewWebhookHandler("test-secret", nil, false)

	body := []byte(`{"ref":"refs/heads/main","after":"abc123","repository":{"full_name":"owner/repo"},"head_commit":{"message":"test"}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-Hub-Signature-256", "sha256=badhex")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
