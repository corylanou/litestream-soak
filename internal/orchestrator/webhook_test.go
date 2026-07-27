package orchestrator

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestWebhookHandlerEmptySecretReturns503(t *testing.T) {
	t.Parallel()

	h := NewWebhookHandler(WebhookHandlerConfig{}, nil, nil)

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
	h := NewWebhookHandler(WebhookHandlerConfig{Secret: secret}, deployer, nil)

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

	h := NewWebhookHandler(WebhookHandlerConfig{Secret: "test-secret"}, nil, nil)

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
	h := NewWebhookHandler(WebhookHandlerConfig{Secret: secret}, nil, nil)

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
	h := NewWebhookHandler(WebhookHandlerConfig{Secret: secret}, nil, nil)

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
	h := NewWebhookHandler(WebhookHandlerConfig{Secret: secret}, nil, nil)

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
	h := NewWebhookHandler(WebhookHandlerConfig{Secret: secret}, nil, nil)

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

	h := NewWebhookHandler(WebhookHandlerConfig{Secret: "test-secret"}, nil, nil)

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

	h := NewWebhookHandler(WebhookHandlerConfig{Secret: "test-secret"}, nil, nil)

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

func TestWebhookHandlerRejectsOversizedBody(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	h := NewWebhookHandler(WebhookHandlerConfig{Secret: secret}, nil, nil)
	body := bytes.Repeat([]byte("x"), 1<<20+1)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "ping")
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestWebhookHandlerIgnoresClosedPullRequestFromNonAllowlistedRepository(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	h := NewWebhookHandler(WebhookHandlerConfig{
		Secret:                secret,
		PRRepositoryAllowlist: []string{"benbjohnson/litestream"},
	}, nil, nil)
	body := []byte(`{"action":"closed","number":160,"repository":{"full_name":"other/litestream"},"pull_request":{"merged":true,"labels":[]}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "not allowlisted") {
		t.Fatalf("body = %q, want repository allowlist response", rec.Body.String())
	}
}

func TestWebhookHandlerKeepsLabeledTerminalPullRequest(t *testing.T) {
	t.Parallel()

	secret := "test-secret"
	h := NewWebhookHandler(WebhookHandlerConfig{
		Secret:                secret,
		PRRepositoryAllowlist: []string{"benbjohnson/litestream"},
		PRKeepAliveLabel:      "soak:keep-alive",
	}, nil, nil)
	body := []byte(`{"action":"closed","number":1324,"repository":{"full_name":"benbjohnson/litestream"},"pull_request":{"merged":false,"labels":[{"name":"SOAK:KEEP-ALIVE"}]}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "keep-alive") {
		t.Fatalf("body = %q, want keep-alive response", rec.Body.String())
	}
}

func TestWebhookHandlerRetiresTerminalPullRequestFleetWithFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		prNumber      int
		merged        bool
		archiveReason string
	}{
		{name: "merged", prNumber: 160, merged: true, archiveReason: "upstream_pr_merged"},
		{name: "closed", prNumber: 161, merged: false, archiveReason: "upstream_pr_closed"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			source := fmt.Sprintf("pr-%d", test.prNumber)
			workerID := fmt.Sprintf("worker-%s-failed", source)
			machineID := fmt.Sprintf("machine-%s-failed", source)
			volumeID := fmt.Sprintf("volume-%s-failed", source)
			createTeardownTestDeployment(t, db, source)
			createTestWorker(t, db, model.Worker{
				ID:            workerID,
				AppName:       "litestream-soak",
				Name:          workerID,
				Status:        model.WorkerDegraded,
				Source:        source,
				GitSHA:        "soak-sha",
				LitestreamSHA: "litestream-sha",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
				FlyMachineID:  machineID,
				FlyVolumeID:   volumeID,
			})
			mustRecordVerification(t, db, &model.Verification{
				WorkerID:     workerID,
				StartedAt:    time.Now().UTC(),
				Status:       "failed",
				CheckType:    "restore",
				ErrorMessage: "integrity check failed",
			})

			volumeFailures := map[string]int(nil)
			if test.merged {
				volumeFailures = map[string]int{volumeID: 1}
			}
			fly := newTeardownTestServer(t, volumeFailures)
			manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{
				Bucket:    "bucket",
				Endpoint:  fly.server.URL,
				AccessKey: "access",
				SecretKey: "secret",
				Region:    "auto",
			}, "", "")
			secret := "test-secret"
			h := NewWebhookHandler(WebhookHandlerConfig{
				Secret:                    secret,
				PRRepositoryAllowlist:     []string{"benbjohnson/litestream"},
				PRRetirementRetryInterval: time.Millisecond,
			}, nil, manager)
			body, err := json.Marshal(map[string]any{
				"action": "closed",
				"number": test.prNumber,
				"repository": map[string]any{
					"full_name": "benbjohnson/litestream",
				},
				"pull_request": map[string]any{
					"merged": test.merged,
					"labels": []any{},
				},
			})
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(body))
			req.Header.Set("X-GitHub-Event", "pull_request")
			req.Header.Set("X-Hub-Signature-256", signWebhookBody(secret, body))
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body.String())
			}
			h.WaitForRetirements()

			archives, err := db.ListRunArchives(source, runArchiveTypeTeardown, 10)
			if err != nil {
				t.Fatalf("ListRunArchives() error = %v", err)
			}
			if len(archives) != 1 {
				t.Fatalf("archive count = %d, want 1", len(archives))
			}
			var payload runArchivePayload
			if err := json.Unmarshal([]byte(archives[0].Payload), &payload); err != nil {
				t.Fatalf("json.Unmarshal() archive error = %v", err)
			}
			if payload.Reason != test.archiveReason {
				t.Fatalf("archive reason = %q, want %q", payload.Reason, test.archiveReason)
			}
			if len(payload.Workers) != 1 || payload.Workers[0].LatestFailure == nil {
				t.Fatalf("archived worker evidence = %+v, want failed verification", payload.Workers)
			}
			if !fly.machineDestroyed(machineID) {
				t.Fatalf("machine %s was not destroyed", machineID)
			}
			if !fly.volumeDestroyed(volumeID) {
				t.Fatalf("volume %s was not destroyed", volumeID)
			}
			worker, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker(%q) error = %v", workerID, err)
			}
			if worker.Status != model.WorkerStopped {
				t.Fatalf("worker status = %q, want %q", worker.Status, model.WorkerStopped)
			}
		})
	}
}
