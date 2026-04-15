package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

type Reporter struct {
	baseURL  string
	client   *http.Client
	identity reporting.WorkerIdentity
}

func NewReporter(cfg Config) *Reporter {
	baseURL := strings.TrimRight(cfg.ControlBaseURL, "/")
	if baseURL == "" {
		return nil
	}

	return &Reporter{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		identity: reporting.WorkerIdentity{
			WorkerID:      cfg.WorkerID,
			Name:          cfg.WorkerName,
			Source:        cfg.Source,
			GitSHA:        cfg.GitSHA,
			LitestreamSHA: cfg.LitestreamSHA,
			ProfileName:   cfg.ProfileName,
			ProfileConfig: cfg.WorkloadConfig().JSON(),
			AppName:       cfg.AppName,
			MachineID:     cfg.MachineID,
			Region:        cfg.Region,
		},
	}
}

func (r *Reporter) Enabled() bool {
	return r != nil && r.baseURL != ""
}

func (r *Reporter) SendHeartbeat(ctx context.Context, payload reporting.HeartbeatPayload) error {
	if !r.Enabled() {
		return nil
	}

	payload.WorkerIdentity = r.identity
	if payload.SentAt.IsZero() {
		payload.SentAt = time.Now().UTC()
	}

	return r.postJSON(ctx, "/api/workers/"+url.PathEscape(r.identity.WorkerID)+"/heartbeat", payload)
}

func (r *Reporter) SendVerification(ctx context.Context, payload reporting.VerificationPayload) error {
	if !r.Enabled() {
		return nil
	}

	payload.WorkerIdentity = r.identity
	return r.postJSON(ctx, "/api/workers/"+url.PathEscape(r.identity.WorkerID)+"/verifications", payload)
}

func (r *Reporter) postJSON(ctx context.Context, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("request failed with status %d", resp.StatusCode)
	}

	return nil
}
