package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type AlertDispatcher struct {
	db          *model.DB
	baseURL     string
	webhookURL  string
	bearerToken string
	client      *http.Client
}

type alertWebhookPayload struct {
	Text  string             `json:"text"`
	Alert alertWebhookDetail `json:"alert"`
}

type alertWebhookDetail struct {
	AlertID          int64                      `json:"alert_id"`
	AlertType        string                     `json:"alert_type"`
	Severity         string                     `json:"severity"`
	GeneratedAt      time.Time                  `json:"generated_at"`
	FailureStage     string                     `json:"failure_stage,omitempty"`
	FailureSignature string                     `json:"failure_signature,omitempty"`
	Message          string                     `json:"message"`
	Worker           *model.Worker              `json:"worker,omitempty"`
	Deployment       *model.Deployment          `json:"deployment,omitempty"`
	Rollout          *DeploymentRolloutResponse `json:"rollout,omitempty"`
	Verification     *model.Verification        `json:"verification,omitempty"`
	TriageCommands   []string                   `json:"triage_commands,omitempty"`
	URLs             map[string]string          `json:"urls"`
}

var (
	controlAlertTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_control_alert_total",
		Help: "Total control-plane alert delivery attempts by result.",
	}, []string{"worker_id", "alert_type", "result"})

	controlWorkerLastAlert = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_last_alert_unixtime",
		Help: "Unix timestamp of the most recent alert sent by the control plane.",
	}, []string{"worker_id", "alert_type"})
)

func NewAlertDispatcher(db *model.DB, baseURL, webhookURL, bearerToken string) *AlertDispatcher {
	return &AlertDispatcher{
		db:          db,
		baseURL:     strings.TrimRight(baseURL, "/"),
		webhookURL:  strings.TrimSpace(webhookURL),
		bearerToken: strings.TrimSpace(bearerToken),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (d *AlertDispatcher) Enabled() bool {
	return d != nil && d.webhookURL != ""
}

func (d *AlertDispatcher) NotifyVerificationFailure(worker model.Worker, verification model.Verification) {
	if !d.Enabled() || verification.Passed {
		return
	}
	if !d.shouldAlertVerificationFailure(worker.ID, verification) {
		controlAlertTotal.WithLabelValues(worker.ID, "verification_failed", "skipped").Inc()
		return
	}

	go d.notifyVerificationFailure(worker, verification)
}

func (d *AlertDispatcher) NotifyWorkerStale(worker model.Worker) {
	if !d.Enabled() {
		return
	}

	go d.notifyWorkerStale(worker)
}

func (d *AlertDispatcher) NotifyDeploymentAttention(rollout DeploymentRolloutResponse) {
	if !d.Enabled() || !rolloutAttentionAlertable(rollout) {
		return
	}

	go d.notifyDeploymentAttention(rollout)
}

func (d *AlertDispatcher) notifyVerificationFailure(worker model.Worker, verification model.Verification) {
	fingerprint := fmt.Sprintf("verification_failed:%d", verification.ID)
	delivery := &model.AlertDelivery{
		WorkerID:         worker.ID,
		VerificationID:   verification.ID,
		AlertType:        "verification_failed",
		Fingerprint:      fingerprint,
		Status:           "pending",
		FailureStage:     inferFailureStage(&verification),
		FailureSignature: inferFailureSignature(&verification),
		Message:          firstMeaningfulLine(verification.ErrorMessage),
	}

	id, created, err := d.db.CreateAlert(delivery)
	if err != nil {
		slog.Error("Failed to create alert record", "worker_id", worker.ID, "alert_type", delivery.AlertType, "error", err)
		return
	}
	if !created {
		controlAlertTotal.WithLabelValues(worker.ID, delivery.AlertType, "duplicate").Inc()
		return
	}

	payload := d.buildVerificationPayload(id, worker, verification)
	d.sendAlert(id, worker.ID, delivery.AlertType, payload)
}

func (d *AlertDispatcher) notifyWorkerStale(worker model.Worker) {
	lastHeartbeat := "unknown"
	if worker.LastHeartbeatAt != nil && !worker.LastHeartbeatAt.IsZero() {
		lastHeartbeat = strconvFormatInt(worker.LastHeartbeatAt.Unix())
	}

	fingerprint := fmt.Sprintf("worker_stale:%s:%s", worker.ID, lastHeartbeat)
	delivery := &model.AlertDelivery{
		WorkerID:         worker.ID,
		AlertType:        "worker_stale",
		Fingerprint:      fingerprint,
		Status:           "pending",
		FailureStage:     "heartbeat",
		FailureSignature: "worker_stale",
		Message:          "worker missed heartbeat deadline",
	}

	id, created, err := d.db.CreateAlert(delivery)
	if err != nil {
		slog.Error("Failed to create stale alert record", "worker_id", worker.ID, "error", err)
		return
	}
	if !created {
		controlAlertTotal.WithLabelValues(worker.ID, delivery.AlertType, "duplicate").Inc()
		return
	}

	payload := d.buildStalePayload(id, worker)
	d.sendAlert(id, worker.ID, delivery.AlertType, payload)
}

func (d *AlertDispatcher) notifyDeploymentAttention(rollout DeploymentRolloutResponse) {
	fingerprint := fmt.Sprintf("deployment_attention:%s:%s:%s", rollout.Deployment.Source, rollout.Deployment.GitSHA, rollout.Status)
	delivery := &model.AlertDelivery{
		AlertType:        "deployment_attention",
		Fingerprint:      fingerprint,
		Status:           "pending",
		FailureStage:     "deployment",
		FailureSignature: rollout.Status,
		Message:          rollout.NextAction,
	}

	id, created, err := d.db.CreateAlert(delivery)
	if err != nil {
		slog.Error("Failed to create deployment alert record", "sha", rollout.Deployment.GitSHA, "status", rollout.Status, "error", err)
		return
	}
	if !created {
		controlAlertTotal.WithLabelValues("", delivery.AlertType, "duplicate").Inc()
		return
	}

	payload := d.buildDeploymentPayload(id, rollout)
	d.sendAlert(id, "", delivery.AlertType, payload)
}

func (d *AlertDispatcher) shouldAlertVerificationFailure(workerID string, current model.Verification) bool {
	verifications, err := d.db.ListVerifications(workerID, 2)
	if err != nil || len(verifications) < 2 {
		return true
	}

	previous := verifications[1]
	if previous.Passed && previous.Status != "failed" {
		return true
	}

	return inferFailureStage(&previous) != inferFailureStage(&current) ||
		inferFailureSignature(&previous) != inferFailureSignature(&current)
}

func (d *AlertDispatcher) buildVerificationPayload(id int64, worker model.Worker, verification model.Verification) alertWebhookPayload {
	stage := inferFailureStage(&verification)
	signature := inferFailureSignature(&verification)
	text := fmt.Sprintf(
		"Litestream soak alert: %s failed %s (%s)",
		worker.ID,
		valueOrUnknown(stage),
		valueOrUnknown(signature),
	)

	return alertWebhookPayload{
		Text: text,
		Alert: alertWebhookDetail{
			AlertID:          id,
			AlertType:        "verification_failed",
			Severity:         "critical",
			GeneratedAt:      time.Now().UTC(),
			FailureStage:     stage,
			FailureSignature: signature,
			Message:          firstMeaningfulLine(verification.ErrorMessage),
			Worker:           &worker,
			Verification:     &verification,
			TriageCommands:   buildTriageCommands(worker, worker.FlyMachineID != ""),
			URLs:             d.alertURLs(worker.ID),
		},
	}
}

func (d *AlertDispatcher) buildStalePayload(id int64, worker model.Worker) alertWebhookPayload {
	text := fmt.Sprintf("Litestream soak alert: %s missed its heartbeat deadline", worker.ID)

	return alertWebhookPayload{
		Text: text,
		Alert: alertWebhookDetail{
			AlertID:          id,
			AlertType:        "worker_stale",
			Severity:         "warning",
			GeneratedAt:      time.Now().UTC(),
			FailureStage:     "heartbeat",
			FailureSignature: "worker_stale",
			Message:          "worker missed heartbeat deadline",
			Worker:           &worker,
			TriageCommands:   buildTriageCommands(worker, worker.FlyMachineID != ""),
			URLs:             d.alertURLs(worker.ID),
		},
	}
}

func (d *AlertDispatcher) buildDeploymentPayload(id int64, rollout DeploymentRolloutResponse) alertWebhookPayload {
	text := fmt.Sprintf(
		"Litestream soak rollout alert: %s is %s after %s",
		trimSHA(rollout.Deployment.GitSHA),
		rollout.Status,
		rolloutAttentionGraceWindow,
	)

	return alertWebhookPayload{
		Text: text,
		Alert: alertWebhookDetail{
			AlertID:          id,
			AlertType:        "deployment_attention",
			Severity:         "warning",
			GeneratedAt:      time.Now().UTC(),
			FailureStage:     "deployment",
			FailureSignature: rollout.Status,
			Message:          rollout.NextAction,
			Deployment:       &rollout.Deployment,
			Rollout:          &rollout,
			TriageCommands:   buildDeploymentTriageCommands(),
			URLs:             d.deploymentAlertURLs(),
		},
	}
}

func (d *AlertDispatcher) alertURLs(workerID string) map[string]string {
	urls := map[string]string{}
	if d.baseURL == "" {
		return urls
	}

	urls["worker_ui"] = d.baseURL + "/ui/workers/" + workerID
	urls["incident_api"] = d.baseURL + "/api/workers/" + workerID + "/incident"
	urls["prompt_api"] = d.baseURL + "/api/workers/" + workerID + "/prompt"
	urls["alerts_api"] = d.baseURL + "/api/alerts"
	return urls
}

func (d *AlertDispatcher) deploymentAlertURLs() map[string]string {
	urls := map[string]string{}
	if d.baseURL == "" {
		return urls
	}

	urls["control_ui"] = d.baseURL + "/ui"
	urls["deployment_api"] = d.baseURL + "/api/deployments/latest"
	urls["diagnosis_api"] = d.baseURL + "/api/diagnosis"
	urls["events_api"] = d.baseURL + "/api/events"
	urls["alerts_api"] = d.baseURL + "/api/alerts"
	return urls
}

func rolloutAttentionAlertable(rollout DeploymentRolloutResponse) bool {
	if !rollout.GraceWindowExceeded {
		return false
	}

	switch rollout.Status {
	case "rolling_out", "probing", "needs_attention":
		return true
	default:
		return false
	}
}

func buildDeploymentTriageCommands() []string {
	return []string{
		`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/deployments/latest | jq .`,
		`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/diagnosis | jq .`,
		`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/events?limit=20 | jq .`,
		"fly machines list -a litestream-soak",
	}
}

func (d *AlertDispatcher) sendAlert(id int64, workerID, alertType string, payload alertWebhookPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("Failed to marshal alert payload", "alert_id", id, "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.webhookURL, bytes.NewReader(body))
	if err != nil {
		slog.Error("Failed to create alert request", "alert_id", id, "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "litestream-soak/alert-dispatcher")
	if d.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+d.bearerToken)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		controlAlertTotal.WithLabelValues(workerID, alertType, "failed").Inc()
		if updateErr := d.db.UpdateAlertDelivery(id, "failed", string(body), err.Error(), nil); updateErr != nil {
			slog.Error("Failed to record alert delivery error", "alert_id", id, "error", updateErr)
		}
		slog.Error("Alert delivery failed", "alert_id", id, "worker_id", workerID, "alert_type", alertType, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		errMsg := fmt.Sprintf("unexpected webhook status %d", resp.StatusCode)
		controlAlertTotal.WithLabelValues(workerID, alertType, "failed").Inc()
		if updateErr := d.db.UpdateAlertDelivery(id, "failed", string(body), errMsg, nil); updateErr != nil {
			slog.Error("Failed to record alert delivery status", "alert_id", id, "error", updateErr)
		}
		slog.Error("Alert delivery returned error status", "alert_id", id, "worker_id", workerID, "alert_type", alertType, "status", resp.StatusCode)
		return
	}

	now := time.Now().UTC()
	controlAlertTotal.WithLabelValues(workerID, alertType, "sent").Inc()
	controlWorkerLastAlert.WithLabelValues(workerID, alertType).Set(float64(now.Unix()))
	if err := d.db.UpdateAlertDelivery(id, "sent", string(body), "", &now); err != nil {
		slog.Error("Failed to update sent alert", "alert_id", id, "error", err)
	}
}

func strconvFormatInt(value int64) string {
	return fmt.Sprintf("%d", value)
}
