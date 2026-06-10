package orchestrator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

type capturedAlert struct {
	method        string
	contentType   string
	userAgent     string
	authorization string
	payload       alertWebhookPayload
}

func newAlertWebhookServer(t *testing.T, statusCode int) (*httptest.Server, <-chan capturedAlert) {
	t.Helper()

	ch := make(chan capturedAlert, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload alertWebhookPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		ch <- capturedAlert{
			method:        r.Method,
			contentType:   r.Header.Get("Content-Type"),
			userAgent:     r.Header.Get("User-Agent"),
			authorization: r.Header.Get("Authorization"),
			payload:       payload,
		}
		w.WriteHeader(statusCode)
	}))
	t.Cleanup(server.Close)
	return server, ch
}

func waitForAlert(t *testing.T, ch <-chan capturedAlert) capturedAlert {
	t.Helper()

	select {
	case a := <-ch:
		return a
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for webhook delivery")
		return capturedAlert{}
	}
}

func failedVerification(workerID string, startedAt time.Time, msg string) *model.Verification {
	return &model.Verification{
		WorkerID:     workerID,
		StartedAt:    startedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: msg,
	}
}

func TestAlertDispatcherEnabled(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	var nilDispatcher *AlertDispatcher
	if nilDispatcher.Enabled() {
		t.Error("nil dispatcher Enabled() = true, want false")
	}

	emptyWebhook := NewAlertDispatcher(db, "", "", "")
	if emptyWebhook.Enabled() {
		t.Error("empty webhook dispatcher Enabled() = true, want false")
	}

	whitespaceWebhook := NewAlertDispatcher(db, "", "   ", "")
	if whitespaceWebhook.Enabled() {
		t.Error("whitespace webhook dispatcher Enabled() = true, want false")
	}

	realWebhook := NewAlertDispatcher(db, "", "http://example.com/hook", "")
	if !realWebhook.Enabled() {
		t.Error("real webhook dispatcher Enabled() = false, want true")
	}
}

func TestNotifyWorkerStaleSendsWebhook(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	server, ch := newAlertWebhookServer(t, http.StatusOK)

	createTestWorker(t, db, model.Worker{
		ID:            "stale-webhook-worker",
		Name:          "stale-webhook-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerHeartbeat("stale-webhook-worker"); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat error = %v", err)
	}

	w, err := db.GetWorker("stale-webhook-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	d := NewAlertDispatcher(db, "http://ctl.example", server.URL, "secret-token")
	d.notifyWorkerStale(*w)

	got := waitForAlert(t, ch)

	if got.method != http.MethodPost {
		t.Errorf("method = %q, want POST", got.method)
	}
	if got.contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got.contentType)
	}
	if got.userAgent != "litestream-soak/alert-dispatcher" {
		t.Errorf("User-Agent = %q, want litestream-soak/alert-dispatcher", got.userAgent)
	}
	if got.authorization != "Bearer secret-token" {
		t.Errorf("Authorization = %q, want Bearer secret-token", got.authorization)
	}
	if got.payload.Alert.AlertType != "worker_stale" {
		t.Errorf("AlertType = %q, want worker_stale", got.payload.Alert.AlertType)
	}
	if got.payload.Alert.Severity != "warning" {
		t.Errorf("Severity = %q, want warning", got.payload.Alert.Severity)
	}
	if got.payload.Alert.Worker == nil || got.payload.Alert.Worker.ID != "stale-webhook-worker" {
		t.Errorf("Worker.ID missing or wrong in payload")
	}
	if len(got.payload.Alert.URLs) == 0 {
		t.Error("URLs map is empty, want non-empty")
	}
	if _, ok := got.payload.Alert.URLs["worker_ui"]; !ok {
		t.Error("URLs missing worker_ui key")
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1", len(alerts))
	}
	a := alerts[0]
	if a.Status != "sent" {
		t.Errorf("alert status = %q, want sent", a.Status)
	}
	if a.SentAt == nil {
		t.Error("SentAt is nil, want non-nil")
	}
	if w.LastHeartbeatAt == nil {
		t.Fatal("worker LastHeartbeatAt is nil after heartbeat update")
	}
	wantFingerprint := fmt.Sprintf("worker_stale:%s:%d", w.ID, w.LastHeartbeatAt.Unix())
	if a.Fingerprint != wantFingerprint {
		t.Errorf("fingerprint = %q, want %q", a.Fingerprint, wantFingerprint)
	}
}

func TestNotifyWorkerStaleNilHeartbeatFingerprint(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	server, ch := newAlertWebhookServer(t, http.StatusOK)

	createTestWorker(t, db, model.Worker{
		ID:            "stale-nil-hb-worker",
		Name:          "stale-nil-hb-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("stale-nil-hb-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	d := NewAlertDispatcher(db, "", server.URL, "")
	d.notifyWorkerStale(*w)

	waitForAlert(t, ch)

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1", len(alerts))
	}
	wantFingerprint := "worker_stale:stale-nil-hb-worker:unknown"
	if alerts[0].Fingerprint != wantFingerprint {
		t.Errorf("fingerprint = %q, want %q", alerts[0].Fingerprint, wantFingerprint)
	}
}

func TestNotifyWorkerStaleDuplicateFingerprintSkipsDelivery(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	var counter atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	createTestWorker(t, db, model.Worker{
		ID:            "stale-dedup-worker",
		Name:          "stale-dedup-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("stale-dedup-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	d := NewAlertDispatcher(db, "", server.URL, "")
	d.notifyWorkerStale(*w)
	d.notifyWorkerStale(*w)

	if counter.Load() != 1 {
		t.Errorf("webhook calls = %d, want 1", counter.Load())
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Errorf("len(alerts) = %d, want 1", len(alerts))
	}
}

func TestNotifyWorkerStaleWebhookErrorStatusMarksFailed(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	server, _ := newAlertWebhookServer(t, http.StatusInternalServerError)

	createTestWorker(t, db, model.Worker{
		ID:            "stale-500-worker",
		Name:          "stale-500-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("stale-500-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	d := NewAlertDispatcher(db, "", server.URL, "")
	d.notifyWorkerStale(*w)

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1", len(alerts))
	}
	a := alerts[0]
	if a.Status != "failed" {
		t.Errorf("status = %q, want failed", a.Status)
	}
	wantErrMsg := "unexpected webhook status 500"
	if a.ErrorMessage != wantErrMsg {
		t.Errorf("error_message = %q, want %q", a.ErrorMessage, wantErrMsg)
	}
	if a.SentAt != nil {
		t.Error("SentAt should be nil on failure")
	}
}

func TestNotifyWorkerStaleNetworkErrorMarksFailed(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	closedURL := server.URL
	server.Close()

	createTestWorker(t, db, model.Worker{
		ID:            "stale-net-err-worker",
		Name:          "stale-net-err-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("stale-net-err-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	d := NewAlertDispatcher(db, "", closedURL, "")
	d.notifyWorkerStale(*w)

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1", len(alerts))
	}
	a := alerts[0]
	if a.Status != "failed" {
		t.Errorf("status = %q, want failed", a.Status)
	}
	if a.ErrorMessage == "" {
		t.Error("ErrorMessage is empty, want non-empty on network error")
	}
}

func TestNotifyWorkerStaleAsyncDelivery(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	server, ch := newAlertWebhookServer(t, http.StatusOK)

	createTestWorker(t, db, model.Worker{
		ID:            "stale-async-worker",
		Name:          "stale-async-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("stale-async-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	d := NewAlertDispatcher(db, "", server.URL, "")
	d.NotifyWorkerStale(*w)

	got := waitForAlert(t, ch)
	if got.payload.Alert.AlertType != "worker_stale" {
		t.Errorf("AlertType = %q, want worker_stale", got.payload.Alert.AlertType)
	}
}

func TestNotifyVerificationFailureSkipsPassedVerification(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	var counter atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	createTestWorker(t, db, model.Worker{
		ID:            "verif-passed-worker",
		Name:          "verif-passed-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("verif-passed-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	passedVerif := model.Verification{
		WorkerID:  w.ID,
		StartedAt: time.Now(),
		Status:    "passed",
		CheckType: "integrity",
		Passed:    true,
	}

	d := NewAlertDispatcher(db, "", server.URL, "")
	d.NotifyVerificationFailure(*w, passedVerif)

	if counter.Load() != 0 {
		t.Errorf("webhook calls = %d, want 0 for passed verification", counter.Load())
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 0 {
		t.Errorf("len(alerts) = %d, want 0", len(alerts))
	}
}

func TestNotifyVerificationFailureDisabledDispatcherNoops(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	createTestWorker(t, db, model.Worker{
		ID:            "verif-disabled-worker",
		Name:          "verif-disabled-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("verif-disabled-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	failedVerif := failedVerification(w.ID, time.Now(), "integrity check failed: checksum mismatch")

	d := NewAlertDispatcher(db, "", "", "")
	d.NotifyVerificationFailure(*w, *failedVerif)

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 0 {
		t.Errorf("len(alerts) = %d, want 0", len(alerts))
	}
}

func TestShouldAlertVerificationFailure(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		workerID   string
		setup      func(t *testing.T, db *model.DB, workerID string)
		currentMsg string
		want       bool
	}{
		{
			name:     "only current recorded",
			workerID: "should-alert-only-current",
			setup: func(t *testing.T, db *model.DB, workerID string) {
				t.Helper()
				createTestWorker(t, db, model.Worker{
					ID: workerID, Name: workerID, Status: model.WorkerRunning,
					Source: "main", ProfileName: "low", ProfileConfig: "{}",
				})
			},
			currentMsg: "integrity check failed: checksum mismatch",
			want:       true,
		},
		{
			name:     "previous passed",
			workerID: "should-alert-prev-passed",
			setup: func(t *testing.T, db *model.DB, workerID string) {
				t.Helper()
				createTestWorker(t, db, model.Worker{
					ID: workerID, Name: workerID, Status: model.WorkerRunning,
					Source: "main", ProfileName: "low", ProfileConfig: "{}",
				})
				mustRecordVerification(t, db, &model.Verification{
					WorkerID: workerID, StartedAt: base.Add(-2 * time.Minute),
					Status: "passed", CheckType: "integrity", Passed: true,
				})
			},
			currentMsg: "integrity check failed: checksum mismatch",
			want:       true,
		},
		{
			name:     "same stage and signature as previous",
			workerID: "should-alert-same-sig",
			setup: func(t *testing.T, db *model.DB, workerID string) {
				t.Helper()
				createTestWorker(t, db, model.Worker{
					ID: workerID, Name: workerID, Status: model.WorkerRunning,
					Source: "main", ProfileName: "low", ProfileConfig: "{}",
				})
				mustRecordVerification(t, db, &model.Verification{
					WorkerID: workerID, StartedAt: base.Add(-2 * time.Minute),
					Status: "failed", CheckType: "integrity", Passed: false,
					ErrorMessage: "integrity check failed: checksum mismatch",
				})
			},
			currentMsg: "integrity check failed: checksum mismatch",
			want:       false,
		},
		{
			name:     "different stage from previous",
			workerID: "should-alert-diff-stage",
			setup: func(t *testing.T, db *model.DB, workerID string) {
				t.Helper()
				createTestWorker(t, db, model.Worker{
					ID: workerID, Name: workerID, Status: model.WorkerRunning,
					Source: "main", ProfileName: "low", ProfileConfig: "{}",
				})
				mustRecordVerification(t, db, &model.Verification{
					WorkerID: workerID, StartedAt: base.Add(-2 * time.Minute),
					Status: "failed", CheckType: "integrity", Passed: false,
					ErrorMessage: "restore failed: missing ltx file",
				})
			},
			currentMsg: "integrity check failed: checksum mismatch",
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			tt.setup(t, db, tt.workerID)

			current := *failedVerification(tt.workerID, base.Add(-time.Minute), tt.currentMsg)
			mustRecordVerification(t, db, &current)

			d := NewAlertDispatcher(db, "", "http://webhook.example", "")
			got := d.shouldAlertVerificationFailure(tt.workerID, current)
			if got != tt.want {
				t.Errorf("shouldAlertVerificationFailure() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNotifyVerificationFailureSendsWebhook(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	server, ch := newAlertWebhookServer(t, http.StatusOK)

	createTestWorker(t, db, model.Worker{
		ID:            "verif-fail-webhook",
		Name:          "verif-fail-webhook",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("verif-fail-webhook")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	v := failedVerification(w.ID, time.Now(), "integrity check failed: checksum mismatch")
	mustRecordVerification(t, db, v)

	d := NewAlertDispatcher(db, "http://ctl.example", server.URL, "")
	d.notifyVerificationFailure(*w, *v)

	got := waitForAlert(t, ch)

	if got.payload.Alert.AlertType != "verification_failed" {
		t.Errorf("AlertType = %q, want verification_failed", got.payload.Alert.AlertType)
	}
	if got.payload.Alert.Severity != "critical" {
		t.Errorf("Severity = %q, want critical", got.payload.Alert.Severity)
	}
	if got.payload.Alert.FailureStage != "integrity_check" {
		t.Errorf("FailureStage = %q, want integrity_check", got.payload.Alert.FailureStage)
	}
	if got.payload.Alert.FailureSignature == "" {
		t.Error("FailureSignature is empty, want non-empty")
	}
	if got.payload.Alert.Verification == nil || got.payload.Alert.Verification.ID != v.ID {
		t.Errorf("Verification.ID mismatch: got %v, want %d", got.payload.Alert.Verification, v.ID)
	}
	wantMessage := firstMeaningfulLine(v.ErrorMessage)
	if got.payload.Alert.Message != wantMessage {
		t.Errorf("Message = %q, want %q", got.payload.Alert.Message, wantMessage)
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1", len(alerts))
	}
	a := alerts[0]
	if a.Status != "sent" {
		t.Errorf("status = %q, want sent", a.Status)
	}
	wantFingerprint := fmt.Sprintf("verification_failed:%d", v.ID)
	if a.Fingerprint != wantFingerprint {
		t.Errorf("fingerprint = %q, want %q", a.Fingerprint, wantFingerprint)
	}
}

func TestNotifyVerificationFailureDuplicateVerificationIDSkipsDelivery(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	var counter atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	createTestWorker(t, db, model.Worker{
		ID:            "verif-dedup-worker",
		Name:          "verif-dedup-worker",
		Status:        model.WorkerRunning,
		Source:        "main",
		ProfileName:   "low",
		ProfileConfig: "{}",
	})

	w, err := db.GetWorker("verif-dedup-worker")
	if err != nil {
		t.Fatalf("GetWorker error = %v", err)
	}

	v := failedVerification(w.ID, time.Now(), "integrity check failed: checksum mismatch")
	mustRecordVerification(t, db, v)

	d := NewAlertDispatcher(db, "", server.URL, "")
	d.notifyVerificationFailure(*w, *v)
	d.notifyVerificationFailure(*w, *v)

	if counter.Load() != 1 {
		t.Errorf("webhook calls = %d, want 1", counter.Load())
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Errorf("len(alerts) = %d, want 1", len(alerts))
	}
}

func TestRolloutAttentionAlertable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		graceWindowExceed bool
		status            string
		want              bool
	}{
		{"grace not exceeded", false, "rolling_out", false},
		{"exceeded rolling_out", true, "rolling_out", true},
		{"exceeded probing", true, "probing", true},
		{"exceeded needs_attention", true, "needs_attention", true},
		{"exceeded stable", true, "stable", false},
		{"exceeded settling", true, "settling", false},
		{"exceeded no_workers", true, "no_workers", false},
		{"exceeded empty", true, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rollout := DeploymentRolloutResponse{
				GraceWindowExceeded: tt.graceWindowExceed,
				Status:              tt.status,
			}
			got := rolloutAttentionAlertable(rollout)
			if got != tt.want {
				t.Errorf("rolloutAttentionAlertable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNotifyDeploymentAttentionNotAlertableNoops(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	var counter atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	rollout := DeploymentRolloutResponse{
		Deployment:          model.Deployment{Source: "main", GitSHA: "abc123"},
		Status:              "rolling_out",
		GraceWindowExceeded: false,
	}

	d := NewAlertDispatcher(db, "", server.URL, "")
	d.NotifyDeploymentAttention(rollout)

	if counter.Load() != 0 {
		t.Errorf("webhook calls = %d, want 0", counter.Load())
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 0 {
		t.Errorf("len(alerts) = %d, want 0", len(alerts))
	}
}

func TestNotifyDeploymentAttentionSendsWebhook(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	server, ch := newAlertWebhookServer(t, http.StatusOK)

	rollout := DeploymentRolloutResponse{
		Deployment: model.Deployment{
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "",
		},
		Status:              "needs_attention",
		GraceWindowExceeded: true,
		NextAction:          "investigate degraded workers",
	}

	d := NewAlertDispatcher(db, "http://ctl.example", server.URL, "")
	d.notifyDeploymentAttention(rollout)

	got := waitForAlert(t, ch)

	if got.payload.Alert.AlertType != "deployment_attention" {
		t.Errorf("AlertType = %q, want deployment_attention", got.payload.Alert.AlertType)
	}
	if got.payload.Alert.Severity != "warning" {
		t.Errorf("Severity = %q, want warning", got.payload.Alert.Severity)
	}
	if got.payload.Alert.Message != "investigate degraded workers" {
		t.Errorf("Message = %q, want investigate degraded workers", got.payload.Alert.Message)
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("len(alerts) = %d, want 1", len(alerts))
	}
	a := alerts[0]
	if a.Status != "sent" {
		t.Errorf("status = %q, want sent", a.Status)
	}
	if a.WorkerID != "" {
		t.Errorf("WorkerID = %q, want empty", a.WorkerID)
	}
	wantFingerprint := "deployment_attention:main:abc123:unknown:needs_attention"
	if a.Fingerprint != wantFingerprint {
		t.Errorf("fingerprint = %q, want %q", a.Fingerprint, wantFingerprint)
	}
}

func TestNotifyDeploymentAttentionDuplicateFingerprintSkipsDelivery(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	var counter atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	rollout := DeploymentRolloutResponse{
		Deployment: model.Deployment{
			Source:        "main",
			GitSHA:        "def456",
			LitestreamSHA: "ls789",
		},
		Status:              "needs_attention",
		GraceWindowExceeded: true,
		NextAction:          "investigate degraded workers",
	}

	d := NewAlertDispatcher(db, "", server.URL, "")
	d.notifyDeploymentAttention(rollout)
	d.notifyDeploymentAttention(rollout)

	if counter.Load() != 1 {
		t.Errorf("webhook calls = %d, want 1", counter.Load())
	}

	alerts, err := db.ListAlerts(10)
	if err != nil {
		t.Fatalf("ListAlerts error = %v", err)
	}
	if len(alerts) != 1 {
		t.Errorf("len(alerts) = %d, want 1", len(alerts))
	}
}

func TestAlertURLsEmptyBaseURL(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	emptyBase := NewAlertDispatcher(db, "", "http://webhook.example", "")
	urls := emptyBase.alertURLs("some-worker")
	if len(urls) != 0 {
		t.Errorf("alertURLs with empty baseURL len = %d, want 0", len(urls))
	}

	deployURLs := emptyBase.deploymentAlertURLs()
	if len(deployURLs) != 0 {
		t.Errorf("deploymentAlertURLs with empty baseURL len = %d, want 0", len(deployURLs))
	}

	trailingSlash := NewAlertDispatcher(db, "http://x/", "http://webhook.example", "")
	workerURLs := trailingSlash.alertURLs("w")
	if workerURLs["worker_ui"] != "http://x/ui/workers/w" {
		t.Errorf("worker_ui = %q, want http://x/ui/workers/w", workerURLs["worker_ui"])
	}
}

func TestAlertDispatcherURLKeys(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	d := NewAlertDispatcher(db, "http://ctl.example", "http://webhook.example", "")

	workerURLs := d.alertURLs("myworker")
	for _, key := range []string{"worker_ui", "incident_api", "prompt_api", "alerts_api"} {
		if _, ok := workerURLs[key]; !ok {
			t.Errorf("alertURLs missing key %q", key)
		}
	}
	if !strings.Contains(workerURLs["worker_ui"], "myworker") {
		t.Errorf("worker_ui %q does not contain worker ID", workerURLs["worker_ui"])
	}

	deployURLs := d.deploymentAlertURLs()
	for _, key := range []string{"control_ui", "deployment_api", "diagnosis_api", "events_api", "alerts_api"} {
		if _, ok := deployURLs[key]; !ok {
			t.Errorf("deploymentAlertURLs missing key %q", key)
		}
	}
}
