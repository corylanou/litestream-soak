package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestReporterSendsWorkerTokenBearerHeader(t *testing.T) {
	t.Setenv("SOAK_WORKER_TOKEN", "test-worker-token")

	var mu sync.Mutex
	headers := map[string]string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		headers[r.URL.Path] = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reporter := NewReporter(Config{
		ControlBaseURL: server.URL,
		WorkerID:       "w1",
	})

	ctx := context.Background()

	if err := reporter.SendHeartbeat(ctx, reporting.HeartbeatPayload{}); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}
	if err := reporter.SendVerification(ctx, reporting.VerificationPayload{}); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	if err := reporter.SendEvent(ctx, reporting.WorkerEventPayload{}); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	want := "Bearer test-worker-token"
	for path, got := range headers {
		if got != want {
			t.Errorf("path %s: Authorization = %q, want %q", path, got, want)
		}
	}
	if len(headers) != 3 {
		t.Errorf("expected 3 requests, got %d", len(headers))
	}
}

func TestReporterOmitsAuthorizationWhenTokenUnset(t *testing.T) {
	t.Setenv("SOAK_WORKER_TOKEN", "")

	var mu sync.Mutex
	headers := map[string]string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		headers[r.URL.Path] = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reporter := NewReporter(Config{
		ControlBaseURL: server.URL,
		WorkerID:       "w1",
	})

	ctx := context.Background()

	if err := reporter.SendHeartbeat(ctx, reporting.HeartbeatPayload{}); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}
	if err := reporter.SendVerification(ctx, reporting.VerificationPayload{}); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	if err := reporter.SendEvent(ctx, reporting.WorkerEventPayload{}); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	for path, got := range headers {
		if got != "" {
			t.Errorf("path %s: Authorization = %q, want empty", path, got)
		}
	}
	if len(headers) != 3 {
		t.Errorf("expected 3 requests, got %d", len(headers))
	}
}

func TestNewReporterEmptyBaseURLReturnsNil(t *testing.T) {
	r := NewReporter(Config{})
	if r != nil {
		t.Fatalf("expected nil reporter for empty ControlBaseURL, got %v", r)
	}
}

func TestNewReporterEmptyBaseURLNotEnabled(t *testing.T) {
	r := NewReporter(Config{})
	if r.Enabled() {
		t.Fatal("nil reporter should not be Enabled()")
	}
}

func TestSendHeartbeatNilReporterReturnsNil(t *testing.T) {
	var r *Reporter
	if err := r.SendHeartbeat(context.Background(), reporting.HeartbeatPayload{}); err != nil {
		t.Fatalf("expected nil error from disabled reporter, got %v", err)
	}
}

func TestSendVerificationNilReporterReturnsNil(t *testing.T) {
	var r *Reporter
	if err := r.SendVerification(context.Background(), reporting.VerificationPayload{}); err != nil {
		t.Fatalf("expected nil error from disabled reporter, got %v", err)
	}
}

func TestSendEventNilReporterReturnsNil(t *testing.T) {
	var r *Reporter
	if err := r.SendEvent(context.Background(), reporting.WorkerEventPayload{}); err != nil {
		t.Fatalf("expected nil error from disabled reporter, got %v", err)
	}
}

func TestSendHeartbeatDisabledConfigReturnsNil(t *testing.T) {
	r := NewReporter(Config{})
	if err := r.SendHeartbeat(context.Background(), reporting.HeartbeatPayload{}); err != nil {
		t.Fatalf("expected nil error from disabled reporter, got %v", err)
	}
}

func TestSendVerificationDisabledConfigReturnsNil(t *testing.T) {
	r := NewReporter(Config{})
	if err := r.SendVerification(context.Background(), reporting.VerificationPayload{}); err != nil {
		t.Fatalf("expected nil error from disabled reporter, got %v", err)
	}
}

func TestSendEventDisabledConfigReturnsNil(t *testing.T) {
	r := NewReporter(Config{})
	if err := r.SendEvent(context.Background(), reporting.WorkerEventPayload{}); err != nil {
		t.Fatalf("expected nil error from disabled reporter, got %v", err)
	}
}

func TestSendHeartbeatPreservesSentAt(t *testing.T) {
	var mu sync.Mutex
	var received reporting.HeartbeatPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p reporting.HeartbeatPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = p
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reporter := NewReporter(Config{
		ControlBaseURL: server.URL,
		WorkerID:       "w1",
	})

	fixedTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	payload := reporting.HeartbeatPayload{}
	payload.SentAt = fixedTime

	if err := reporter.SendHeartbeat(context.Background(), payload); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !received.SentAt.Equal(fixedTime) {
		t.Errorf("SentAt = %v, want %v", received.SentAt, fixedTime)
	}
}

func TestSendEventPreservesSentAt(t *testing.T) {
	var mu sync.Mutex
	var received reporting.WorkerEventPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p reporting.WorkerEventPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = p
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reporter := NewReporter(Config{
		ControlBaseURL: server.URL,
		WorkerID:       "w1",
	})

	fixedTime := time.Date(2024, 3, 20, 8, 0, 0, 0, time.UTC)
	payload := reporting.WorkerEventPayload{}
	payload.SentAt = fixedTime

	if err := reporter.SendEvent(context.Background(), payload); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !received.SentAt.Equal(fixedTime) {
		t.Errorf("SentAt = %v, want %v", received.SentAt, fixedTime)
	}
}

func TestPostJSONServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	reporter := NewReporter(Config{
		ControlBaseURL: server.URL,
		WorkerID:       "w1",
	})

	err := reporter.SendHeartbeat(context.Background(), reporting.HeartbeatPayload{})
	if err == nil {
		t.Fatal("expected error from 500 response, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error to mention status 500, got: %v", err)
	}
}

func TestPostJSONUnreachableServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	serverURL := server.URL
	server.Close()

	reporter := NewReporter(Config{
		ControlBaseURL: serverURL,
		WorkerID:       "w1",
	})

	err := reporter.SendHeartbeat(context.Background(), reporting.HeartbeatPayload{})
	if err == nil {
		t.Fatal("expected error sending to closed server, got nil")
	}
	if !strings.Contains(err.Error(), "send request") {
		t.Errorf("expected error to mention 'send request', got: %v", err)
	}
}
