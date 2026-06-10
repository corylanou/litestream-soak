package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type blockingShutdowner struct {
	ctxCh chan context.Context
}

func (s *blockingShutdowner) Shutdown(ctx context.Context) error {
	s.ctxCh <- ctx
	<-ctx.Done()
	return ctx.Err()
}

func TestShutdownOnCancelUsesBoundedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &blockingShutdowner{ctxCh: make(chan context.Context, 1)}
	errCh := make(chan error, 1)

	go func() {
		errCh <- shutdownOnCancel(ctx, server, 20*time.Millisecond)
	}()

	cancel()

	var shutdownCtx context.Context
	select {
	case shutdownCtx = <-server.ctxCh:
	case <-time.After(time.Second):
		t.Fatal("expected shutdown to start after cancellation")
	}

	if _, ok := shutdownCtx.Deadline(); !ok {
		t.Fatal("expected shutdown context to have a deadline")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown returned %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("expected shutdown to return after timeout")
	}
}

func TestIsAdminBearerAuthorized(t *testing.T) {
	request := httptest.NewRequest("POST", "/api/admin/deployments/ready", nil)
	request.Header.Set("Authorization", "Bearer test-token")

	if !isAdminBearerAuthorized(request, "test-token") {
		t.Fatal("expected admin bearer token to authorize admin request")
	}
}

func TestIsAdminBearerAuthorizedRejectsNonAdminRoute(t *testing.T) {
	request := httptest.NewRequest("GET", "/ui", nil)
	request.Header.Set("Authorization", "Bearer test-token")

	if isAdminBearerAuthorized(request, "test-token") {
		t.Fatal("expected non-admin route to reject bearer-only authorization")
	}
}

func TestSkipBasicAuthAllowsWorkerReportsAndWebhook(t *testing.T) {
	paths := []string{
		"/webhooks/github",
		"/api/workers/example/heartbeat",
		"/api/workers/example/verifications",
		"/api/workers/example/events",
	}

	for _, path := range paths {
		request := httptest.NewRequest("POST", path, nil)
		if !skipBasicAuth(request) {
			t.Fatalf("expected skipBasicAuth(%q) to be true", path)
		}
	}
}

func TestAdminRoutesRejectBasicAuth(t *testing.T) {
	handler := newAuthMiddleware("soak", "ui-password", "admin-token", false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest("POST", "/api/admin/deployments/ready", nil)
	request.SetBasicAuth("soak", "ui-password")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("admin route with basic auth returned %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestAdminRoutesAllowBasicAuthWithFallbackEnabled(t *testing.T) {
	handler := newAuthMiddleware("soak", "ui-password", "admin-token", true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest("POST", "/api/admin/pause-source", nil)
	request.SetBasicAuth("soak", "ui-password")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("admin route with basic auth returned %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestNonAdminRoutesStillAllowBasicAuth(t *testing.T) {
	handler := newAuthMiddleware("soak", "ui-password", "admin-token", false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest("GET", "/ui", nil)
	request.SetBasicAuth("soak", "ui-password")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("ui route with basic auth returned %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestIsWorkerReportPath(t *testing.T) {
	trueRequests := []*http.Request{
		httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil),
		httptest.NewRequest("POST", "/api/workers/example/verifications", nil),
		httptest.NewRequest("POST", "/api/workers/example/events", nil),
		httptest.NewRequest("POST", "/api/workers/abc-123/heartbeat", nil),
	}
	for _, r := range trueRequests {
		if !isWorkerReportPath(r) {
			t.Fatalf("expected isWorkerReportPath to be true for %s %s", r.Method, r.URL.Path)
		}
	}

	falseRequests := []*http.Request{
		httptest.NewRequest("GET", "/api/workers/example/heartbeat", nil),
		httptest.NewRequest("POST", "/api/workers/example/status", nil),
		httptest.NewRequest("POST", "/ui", nil),
		httptest.NewRequest("POST", "/webhooks/github", nil),
		httptest.NewRequest("GET", "/ui", nil),
	}
	for _, r := range falseRequests {
		if isWorkerReportPath(r) {
			t.Fatalf("expected isWorkerReportPath to be false for %s %s", r.Method, r.URL.Path)
		}
	}
}

func TestIsWorkerBearerAuthorized(t *testing.T) {
	validRequest := httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil)
	validRequest.Header.Set("Authorization", "Bearer my-token")
	if !isWorkerBearerAuthorized(validRequest, "my-token") {
		t.Fatal("expected valid bearer token to be authorized")
	}

	emptyTokenRequest := httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil)
	emptyTokenRequest.Header.Set("Authorization", "Bearer my-token")
	if isWorkerBearerAuthorized(emptyTokenRequest, "") {
		t.Fatal("expected empty configured token to fail closed")
	}

	wrongTokenRequest := httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil)
	wrongTokenRequest.Header.Set("Authorization", "Bearer wrong-token")
	if isWorkerBearerAuthorized(wrongTokenRequest, "my-token") {
		t.Fatal("expected wrong bearer token to be unauthorized")
	}

	noHeaderRequest := httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil)
	if isWorkerBearerAuthorized(noHeaderRequest, "my-token") {
		t.Fatal("expected missing authorization header to be unauthorized")
	}
}

func TestWorkerAuthMiddlewareAllowsValidToken(t *testing.T) {
	handler := newWorkerAuthMiddleware("secret-token")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil)
	request.Header.Set("Authorization", "Bearer secret-token")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("valid worker token returned %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestWorkerAuthMiddlewareMissingAuthHeader(t *testing.T) {
	handler := newWorkerAuthMiddleware("secret-token")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth header returned %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestWorkerAuthMiddlewareWrongToken(t *testing.T) {
	handler := newWorkerAuthMiddleware("secret-token")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil)
	request.Header.Set("Authorization", "Bearer wrong-token")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token returned %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestWorkerAuthMiddlewareEmptyConfiguredToken(t *testing.T) {
	handler := newWorkerAuthMiddleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest("POST", "/api/workers/example/heartbeat", nil)
	request.Header.Set("Authorization", "Bearer any-token")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("empty configured token returned %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestWorkerAuthMiddlewarePassesThroughNonWorkerPaths(t *testing.T) {
	handler := newWorkerAuthMiddleware("secret-token")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	passthroughRequests := []*http.Request{
		httptest.NewRequest("GET", "/ui", nil),
		httptest.NewRequest("POST", "/webhooks/github", nil),
		httptest.NewRequest("GET", "/api/workers/example/heartbeat", nil),
	}

	for _, r := range passthroughRequests {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		if response.Code != http.StatusNoContent {
			t.Fatalf("non-worker-report path %s %s returned %d, want %d", r.Method, r.URL.Path, response.Code, http.StatusNoContent)
		}
	}
}

func TestWorkerAuthMiddlewareAllowsVerificationsAndEvents(t *testing.T) {
	handler := newWorkerAuthMiddleware("secret-token")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	paths := []string{
		"/api/workers/example/verifications",
		"/api/workers/example/events",
	}

	for _, path := range paths {
		request := httptest.NewRequest("POST", path, nil)
		request.Header.Set("Authorization", "Bearer secret-token")
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusNoContent {
			t.Fatalf("valid token on %s returned %d, want %d", path, response.Code, http.StatusNoContent)
		}
	}
}
