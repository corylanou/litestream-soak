package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

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
	handler := newAuthMiddleware("soak", "ui-password", "admin-token")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

func TestNonAdminRoutesStillAllowBasicAuth(t *testing.T) {
	handler := newAuthMiddleware("soak", "ui-password", "admin-token")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
