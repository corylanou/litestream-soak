package main

import (
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
	}

	for _, path := range paths {
		request := httptest.NewRequest("POST", path, nil)
		if !skipBasicAuth(request) {
			t.Fatalf("expected skipBasicAuth(%q) to be true", path)
		}
	}
}
