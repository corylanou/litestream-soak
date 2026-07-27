package flyapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIsNotFound(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("wrapped: %w", &APIError{
		StatusCode: http.StatusNotFound,
		Body:       "not found",
	})
	if !IsNotFound(err) {
		t.Fatal("IsNotFound() = false, want true")
	}
}

func TestIsNotFoundRejectsOtherStatus(t *testing.T) {
	t.Parallel()

	err := &APIError{
		StatusCode: http.StatusInternalServerError,
		Body:       "server error",
	}
	if IsNotFound(err) {
		t.Fatal("IsNotFound() = true, want false")
	}
}

func TestListVolumesRejectsOversizedResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"id":"`)
		_, _ = io.CopyN(w, strings.NewReader(strings.Repeat("x", int(maxResponseBodyBytes))), maxResponseBodyBytes)
		_, _ = io.WriteString(w, `"}]`)
	}))
	t.Cleanup(server.Close)

	client := NewClientWithBaseURL("litestream-soak", "test-token", server.URL)
	_, err := client.ListVolumes(t.Context())
	if err == nil {
		t.Fatal("ListVolumes() error = nil, want oversized response error")
	}
	want := responseBodyTooLarge(maxResponseBodyBytes)
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("ListVolumes() error = %q, want %q", err, want)
	}
}

func TestListVolumesBoundsOversizedErrorResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.CopyN(w, strings.NewReader(strings.Repeat("x", int(maxErrorBodyBytes+1))), maxErrorBodyBytes+1)
	}))
	t.Cleanup(server.Close)

	client := NewClientWithBaseURL("litestream-soak", "test-token", server.URL)
	_, err := client.ListVolumes(t.Context())
	if err == nil {
		t.Fatal("ListVolumes() error = nil, want API error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("ListVolumes() error = %T, want *APIError", err)
	}
	if apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("APIError.StatusCode = %d, want %d", apiErr.StatusCode, http.StatusServiceUnavailable)
	}
	want := responseBodyTooLarge(maxErrorBodyBytes)
	if apiErr.Body != want {
		t.Fatalf("APIError.Body = %q, want %q", apiErr.Body, want)
	}
}
