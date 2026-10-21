package flyapi

import (
	"encoding/json"
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

func TestGetVolume(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/apps/litestream-soak/volumes/volume-one" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"volume-one","name":"soak_worker_main_low_vol","state":"pending_destroy"}`)
	}))
	t.Cleanup(server.Close)

	client := NewClientWithBaseURL("litestream-soak", "test-token", server.URL)
	volume, err := client.GetVolume(t.Context(), "volume-one")
	if err != nil {
		t.Fatalf("GetVolume() error = %v", err)
	}
	if volume.ID != "volume-one" || volume.State != "pending_destroy" {
		t.Fatalf("GetVolume() = %+v", volume)
	}
}

func TestGetVolumePreservesNotFound(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)

	client := NewClientWithBaseURL("litestream-soak", "test-token", server.URL)
	_, err := client.GetVolume(t.Context(), "missing-volume")
	if !IsNotFound(err) {
		t.Fatalf("GetVolume() error = %v, want not found", err)
	}
}

func captureVolumeCreateBody(t *testing.T) (*Client, *string) {
	t.Helper()

	var captured string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/apps/litestream-soak/volumes" {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		captured = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"vol-new","name":"soak_fork","state":"hydrating"}`)
	}))
	t.Cleanup(server.Close)
	return NewClientWithBaseURL("litestream-soak", "test-token", server.URL), &captured
}

func TestForkVolumeSendsOnlySourceVolumeIDNameAndEncryption(t *testing.T) {
	t.Parallel()

	client, body := captureVolumeCreateBody(t)
	if _, err := client.ForkVolume(t.Context(), "vol-main-001", "soak_fork"); err != nil {
		t.Fatalf("ForkVolume() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(*body), &decoded); err != nil {
		t.Fatalf("decode fork body %q: %v", *body, err)
	}
	if got := decoded["source_volume_id"]; got != "vol-main-001" {
		t.Fatalf("fork body source_volume_id=%v want vol-main-001 (body %s)", got, *body)
	}
	for _, forbidden := range []string{"size_gb", "region", "source_vol_id"} {
		if _, present := decoded[forbidden]; present {
			t.Fatalf("fork body must omit %q: %s", forbidden, *body)
		}
	}
	if got := decoded["encrypted"]; got != true {
		t.Fatalf("fork body encrypted=%v want true", got)
	}
}

func TestCreateVolumeSendsSizeAndRegionForFreshVolumes(t *testing.T) {
	t.Parallel()

	client, body := captureVolumeCreateBody(t)
	if _, err := client.CreateVolume(t.Context(), CreateVolumeRequest{Name: "soak_fresh", SizeGB: 50, Region: "ord", Encrypted: true}); err != nil {
		t.Fatalf("CreateVolume() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(*body), &decoded); err != nil {
		t.Fatalf("decode body %q: %v", *body, err)
	}
	if decoded["size_gb"] != float64(50) || decoded["region"] != "ord" {
		t.Fatalf("fresh body missing size/region: %s", *body)
	}
	if _, present := decoded["source_volume_id"]; present {
		t.Fatalf("fresh body must omit source_volume_id: %s", *body)
	}
}
