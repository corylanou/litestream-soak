package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAssetsHandlerServesFilesButNotDirectoryListings(t *testing.T) {
	t.Parallel()

	handler := http.StripPrefix("/assets/", assetsHandler())

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/assets/dashboard.css", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /assets/dashboard.css = %d, want 200", recorder.Code)
	}
	if cc := recorder.Header().Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
		t.Fatalf("asset Cache-Control = %q, want immutable long-lived cache", cc)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/assets/", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET /assets/ = %d, want 404 (no directory listings)", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/assets/missing.js", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET /assets/missing.js = %d, want 404", recorder.Code)
	}
	if cc := recorder.Header().Get("Cache-Control"); cc != "" {
		t.Fatalf("404 Cache-Control = %q, want empty (must not cache misses)", cc)
	}
}

func TestAssetVersionIsStableAndNonEmpty(t *testing.T) {
	t.Parallel()

	if assetVersion == "" {
		t.Fatal("assetVersion is empty")
	}
	if len(assetVersion) < 8 {
		t.Fatalf("assetVersion %q too short to be a content hash", assetVersion)
	}
}
