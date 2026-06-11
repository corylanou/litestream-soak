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

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/assets/", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET /assets/ = %d, want 404 (no directory listings)", recorder.Code)
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
