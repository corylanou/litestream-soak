package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestHomePageRendersStalestRuntimeLabel(t *testing.T) {
	db := openTestDB(t)

	createTestWorker(t, db, model.Worker{
		ID:            "worker-smoke",
		Name:          "worker-smoke",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-a",
		LitestreamSHA: "ls-a",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	if err := db.UpdateWorkerRuntimeSnapshot("worker-smoke", reporting.RuntimePayload{
		LitestreamSnapshotHealthy: true,
		SnapshotCollectedAt:       time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	recorder := httptest.NewRecorder()
	api.handleHome(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", recorder.Code, recorder.Body.String()[:min(500, recorder.Body.Len())])
	}
	body := recorder.Body.String()
	for _, label := range []string{"Stalest runtime", "Stalest heartbeat", "Fleet health", "Pass rate 24h", "chart-data"} {
		if !strings.Contains(body, label) {
			t.Fatalf("home page body missing %q", label)
		}
	}
	if strings.Contains(body, "Latest runtime") {
		t.Fatalf("home page body still contains old %q label", "Latest runtime")
	}
}
