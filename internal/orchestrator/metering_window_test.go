package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

const meteringBannerText = "Metering window: not test evidence"

func TestMeteringWindowSourcesParsesEnv(t *testing.T) {
	t.Setenv("SOAK_METERING_WINDOW_SOURCES", " main, ,pr-1228,main ")
	got := meteringWindowSources()
	if strings.Join(got, ",") != "main,pr-1228" {
		t.Fatalf("meteringWindowSources() = %q, want [main pr-1228]", got)
	}
}

func TestMeteringWindowEnablesObserveOnlyForListedSource(t *testing.T) {
	t.Setenv("SOAK_METERING_WINDOW_SOURCES", "main")

	mainSpec := DefaultMainFleet()
	if len(mainSpec.Workers) == 0 {
		t.Fatal("DefaultMainFleet() has no workers")
	}
	for _, spec := range []FleetSpec{mainSpec, DefaultFleetForSource("main", "sha", "ls")} {
		for _, worker := range spec.Workers {
			if !worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != "observe" {
				t.Fatalf("%s proxy = %v/%q, want enabled observe for metering source", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode)
			}
		}
	}
	for _, worker := range DefaultFleetForSource("pr-1228", "sha", "ls").Workers {
		if worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != "" {
			t.Fatalf("%s proxy = %v/%q, want direct replication for unlisted source", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode)
		}
	}
}

func TestMeteringWindowKeepsFaultModeProfiles(t *testing.T) {
	t.Setenv("SOAK_METERING_WINDOW_SOURCES", "main")

	workers := withMeteringWindowObserveProxy([]DesiredWorker{
		{Name: "fault", Source: "main", Workload: workloadWithProxy(true, "source-get-reset")},
		{Name: "default-fault", Source: "main", Workload: workloadWithProxy(true, "")},
		{Name: "plain", Source: "main"},
	})
	want := map[string]string{"fault": "source-get-reset", "default-fault": "", "plain": "observe"}
	for _, worker := range workers {
		if !worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != want[worker.Name] {
			t.Fatalf("%s proxy = %v/%q, want enabled %q", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode, want[worker.Name])
		}
	}
}

func TestMeteringWindowEmptyLeavesFleetSpecUnchanged(t *testing.T) {
	t.Setenv("SOAK_METERING_WINDOW_SOURCES", "")
	baseline := fleetSpecJSON(t)
	for _, worker := range DefaultMainFleet().Workers {
		if worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != "" {
			t.Fatalf("%s proxy = %v/%q, want direct replication with no metering window", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode)
		}
	}
	for _, value := range []string{" ", ",", "pr-9999"} {
		t.Setenv("SOAK_METERING_WINDOW_SOURCES", value)
		if got := fleetSpecJSON(t); got != baseline {
			t.Fatalf("fleet spec with SOAK_METERING_WINDOW_SOURCES=%q drifted from empty setting", value)
		}
	}
}

func fleetSpecJSON(t *testing.T) string {
	t.Helper()
	body, err := json.Marshal([]FleetSpec{DefaultMainFleet(), DefaultFleetForSource("main", "sha", "ls")})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestMeteringWindowResultsExcludedFromScoringAndComparison(t *testing.T) {
	db := openTestDB(t)
	for _, deployment := range []model.Deployment{
		{GitSHA: "sha-main", LitestreamSHA: "ls-main", ImageRef: "registry.fly.io/litestream-soak:sha-main", Source: "main", Status: "ready"},
		{GitSHA: "sha-pr", LitestreamSHA: "ls-pr", ImageRef: "registry.fly.io/litestream-soak:sha-pr", Source: "pr-1228", PRNumber: 1228, Status: "ready"},
	} {
		if err := db.UpsertReadyDeployment(&deployment); err != nil {
			t.Fatalf("UpsertReadyDeployment(%s) error = %v", deployment.Source, err)
		}
	}
	mainDeployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	prDeployment, err := db.GetLatestDeployment("pr-1228")
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range []model.Worker{
		{ID: "worker-main-low", Name: "worker-main-low", Source: "main", GitSHA: mainDeployment.GitSHA, LitestreamSHA: mainDeployment.LitestreamSHA, ProfileName: "low-volume"},
		{ID: "worker-main-high", Name: "worker-main-high", Source: "main", GitSHA: mainDeployment.GitSHA, LitestreamSHA: mainDeployment.LitestreamSHA, ProfileName: "high-volume"},
		{ID: "worker-pr-1228-low", Name: "worker-pr-1228-low", Source: "pr-1228", GitSHA: prDeployment.GitSHA, LitestreamSHA: prDeployment.LitestreamSHA, ProfileName: "low-volume"},
		{ID: "worker-pr-1228-high", Name: "worker-pr-1228-high", Source: "pr-1228", GitSHA: prDeployment.GitSHA, LitestreamSHA: prDeployment.LitestreamSHA, ProfileName: "high-volume"},
	} {
		worker.Status = model.WorkerRunning
		worker.ProfileConfig = "{}"
		createTestWorker(t, db, worker)
	}

	if err := db.SyncMeteringWindows([]string{"main"}, mainDeployment.StartedAt); err != nil {
		t.Fatalf("SyncMeteringWindows() error = %v", err)
	}

	mainAt := mainDeployment.StartedAt.Add(time.Second).UTC()
	prAt := prDeployment.StartedAt.Add(time.Second).UTC()
	for _, fixture := range []struct {
		workerID string
		at       time.Time
		passed   bool
	}{
		{"worker-main-low", mainAt, true},
		{"worker-main-high", mainAt, false},
		{"worker-pr-1228-low", prAt, true},
		{"worker-pr-1228-high", prAt, true},
	} {
		completed := fixture.at
		verification := &model.Verification{WorkerID: fixture.workerID, StartedAt: completed.Add(-15 * time.Second), CompletedAt: &completed, Status: "passed", CheckType: "integrity", Passed: fixture.passed, DurationMS: 15000}
		if !fixture.passed {
			verification.Status = "failed"
			verification.ErrorMessage = "wrong # of entries in index idx_load_test_timestamp"
		}
		mustRecordAttributedFixture(t, db, verification)
	}

	scorecard, err := buildDeploymentScorecard(db, *mainDeployment, nil)
	if err != nil {
		t.Fatalf("buildDeploymentScorecard(main) error = %v", err)
	}
	if scorecard.VerifiedWorkers != 0 || scorecard.FailedWorkers != 0 || scorecard.MeteringWindowExcluded != 2 {
		t.Fatalf("main scorecard verified/failed/excluded = %d/%d/%d, want 0/0/2", scorecard.VerifiedWorkers, scorecard.FailedWorkers, scorecard.MeteringWindowExcluded)
	}
	for _, evidence := range scorecard.Reliability {
		if evidence.VerificationCount != 0 || evidence.UnexpectedFailures != 0 || len(evidence.Incidents) != 0 {
			t.Fatalf("main reliability %s = %+v, want metering-window results excluded", evidence.WorkerID, evidence)
		}
	}

	prScorecard, err := buildDeploymentScorecard(db, *prDeployment, nil)
	if err != nil {
		t.Fatalf("buildDeploymentScorecard(pr) error = %v", err)
	}
	if prScorecard.PassedWorkers != 2 || prScorecard.MeteringWindowExcluded != 0 {
		t.Fatalf("pr scorecard passed/excluded = %d/%d, want 2/0", prScorecard.PassedWorkers, prScorecard.MeteringWindowExcluded)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	comparison, err := api.buildRequestedDeploymentComparison("", "main", "pr-1228")
	if err != nil {
		t.Fatalf("buildRequestedDeploymentComparison() error = %v", err)
	}
	if comparison.Base == nil || comparison.Base.FailedWorkers != 0 || comparison.FailDelta != 0 || len(comparison.ImprovedWorkers) != 0 {
		t.Fatalf("comparison base failed=%v failDelta=%d improved=%+v, want metering-window failure excluded", comparison.Base, comparison.FailDelta, comparison.ImprovedWorkers)
	}
}

func TestMeteringWindowBannerOnHomeAndWorkerPages(t *testing.T) {
	for _, listed := range []bool{false, true} {
		db := openTestDB(t)
		createTestWorker(t, db, model.Worker{ID: "worker-main-low", Name: "worker-main-low", Status: model.WorkerRunning, Source: "main", ProfileName: "low-volume", ProfileConfig: "{}"})
		createTestWorker(t, db, model.Worker{ID: "worker-pr-7-low", Name: "worker-pr-7-low", Status: model.WorkerRunning, Source: "pr-7", ProfileName: "low-volume", ProfileConfig: "{}"})
		if listed {
			if err := db.SyncMeteringWindows([]string{"main"}, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
		}
		api := NewAPI(db, nil, nil, nil, nil, nil)

		home := httptest.NewRecorder()
		api.handleHome(home, httptest.NewRequest(http.MethodGet, "/ui", nil))
		if home.Code != http.StatusOK {
			t.Fatalf("home status = %d", home.Code)
		}
		if got := strings.Contains(home.Body.String(), meteringBannerText); got != listed {
			t.Fatalf("listed=%v: home banner shown = %v", listed, got)
		}

		for workerID, want := range map[string]bool{"worker-main-low": listed, "worker-pr-7-low": false} {
			request := httptest.NewRequest(http.MethodGet, "/ui/workers/"+workerID, nil)
			request.SetPathValue("id", workerID)
			page := httptest.NewRecorder()
			api.handleWorkerPage(page, request)
			if page.Code != http.StatusOK {
				t.Fatalf("worker page %s status = %d", workerID, page.Code)
			}
			if got := strings.Contains(page.Body.String(), meteringBannerText); got != want {
				t.Fatalf("listed=%v: worker page %s banner shown = %v, want %v", listed, workerID, got, want)
			}
		}
	}
}

func TestCostMeterReportsMeteringWindow(t *testing.T) {
	db := openTestDB(t)
	since := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if err := db.SyncMeteringWindows([]string{"meter-window"}, since); err != nil {
		t.Fatal(err)
	}
	api := NewAPI(db, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/cost-meter?source=meter-window", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var body CostMeterResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Sources) != 1 || body.Sources[0].Source != "meter-window" {
		t.Fatalf("sources = %+v, want meter-window", body.Sources)
	}
	window := body.Sources[0].MeteringWindow
	if window == nil || !window.Active || !window.StartedAt.Equal(since) {
		t.Fatalf("metering window = %+v, want active since %s", window, since)
	}

	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/cost-meter?source=meter-other", nil))
	body = CostMeterResponse{}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Sources) != 0 {
		t.Fatalf("sources = %+v, want none for unlisted source", body.Sources)
	}
}

func workloadWithProxy(enabled bool, mode string) workload.Config {
	return workload.Config{S3FaultProxyEnabled: enabled, S3FaultProxyMode: mode}
}
