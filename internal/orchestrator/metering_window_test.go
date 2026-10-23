package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	workerconfig "github.com/corylanou/litestream-soak/internal/worker"
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
			if !worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != "observe" || !worker.Workload.MeteringWindow {
				t.Fatalf("%s proxy = %v/%q metering=%v, want enabled observe metering run for metering source", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode, worker.Workload.MeteringWindow)
			}
		}
	}
	for _, worker := range DefaultFleetForSource("pr-1228", "sha", "ls").Workers {
		if worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != "" || worker.Workload.MeteringWindow {
			t.Fatalf("%s proxy = %v/%q metering=%v, want direct replication for unlisted source", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode, worker.Workload.MeteringWindow)
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
		if !worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != want[worker.Name] || !worker.Workload.MeteringWindow {
			t.Fatalf("%s proxy = %v/%q metering=%v, want enabled %q metering run", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode, worker.Workload.MeteringWindow, want[worker.Name])
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

func TestMeteringRunResultsExcludedFromScoringAndComparison(t *testing.T) {
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

	mainAt := mainDeployment.StartedAt.Add(time.Second).UTC()
	prAt := prDeployment.StartedAt.Add(time.Second).UTC()
	for _, fixture := range []struct {
		workerID string
		at       time.Time
		passed   bool
		metering bool
	}{
		{"worker-main-low", mainAt, true, true},
		{"worker-main-high", mainAt, false, true},
		{"worker-pr-1228-low", prAt, true, false},
		{"worker-pr-1228-high", prAt, true, false},
	} {
		completed := fixture.at
		verification := &model.Verification{WorkerID: fixture.workerID, StartedAt: completed.Add(-15 * time.Second), CompletedAt: &completed, Status: "passed", CheckType: "integrity", Passed: fixture.passed, DurationMS: 15000}
		if !fixture.passed {
			verification.Status = "failed"
			verification.ErrorMessage = "wrong # of entries in index idx_load_test_timestamp"
		}
		mustRecordRunFixture(t, db, verification, fixture.metering)
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
			t.Fatalf("main reliability %s = %+v, want metering-run results excluded", evidence.WorkerID, evidence)
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
		t.Fatalf("comparison base failed=%v failDelta=%d improved=%+v, want metering-run failure excluded", comparison.Base, comparison.FailDelta, comparison.ImprovedWorkers)
	}
}

func TestMeteringRunRuntimeErrorsDoNotLeakIntoLaterRun(t *testing.T) {
	db := openTestDB(t)
	deployment := model.Deployment{GitSHA: "soak", LitestreamSHA: "litestream", WorkloadSHA: "generator", Source: "main", ImageRef: "image", Status: "ready"}
	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	deployment = *stored
	worker := model.Worker{ID: "meter-churn", Name: "meter-churn", Source: "main", ProfileName: "high-volume", ProfileConfig: "{}", FlyMachineID: "machine"}
	createTestWorker(t, db, worker)
	direct := fixtureRun(worker, deployment)
	metered := meteringRun(direct)
	metered.RunID = "metered"
	direct.RunID = "direct"
	for _, sample := range []struct {
		run  reporting.WorkerIdentity
		body string
	}{
		{metered, `{"litestream_snapshot_healthy":true,"workload_counters_present":true,"workload_counter_epoch":"e1","workload_attempts_total":20,"workload_mutations_total":10,"workload_errors_total":10,"maintenance_epoch":"m1","maintenance_observer_complete":true,"maintenance_observer_started_at":"2026-10-23T00:00:00Z","litestream_log_errors_total":4}`},
		{direct, `{"litestream_snapshot_healthy":true,"workload_counters_present":true,"workload_counter_epoch":"e1","workload_attempts_total":30,"workload_mutations_total":27,"workload_errors_total":3}`},
	} {
		if err := db.RecordRuntimeEvidence(sample.run, json.RawMessage(sample.body), true); err != nil {
			t.Fatal(err)
		}
	}
	evidence, err := buildRunReliability(db, deployment, nil)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
	if e := evidence[0]; e.WorkloadErrors != 3 || e.UnexpectedFailures != 3 || e.WorkloadAttempts != 30 {
		t.Fatalf("errors/unexpected/attempts = %d/%d/%d, want 3/3/30 with metering run excluded", e.WorkloadErrors, e.UnexpectedFailures, e.WorkloadAttempts)
	}
}

func TestMeteringRunFailuresDoNotPauseSource(t *testing.T) {
	for _, metering := range []bool{false, true} {
		db := openTestDB(t)
		if err := db.UpsertReadyDeployment(&model.Deployment{GitSHA: "soak-sha", LitestreamSHA: "litestream-sha", ImageRef: "registry.fly.io/litestream-soak:soak-sha", Source: "main", Status: "ready"}); err != nil {
			t.Fatal(err)
		}
		deployment, err := db.GetLatestDeployment("main")
		if err != nil {
			t.Fatal(err)
		}
		createTestWorker(t, db, model.Worker{ID: "worker-main-low-vol", Name: "worker-main-low-vol", Status: model.WorkerDegraded, Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, ProfileName: "low-volume", ProfileConfig: "{}"})
		createTestWorker(t, db, model.Worker{ID: "worker-main-read-heavy", Name: "worker-main-read-heavy", Status: model.WorkerRunning, Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, ProfileName: "read-heavy", ProfileConfig: "{}"})

		verifiedAt := time.Now().UTC().Add(time.Minute)
		for _, age := range []time.Duration{40 * time.Second, 15 * time.Second} {
			done := verifiedAt.Add(-age).Add(5 * time.Second)
			mustRecordRunFixture(t, db, &model.Verification{WorkerID: "worker-main-low-vol", StartedAt: verifiedAt.Add(-age), CompletedAt: &done, Status: "failed", CheckType: "integrity", ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": context deadline exceeded`}, metering)
		}
		mustRecordRunFixture(t, db, &model.Verification{WorkerID: "worker-main-read-heavy", StartedAt: verifiedAt.Add(-10 * time.Second), CompletedAt: &verifiedAt, Status: "passed", CheckType: "integrity", Passed: true}, metering)

		_, ok, err := failedSourcePauseCandidate(db, *deployment, FailedSourcePausePolicy{})
		if err != nil {
			t.Fatalf("metering=%v: failedSourcePauseCandidate() error = %v", metering, err)
		}
		if ok == metering {
			t.Fatalf("metering=%v: failedSourcePauseCandidate() = %v, want %v", metering, ok, !metering)
		}
	}
}

func TestMeteringRunFailuresDoNotTriggerDormancy(t *testing.T) {
	now := time.Now().UTC()
	worker := model.Worker{ID: "worker-meter", CreatedAt: now.Add(-72 * time.Hour)}
	for _, metering := range []bool{false, true} {
		var verifications []model.Verification
		for _, age := range []time.Duration{time.Hour, 12 * time.Hour, 30 * time.Hour} {
			started := now.Add(-age)
			completed := started.Add(10 * time.Second)
			run := reporting.WorkerIdentity{RunID: "run", ProfileConfig: "{}"}
			if metering {
				run = meteringRun(run)
			}
			verifications = append(verifications, model.Verification{WorkerID: worker.ID, StartedAt: started, CompletedAt: &completed, Status: "failed", CheckType: "integrity", ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`, Run: run})
		}
		_, ok := detectDormancyCandidate(dormancyVerifications(worker, verifications), now, 24*time.Hour, 3)
		if ok == metering {
			t.Fatalf("metering=%v: dormancy candidate = %v, want %v", metering, ok, !metering)
		}
	}
}

func TestMeteringWindowMarkerRoundTripsThroughWorkerEnv(t *testing.T) {
	m := &Manager{}
	for _, metering := range []bool{false, true} {
		env := m.workerEnv(model.Worker{ID: "meter-env"}, workload.Config{LoadMode: "synthetic", S3FaultProxyEnabled: metering, S3FaultProxyMode: "observe", MeteringWindow: metering})
		effective, err := workerconfig.WorkloadFromEnvironment(env)
		if err != nil {
			t.Fatal(err)
		}
		if effective.MeteringWindow != metering || runInMeteringWindow(reporting.WorkerIdentity{ProfileConfig: effective.JSON()}) != metering {
			t.Fatalf("metering=%v: effective profile %s lost the marker", metering, effective.JSON())
		}
	}
}

func meteringRun(run reporting.WorkerIdentity) reporting.WorkerIdentity {
	run.ProfileConfig = workload.Config{S3FaultProxyEnabled: true, S3FaultProxyMode: "observe", MeteringWindow: true}.JSON()
	run.ProfileHash = "metering"
	return run
}

func mustRecordRunFixture(t *testing.T, db *model.DB, verification *model.Verification, metering bool) {
	t.Helper()
	worker, err := db.GetWorker(verification.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetLatestDeployment(worker.Source)
	if err != nil {
		t.Fatal(err)
	}
	if worker.FlyMachineID == "" {
		worker.FlyMachineID = "machine-" + worker.ID
		if err := db.UpdateWorkerMachine(worker.ID, worker.FlyMachineID, worker.FlyVolumeID); err != nil {
			t.Fatal(err)
		}
	}
	verification.Run = fixtureRun(*worker, *deployment)
	if metering {
		verification.Run = meteringRun(verification.Run)
	}
	verification.Attributed = true
	if err := db.RecordVerification(verification); err != nil {
		t.Fatal(err)
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
