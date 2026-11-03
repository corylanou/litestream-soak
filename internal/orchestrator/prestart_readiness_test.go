package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

// TestGateReadinessIgnoresPreStartWorkerFailures provisions an observe_proxy
// gate and reports the required proxy failing on both sides, once with the
// startup phase the worker now sets before the proxy starts and once as a
// legacy report without a phase. Neither may flip the worker to running or
// pair the gate; a real post-startup heartbeat still does.
func TestGateReadinessIgnoresPreStartWorkerFailures(t *testing.T) {
	fake := &provisioningFly{}
	server := httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(server.Close)
	db := openTestDB(t)
	manager := NewManager(flyapi.NewClientWithBaseURL("app", "test", server.URL), db, nil, nil, "app", ReplicaConfig{}, "", "")
	api := NewAPI(db, nil, nil, nil, manager, nil)
	ctx := context.Background()

	gate, _, err := manager.RegisterRCGate(RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "24h",
		Profiles:     []string{gateLargeDBProfile},
		SourceSuffix: "obsfail",
		ObserveProxy: true,
	}, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatalf("RegisterRCGate() error = %v", err)
	}
	workerIDs := map[string]string{}
	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		image := testGateImage(source, litestreamSHA)
		if err := db.UpsertReadyDeployment(&model.Deployment{GitSHA: testGateSoakSHA, LitestreamSHA: litestreamSHA, ImageRef: image, Source: source, Status: "ready"}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.PinRCGateBuild(gate.ID, source, image, testGateSoakSHA); err != nil {
			t.Fatal(err)
		}
		spec, err := manager.rcGateFleetForSource(source, testGateSoakSHA, litestreamSHA, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if len(spec.Workers) != 1 || !spec.Workers[0].Workload.S3ObserveProxyRequired {
			t.Fatalf("%s fleet = %+v, want one worker with a required observe proxy", source, spec.Workers)
		}
		if err := manager.ensureFleetSpec(ctx, spec, image); err != nil {
			t.Fatal(err)
		}
		workerIDs[source] = spec.Workers[0].WorkerID
	}

	identity := func(t *testing.T, workerID string) reporting.WorkerIdentity {
		t.Helper()
		expected, err := db.ExpectedWorkerRun(workerID)
		if err != nil || expected == nil {
			t.Fatalf("ExpectedWorkerRun(%s) = %+v, %v", workerID, expected, err)
		}
		digest := sha256.Sum256([]byte(expected.ProfileConfig))
		expected.ProfileHash = fmt.Sprintf("%x", digest[:8])
		expected.ValidatorID = "soak-verifier:" + expected.GitSHA
		return *expected
	}
	report := func(t *testing.T, workerID, path string, body any) {
		t.Helper()
		payload, _ := json.Marshal(body)
		request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/"+path, bytes.NewReader(payload))
		request.SetPathValue("id", workerID)
		response := httptest.NewRecorder()
		if path == "events" {
			api.handleWorkerEvent(response, request)
		} else {
			api.handleHeartbeat(response, request)
		}
		if response.Code != http.StatusAccepted {
			t.Fatalf("%s %s: %d %s", path, workerID, response.Code, response.Body.String())
		}
	}
	assertUnpaired := func(t *testing.T, stage string, wantStatus model.WorkerStatus) map[string]time.Time {
		t.Helper()
		stored, err := db.GetRCGate(gate.ID)
		if err != nil {
			t.Fatal(err)
		}
		heartbeats := map[string]time.Time{}
		for _, source := range stored.Sources() {
			worker := mustWorker(t, db, workerIDs[source])
			if worker.Status != wantStatus {
				t.Fatalf("%s: %s status = %s, want %s", stage, worker.ID, worker.Status, wantStatus)
			}
			if worker.LastHeartbeatAt == nil {
				t.Fatalf("%s: %s has no heartbeat timestamp, so the stale clock never starts", stage, worker.ID)
			}
			heartbeats[source] = *worker.LastHeartbeatAt
			if ready, err := manager.rcGateSideReady(*stored, source); err != nil || ready {
				t.Fatalf("%s: rcGateSideReady(%s) = %v, %v; want not ready", stage, source, ready, err)
			}
		}
		if err := manager.evaluateRCGate(ctx, *stored, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if stored, err = db.GetRCGate(gate.ID); err != nil || stored.PairedReadyAt != nil || stored.CandidateReadyAt != nil || stored.BaselineReadyAt != nil {
			t.Fatalf("%s: gate paired after a pre-start failure: %+v, %v", stage, stored, err)
		}
		return heartbeats
	}

	failure := "start s3 fault proxy: s3 observe proxy is required by this run and could not start: s3 fault proxy upstream request failed"
	for _, source := range gate.Sources() {
		sentAt := time.Now().UTC()
		report(t, workerIDs[source], "events", reporting.WorkerEventPayload{
			WorkerIdentity: identity(t, workerIDs[source]),
			EventType:      "worker_failed",
			Message:        failure,
			SentAt:         sentAt,
			RuntimePayload: reporting.RuntimePayload{StartupPhase: reporting.StartupPhaseProxy, SnapshotCollectedAt: sentAt},
		})
	}
	firstHeartbeats := assertUnpaired(t, "proxy-phase worker_failed", model.WorkerStarting)

	// last_heartbeat_at has one-second resolution, so a later restart-loop
	// report must land in a later second to prove it did not refresh it.
	time.Sleep(1100 * time.Millisecond)
	for _, source := range gate.Sources() {
		sentAt := time.Now().UTC()
		report(t, workerIDs[source], "events", reporting.WorkerEventPayload{
			WorkerIdentity: identity(t, workerIDs[source]),
			EventType:      "worker_failed",
			Message:        failure,
			SentAt:         sentAt,
			RuntimePayload: reporting.RuntimePayload{SnapshotCollectedAt: sentAt},
		})
	}
	repeatedHeartbeats := assertUnpaired(t, "legacy worker_failed without a startup phase", model.WorkerStarting)
	for source, first := range firstHeartbeats {
		if !repeatedHeartbeats[source].Equal(first) {
			t.Fatalf("%s heartbeat advanced from %v to %v on a repeated pre-start failure, so the worker would never go stale", source, first, repeatedHeartbeats[source])
		}
	}

	manager.checkStaleWorkers(ctx, time.Millisecond)
	for _, source := range gate.Sources() {
		if worker := mustWorker(t, db, workerIDs[source]); worker.Status != model.WorkerDegraded || worker.ErrorMessage != "worker missed heartbeat deadline" {
			t.Fatalf("%s after the stale check = %s %q, want degraded by the heartbeat deadline", worker.ID, worker.Status, worker.ErrorMessage)
		}
	}
	assertUnpaired(t, "stale restart loop", model.WorkerDegraded)

	for _, source := range gate.Sources() {
		sentAt := time.Now().UTC()
		report(t, workerIDs[source], "heartbeat", reporting.HeartbeatPayload{
			WorkerIdentity: identity(t, workerIDs[source]),
			SentAt:         sentAt,
			RuntimePayload: reporting.RuntimePayload{SnapshotCollectedAt: sentAt},
		})
		if worker := mustWorker(t, db, workerIDs[source]); worker.Status != model.WorkerRunning {
			t.Fatalf("%s status after a post-startup heartbeat = %s, want running", worker.ID, worker.Status)
		}
	}
	stored, err := db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.evaluateRCGate(ctx, *stored, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if stored, err = db.GetRCGate(gate.ID); err != nil || stored.PairedReadyAt == nil {
		t.Fatalf("gate not paired after real startup heartbeats: %+v, %v", stored, err)
	}
}

// TestGateReadinessRunningWorkerCrashLoopGoesStale covers a side that reached
// running and then fails on every restart before its proxy starts: the
// failures never refresh its heartbeat or change its status, so the stale
// check degrades it and the gate does not pair; further failures while
// degraded change nothing; a real heartbeat recovers it.
func TestGateReadinessRunningWorkerCrashLoopGoesStale(t *testing.T) {
	fake := &provisioningFly{}
	server := httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(server.Close)
	db := openTestDB(t)
	manager := NewManager(flyapi.NewClientWithBaseURL("app", "test", server.URL), db, nil, nil, "app", ReplicaConfig{}, "", "")
	api := NewAPI(db, nil, nil, nil, manager, nil)
	ctx := context.Background()

	gate, _, err := manager.RegisterRCGate(RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "24h",
		Profiles:     []string{gateLargeDBProfile},
		SourceSuffix: "runloop",
		ObserveProxy: true,
	}, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatalf("RegisterRCGate() error = %v", err)
	}
	workerIDs := map[string]string{}
	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		image := testGateImage(source, litestreamSHA)
		if err := db.UpsertReadyDeployment(&model.Deployment{GitSHA: testGateSoakSHA, LitestreamSHA: litestreamSHA, ImageRef: image, Source: source, Status: "ready"}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.PinRCGateBuild(gate.ID, source, image, testGateSoakSHA); err != nil {
			t.Fatal(err)
		}
		spec, err := manager.rcGateFleetForSource(source, testGateSoakSHA, litestreamSHA, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.ensureFleetSpec(ctx, spec, image); err != nil {
			t.Fatal(err)
		}
		workerIDs[source] = spec.Workers[0].WorkerID
	}
	candidate := workerIDs[gate.CandidateSource]

	identity := func(t *testing.T, workerID string) reporting.WorkerIdentity {
		t.Helper()
		expected, err := db.ExpectedWorkerRun(workerID)
		if err != nil || expected == nil {
			t.Fatalf("ExpectedWorkerRun(%s) = %+v, %v", workerID, expected, err)
		}
		digest := sha256.Sum256([]byte(expected.ProfileConfig))
		expected.ProfileHash = fmt.Sprintf("%x", digest[:8])
		expected.ValidatorID = "soak-verifier:" + expected.GitSHA
		return *expected
	}
	heartbeat := func(t *testing.T, workerID string) {
		t.Helper()
		sentAt := time.Now().UTC()
		payload, _ := json.Marshal(reporting.HeartbeatPayload{WorkerIdentity: identity(t, workerID), SentAt: sentAt, RuntimePayload: reporting.RuntimePayload{SnapshotCollectedAt: sentAt}})
		request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/heartbeat", bytes.NewReader(payload))
		request.SetPathValue("id", workerID)
		response := httptest.NewRecorder()
		api.handleHeartbeat(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("heartbeat %s: %d %s", workerID, response.Code, response.Body.String())
		}
	}
	proxyFailure := func(t *testing.T, workerID string) {
		t.Helper()
		sentAt := time.Now().UTC()
		payload, _ := json.Marshal(reporting.WorkerEventPayload{
			WorkerIdentity: identity(t, workerID),
			EventType:      "worker_failed",
			Message:        "start s3 fault proxy: s3 observe proxy is required by this run and could not start: s3 fault proxy upstream request failed",
			SentAt:         sentAt,
			RuntimePayload: reporting.RuntimePayload{StartupPhase: reporting.StartupPhaseProxy, SnapshotCollectedAt: sentAt},
		})
		request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/events", bytes.NewReader(payload))
		request.SetPathValue("id", workerID)
		response := httptest.NewRecorder()
		api.handleWorkerEvent(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("worker_failed %s: %d %s", workerID, response.Code, response.Body.String())
		}
	}
	assertWorker := func(t *testing.T, stage string, status model.WorkerStatus, heartbeatAt *time.Time) time.Time {
		t.Helper()
		worker := mustWorker(t, db, candidate)
		if worker.Status != status || worker.LastHeartbeatAt == nil || (heartbeatAt != nil && !worker.LastHeartbeatAt.Equal(*heartbeatAt)) {
			t.Fatalf("%s: candidate = %s heartbeat %v, want %s with heartbeat %v", stage, worker.Status, worker.LastHeartbeatAt, status, heartbeatAt)
		}
		return *worker.LastHeartbeatAt
	}
	assertGate := func(t *testing.T, stage string, wantCandidateReady, wantPaired bool) {
		t.Helper()
		stored, err := db.GetRCGate(gate.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ready, err := manager.rcGateSideReady(*stored, gate.CandidateSource); err != nil || ready != wantCandidateReady {
			t.Fatalf("%s: rcGateSideReady(candidate) = %v, %v; want %v", stage, ready, err, wantCandidateReady)
		}
		if err := manager.evaluateRCGate(ctx, *stored, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if stored, err = db.GetRCGate(gate.ID); err != nil || (stored.PairedReadyAt != nil) != wantPaired || stored.BaselineReadyAt != nil {
			t.Fatalf("%s: gate = %+v, %v; want paired %v with the baseline never ready", stage, stored, err, wantPaired)
		}
	}

	heartbeat(t, candidate)
	runningAt := assertWorker(t, "after a real heartbeat", model.WorkerRunning, nil)
	assertGate(t, "candidate running, baseline silent", true, false)

	// last_heartbeat_at has one-second resolution, so a later restart-loop
	// report must land in a later second to prove it did not refresh it.
	time.Sleep(1100 * time.Millisecond)
	for range 3 {
		proxyFailure(t, candidate)
	}
	assertWorker(t, "running worker in a proxy crash loop", model.WorkerRunning, &runningAt)
	manager.checkStaleWorkers(ctx, time.Millisecond)
	if worker := mustWorker(t, db, candidate); worker.Status != model.WorkerDegraded || worker.ErrorMessage != "worker missed heartbeat deadline" {
		t.Fatalf("after the stale check: candidate = %s %q, want degraded by the heartbeat deadline", worker.Status, worker.ErrorMessage)
	}
	assertGate(t, "stale crash loop", false, false)

	time.Sleep(1100 * time.Millisecond)
	for range 3 {
		proxyFailure(t, candidate)
	}
	assertWorker(t, "degraded worker in a proxy crash loop", model.WorkerDegraded, &runningAt)
	if worker := mustWorker(t, db, candidate); worker.ErrorMessage != "worker missed heartbeat deadline" {
		t.Fatalf("degraded reason changed to %q", worker.ErrorMessage)
	}
	assertGate(t, "degraded crash loop", false, false)

	heartbeat(t, candidate)
	recoveredAt := assertWorker(t, "after a recovering heartbeat", model.WorkerRunning, nil)
	if !recoveredAt.After(runningAt) {
		t.Fatalf("recovering heartbeat did not advance the clock: %v vs %v", recoveredAt, runningAt)
	}
	assertGate(t, "recovered candidate, baseline still silent", true, false)
}

// TestGateReadinessFollowsProvisioningThenStartupHeartbeats drives the real
// path: the gate fleet is provisioned through the Fly API fake, the worker
// heartbeats while it is still populating, and only a heartbeat sent after
// startup completes makes the side ready and the gate paired.
func TestGateReadinessFollowsProvisioningThenStartupHeartbeats(t *testing.T) {
	fake := &provisioningFly{}
	server := httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(server.Close)
	db := openTestDB(t)
	manager := NewManager(flyapi.NewClientWithBaseURL("app", "test", server.URL), db, nil, nil, "app", ReplicaConfig{}, "", "")
	api := NewAPI(db, nil, nil, nil, manager, nil)
	ctx := context.Background()

	registered := time.Now().UTC().Add(-time.Minute)
	gate, _, err := manager.RegisterRCGate(RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateRef: "pull/1514/head",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "24h",
		Profiles:     []string{gateLargeDBProfile},
		SourceSuffix: "dfdc1c35",
	}, registered)
	if err != nil {
		t.Fatalf("RegisterRCGate() error = %v", err)
	}

	workerIDs := map[string]string{}
	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		image := testGateImage(source, litestreamSHA)
		if err := db.UpsertReadyDeployment(&model.Deployment{GitSHA: testGateSoakSHA, LitestreamSHA: litestreamSHA, ImageRef: image, Source: source, Status: "ready"}); err != nil {
			t.Fatalf("UpsertReadyDeployment(%s) error = %v", source, err)
		}
		if _, _, err := db.PinRCGateBuild(gate.ID, source, image, testGateSoakSHA); err != nil {
			t.Fatalf("PinRCGateBuild(%s) error = %v", source, err)
		}
		spec, err := manager.rcGateFleetForSource(source, testGateSoakSHA, litestreamSHA, time.Now().UTC())
		if err != nil {
			t.Fatalf("rcGateFleetForSource(%s) error = %v", source, err)
		}
		if len(spec.Workers) != 1 || spec.Workers[0].ProfileName != gateLargeDBProfile {
			t.Fatalf("%s fleet = %+v, want only gate-large-db", source, spec.Workers)
		}
		if err := manager.ensureFleetSpec(ctx, spec, image); err != nil {
			t.Fatalf("ensureFleetSpec(%s) error = %v", source, err)
		}
		workerIDs[source] = spec.Workers[0].WorkerID
		worker := mustWorker(t, db, workerIDs[source])
		if worker.Status != model.WorkerStarting || worker.FlyMachineID == "" {
			t.Fatalf("provisioned %s = status %s machine %q, want starting with a machine", worker.ID, worker.Status, worker.FlyMachineID)
		}
	}
	if fake.machinePosts != 2 || fake.volumePosts != 2 {
		t.Fatalf("provisioned machines=%d volumes=%d, want one per side", fake.machinePosts, fake.volumePosts)
	}

	heartbeat := func(t *testing.T, workerID, phase string) {
		t.Helper()
		expected, err := db.ExpectedWorkerRun(workerID)
		if err != nil || expected == nil {
			t.Fatalf("ExpectedWorkerRun(%s) = %+v, %v", workerID, expected, err)
		}
		digest := sha256.Sum256([]byte(expected.ProfileConfig))
		expected.ProfileHash = fmt.Sprintf("%x", digest[:8])
		expected.ValidatorID = "soak-verifier:" + expected.GitSHA
		sentAt := time.Now().UTC()
		payload, _ := json.Marshal(reporting.HeartbeatPayload{
			WorkerIdentity: *expected,
			SentAt:         sentAt,
			RuntimePayload: reporting.RuntimePayload{StartupPhase: phase, SnapshotCollectedAt: sentAt},
		})
		request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/heartbeat", bytes.NewReader(payload))
		request.SetPathValue("id", workerID)
		response := httptest.NewRecorder()
		api.handleHeartbeat(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("heartbeat %s phase %q: %d %s", workerID, phase, response.Code, response.Body.String())
		}
	}
	reload := func(t *testing.T) model.RCGate {
		t.Helper()
		stored, err := db.GetRCGate(gate.ID)
		if err != nil {
			t.Fatalf("GetRCGate() error = %v", err)
		}
		return *stored
	}
	assertSides := func(t *testing.T, stage string, wantReady bool) {
		t.Helper()
		current := reload(t)
		for _, source := range current.Sources() {
			ready, err := manager.rcGateSideReady(current, source)
			if err != nil || ready != wantReady {
				t.Fatalf("%s: rcGateSideReady(%s) = %v, %v; want %v", stage, source, ready, err, wantReady)
			}
		}
	}

	steps := []struct {
		name       string
		phase      string
		wantStatus model.WorkerStatus
		wantReady  bool
		wantPaired bool
	}{
		{name: "populate heartbeat keeps the side unready", phase: reporting.StartupPhasePopulate, wantStatus: model.WorkerStarting},
		{name: "a second populate heartbeat still does not latch readiness", phase: reporting.StartupPhasePopulate, wantStatus: model.WorkerStarting},
		{name: "the first post-startup heartbeat makes both sides ready and pairs the gate", phase: "", wantStatus: model.WorkerRunning, wantReady: true, wantPaired: true},
	}
	assertSides(t, "after provisioning", false)
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			for _, source := range gate.Sources() {
				heartbeat(t, workerIDs[source], step.phase)
				if worker := mustWorker(t, db, workerIDs[source]); worker.Status != step.wantStatus || worker.LastHeartbeatAt == nil {
					t.Fatalf("%s status = %s heartbeat %v, want %s with a heartbeat", worker.ID, worker.Status, worker.LastHeartbeatAt, step.wantStatus)
				}
			}
			assertSides(t, step.name, step.wantReady)
			if err := manager.evaluateRCGate(ctx, reload(t), time.Now().UTC()); err != nil {
				t.Fatalf("evaluateRCGate() error = %v", err)
			}
			current := reload(t)
			if (current.PairedReadyAt != nil) != step.wantPaired || (current.CandidateReadyAt != nil) != step.wantReady || (current.BaselineReadyAt != nil) != step.wantReady {
				t.Fatalf("gate readiness = candidate %v baseline %v paired %v, want ready %v paired %v", current.CandidateReadyAt, current.BaselineReadyAt, current.PairedReadyAt, step.wantReady, step.wantPaired)
			}
			if current.Status != model.RCGateRunning {
				t.Fatalf("gate status = %s, want running", current.Status)
			}
		})
	}
}
