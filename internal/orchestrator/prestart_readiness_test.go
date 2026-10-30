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
