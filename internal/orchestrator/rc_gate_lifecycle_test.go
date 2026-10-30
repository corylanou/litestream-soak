package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func teardownGateManager(t *testing.T, db *model.DB, volumeFailures map[string]int) (*Manager, *teardownTestServer) {
	t.Helper()
	fly := newTeardownTestServer(t, volumeFailures)
	return NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{
		Bucket: "bucket", Endpoint: fly.server.URL, AccessKey: "access", SecretKey: "secret", Region: "auto",
	}, "", ""), fly
}

func TestRCGateTeardownDestroysStoppedWorkersAndRetriesUntilClean(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-time.Hour))
	createTestGateFleet(t, db, gate)
	failing := "worker-" + testGateBaseline + "-" + gateRemoteProfile + "-volume"
	manager, fly := teardownGateManager(t, db, map[string]int{failing: 1})
	ctx := context.Background()
	if err := manager.evaluateRCGates(ctx, gate.StartedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	stopped := "worker-" + testGateCandidate + "-" + gateLargeDBProfile
	if err := db.UpdateWorkerStatus(stopped, model.WorkerStopped, "stopped without cleanup"); err != nil {
		t.Fatal(err)
	}

	if err := manager.evaluateRCGates(ctx, gate.ExpiresAt.Add(time.Minute)); err == nil {
		t.Fatal("evaluateRCGates() succeeded despite a failed volume cleanup")
	}
	assertGateStatus(t, db, gate.ID, model.RCGateRunning)
	if !fly.machineDestroyed(stopped+"-machine") || !fly.volumeDestroyed(stopped+"-volume") {
		t.Fatal("stopped gate worker kept its machine or volume")
	}

	if err := manager.evaluateRCGates(ctx, gate.ExpiresAt.Add(2*time.Minute)); err != nil {
		t.Fatalf("retry evaluateRCGates() error = %v", err)
	}
	assertGateStatus(t, db, gate.ID, model.RCGateCompleted)
	for _, source := range gate.Sources() {
		for _, worker := range mustSourceWorkers(t, db, source) {
			if !fly.machineDestroyed(worker.FlyMachineID) || !fly.volumeDestroyed(worker.FlyVolumeID) {
				t.Fatalf("%s resources not destroyed", worker.Name)
			}
		}
	}
}

func TestRCGateTailRejectsReplayedRollout(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, true, time.Now().UTC().Add(-73*time.Hour))
	createTestGateFleet(t, db, gate)
	teardown, _ := teardownGateManager(t, db, nil)
	ctx := context.Background()
	if err := db.MarkRCGatePairedReady(gate.ID, gate.StartedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := teardown.evaluateRCGates(ctx, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	assertGateStatus(t, db, gate.ID, model.RCGateTail)

	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	deployer := NewDeployer(manager, db, "litestream-soak", false)
	deployment, err := db.GetLatestDeployment(testGateCandidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deployer.rolloutDeploymentReady(ctx, *deployment, "rc_gate_replay"); err != nil {
		t.Fatalf("rolloutDeploymentReady() error = %v", err)
	}
	if machines := fly.machineRequests(); len(machines) != 0 {
		t.Fatalf("tail rollout recreated %d machines", len(machines))
	}
	for _, worker := range mustSourceWorkers(t, db, testGateCandidate) {
		if (worker.ProfileName == gateStressProfile) != (worker.Status == model.WorkerRunning) {
			t.Fatalf("%s status = %s during tail", worker.Name, worker.Status)
		}
	}
}

func TestRCGateRejectsProvisioningOutsideItsWindow(t *testing.T) {
	ctx := context.Background()
	for name, setup := range map[string]func(*testing.T, *model.DB) model.RCGate{
		"past deadline": func(t *testing.T, db *model.DB) model.RCGate {
			gate := registerTestGate(t, db, false, time.Now().UTC().Add(-73*time.Hour))
			createTestGateFleet(t, db, gate)
			if err := db.MarkRCGatePairedReady(gate.ID, gate.StartedAt.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			return gate
		},
		"completed": func(t *testing.T, db *model.DB) model.RCGate {
			gate := registerTestGate(t, db, false, time.Now().UTC())
			createTestGateFleet(t, db, gate)
			if err := db.UpdateRCGateStatus(gate.ID, model.RCGateCompleted, new(time.Now().UTC()), `{"verdict":"incomplete"}`); err != nil {
				t.Fatal(err)
			}
			return gate
		},
	} {
		t.Run(name, func(t *testing.T) {
			db := openTestDB(t)
			gate := setup(t, db)
			dormant := "worker-" + testGateCandidate + "-" + gateRemoteProfile
			if err := db.MarkWorkerDormant(dormant, "broken", "integrity_check_mismatch", "known_bad_source"); err != nil {
				t.Fatal(err)
			}
			fly := newCreateWorkerFlyServer(t)
			manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
			image := testGateImage(testGateCandidate, testGateCandidateSHA)

			if _, err := manager.RollWorker(ctx, "worker-"+testGateCandidate+"-"+gateLargeDBProfile, image, testGateSoakSHA, testGateCandidateSHA); !errors.Is(err, errRCGateClosed) {
				t.Fatalf("RollWorker() err = %v, want errRCGateClosed", err)
			}
			if err := manager.ResumeDormantWorkers(ctx, testGateCandidate, "", "", "", "operator"); !errors.Is(err, errRCGateClosed) {
				t.Fatalf("ResumeDormantWorkers() err = %v, want errRCGateClosed", err)
			}
			if err := manager.EnsureSourceFleet(ctx, testGateCandidate, testGateSoakSHA, testGateCandidateSHA, image); err != nil && !errors.Is(err, errRCGateClosed) {
				t.Fatalf("EnsureSourceFleet() err = %v", err)
			}
			request, err := workerRequestForDesired(gateWorker(t, GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA), gateRemoteProfile), image, "worker-rc-new", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Name = "worker-rc-new"
			if _, err := manager.CreateWorker(ctx, request); !errors.Is(err, errRCGateClosed) {
				t.Fatalf("CreateWorker() err = %v, want errRCGateClosed", err)
			}
			if machines := fly.machineRequests(); len(machines) != 0 {
				t.Fatalf("closed gate provisioned %d machines", len(machines))
			}
			stops, deletes, volumes := fly.replacementCounts()
			if stops+deletes+volumes != 0 {
				t.Fatalf("closed gate touched machines: %d %d %d", stops, deletes, volumes)
			}
			_ = gate
		})
	}
}

func TestRCGateRejectsUnpinnedBuildsAtExecution(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC())
	createTestGateFleet(t, db, gate)
	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	other := "registry.fly.io/litestream-soak:sha-" + testGateSoakSHA[:12] + "-" + testGateCandidate + "-ls-" + testGateCandidateSHA[:12] + "-other"
	request, err := workerRequestForDesired(gateWorker(t, GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA), gateRemoteProfile), other, "worker-rc-extra", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Name = "worker-rc-extra"
	if _, err := manager.CreateWorker(context.Background(), request); !errors.Is(err, errRCGatePinned) {
		t.Fatalf("CreateWorker(unpinned image) err = %v, want errRCGatePinned", err)
	}
	if len(fly.machineRequests()) != 0 {
		t.Fatal("unpinned build provisioned a machine")
	}
}

func TestRCGateAbortsUnpairedLaunchAndCleansPartialResources(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC())
	createTestGateFleet(t, db, gate)
	baselineWorkers := mustSourceWorkers(t, db, testGateBaseline)
	if err := db.UpdateWorkerStatus(baselineWorkers[0].ID, model.WorkerPending, "provisioning"); err != nil {
		t.Fatal(err)
	}
	manager, fly := teardownGateManager(t, db, nil)
	ctx := context.Background()

	if err := manager.evaluateRCGates(ctx, gate.ProvisionDeadline.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertGateStatus(t, db, gate.ID, model.RCGateRunning)
	if err := manager.evaluateRCGates(ctx, gate.ProvisionDeadline.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertGateStatus(t, db, gate.ID, model.RCGateAborted)
	for _, source := range gate.Sources() {
		for _, worker := range mustSourceWorkers(t, db, source) {
			if worker.Status != model.WorkerStopped || !fly.machineDestroyed(worker.FlyMachineID) || !fly.volumeDestroyed(worker.FlyVolumeID) {
				t.Fatalf("%s not cleaned up after unpaired abort: %s", worker.Name, worker.Status)
			}
		}
	}
	stored, err := db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	var results rcGateResults
	if err := json.Unmarshal([]byte(stored.Results), &results); err != nil || results.Verdict != rcGateVerdictIncomplete {
		t.Fatalf("aborted results = %q, %v", stored.Results, err)
	}
}

func TestRCGatePairedLaunchDoesNotAbort(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC())
	createTestGateFleet(t, db, gate)
	manager, _ := teardownGateManager(t, db, nil)
	if err := manager.evaluateRCGates(context.Background(), gate.StartedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := manager.evaluateRCGates(context.Background(), gate.ProvisionDeadline.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetRCGate(gate.ID)
	if err != nil || stored.Status != model.RCGateRunning || stored.PairedReadyAt == nil {
		t.Fatalf("gate = %+v, %v", stored, err)
	}
}

func TestRCGateClosedLedgerServesBoundedSnapshot(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-73*time.Hour))
	createTestGateFleet(t, db, gate)
	if err := db.MarkRCGatePairedReady(gate.ID, gate.StartedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	inside := gate.StartedAt.Add(time.Hour)
	for _, source := range gate.Sources() {
		for _, scenario := range []struct{ profile, identity string }{
			{gateStressProfile, "overload-truncate0"},
			{gateStressProfile, "pinned-reader"},
			{gateManyDB500Profile, ""},
			{gateLargeDBProfile, ""},
			{gateRemoteProfile, ""},
		} {
			recordGateVerification(t, db, source, scenario.profile, scenario.identity, true, inside)
			if scenario.profile != gateManyDB500Profile {
				recordGateCoverageCheck(t, db, source, scenario.profile, scenario.identity, true, inside)
			}
		}
	}
	manager, _ := teardownGateManager(t, db, nil)
	closedAt := gate.ExpiresAt.Add(time.Minute)
	if err := manager.evaluateRCGates(context.Background(), closedAt); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetRCGate(gate.ID)
	if err != nil || stored.Status != model.RCGateCompleted {
		t.Fatalf("gate = %+v, %v", stored, err)
	}
	before, err := buildRCGateLedgerEntry(db, *stored, time.Now().UTC())
	if err != nil || before.Verdict != rcGateVerdictPass {
		t.Fatalf("closed verdict = %q, %v", before.Verdict, err)
	}

	recordGateVerification(t, db, testGateCandidate, gateLargeDBProfile, "", false, inside.Add(time.Minute))
	after, err := buildRCGateLedgerEntry(db, *stored, time.Now().UTC())
	if err != nil || after.Verdict != rcGateVerdictPass {
		t.Fatalf("closed verdict changed to %q after new evidence, %v", after.Verdict, err)
	}
}
