package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func assertGateWorkersDestroyed(t *testing.T, db *model.DB, fly *teardownTestServer, gate model.RCGate) {
	t.Helper()
	for _, source := range gate.Sources() {
		for _, worker := range mustSourceWorkers(t, db, source) {
			if worker.Status != model.WorkerStopped || !fly.machineDestroyed(worker.FlyMachineID) || !fly.volumeDestroyed(worker.FlyVolumeID) {
				t.Fatalf("%s not destroyed: %s", worker.Name, worker.Status)
			}
		}
	}
}

func TestRCGateTTLAppliesWhileAwaitingPair(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGateFor(t, db, "1h", false, time.Now().UTC())
	createTestGateFleet(t, db, gate)
	pending := "worker-" + testGateBaseline + "-" + gateRemoteProfile
	if err := db.UpdateWorkerStatus(pending, model.WorkerPending, "provisioning"); err != nil {
		t.Fatal(err)
	}
	manager, fly := teardownGateManager(t, db, nil)
	expiredBeforeLaunchDeadline := gate.ExpiresAt.Add(time.Minute)
	if !expiredBeforeLaunchDeadline.Before(gate.ProvisionDeadline) {
		t.Fatalf("fixture expiry %s is not before launch deadline %s", expiredBeforeLaunchDeadline, gate.ProvisionDeadline)
	}
	if err := manager.evaluateRCGates(context.Background(), expiredBeforeLaunchDeadline); err != nil {
		t.Fatal(err)
	}
	assertGateStatus(t, db, gate.ID, model.RCGateAborted)
	assertGateWorkersDestroyed(t, db, fly, gate)
}

func TestRCGateRejectsPairFirstObservedAfterLaunchDeadline(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC())
	createTestGateFleet(t, db, gate)
	manager, fly := teardownGateManager(t, db, nil)
	if err := manager.evaluateRCGates(context.Background(), gate.ProvisionDeadline.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertGateStatus(t, db, gate.ID, model.RCGateAborted)
	assertGateWorkersDestroyed(t, db, fly, gate)
}

func TestRCGateRecordsPerSideReadinessBeforeDeadline(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC())
	createTestGateFleet(t, db, gate)
	pending := "worker-" + testGateBaseline + "-" + gateRemoteProfile
	if err := db.UpdateWorkerStatus(pending, model.WorkerPending, "provisioning"); err != nil {
		t.Fatal(err)
	}
	manager, _ := teardownGateManager(t, db, nil)
	ctx := context.Background()
	candidateReady := gate.StartedAt.Add(time.Minute)
	if err := manager.evaluateRCGates(ctx, candidateReady); err != nil {
		t.Fatal(err)
	}
	stored, err := db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.PairedReadyAt != nil || stored.CandidateReadyAt == nil || !stored.CandidateReadyAt.Equal(candidateReady) || stored.BaselineReadyAt != nil {
		t.Fatalf("readiness = candidate %v baseline %v paired %v", stored.CandidateReadyAt, stored.BaselineReadyAt, stored.PairedReadyAt)
	}

	if err := db.UpdateWorkerStatus(pending, model.WorkerRunning, ""); err != nil {
		t.Fatal(err)
	}
	baselineReady := gate.StartedAt.Add(90 * time.Minute)
	if err := manager.evaluateRCGates(ctx, baselineReady); err != nil {
		t.Fatal(err)
	}
	stored, err = db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.RCGateRunning || stored.PairedReadyAt == nil || !stored.PairedReadyAt.Equal(baselineReady) || !stored.CandidateReadyAt.Equal(candidateReady) {
		t.Fatalf("paired gate = status %s paired %v candidate %v", stored.Status, stored.PairedReadyAt, stored.CandidateReadyAt)
	}
}
