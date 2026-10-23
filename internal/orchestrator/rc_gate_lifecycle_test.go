package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestRCGateTeardownDestroysStoppedWorkersAndRetriesUntilClean(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-time.Hour))
	createTestGateFleet(t, db, gate)
	stopped := "worker-" + testGateCandidate + "-" + gateLargeDBProfile
	if err := db.UpdateWorkerStatus(stopped, model.WorkerStopped, "stopped without cleanup"); err != nil {
		t.Fatal(err)
	}
	failing := "worker-" + testGateBaseline + "-" + gateRemoteProfile + "-volume"
	fly := newTeardownTestServer(t, map[string]int{failing: 1})
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{
		Bucket: "bucket", Endpoint: fly.server.URL, AccessKey: "access", SecretKey: "secret", Region: "auto",
	}, "", "")
	ctx := context.Background()

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
