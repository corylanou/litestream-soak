package orchestrator

import (
	"testing"
	"time"
)

func TestGateFleetKeepsRegisteredVolumeForRunningGates(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-time.Hour))
	for i := range gate.Resources {
		if gate.Resources[i].Profile == gateStressProfile {
			gate.Resources[i].VolumeGB = 10
		}
	}
	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		spec := applyGateWorkload(gate, gateFleetForSource(gate, source, testGateSoakSHA, litestreamSHA))
		stress := gateWorker(t, spec, gateStressProfile)
		if stress.VolumeSizeGB != 10 || stress.Workload.VolumeSizeGB != 10 {
			t.Fatalf("%s gate-stress volume = %d/%d, want the registered 10 GB", source, stress.VolumeSizeGB, stress.Workload.VolumeSizeGB)
		}
		large := gateWorker(t, spec, gateLargeDBProfile)
		if large.VolumeSizeGB != 40 {
			t.Fatalf("%s gate-large-db volume = %d, want 40", source, large.VolumeSizeGB)
		}
	}

	fresh := registerTestGateFor(t, openTestDB(t), "72h", false, time.Now().UTC())
	for _, resource := range fresh.Resources {
		if resource.Profile == gateStressProfile && resource.VolumeGB != 20 {
			t.Fatalf("new gate gate-stress volume = %d, want 20", resource.VolumeGB)
		}
	}
	if stress := gateWorker(t, GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA), gateStressProfile); stress.VolumeSizeGB != 20 || stress.Workload.VolumeSizeGB != 20 {
		t.Fatalf("fleet default gate-stress volume = %d/%d, want 20", stress.VolumeSizeGB, stress.Workload.VolumeSizeGB)
	}
}
