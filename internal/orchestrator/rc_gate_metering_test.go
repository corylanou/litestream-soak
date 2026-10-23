package orchestrator

import (
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestRCGateScoringExcludesMeteringWindowRuns(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-2*time.Hour))
	createTestGateFleet(t, db, gate)
	at := gate.StartedAt.Add(time.Hour)
	recordGateVerification(t, db, testGateCandidate, gateStressProfile, "overload-truncate0", true, at)
	recordGateVerification(t, db, testGateBaseline, gateStressProfile, "overload-truncate0", true, at)

	worker, err := db.GetWorker("worker-" + testGateCandidate + "-" + gateStressProfile)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetDeploymentByVersion(testGateCandidate, worker.GitSHA, worker.LitestreamSHA)
	if err != nil || deployment == nil {
		t.Fatalf("deployment: %v", err)
	}
	metered := fixtureRun(*worker, *deployment)
	metered.Scenario = "overload-truncate0"
	metered.ProfileConfig = `{"metering_window":true}`
	failedAt := at.Add(time.Minute)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: worker.ID, StartedAt: failedAt, CompletedAt: &failedAt, Status: "failed", CheckType: "integrity", Run: metered, Attributed: true,
	})

	stored, err := db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := buildRCGateLedgerEntry(db, *stored, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range entry.Scenarios {
		if scenario.Scenario != "overload-truncate0" {
			continue
		}
		if scenario.Verdict != rcGateVerdictPass || scenario.Candidate.Failed != 0 {
			t.Fatalf("metering-window run counted in gate verdict: %+v", scenario)
		}
		return
	}
	t.Fatal("overload-truncate0 scenario missing")
}

func TestGateFleetJoinsMeteringWindowOnlyWhenListed(t *testing.T) {
	t.Setenv("SOAK_METERING_WINDOW_SOURCES", "main,pr-1568")
	for _, source := range []string{testGateCandidate, testGateBaseline} {
		for _, worker := range GateFleetForSource(source, testGateSoakSHA, testGateCandidateSHA).Workers {
			if worker.Workload.MeteringWindow || worker.Workload.S3FaultProxyEnabled {
				t.Fatalf("%s joined an unlisted metering window", worker.Name)
			}
		}
	}

	t.Setenv("SOAK_METERING_WINDOW_SOURCES", testGateCandidate)
	for _, worker := range GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA).Workers {
		if !worker.Workload.MeteringWindow || worker.Workload.S3FaultProxyMode != "observe" {
			t.Fatalf("%s not in its listed metering window", worker.Name)
		}
	}
	for _, worker := range GateFleetForSource(testGateBaseline, testGateSoakSHA, testGateBaselineSHA).Workers {
		if worker.Workload.MeteringWindow {
			t.Fatalf("%s joined the candidate's metering window", worker.Name)
		}
	}
}
