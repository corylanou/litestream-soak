package orchestrator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestHandleVerificationRecordsRestoreMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		passed         bool
		classification *reporting.FailureClassification
		errorMessage   string
		wantSignature  string
	}{
		{name: "passed", passed: true},
		{
			name:          "computed failure",
			errorMessage:  "restore failed: latest restore behind boundary: restored=0000000000000041 boundary=0000000000000042",
			wantSignature: "latest_restore_behind_boundary",
		},
		{
			name:           "disk override",
			errorMessage:   "sync database: disk full: write header",
			classification: &reporting.FailureClassification{Stage: "disk_capacity", Signature: "disk_capacity_full", RestoreMode: "latest"},
			wantSignature:  "disk_capacity_full",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			workerID := "worker-restore-mode"
			createTestWorker(t, db, model.Worker{
				ID:            workerID,
				Name:          workerID,
				Status:        model.WorkerRunning,
				Source:        "main",
				GitSHA:        "abc123",
				LitestreamSHA: "ls123",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
			})
			completedAt := time.Now().UTC()
			status := "failed"
			if tt.passed {
				status = "passed"
			}
			payload := reporting.VerificationPayload{
				WorkerIdentity: reporting.WorkerIdentity{
					WorkerID:      workerID,
					Name:          workerID,
					Source:        "main",
					GitSHA:        "abc123",
					LitestreamSHA: "ls123",
					ProfileName:   "low-volume",
					ProfileConfig: "{}",
				},
				StartedAt:             completedAt.Add(-time.Minute),
				CompletedAt:           completedAt,
				CheckType:             "integrity",
				Status:                status,
				Passed:                tt.passed,
				ErrorMessage:          tt.errorMessage,
				FailureClassification: tt.classification,
				RestoreMode:           "latest",
				RestoreModeFallback:   "",
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			api := NewAPI(db, nil, nil, nil, nil, nil)
			request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/verifications", bytes.NewReader(body))
			request.SetPathValue("id", workerID)
			recorder := httptest.NewRecorder()
			api.handleVerification(recorder, request)
			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status code = %d; body: %s", recorder.Code, recorder.Body.String())
			}

			events, err := db.ListWorkerEvents(workerID, 10)
			if err != nil {
				t.Fatal(err)
			}
			var recorded bool
			for _, event := range events {
				var details reporting.VerificationPayload
				if json.Unmarshal([]byte(event.Details), &details) == nil && details.CheckType == "integrity" {
					recorded = true
					if details.RestoreMode != "latest" {
						t.Fatalf("event restore mode = %q", details.RestoreMode)
					}
				}
			}
			if !recorded {
				t.Fatalf("verification event not recorded: %+v", events)
			}

			verifications, err := db.ListVerifications(workerID, 10)
			if err != nil || len(verifications) != 1 {
				t.Fatalf("ListVerifications() = %+v, %v", verifications, err)
			}
			if verifications[0].RestoreMode != "latest" {
				t.Fatalf("stored restore mode = %q", verifications[0].RestoreMode)
			}
			classification := verifications[0].FailureClassification
			if tt.passed {
				if classification != nil {
					t.Fatalf("passed classification = %+v", classification)
				}
				return
			}
			if classification == nil || classification.RestoreMode != "latest" || classification.Signature != tt.wantSignature {
				t.Fatalf("stored classification = %+v", classification)
			}
		})
	}
}

func TestRCGateModeVerdictSeparatesFailuresByMode(t *testing.T) {
	failing := RCGateSideResult{Status: rcGateSideFailing}
	modes := rcGateModeResults(failing, failing,
		map[string]*RCGateSideResult{"txid": {Passed: 1, Status: rcGateSidePassing}, "latest": {Failed: 1, Status: rcGateSideFailing}},
		map[string]*RCGateSideResult{"txid": {Failed: 1, Status: rcGateSideFailing}, "latest": {Passed: 1, Status: rcGateSidePassing}},
	)
	if got := rcGateModesVerdict(modes); got != rcGateVerdictRegression {
		t.Fatalf("verdict = %q, want regression for candidate-only latest failure (%+v)", got, modes)
	}
}

func recordGateModeVerification(t *testing.T, db *model.DB, source, profile, mode string, passed bool, at time.Time) {
	t.Helper()
	worker, err := db.GetWorker("worker-" + source + "-" + profile)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetDeploymentByVersion(source, worker.GitSHA, worker.LitestreamSHA)
	if err != nil || deployment == nil {
		t.Fatalf("deployment for %s: %v", source, err)
	}
	status := "passed"
	if !passed {
		status = "failed"
	}
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: worker.ID, StartedAt: at, CompletedAt: &at, Status: status, Passed: passed, CheckType: "integrity",
		Run: fixtureRun(*worker, *deployment), Attributed: true, RestoreMode: mode,
	})
}

func TestRCGateScoresPerRestoreMode(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-2*time.Hour))
	createTestGateFleet(t, db, gate)
	at := gate.StartedAt.Add(time.Hour)
	for _, source := range []string{testGateCandidate, testGateBaseline} {
		recordGateModeVerification(t, db, source, gateLargeDBProfile, "txid", true, at)
		recordGateModeVerification(t, db, source, gateRemoteProfile, "txid", true, at)
		recordGateModeVerification(t, db, source, gateRemoteProfile, "latest", true, at)
		recordGateModeVerification(t, db, source, gateManyDB500Profile, "", true, at)
	}
	recordGateModeVerification(t, db, testGateCandidate, gateLargeDBProfile, "latest", true, at)
	recordGateModeVerification(t, db, testGateCandidate, gateRemoteProfile, "timestamp", false, at)
	recordGateModeVerification(t, db, testGateBaseline, gateRemoteProfile, "timestamp", true, at)

	stored, err := db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := buildRCGateLedgerEntry(db, *stored, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	scenarios := map[string]RCGateScenarioResult{}
	for _, scenario := range entry.Scenarios {
		scenarios[scenario.Scenario] = scenario
	}
	modes := func(scenario string) map[string]string {
		got := map[string]string{}
		for _, mode := range scenarios[scenario].Modes {
			got[mode.Mode] = mode.Verdict
		}
		return got
	}
	if got := scenarios["large-db"]; got.Verdict != rcGateVerdictIncomplete {
		t.Fatalf("large-db verdict = %q, want incomplete when baseline lacks latest (%+v)", got.Verdict, got.Modes)
	}
	if got := modes("large-db"); got["txid"] != rcGateVerdictPass || got["latest"] != rcGateVerdictIncomplete {
		t.Fatalf("large-db modes = %v", got)
	}
	if got := scenarios["remote"]; got.Verdict != rcGateVerdictRegression {
		t.Fatalf("remote verdict = %q, want regression (%+v)", got.Verdict, got.Modes)
	}
	if got := modes("remote"); got["txid"] != rcGateVerdictPass || got["latest"] != rcGateVerdictPass || got["timestamp"] != rcGateVerdictRegression {
		t.Fatalf("remote modes = %v", got)
	}
	if got := scenarios["manydb-500"]; got.Verdict != rcGateVerdictPass || modes("manydb-500")["txid"] != rcGateVerdictPass {
		t.Fatalf("manydb-500 = %+v, want legacy unlabeled runs scored as txid", got)
	}
	if entry.Verdict != rcGateVerdictFail {
		t.Fatalf("gate verdict = %q, want fail", entry.Verdict)
	}
}
