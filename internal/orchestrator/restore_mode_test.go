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
