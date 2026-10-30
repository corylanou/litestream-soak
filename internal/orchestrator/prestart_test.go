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

func TestHeartbeatStartupPhaseKeepsWorkerStarting(t *testing.T) {
	t.Parallel()

	const workerID = "worker-rc-aaaaaaaa-gate-large-db"
	tests := []struct {
		name   string
		phases []string
		want   model.WorkerStatus
	}{
		{name: "populate heartbeat keeps the worker starting", phases: []string{reporting.StartupPhasePopulate}, want: model.WorkerStarting},
		{name: "repeated populate heartbeats never flip to running", phases: []string{reporting.StartupPhasePopulate, reporting.StartupPhasePopulate}, want: model.WorkerStarting},
		{name: "first plain heartbeat after populate marks running", phases: []string{reporting.StartupPhasePopulate, ""}, want: model.WorkerRunning},
		{name: "plain heartbeat marks running", phases: []string{""}, want: model.WorkerRunning},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			createTestWorker(t, db, model.Worker{
				ID:            workerID,
				Name:          workerID,
				Status:        model.WorkerStarting,
				Source:        "rc-aaaaaaaa",
				GitSHA:        "sha-a",
				LitestreamSHA: "ls-a",
				ProfileName:   "gate-large-db",
				ProfileConfig: "{}",
			})
			api := NewAPI(db, nil, nil, nil, nil, nil)
			for i, phase := range tt.phases {
				sentAt := time.Date(2026, 10, 29, 17, 20, i, 0, time.UTC)
				payload := reporting.HeartbeatPayload{
					WorkerIdentity: reporting.WorkerIdentity{
						WorkerID:      workerID,
						Name:          workerID,
						Source:        "rc-aaaaaaaa",
						GitSHA:        "sha-a",
						LitestreamSHA: "ls-a",
						ProfileName:   "gate-large-db",
						ProfileConfig: "{}",
					},
					SentAt:         sentAt,
					RuntimePayload: reporting.RuntimePayload{StartupPhase: phase, SnapshotCollectedAt: sentAt},
				}
				body, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/heartbeat", bytes.NewReader(body))
				request.SetPathValue("id", workerID)
				recorder := httptest.NewRecorder()
				api.handleHeartbeat(recorder, request)
				if recorder.Code != http.StatusAccepted {
					t.Fatalf("heartbeat %d status = %d, body %s", i, recorder.Code, recorder.Body.String())
				}
			}
			stored, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker() error = %v", err)
			}
			if stored.Status != tt.want {
				t.Fatalf("Status = %s, want %s", stored.Status, tt.want)
			}
			if stored.LastHeartbeatAt == nil {
				t.Fatal("LastHeartbeatAt = nil, want populate heartbeats to count as heartbeats")
			}
		})
	}
}
