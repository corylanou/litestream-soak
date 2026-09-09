package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
)

func TestMachineDiagnosticsExcludeCredentials(t *testing.T) {
	for _, providerFailure := range []bool{false, true} {
		name := "machine"
		if providerFailure {
			name = "provider-error"
		}
		t.Run(name, func(t *testing.T) {
			db := openTestDB(t)
			createTestWorker(t, db, model.Worker{ID: "diagnostic-worker", Name: "diagnostic-worker", FlyMachineID: "machine-safe", Source: "main", ProfileName: "low-volume", ProfileConfig: "{}", Status: model.WorkerRunning})
			secrets := []string{"sentinel-storage-access-260", "sentinel-storage-secret-260", "sentinel-report-token-260", "sentinel-future-credential-260"}
			machine := flyapi.Machine{ID: "machine-safe", Name: "diagnostic-worker", State: "started", Region: "ord", InstanceID: "instance-safe", ImageRef: flyapi.ImageRef{Digest: "sha256:image-safe"}, Config: flyapi.MachineConfig{Image: "registry.example/worker:image-safe", Env: map[string]string{"AWS_ACCESS_KEY_ID": secrets[0], "AWS_SECRET_ACCESS_KEY": secrets[1], "SOAK_WORKER_TOKEN": secrets[2], "NEW_PROVIDER_SETTING": secrets[3]}, Guest: flyapi.Guest{CPUs: 2, MemoryMB: 1024}, Mounts: []flyapi.Mount{{Volume: "volume-safe", Path: "/data"}}}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if providerFailure {
					w.WriteHeader(http.StatusBadGateway)
				}
				if err := json.NewEncoder(w).Encode(machine); err != nil {
					t.Error("encode fixture failed")
				}
			}))
			defer server.Close()
			api := NewAPI(db, flyapi.NewClientWithBaseURL("app", "synthetic-token", server.URL), nil, nil, nil, nil)
			surfaces := []struct {
				name    string
				handler http.HandlerFunc
			}{
				{"detail", api.handleGetWorker}, {"incident", api.handleGetIncident}, {"prompt", api.handleGetPrompt}, {"ui", api.handleWorkerPage},
			}
			for _, mode := range buildPromptModes("") {
				surfaces = append(surfaces, struct {
					name    string
					handler http.HandlerFunc
				}{name: "prompt-" + mode.ID, handler: func(w http.ResponseWriter, r *http.Request) {
					r.URL.RawQuery = "mode=" + mode.ID
					api.handleGetPrompt(w, r)
				}})
			}
			for _, surface := range surfaces {
				t.Run(surface.name, func(t *testing.T) {
					req := httptest.NewRequest(http.MethodGet, "/diagnostic", nil)
					req.SetPathValue("id", "diagnostic-worker")
					response := httptest.NewRecorder()
					surface.handler(response, req)
					if response.Code != http.StatusOK {
						t.Fatalf("status = %d", response.Code)
					}
					body := response.Body.String()
					for i, secret := range secrets {
						if strings.Contains(body, secret) {
							t.Errorf("credential sentinel %d exposed", i)
						}
					}
					if !providerFailure {
						for _, value := range []string{"machine-safe", "image-safe", "ord", "started", "volume-safe"} {
							if !strings.Contains(body, value) {
								t.Errorf("missing safe metadata %q", value)
							}
						}
						if strings.Contains(body, "AWS_SECRET_ACCESS_KEY") || strings.Contains(body, "NEW_PROVIDER_SETTING") {
							t.Error("environment fields exposed")
						}
					}
				})
			}
		})
	}
}

func TestMachineProviderErrorCannotEnterArchivedEvidence(t *testing.T) {
	db := openTestDB(t)
	worker := model.Worker{ID: "archive-worker", Name: "archive-worker", FlyMachineID: "machine", Source: "main", ProfileName: "low-volume", ProfileConfig: "{}", Status: model.WorkerRunning}
	createTestWorker(t, db, worker)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"config":{"env":{"TOKEN":"sentinel-archive-secret-260"}}}`))
	}))
	defer server.Close()
	manager := NewManager(flyapi.NewClientWithBaseURL("app", "synthetic-token", server.URL), db, nil, nil, "app", ReplicaConfig{}, "", "")
	err := manager.DestroyWorker(context.Background(), worker.ID)
	if err == nil {
		t.Fatal("expected provider failure")
	}
	if strings.Contains(err.Error(), "sentinel-archive-secret-260") {
		t.Error("provider body exposed in operational error")
	}
	evidence, err := manager.workerRunEvidence(*mustWorker(t, db, worker.ID))
	if err != nil {
		t.Fatal("load archive evidence")
	}
	body, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal("encode archive evidence")
	}
	if strings.Contains(string(body), "sentinel-archive-secret-260") {
		t.Error("provider body persisted in archive evidence")
	}
}

func TestMachineRetryKeepsInternalProviderClassification(t *testing.T) {
	if !retriableMachineCreateError(fmt.Errorf("create: %w", &flyapi.APIError{StatusCode: 400, Body: "failed to get manifest: sentinel-private"})) {
		t.Error("internal manifest retry classification lost")
	}
}
