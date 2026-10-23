package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestWorkerEnvSelectsReplicaBackend(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica:        ReplicaConfig{Bucket: "bucket", Endpoint: "https://fly.storage.tigris.dev", AccessKey: "access", SecretKey: "secret"},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}
	tests := []struct {
		backend     string
		wantBackend string
		wantType    string
		wantProxy   bool
	}{
		{backend: "", wantBackend: workload.ReplicaBackendTigris, wantType: "s3", wantProxy: true},
		{backend: workload.ReplicaBackendMinIOSidecar, wantBackend: workload.ReplicaBackendMinIOSidecar, wantType: "s3"},
		{backend: workload.ReplicaBackendFile, wantBackend: workload.ReplicaBackendFile, wantType: "file"},
	}
	for _, tt := range tests {
		env := mgr.workerEnv(model.Worker{ID: "worker", Name: "worker"}, workload.Config{ReplicaBackend: tt.backend, S3FaultProxyEnabled: true})
		if got := env["REPLICA_BACKEND"]; got != tt.wantBackend {
			t.Errorf("%q REPLICA_BACKEND = %q, want %q", tt.backend, got, tt.wantBackend)
		}
		if got := env["REPLICA_TYPE"]; got != tt.wantType {
			t.Errorf("%q REPLICA_TYPE = %q, want %q", tt.backend, got, tt.wantType)
		}
		if got := env["S3_BUCKET"]; got != "bucket" {
			t.Errorf("%q S3_BUCKET = %q, want Tigris bucket for profile artifacts", tt.backend, got)
		}
		_, proxied := env["S3_FAULT_PROXY_TARGET_ENDPOINT"]
		if proxied != tt.wantProxy {
			t.Errorf("%q S3_FAULT_PROXY_TARGET_ENDPOINT present = %v, want %v", tt.backend, proxied, tt.wantProxy)
		}
	}
}

func TestClearWorkerReplicaPrefixSkipsNonTigrisBackends(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	manager := &Manager{replica: ReplicaConfig{Bucket: "bucket", Endpoint: server.URL, AccessKey: "access", SecretKey: "secret"}}
	for _, backend := range []string{workload.ReplicaBackendMinIOSidecar, workload.ReplicaBackendFile} {
		worker := model.Worker{ID: "worker", Name: "worker", FlyVolumeID: "vol_1", ProfileConfig: workload.Config{ReplicaBackend: backend}.JSON()}
		if err := manager.clearWorkerReplicaPrefix(t.Context(), worker); err != nil {
			t.Fatalf("%s clearWorkerReplicaPrefix() error = %v", backend, err)
		}
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("Tigris requests = %d, want 0 for non-Tigris backends", got)
	}

	tigris := model.Worker{ID: "worker", Name: "worker", FlyVolumeID: "vol_1", ProfileConfig: "{}"}
	if err := manager.clearWorkerReplicaPrefix(t.Context(), tigris); err == nil {
		t.Fatal("tigris clearWorkerReplicaPrefix() error = nil, want failing Tigris request")
	}
	if requests.Load() == 0 {
		t.Fatal("tigris backend did not reach the Tigris prefix cleanup")
	}
}

func TestDefaultMainFleetStaysOnTigris(t *testing.T) {
	t.Parallel()

	for _, worker := range DefaultFleetForSource("pr-1221", "", "").Workers {
		if !worker.Workload.UsesTigrisReplica() {
			t.Errorf("%s ReplicaBackend = %q; alternate backends run in the Actions coverage matrix, not on Fly", worker.ProfileName, worker.Workload.ReplicaBackend)
		}
	}
}
