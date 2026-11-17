package orchestrator

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
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
		{backend: workload.ReplicaBackendSFTP, wantBackend: workload.ReplicaBackendSFTP, wantType: "sftp"},
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
	for _, backend := range []string{workload.ReplicaBackendSFTP, workload.ReplicaBackendFile} {
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

func TestClearWorkerReplicaPrefixDeletesThePreservedUpgradeChain(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var listed []string
	var deleted []string
	objects := map[string][]string{
		"soak/worker-up-lane/vol_1/":             {"soak/worker-up-lane/vol_1/ltx/0000/a.ltx"},
		"soak/worker-up-lane/vol_1-pre-upgrade/": {"soak/worker-up-lane/vol_1-pre-upgrade/ltx/0000/a.ltx", "soak/worker-up-lane/vol_1-pre-upgrade/ltx/0009/partial.ltx"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodGet {
			prefix := r.URL.Query().Get("prefix")
			listed = append(listed, prefix)
			var contents strings.Builder
			for _, key := range objects[prefix] {
				contents.WriteString("<Contents><Key>" + key + "</Key></Contents>")
			}
			_, _ = fmt.Fprintf(w, "<ListBucketResult><IsTruncated>false</IsTruncated>%s</ListBucketResult>", contents.String())
			return
		}
		body, _ := io.ReadAll(r.Body)
		for _, part := range strings.Split(string(body), "<Key>")[1:] {
			deleted = append(deleted, part[:strings.Index(part, "</Key>")])
		}
		_, _ = w.Write([]byte("<DeleteResult/>"))
	}))
	defer server.Close()

	manager := &Manager{replica: ReplicaConfig{Bucket: "bucket", Endpoint: server.URL, AccessKey: "access", SecretKey: "secret"}}
	worker := model.Worker{ID: "worker-up-lane", Name: "worker-up-lane", FlyVolumeID: "vol_1", ProfileConfig: "{}"}
	if err := manager.clearWorkerReplicaPrefix(t.Context(), worker); err != nil {
		t.Fatalf("clearWorkerReplicaPrefix() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(listed, []string{"soak/worker-up-lane/vol_1/", "soak/worker-up-lane/vol_1-pre-upgrade/"}) {
		t.Fatalf("listed prefixes = %v, want the replica prefix and its preserved pre-upgrade sibling", listed)
	}
	slices.Sort(deleted)
	want := []string{"soak/worker-up-lane/vol_1-pre-upgrade/ltx/0000/a.ltx", "soak/worker-up-lane/vol_1-pre-upgrade/ltx/0009/partial.ltx", "soak/worker-up-lane/vol_1/ltx/0000/a.ltx"}
	if !slices.Equal(deleted, want) {
		t.Fatalf("deleted = %v, want %v, including a partial preserved copy", deleted, want)
	}
}
