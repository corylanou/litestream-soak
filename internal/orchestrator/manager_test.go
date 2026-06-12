package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestManagerWorkerEnvIncludesReplicaConfig(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica: ReplicaConfig{
			Bucket:    "litestream-soak-replicas-shared",
			Endpoint:  "https://fly.storage.tigris.dev",
			AccessKey: "access-key",
			SecretKey: "secret-key",
			Region:    "auto",
		},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:            "worker-main-gharchive",
		Name:          "worker-main-gharchive",
		FlyVolumeID:   "vol_test123",
		LitestreamSHA: "abc123",
	}, workload.Config{
		InitialSize:      "10MB",
		VerifyInterval:   "30m",
		VerifyType:       "integrity",
		SnapshotInterval: "10m",
		SyncInterval:     "1s",
		LoadMode:         "replay",
	})

	if got, want := env["S3_BUCKET"], "litestream-soak-replicas-shared"; got != want {
		t.Fatalf("S3_BUCKET=%q, want %q", got, want)
	}
	if got, want := env["BUCKET_NAME"], "litestream-soak-replicas-shared"; got != want {
		t.Fatalf("BUCKET_NAME=%q, want %q", got, want)
	}
	if got, want := env["S3_ENDPOINT"], "https://fly.storage.tigris.dev"; got != want {
		t.Fatalf("S3_ENDPOINT=%q, want %q", got, want)
	}
	if got, want := env["S3_PATH"], "soak/worker-main-gharchive/vol_test123"; got != want {
		t.Fatalf("S3_PATH=%q, want %q", got, want)
	}
	if got, want := env["AWS_ENDPOINT_URL_S3"], "https://fly.storage.tigris.dev"; got != want {
		t.Fatalf("AWS_ENDPOINT_URL_S3=%q, want %q", got, want)
	}
	if got, want := env["AWS_ACCESS_KEY_ID"], "access-key"; got != want {
		t.Fatalf("AWS_ACCESS_KEY_ID=%q, want %q", got, want)
	}
	if got, want := env["AWS_SECRET_ACCESS_KEY"], "secret-key"; got != want {
		t.Fatalf("AWS_SECRET_ACCESS_KEY=%q, want %q", got, want)
	}
	if got, want := env["AWS_REGION"], "auto"; got != want {
		t.Fatalf("AWS_REGION=%q, want %q", got, want)
	}
}

func TestManagerWorkerEnvOmitsEmptyReplicaCredentials(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica: ReplicaConfig{
			Bucket:   "litestream-soak-replicas-shared",
			Endpoint: "https://fly.storage.tigris.dev",
		},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-low-vol",
		Name: "worker-main-low-vol",
	}, workload.Config{
		InitialSize:      "5MB",
		VerifyInterval:   "30m",
		VerifyType:       "integrity",
		SnapshotInterval: "10m",
		SyncInterval:     "1s",
		LoadMode:         "synthetic",
	})

	if got, want := env["S3_PATH"], "soak/worker-main-low-vol"; got != want {
		t.Fatalf("S3_PATH=%q, want %q", got, want)
	}
	if _, ok := env["AWS_ACCESS_KEY_ID"]; ok {
		t.Fatal("AWS_ACCESS_KEY_ID should be omitted when unset")
	}
	if _, ok := env["AWS_SECRET_ACCESS_KEY"]; ok {
		t.Fatal("AWS_SECRET_ACCESS_KEY should be omitted when unset")
	}
	if _, ok := env["AWS_REGION"]; ok {
		t.Fatal("AWS_REGION should be omitted when unset")
	}
}

func TestReplacementRequestUsesWorkerID(t *testing.T) {
	t.Parallel()

	w := model.Worker{
		ID:            "11111111-2222-3333-4444-555555555555",
		Name:          "worker-main-gharchive",
		Source:        "main",
		GitSHA:        "oldsha",
		LitestreamSHA: "oldls",
		PRNumber:      7,
		ProfileName:   "default",
		Region:        "ord",
	}

	req := replacementRequest(w, "registry/img:new", "newsha", "newls")

	if req.WorkerID != w.ID {
		t.Fatalf("WorkerID=%q, want %q", req.WorkerID, w.ID)
	}
	if req.Name != w.Name {
		t.Fatalf("Name=%q, want %q", req.Name, w.Name)
	}
	if req.Source != w.Source {
		t.Fatalf("Source=%q, want %q", req.Source, w.Source)
	}
	if req.PRNumber != w.PRNumber {
		t.Fatalf("PRNumber=%d, want %d", req.PRNumber, w.PRNumber)
	}
	if req.Region != w.Region {
		t.Fatalf("Region=%q, want %q", req.Region, w.Region)
	}
	if req.GitSHA != "newsha" || req.LitestreamSHA != "newls" || req.ImageRef != "registry/img:new" {
		t.Fatalf("new deployment fields not propagated: %+v", req)
	}
}

func TestReplacementRequestPreservesExpiresAt(t *testing.T) {
	t.Parallel()

	expires := time.Now().Add(3 * time.Hour).UTC()
	withExpiry := replacementRequest(model.Worker{ID: "w1", Name: "w1", ExpiresAt: &expires}, "img", "sha", "ls")
	if withExpiry.ExpiresAt == nil || !withExpiry.ExpiresAt.Equal(expires) {
		t.Fatalf("ExpiresAt=%v, want %v", withExpiry.ExpiresAt, expires)
	}

	withoutExpiry := replacementRequest(model.Worker{ID: "w2", Name: "w2"}, "img", "sha", "ls")
	if withoutExpiry.ExpiresAt != nil {
		t.Fatalf("ExpiresAt=%v, want nil", withoutExpiry.ExpiresAt)
	}
}

func TestNewWorkerRecordCopiesExpiresAt(t *testing.T) {
	t.Parallel()

	mgr := &Manager{appName: "litestream-soak"}
	expires := time.Now().Add(2 * time.Hour).UTC()

	withExpiry := mgr.newWorkerRecord(WorkerRequest{WorkerID: "w1", Name: "w1", Source: "main", ExpiresAt: &expires})
	if withExpiry.ExpiresAt == nil || !withExpiry.ExpiresAt.Equal(expires) {
		t.Fatalf("ExpiresAt=%v, want %v", withExpiry.ExpiresAt, expires)
	}

	withoutExpiry := mgr.newWorkerRecord(WorkerRequest{WorkerID: "w2", Name: "w2", Source: "main"})
	if withoutExpiry.ExpiresAt != nil {
		t.Fatalf("ExpiresAt=%v, want nil", withoutExpiry.ExpiresAt)
	}
}

func TestCreateWorkerForksPRWorkerFromEligibleMainVolume(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low-vol",
		AppName:       "litestream-soak",
		Name:          "worker-main-low-vol",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "main",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		Region:        "ord",
		FlyMachineID:  "machine-main",
		FlyVolumeID:   "vol-main-001",
	})

	fly := newCreateWorkerFlyServer(t)
	mgr := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")

	worker, err := mgr.CreateWorker(context.Background(), WorkerRequest{
		WorkerID:    "worker-pr-62-low-vol",
		Name:        "worker-pr-62-low-vol",
		Source:      "pr-62",
		GitSHA:      "soak-sha",
		ProfileName: "low-volume",
		ImageRef:    "registry.fly.io/litestream-soak:test",
		Region:      "ord",
		Workload: workload.Config{
			LoadMode:    "synthetic",
			InitialSize: "5MB",
		},
	})
	if err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	volumes := fly.volumeRequests()
	if len(volumes) != 1 {
		t.Fatalf("volume requests=%d want 1", len(volumes))
	}
	if got := volumes[0].SourceID; got != "vol-main-001" {
		t.Fatalf("fork source_vol_id=%q want %q", got, "vol-main-001")
	}

	machines := fly.machineRequests()
	if len(machines) != 1 {
		t.Fatalf("machine requests=%d want 1", len(machines))
	}
	if got := machines[0].Config.Env["S3_PATH"]; strings.Contains(got, "worker-main") {
		t.Fatalf("S3_PATH=%q should not target a main prefix", got)
	}
	if got, want := machines[0].Config.Env["S3_PATH"], "soak/worker-pr-62-low-vol/"+worker.FlyVolumeID; got != want {
		t.Fatalf("S3_PATH=%q want %q", got, want)
	}

	event := requireWorkerEvent(t, db, worker.ID, "worker_volume_forked")
	if !strings.Contains(event.Details, "vol-main-001") || !strings.Contains(event.Details, "worker-main-low-vol") {
		t.Fatalf("fork event details=%q, want source worker and volume", event.Details)
	}
}

func TestCreateWorkerUsesFreshVolumeWithoutEligibleMainCounterpart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		main      *model.Worker
		wantCause string
	}{
		{
			name:      "no matching main worker",
			wantCause: "no_matching_main_worker",
		},
		{
			name: "main worker degraded",
			main: &model.Worker{
				Status:      model.WorkerDegraded,
				Region:      "ord",
				FlyVolumeID: "vol-main-001",
			},
			wantCause: "main_worker_not_running",
		},
		{
			name: "main worker missing volume",
			main: &model.Worker{
				Status: model.WorkerRunning,
				Region: "ord",
			},
			wantCause: "main_worker_missing_volume",
		},
		{
			name: "main worker region mismatch",
			main: &model.Worker{
				Status:      model.WorkerRunning,
				Region:      "ams",
				FlyVolumeID: "vol-main-001",
			},
			wantCause: "main_worker_region_mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			if tt.main != nil {
				main := *tt.main
				main.ID = "worker-main-low-vol"
				main.AppName = "litestream-soak"
				main.Name = "worker-main-low-vol"
				main.Source = "main"
				main.GitSHA = "main"
				main.ProfileName = "low-volume"
				main.ProfileConfig = "{}"
				createTestWorker(t, db, main)
			}

			fly := newCreateWorkerFlyServer(t)
			mgr := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")

			worker, err := mgr.CreateWorker(context.Background(), WorkerRequest{
				WorkerID:     "worker-pr-62-low-vol",
				Name:         "worker-pr-62-low-vol",
				Source:       "pr-62",
				GitSHA:       "soak-sha",
				ProfileName:  "low-volume",
				ImageRef:     "registry.fly.io/litestream-soak:test",
				Region:       "ord",
				VolumeSizeGB: 20,
				Workload: workload.Config{
					LoadMode:    "synthetic",
					InitialSize: "5MB",
				},
			})
			if err != nil {
				t.Fatalf("CreateWorker() error = %v", err)
			}

			volumes := fly.volumeRequests()
			if len(volumes) != 1 {
				t.Fatalf("volume requests=%d want 1", len(volumes))
			}
			if got := volumes[0].SourceID; got != "" {
				t.Fatalf("fresh volume source_vol_id=%q want empty", got)
			}
			if got := volumes[0].Region; got != "ord" {
				t.Fatalf("fresh volume region=%q want ord", got)
			}
			if got := volumes[0].SizeGB; got != 20 {
				t.Fatalf("fresh volume size=%d want 20", got)
			}

			event := requireWorkerEvent(t, db, worker.ID, "worker_volume_fresh")
			if !strings.Contains(event.Details, tt.wantCause) {
				t.Fatalf("fresh event details=%q, want cause %q", event.Details, tt.wantCause)
			}
		})
	}
}

func TestExpiresAtRoundTripMatchesExpiryQuery(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{db: db, appName: "litestream-soak"}

	past := time.Now().Add(-1 * time.Hour).UTC()
	expired := mgr.newWorkerRecord(WorkerRequest{WorkerID: "expired", Name: "expired", Source: "main", ExpiresAt: &past})
	if err := db.CreateWorker(expired); err != nil {
		t.Fatalf("create expired worker: %v", err)
	}

	never := mgr.newWorkerRecord(WorkerRequest{WorkerID: "never", Name: "never", Source: "main"})
	if err := db.CreateWorker(never); err != nil {
		t.Fatalf("create never worker: %v", err)
	}

	workers, err := db.ListExpiredWorkers()
	if err != nil {
		t.Fatalf("list expired workers: %v", err)
	}
	if len(workers) != 1 || workers[0].ID != "expired" {
		t.Fatalf("ListExpiredWorkers()=%+v, want only [expired]", workers)
	}
}

func TestClearOldWorkerReplicaPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		volumeID     string
		wantRequests bool
		wantPrefix   string
	}{
		{name: "volume set clears prefix", volumeID: "vol_abc", wantRequests: true, wantPrefix: "soak/worker-main-x/vol_abc/"},
		{name: "empty volume skips clear", volumeID: "", wantRequests: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var requests atomic.Int32
			var gotPrefix atomic.Value
			gotPrefix.Store("")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method == http.MethodGet {
					gotPrefix.Store(r.URL.Query().Get("prefix"))
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
			}))
			defer server.Close()

			mgr := &Manager{
				replica: ReplicaConfig{
					Bucket:    "bucket",
					Endpoint:  server.URL,
					AccessKey: "access",
					SecretKey: "secret",
					Region:    "auto",
				},
			}

			mgr.clearOldWorkerReplicaPrefix(context.Background(), model.Worker{
				ID:          "w1",
				Name:        "worker-main-x",
				FlyVolumeID: tt.volumeID,
			})

			if tt.wantRequests {
				if requests.Load() == 0 {
					t.Fatal("expected S3 requests, got none")
				}
				if got := gotPrefix.Load().(string); got != tt.wantPrefix {
					t.Fatalf("prefix=%q, want %q", got, tt.wantPrefix)
				}
			} else if requests.Load() != 0 {
				t.Fatalf("expected no S3 requests, got %d", requests.Load())
			}
		})
	}
}

func TestLockSourceSerializesSameSource(t *testing.T) {
	t.Parallel()

	mgr := &Manager{}
	unlock := mgr.lockSource("main")

	acquired := make(chan struct{})
	go func() {
		unlock2 := mgr.lockSource("main")
		close(acquired)
		unlock2()
	}()

	select {
	case <-acquired:
		t.Fatal("second lockSource(main) acquired while first held")
	case <-time.After(50 * time.Millisecond):
	}

	unlock()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second lockSource(main) did not acquire after unlock")
	}
}

func TestLockSourceDistinctSourcesDoNotBlock(t *testing.T) {
	t.Parallel()

	mgr := &Manager{}
	unlock := mgr.lockSource("main")
	defer unlock()

	acquired := make(chan struct{})
	go func() {
		unlock2 := mgr.lockSource("pr-123")
		unlock2()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("lockSource(pr-123) blocked behind lockSource(main)")
	}
}

func TestLockWorkerSerializesSameID(t *testing.T) {
	t.Parallel()

	mgr := &Manager{}
	unlock := mgr.lockWorker("w1")

	acquired := make(chan struct{})
	go func() {
		unlock2 := mgr.lockWorker("w1")
		close(acquired)
		unlock2()
	}()

	select {
	case <-acquired:
		t.Fatal("second lockWorker(w1) acquired while first held")
	case <-time.After(50 * time.Millisecond):
	}

	unlock()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second lockWorker(w1) did not acquire after unlock")
	}
}

func TestLockWorkerDistinctIDsDoNotBlock(t *testing.T) {
	t.Parallel()

	mgr := &Manager{}
	unlock := mgr.lockWorker("w1")
	defer unlock()

	acquired := make(chan struct{})
	go func() {
		unlock2 := mgr.lockWorker("w2")
		unlock2()
		close(acquired)
	}()

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("lockWorker(w2) blocked behind lockWorker(w1)")
	}
}

type createWorkerFlyServer struct {
	client *flyapi.Client
	server *httptest.Server

	mu       sync.Mutex
	volumes  []flyapi.CreateVolumeRequest
	machines []flyapi.CreateMachineRequest
}

func newCreateWorkerFlyServer(t *testing.T) *createWorkerFlyServer {
	t.Helper()

	fake := &createWorkerFlyServer{}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)
	fake.client = flyapi.NewClientWithBaseURL("litestream-soak", "test-token", fake.server.URL)
	return fake
}

func (f *createWorkerFlyServer) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/apps/litestream-soak/volumes":
		var req flyapi.CreateVolumeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.volumes = append(f.volumes, req)
		id := fmt.Sprintf("vol-%03d", len(f.volumes))
		f.mu.Unlock()
		writeCreateWorkerJSON(w, flyapi.Volume{ID: id, Name: req.Name, SizeGB: req.SizeGB, Region: req.Region, State: "created"})
	case r.Method == http.MethodPost && r.URL.Path == "/apps/litestream-soak/machines":
		var req flyapi.CreateMachineRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.machines = append(f.machines, req)
		id := fmt.Sprintf("machine-%03d", len(f.machines))
		f.mu.Unlock()
		writeCreateWorkerJSON(w, flyapi.Machine{ID: id, Name: req.Name, State: "started", Config: req.Config})
	default:
		http.NotFound(w, r)
	}
}

func (f *createWorkerFlyServer) volumeRequests() []flyapi.CreateVolumeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]flyapi.CreateVolumeRequest(nil), f.volumes...)
}

func (f *createWorkerFlyServer) machineRequests() []flyapi.CreateMachineRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]flyapi.CreateMachineRequest(nil), f.machines...)
}

func writeCreateWorkerJSON(w http.ResponseWriter, value any) {
	_ = json.NewEncoder(w).Encode(value)
}

func requireWorkerEvent(t *testing.T, db *model.DB, workerID, eventType string) model.Event {
	t.Helper()

	events, err := db.ListWorkerEvents(workerID, 20)
	if err != nil {
		t.Fatalf("ListWorkerEvents(%q) error = %v", workerID, err)
	}
	for _, event := range events {
		if event.EventType == eventType {
			return event
		}
	}
	t.Fatalf("event %q not recorded for %s; got %+v", eventType, workerID, events)
	return model.Event{}
}

func TestRollingUpdateSourceSkipsUpToDateWorkers(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{db: db, appName: "litestream-soak"}

	if err := db.CreateWorker(&model.Worker{
		ID:            "w1",
		AppName:       "litestream-soak",
		Name:          "worker-main-x",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "newsha",
		LitestreamSHA: "newls",
		FlyMachineID:  "m1",
		FlyVolumeID:   "v1",
	}); err != nil {
		t.Fatalf("create worker: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- mgr.RollingUpdateSource(context.Background(), "main", "registry/img:new", "newsha", "newls")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RollingUpdateSource returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RollingUpdateSource did not return")
	}

	worker, err := db.GetWorker("w1")
	if err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if worker.FlyMachineID != "m1" || worker.FlyVolumeID != "v1" {
		t.Fatalf("worker was modified: machine=%q volume=%q", worker.FlyMachineID, worker.FlyVolumeID)
	}

	unlock := mgr.lockSource("main")
	unlock()
}

func TestRollingUpdateSourceSkipsSupersededTarget(t *testing.T) {
	db := openTestDB(t)
	source := "main"
	oldSHA := "1111111111111111111111111111111111111111"
	oldLitestreamSHA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	oldImageRef := "registry.fly.io/litestream-soak:sha-111111111111"
	latestSHA := "2222222222222222222222222222222222222222"
	latestLitestreamSHA := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	latestImageRef := "registry.fly.io/litestream-soak:sha-222222222222"

	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        latestSHA,
		LitestreamSHA: latestLitestreamSHA,
		ImageRef:      latestImageRef,
		Source:        source,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-low-vol",
		AppName:       "litestream-soak",
		Name:          "worker-main-low-vol",
		Status:        model.WorkerRunning,
		Source:        source,
		GitSHA:        latestSHA,
		LitestreamSHA: latestLitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: workload.Config{LoadMode: "synthetic", InitialSize: "5MB"}.JSON(),
		FlyMachineID:  "latest-machine",
		FlyVolumeID:   "latest-volume",
	})

	fly := newDeployTestFlyServer(t, db, source, latestSHA, latestLitestreamSHA, latestImageRef)
	mgr := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")

	if err := mgr.RollingUpdateSource(context.Background(), source, oldImageRef, oldSHA, oldLitestreamSHA); err != nil {
		t.Fatalf("RollingUpdateSource() error = %v", err)
	}

	worker := mustWorker(t, db, "worker-main-low-vol")
	if worker.GitSHA != latestSHA {
		t.Fatalf("worker.GitSHA = %q, want %q", worker.GitSHA, latestSHA)
	}
	if worker.LitestreamSHA != latestLitestreamSHA {
		t.Fatalf("worker.LitestreamSHA = %q, want %q", worker.LitestreamSHA, latestLitestreamSHA)
	}
	if worker.FlyMachineID != "latest-machine" || worker.FlyVolumeID != "latest-volume" {
		t.Fatalf("worker machine/volume changed: machine=%q volume=%q", worker.FlyMachineID, worker.FlyVolumeID)
	}
	fly.assertCreateCounts(t, 0, 0)
	fly.assertNoErrors(t)
}
