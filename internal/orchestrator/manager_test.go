package orchestrator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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

func TestExpiresAtRoundTripMatchesExpiryQuery(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

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

func TestRollingUpdateSourceSkipsUpToDateWorkers(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

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
