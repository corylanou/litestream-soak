package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestTrimSHA(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"shorter_than_12", "abc123", "abc123"},
		{"exactly_12", "abc123def456", "abc123def456"},
		{"longer_than_12_40char", "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0", "a1b2c3d4e5f6"},
		{"13_chars", "abc123def4567", "abc123def456"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := trimSHA(tc.in)
			if got != tc.want {
				t.Fatalf("trimSHA(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNotifyDeploymentReadyRejectsInvalidSHA(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sha  string
	}{
		{"empty_sha", ""},
		{"too_short", "abc12"},
		{"non_hex", "xyz!@#$%^&*()"},
		{"whitespace_only", "   "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deployer := &Deployer{db: openTestDB(t)}
			_, err := deployer.NotifyDeploymentReady(context.Background(), "main", tc.sha, "", "registry.fly.io/app:latest", "test")
			if err == nil {
				t.Fatalf("NotifyDeploymentReady(%q) succeeded, want error", tc.sha)
			}
		})
	}
}

func TestDeployNewSHARequiresRuntimeBuild(t *testing.T) {
	t.Parallel()

	deployer := &Deployer{db: openTestDB(t), allowRuntimeBuild: false}
	err := deployer.DeployNewSHA("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0")
	if err == nil || !strings.Contains(err.Error(), "runtime builds are disabled") {
		t.Fatalf("DeployNewSHA() error = %v, want runtime-builds-disabled error", err)
	}
}

func TestResolveLitestreamBuildSHAPassesThroughFullSHA(t *testing.T) {
	t.Parallel()

	full := "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
	got, err := resolveLitestreamBuildSHA(context.Background(), full)
	if err != nil {
		t.Fatalf("resolveLitestreamBuildSHA(%q) error = %v", full, err)
	}
	if got != full {
		t.Fatalf("resolveLitestreamBuildSHA(%q) = %q, want passthrough", full, got)
	}
}

func TestDeployNewSHARejectsInvalidSHA(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sha  string
	}{
		{"empty_sha", ""},
		{"too_short", "abc12"},
		{"non_hex", "not-a-sha!"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deployer := &Deployer{db: openTestDB(t), allowRuntimeBuild: true}
			err := deployer.DeployNewSHA(tc.sha)
			if err == nil {
				t.Fatalf("DeployNewSHA(%q) succeeded, want error", tc.sha)
			}
		})
	}
}

func TestDeployNewSHABuildsReadyDeploymentAndRollsFleet(t *testing.T) {
	db := openTestDB(t)
	sha := "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
	litestreamSHA := "b1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
	imageRef := "registry.fly.io/litestream-soak:sha-a1b2c3d4e5f6"
	fly := newDeployTestFlyServer(t, db, "main", sha, litestreamSHA, imageRef)

	deployer := NewDeployer(
		NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", ""),
		db,
		"litestream-soak",
		true,
	)

	t.Setenv("LITESTREAM_SHA", litestreamSHA)
	t.Setenv("PATH", fakeFlyPath(t, imageRef)+string(os.PathListSeparator)+os.Getenv("PATH"))

	if err := deployer.DeployNewSHA(sha); err != nil {
		t.Fatalf("DeployNewSHA() error = %v", err)
	}

	deployment := mustDeploymentByVersion(t, db, "main", sha, litestreamSHA)
	if deployment.Status != "ready" {
		t.Fatalf("deployment.Status=%q want ready", deployment.Status)
	}
	if deployment.ImageRef != imageRef {
		t.Fatalf("deployment.ImageRef=%q want %q", deployment.ImageRef, imageRef)
	}
	if deployment.CompletedAt == nil {
		t.Fatal("deployment.CompletedAt=nil want timestamp")
	}

	rollout, err := buildDeploymentRollout(db, *deployment)
	if err != nil {
		t.Fatalf("buildDeploymentRollout() error = %v", err)
	}
	if rollout.Status != "settling" {
		t.Fatalf("rollout.Status=%q want settling", rollout.Status)
	}
	if rollout.TotalWorkers != len(DefaultMainFleet().Workers) {
		t.Fatalf("rollout.TotalWorkers=%d want %d", rollout.TotalWorkers, len(DefaultMainFleet().Workers))
	}
	if rollout.UpdatedWorkers != rollout.TotalWorkers {
		t.Fatalf("rollout.UpdatedWorkers=%d want %d", rollout.UpdatedWorkers, rollout.TotalWorkers)
	}

	workers := mustWorkersForSource(t, db, "main")
	for _, worker := range workers {
		if worker.Status != model.WorkerRunning {
			t.Fatalf("%s Status=%q want running", worker.ID, worker.Status)
		}
		if worker.GitSHA != sha {
			t.Fatalf("%s GitSHA=%q want %q", worker.ID, worker.GitSHA, sha)
		}
		if worker.LitestreamSHA != litestreamSHA {
			t.Fatalf("%s LitestreamSHA=%q want %q", worker.ID, worker.LitestreamSHA, litestreamSHA)
		}
	}

	fly.assertNoErrors(t)
	fly.assertCreateCounts(t, len(DefaultMainFleet().Workers), len(DefaultMainFleet().Workers))
}

func TestNotifyDeploymentReadyRecordsReadyDeploymentBeforeRolloutAndIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	source := "pr-1228"
	oldSHA := "0123456789abcdef0123456789abcdef01234567"
	oldLitestreamSHA := "1111111111111111111111111111111111111111"
	sha := "2222222222222222222222222222222222222222"
	litestreamSHA := "3333333333333333333333333333333333333333"
	imageRef := "registry.fly.io/litestream-soak:sha-222222222222-pr-1228"

	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1228-low-vol",
		AppName:       "litestream-soak",
		Name:          "worker-pr-1228-low-vol",
		Status:        model.WorkerRunning,
		Source:        source,
		GitSHA:        oldSHA,
		LitestreamSHA: oldLitestreamSHA,
		PRNumber:      1228,
		ProfileName:   "low-volume",
		ProfileConfig: workload.Config{LoadMode: "synthetic", InitialSize: "5MB"}.JSON(),
		FlyMachineID:  "old-machine-low",
		FlyVolumeID:   "old-volume-low",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1228-dormant",
		AppName:       "litestream-soak",
		Name:          "worker-pr-1228-dormant",
		Status:        model.WorkerRunning,
		Source:        source,
		GitSHA:        oldSHA,
		LitestreamSHA: oldLitestreamSHA,
		PRNumber:      1228,
		ProfileName:   "low-volume",
		ProfileConfig: workload.Config{LoadMode: "synthetic", InitialSize: "5MB"}.JSON(),
		FlyVolumeID:   "dormant-volume",
	})
	if err := db.MarkWorkerDormant("worker-pr-1228-dormant", "stale failure", "same_failure", "test"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}

	fly := newDeployTestFlyServer(t, db, source, sha, litestreamSHA, imageRef)
	deployer := NewDeployer(
		NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", ""),
		db,
		"litestream-soak",
		false,
	)

	gotImageRef, err := deployer.NotifyDeploymentReady(context.Background(), source, sha, litestreamSHA, imageRef, "github_actions_pr_soak")
	if err != nil {
		t.Fatalf("NotifyDeploymentReady() error = %v", err)
	}
	if gotImageRef != imageRef {
		t.Fatalf("imageRef=%q want %q", gotImageRef, imageRef)
	}

	deployment := mustDeploymentByVersion(t, db, source, sha, litestreamSHA)
	if deployment.Status != "ready" {
		t.Fatalf("deployment.Status=%q want ready", deployment.Status)
	}
	if deployment.PRNumber != 1228 {
		t.Fatalf("deployment.PRNumber=%d want 1228", deployment.PRNumber)
	}
	if deployment.ImageRef != imageRef {
		t.Fatalf("deployment.ImageRef=%q want %q", deployment.ImageRef, imageRef)
	}

	rollout, err := buildDeploymentRollout(db, *deployment)
	if err != nil {
		t.Fatalf("buildDeploymentRollout() error = %v", err)
	}
	if rollout.Status != "probing" {
		t.Fatalf("rollout.Status=%q want probing", rollout.Status)
	}
	if rollout.TotalWorkers != len(DefaultMainFleet().Workers)+1 {
		t.Fatalf("rollout.TotalWorkers=%d want %d", rollout.TotalWorkers, len(DefaultMainFleet().Workers)+1)
	}
	if rollout.UpdatedWorkers != rollout.TotalWorkers {
		t.Fatalf("rollout.UpdatedWorkers=%d want %d", rollout.UpdatedWorkers, rollout.TotalWorkers)
	}
	if rollout.ProbingWorkers != 1 {
		t.Fatalf("rollout.ProbingWorkers=%d want 1", rollout.ProbingWorkers)
	}

	resumed := mustWorker(t, db, "worker-pr-1228-dormant")
	if resumed.Status != model.WorkerProbing {
		t.Fatalf("resumed.Status=%q want probing", resumed.Status)
	}
	if resumed.GitSHA != sha {
		t.Fatalf("resumed.GitSHA=%q want %q", resumed.GitSHA, sha)
	}
	if resumed.LitestreamSHA != litestreamSHA {
		t.Fatalf("resumed.LitestreamSHA=%q want %q", resumed.LitestreamSHA, litestreamSHA)
	}
	if resumed.ResumeTrigger != "github_actions_pr_soak" {
		t.Fatalf("resumed.ResumeTrigger=%q want github_actions_pr_soak", resumed.ResumeTrigger)
	}

	workerCount := len(mustWorkersForSource(t, db, source))
	machineCreates := fly.machineCreates()
	volumeCreates := fly.volumeCreates()

	gotImageRef, err = deployer.NotifyDeploymentReady(context.Background(), source, sha, litestreamSHA, imageRef, "github_actions_pr_soak")
	if err != nil {
		t.Fatalf("duplicate NotifyDeploymentReady() error = %v", err)
	}
	if gotImageRef != imageRef {
		t.Fatalf("duplicate imageRef=%q want %q", gotImageRef, imageRef)
	}
	if got := len(mustDeployments(t, db, source)); got != 1 {
		t.Fatalf("deployment count=%d want 1 after duplicate notify", got)
	}
	if got := len(mustWorkersForSource(t, db, source)); got != workerCount {
		t.Fatalf("worker count=%d want %d after duplicate notify", got, workerCount)
	}
	if got := fly.machineCreates(); got != machineCreates {
		t.Fatalf("machine creates=%d want %d after duplicate notify", got, machineCreates)
	}
	if got := fly.volumeCreates(); got != volumeCreates {
		t.Fatalf("volume creates=%d want %d after duplicate notify", got, volumeCreates)
	}

	fly.assertNoErrors(t)
	if !fly.readyObservedBeforeRollout() {
		t.Fatal("fake Fly server did not observe ready deployment state before rollout calls")
	}
}

func TestNotifyDeploymentReadySkipsSupersededDeployment(t *testing.T) {
	db := openTestDB(t)
	source := "pr-1228"
	oldSHA := "1111111111111111111111111111111111111111"
	oldLitestreamSHA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	oldImageRef := "registry.fly.io/litestream-soak:sha-111111111111-pr-1228"
	latestSHA := "2222222222222222222222222222222222222222"
	latestLitestreamSHA := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	latestImageRef := "registry.fly.io/litestream-soak:sha-222222222222-pr-1228"

	if _, err := db.CreateDeployment(&model.Deployment{
		GitSHA:        oldSHA,
		LitestreamSHA: oldLitestreamSHA,
		ImageRef:      "",
		Source:        source,
		PRNumber:      1228,
		Status:        "building",
	}); err != nil {
		t.Fatalf("CreateDeployment(old) error = %v", err)
	}
	latestID, err := db.CreateDeployment(&model.Deployment{
		GitSHA:        latestSHA,
		LitestreamSHA: latestLitestreamSHA,
		ImageRef:      "",
		Source:        source,
		PRNumber:      1228,
		Status:        "building",
	})
	if err != nil {
		t.Fatalf("CreateDeployment(latest) error = %v", err)
	}
	if err := db.UpdateDeployment(latestID, "ready", latestImageRef, ""); err != nil {
		t.Fatalf("UpdateDeployment(latest) error = %v", err)
	}
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1228-low-vol",
		AppName:       "litestream-soak",
		Name:          "worker-pr-1228-low-vol",
		Status:        model.WorkerRunning,
		Source:        source,
		GitSHA:        latestSHA,
		LitestreamSHA: latestLitestreamSHA,
		PRNumber:      1228,
		ProfileName:   "low-volume",
		ProfileConfig: workload.Config{LoadMode: "synthetic", InitialSize: "5MB"}.JSON(),
		FlyVolumeID:   "latest-dormant-volume",
	})
	if err := db.MarkWorkerDormant("worker-pr-1228-low-vol", "stale failure", "same_failure", "test"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}

	fly := newDeployTestFlyServer(t, db, source, latestSHA, latestLitestreamSHA, latestImageRef)
	deployer := NewDeployer(
		NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", ""),
		db,
		"litestream-soak",
		false,
	)

	gotImageRef, err := deployer.NotifyDeploymentReady(context.Background(), source, oldSHA, oldLitestreamSHA, oldImageRef, "github_actions_pr_soak")
	if err != nil {
		t.Fatalf("NotifyDeploymentReady() error = %v", err)
	}
	if gotImageRef != oldImageRef {
		t.Fatalf("imageRef = %q, want %q", gotImageRef, oldImageRef)
	}

	latest, err := db.GetLatestReadyDeployment(source)
	if err != nil {
		t.Fatalf("GetLatestReadyDeployment() error = %v", err)
	}
	if latest == nil {
		t.Fatal("GetLatestReadyDeployment() = nil, want deployment")
	}
	if latest.GitSHA != latestSHA {
		t.Fatalf("latest.GitSHA = %q, want %q", latest.GitSHA, latestSHA)
	}

	oldDeployment := mustDeploymentByVersion(t, db, source, oldSHA, oldLitestreamSHA)
	if oldDeployment.Status != "ready" {
		t.Fatalf("oldDeployment.Status = %q, want ready", oldDeployment.Status)
	}

	workers := mustWorkersForSource(t, db, source)
	if len(workers) != 1 {
		t.Fatalf("len(workers) = %d, want 1", len(workers))
	}
	dormant := mustWorker(t, db, "worker-pr-1228-low-vol")
	if dormant.Status != model.WorkerDormant {
		t.Fatalf("dormant.Status = %q, want dormant", dormant.Status)
	}
	if dormant.GitSHA != latestSHA {
		t.Fatalf("dormant.GitSHA = %q, want %q", dormant.GitSHA, latestSHA)
	}
	if dormant.LitestreamSHA != latestLitestreamSHA {
		t.Fatalf("dormant.LitestreamSHA = %q, want %q", dormant.LitestreamSHA, latestLitestreamSHA)
	}
	fly.assertCreateCounts(t, 0, 0)
	fly.assertNoErrors(t)
}

func TestNotifyDeploymentReadyRejectsMalformedImageRef(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	deployer := &Deployer{
		db: db,
		manager: &Manager{
			db:      db,
			appName: "litestream-soak",
		},
	}

	_, err := deployer.NotifyDeploymentReady(
		context.Background(),
		"custom",
		"2222222222222222222222222222222222222222",
		"3333333333333333333333333333333333333333",
		"registry.fly.io/litestream-soak:bad image",
		"test",
	)
	if err == nil {
		t.Fatal("NotifyDeploymentReady() error = nil, want malformed image ref error")
	}
	if !strings.Contains(err.Error(), "invalid deployment image ref") {
		t.Fatalf("NotifyDeploymentReady() error = %q, want invalid image ref", err)
	}
}

type deployTestFlyServer struct {
	client         *flyapi.Client
	server         *httptest.Server
	db             *model.DB
	source         string
	sha            string
	litestreamSHA  string
	imageRef       string
	mu             sync.Mutex
	errors         []string
	machines       int
	volumes        int
	readyBeforeFly bool
}

func newDeployTestFlyServer(t *testing.T, db *model.DB, source, sha, litestreamSHA, imageRef string) *deployTestFlyServer {
	t.Helper()

	fake := &deployTestFlyServer{
		db:            db,
		source:        source,
		sha:           sha,
		litestreamSHA: litestreamSHA,
		imageRef:      imageRef,
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)

	fake.client = flyapi.NewClientWithBaseURL("litestream-soak", "test-token", fake.server.URL)
	return fake
}

func (f *deployTestFlyServer) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/apps/litestream-soak/machines":
		f.writeJSON(w, []flyapi.Machine{{
			ID:        "current-machine",
			State:     "started",
			Config:    flyapi.MachineConfig{Image: f.imageRef},
			UpdatedAt: time.Now().UTC(),
		}})
	case r.Method == http.MethodPost && r.URL.Path == "/apps/litestream-soak/volumes":
		f.observeReadyBeforeFly()
		var req flyapi.CreateVolumeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			f.recordError("decode create volume: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.volumes++
		id := fmt.Sprintf("vol-%03d", f.volumes)
		f.mu.Unlock()
		f.writeJSON(w, flyapi.Volume{ID: id, Name: req.Name, SizeGB: req.SizeGB, Region: req.Region, State: "created"})
	case r.Method == http.MethodPost && r.URL.Path == "/apps/litestream-soak/machines":
		f.observeReadyBeforeFly()
		var req flyapi.CreateMachineRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			f.recordError("decode create machine: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Config.Image != f.imageRef {
			f.recordError("create machine image=%q want %q", req.Config.Image, f.imageRef)
		}
		f.mu.Lock()
		f.machines++
		id := fmt.Sprintf("machine-%03d", f.machines)
		f.mu.Unlock()
		f.writeJSON(w, flyapi.Machine{ID: id, Name: req.Name, State: "started", Config: req.Config, UpdatedAt: time.Now().UTC()})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apps/litestream-soak/machines/"):
		id := strings.TrimPrefix(r.URL.Path, "/apps/litestream-soak/machines/")
		f.writeJSON(w, flyapi.Machine{
			ID: id,
			Config: flyapi.MachineConfig{Mounts: []flyapi.Mount{{
				Volume: "vol-" + id,
				Path:   "/data",
			}}},
		})
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/stop"):
		f.observeReadyBeforeFly()
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/apps/litestream-soak/machines/"):
		f.observeReadyBeforeFly()
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/apps/litestream-soak/volumes/"):
		f.observeReadyBeforeFly()
		w.WriteHeader(http.StatusOK)
	default:
		f.recordError("unexpected fly request %s %s", r.Method, r.URL.String())
		http.NotFound(w, r)
	}
}

func (f *deployTestFlyServer) observeReadyBeforeFly() {
	deployment, err := f.db.GetDeploymentByVersion(f.source, f.sha, f.litestreamSHA)
	if err != nil {
		f.recordError("deployment missing before rollout: %v", err)
		return
	}
	if deployment.Status != "ready" {
		f.recordError("deployment status before rollout=%q want ready", deployment.Status)
		return
	}
	if deployment.ImageRef != f.imageRef {
		f.recordError("deployment image before rollout=%q want %q", deployment.ImageRef, f.imageRef)
		return
	}

	events, err := f.db.ListEvents(20)
	if err != nil {
		f.recordError("list events before rollout: %v", err)
		return
	}
	for _, event := range events {
		if event.EventType == "deploy_ready_received" && event.Details == f.imageRef {
			f.mu.Lock()
			f.readyBeforeFly = true
			f.mu.Unlock()
			return
		}
	}
	f.recordError("deploy_ready_received event missing before rollout")
}

func (f *deployTestFlyServer) recordError(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *deployTestFlyServer) writeJSON(w http.ResponseWriter, value any) {
	if err := json.NewEncoder(w).Encode(value); err != nil {
		f.recordError("encode response: %v", err)
	}
}

func (f *deployTestFlyServer) assertNoErrors(t *testing.T) {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.errors) > 0 {
		t.Fatalf("fake fly errors: %s", strings.Join(f.errors, "; "))
	}
}

func (f *deployTestFlyServer) assertCreateCounts(t *testing.T, volumes, machines int) {
	t.Helper()

	if got := f.volumeCreates(); got != volumes {
		t.Fatalf("volume creates=%d want %d", got, volumes)
	}
	if got := f.machineCreates(); got != machines {
		t.Fatalf("machine creates=%d want %d", got, machines)
	}
}

func (f *deployTestFlyServer) machineCreates() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.machines
}

func (f *deployTestFlyServer) volumeCreates() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.volumes
}

func (f *deployTestFlyServer) readyObservedBeforeRollout() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readyBeforeFly
}

func fakeFlyPath(t *testing.T, imageRef string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "fly")
	body := fmt.Sprintf("#!/bin/sh\necho 'image: %s'\n", imageRef)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write fake fly: %v", err)
	}
	return dir
}

func mustDeploymentByVersion(t *testing.T, db *model.DB, source, sha, litestreamSHA string) *model.Deployment {
	t.Helper()

	deployment, err := db.GetDeploymentByVersion(source, sha, litestreamSHA)
	if err != nil {
		t.Fatalf("GetDeploymentByVersion(%q, %q, %q) error = %v", source, sha, litestreamSHA, err)
	}
	return deployment
}

func mustDeployments(t *testing.T, db *model.DB, source string) []model.Deployment {
	t.Helper()

	deployments, err := db.ListDeployments(source, 20)
	if err != nil {
		t.Fatalf("ListDeployments(%q) error = %v", source, err)
	}
	return deployments
}

func mustWorkersForSource(t *testing.T, db *model.DB, source string) []model.Worker {
	t.Helper()

	workers, err := db.ListWorkersForSource(source)
	if err != nil {
		t.Fatalf("ListWorkersForSource(%q) error = %v", source, err)
	}
	return workers
}

func mustWorker(t *testing.T, db *model.DB, id string) *model.Worker {
	t.Helper()

	worker, err := db.GetWorker(id)
	if err != nil {
		t.Fatalf("GetWorker(%q) error = %v", id, err)
	}
	return worker
}
