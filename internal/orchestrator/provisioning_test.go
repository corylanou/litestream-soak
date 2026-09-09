package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestPendingMatchingWorkerInspectsActualResources(t *testing.T) {
	db := openTestDB(t)
	desired := DesiredWorker{WorkerID: "pending", Name: "pending", Source: "main", GitSHA: "sha", LitestreamSHA: "ls", ProfileName: "low-volume", Region: "ord", Workload: workload.Config{LoadMode: "synthetic"}}
	request, err := workerRequestForDesired(desired, "image", desired.WorkerID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		http.Error(w, "temporary provider failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	manager := NewManager(flyapi.NewClientWithBaseURL("app", "test", server.URL), db, nil, nil, "app", ReplicaConfig{}, "", "")
	config, err := marshalWorkloadConfig(normalizeWorkloadConfig(request.Workload))
	if err != nil {
		t.Fatal(err)
	}
	worker := manager.newWorkerRecord(request, config)
	worker.FlyMachineID = "destroyed"
	if err := db.CreateWorker(worker); err != nil {
		t.Fatal(err)
	}
	err = manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, "image")
	if reads.Load() == 0 || err == nil {
		t.Fatalf("pending worker silently matched: reads=%d err=%v", reads.Load(), err)
	}
	stored := mustWorker(t, db, worker.ID)
	if stored.Status != model.WorkerPending {
		t.Fatalf("transient observation changed status: %s", stored.Status)
	}
}

type provisioningFly struct {
	machineState                       string
	mu                                 sync.Mutex
	machines                           []flyapi.Machine
	volumes                            []flyapi.Volume
	volumePosts, machinePosts, deletes int
	loseVolume, loseMachine            bool
	readFailure                        bool
}

func (f *provisioningFly) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet:
		if f.readFailure {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/apps/app/machines" {
			_ = json.NewEncoder(w).Encode(f.machines)
			return
		}
		if r.URL.Path == "/apps/app/volumes" {
			_ = json.NewEncoder(w).Encode(f.volumes)
			return
		}
		http.Error(w, "missing", 404)
	case r.Method == http.MethodPost && r.URL.Path == "/apps/app/volumes":
		f.volumePosts++
		var request flyapi.CreateVolumeRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		volume := flyapi.Volume{ID: fmt.Sprintf("volume-%d", f.volumePosts), Name: request.Name, Region: request.Region, SizeGB: request.SizeGB, State: "created", CreatedAt: time.Now().UTC()}
		f.volumes = append(f.volumes, volume)
		if f.loseVolume {
			http.Error(w, "response lost", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(volume)
	case r.Method == http.MethodPost && r.URL.Path == "/apps/app/machines":
		f.machinePosts++
		var request flyapi.CreateMachineRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		machine := flyapi.Machine{ID: fmt.Sprintf("machine-%d", f.machinePosts), Name: request.Name, Region: request.Region, Config: request.Config, State: "started", CreatedAt: time.Now().UTC()}
		if f.machineState != "" {
			machine.State = f.machineState
		}
		f.machines = append(f.machines, machine)
		for i := range f.volumes {
			if f.volumes[i].ID == request.Config.Mounts[0].Volume {
				f.volumes[i].AttachedMachineID = machine.ID
			}
		}
		if f.loseMachine {
			http.Error(w, "response lost", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(machine)
	default:
		f.deletes++
		http.Error(w, "unexpected mutation", 500)
	}
}

func provisioningFixture(t *testing.T, f *provisioningFly, paths ...string) (*model.DB, *Manager, DesiredWorker, WorkerRequest) {
	t.Helper()
	var db *model.DB
	if len(paths) == 0 {
		db = openTestDB(t)
	} else {
		var err error
		db, err = model.Open(paths[0])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	manager := NewManager(flyapi.NewClientWithBaseURL("app", "test", server.URL), db, nil, nil, "app", ReplicaConfig{}, "", "")
	desired := DesiredWorker{WorkerID: "pending", Name: "pending", Source: "main", GitSHA: "sha", LitestreamSHA: "ls", ProfileName: "low-volume", Region: "ord", Workload: workload.Config{LoadMode: "synthetic"}}
	request, err := workerRequestForDesired(desired, "image", desired.WorkerID, nil)
	if err != nil {
		t.Fatal(err)
	}
	return db, manager, desired, request
}

func TestProvisioningAdoptsLostCreationResponsesAfterRestart(t *testing.T) {
	for _, phase := range []string{"volume", "machine"} {
		t.Run(phase, func(t *testing.T) {
			fake := &provisioningFly{loseVolume: phase == "volume", loseMachine: phase == "machine"}
			path := filepath.Join(t.TempDir(), "restart.db")
			db, manager, desired, request := provisioningFixture(t, fake, path)
			if _, err := manager.CreateWorker(context.Background(), request); err == nil {
				t.Fatal("lost response did not report uncertainty")
			}
			if got := mustWorker(t, db, request.WorkerID); got.Status != model.WorkerPending {
				t.Fatalf("uncertain worker=%s", got.Status)
			}
			fake.mu.Lock()
			fake.loseVolume = false
			fake.loseMachine = false
			fake.mu.Unlock()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			var reopenErr error
			db, reopenErr = model.Open(path)
			if reopenErr != nil {
				t.Fatal(reopenErr)
			}
			t.Cleanup(func() { _ = db.Close() })
			restarted := NewManager(manager.fly, db, nil, nil, "app", ReplicaConfig{}, "", "")
			if err := restarted.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, "image"); err != nil {
				t.Fatal(err)
			}
			worker := mustWorker(t, db, request.WorkerID)
			if worker.Status != model.WorkerRunning || worker.FlyMachineID != "machine-1" || worker.FlyVolumeID != "volume-1" {
				t.Fatalf("not recovered: %+v", worker)
			}
			if fake.volumePosts != 1 || fake.machinePosts != 1 || fake.deletes != 0 {
				t.Fatalf("unsafe mutation counts: volumes=%d machines=%d deletes=%d", fake.volumePosts, fake.machinePosts, fake.deletes)
			}
			events, err := db.ListEvidenceEvents("main")
			if err != nil {
				t.Fatal(err)
			}
			var unavailable, recovered bool
			for _, event := range events {
				unavailable = unavailable || event.EventType == "worker_provisioning_unavailable"
				recovered = recovered || event.EventType == "worker_provisioning_recovered"
			}
			if !unavailable || !recovered {
				t.Fatalf("lost attempt/recovery history: unavailable=%v recovered=%v", unavailable, recovered)
			}
		})
	}
}

func TestLegacyPendingMissingResourcesRecoversWithoutCleanup(t *testing.T) {
	for _, machineID := range []string{"", "destroyed"} {
		t.Run(machineID, func(t *testing.T) {
			fake := &provisioningFly{}
			db, manager, desired, request := provisioningFixture(t, fake)
			config, _ := marshalWorkloadConfig(normalizeWorkloadConfig(request.Workload))
			worker := manager.newWorkerRecord(request, config)
			worker.FlyMachineID = machineID
			worker.FlyVolumeID = "old-volume"
			if err := db.CreateWorker(worker); err != nil {
				t.Fatal(err)
			}
			if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, "image"); err != nil {
				t.Fatal(err)
			}
			if got := mustWorker(t, db, worker.ID); got.Status != model.WorkerRunning || got.FlyMachineID == machineID {
				t.Fatalf("not recovered: %+v", got)
			}
			if fake.deletes != 0 || fake.volumePosts != 1 || fake.machinePosts != 1 {
				t.Fatal("unsafe recovery mutation")
			}
		})
	}
}

func TestLegacyPendingRetainsUnattributedVolume(t *testing.T) {
	fake := &provisioningFly{volumes: []flyapi.Volume{{ID: "retained", Name: flyVolumeName("pending"), State: "created", Region: "ord", SizeGB: 10}}}
	db, manager, desired, request := provisioningFixture(t, fake)
	config, _ := marshalWorkloadConfig(normalizeWorkloadConfig(request.Workload))
	if err := db.CreateWorker(manager.newWorkerRecord(request, config)); err != nil {
		t.Fatal(err)
	}
	if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, "image"); err == nil {
		t.Fatal("unproven volume silently adopted")
	}
	if fake.deletes != 0 || fake.volumePosts != 0 || fake.machinePosts != 0 {
		t.Fatal("mutated uncertain resources")
	}
}

func TestConcurrentPendingRecoveryCreatesOnce(t *testing.T) {
	fake := &provisioningFly{}
	db, manager, desired, request := provisioningFixture(t, fake)
	config, _ := marshalWorkloadConfig(normalizeWorkloadConfig(request.Workload))
	if err := db.CreateWorker(manager.newWorkerRecord(request, config)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		manager := NewManager(manager.fly, db, nil, nil, "app", ReplicaConfig{}, "", "")
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, "image")
		}()
	}
	wg.Wait()
	if fake.volumePosts != 1 || fake.machinePosts != 1 || fake.deletes != 0 {
		t.Fatalf("duplicate recovery: volumes=%d machines=%d deletes=%d", fake.volumePosts, fake.machinePosts, fake.deletes)
	}
}

func TestHeartbeatBeforeReconcileCompletesProvisioning(t *testing.T) {
	fake := &provisioningFly{machineState: "created"}
	db, manager, desired, request := provisioningFixture(t, fake)
	if _, err := manager.CreateWorker(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	expected, err := db.ExpectedWorkerRun(request.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(expected.ProfileConfig))
	expected.ProfileHash = fmt.Sprintf("%x", digest[:8])
	expected.ValidatorID = "soak-verifier:" + expected.GitSHA
	payload, _ := json.Marshal(reporting.HeartbeatPayload{WorkerIdentity: *expected, SentAt: time.Now().UTC()})
	heartbeat := httptest.NewRequest(http.MethodPost, "/heartbeat", bytes.NewReader(payload))
	heartbeat.SetPathValue("id", request.WorkerID)
	response := httptest.NewRecorder()
	NewAPI(db, nil, nil, nil, nil, nil).handleHeartbeat(response, heartbeat)
	if response.Code != http.StatusAccepted {
		t.Fatalf("heartbeat failed: %d %s", response.Code, response.Body.String())
	}
	if worker := mustWorker(t, db, request.WorkerID); worker.Status != model.WorkerRunning {
		t.Fatalf("heartbeat did not transition to running: %s", worker.Status)
	}
	fake.mu.Lock()
	fake.machines[0].State = "started"
	fake.mu.Unlock()
	if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, "image"); err != nil {
		t.Fatal(err)
	}
	attempt, err := db.ActiveProvisioning(request.WorkerID)
	if err != nil || attempt != nil {
		t.Fatalf("heartbeat stranded active attempt: %+v %v", attempt, err)
	}
	worker := mustWorker(t, db, request.WorkerID)
	worker.GitSHA = "next"
	worker.Status = model.WorkerPending
	if err := db.CreateWorker(worker); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.beginWorkerProvisioning(*worker, "next-image"); err != nil {
		t.Fatalf("next deployment blocked: %v", err)
	}
}

func TestStaleProvisioningCannotOverwriteMachine(t *testing.T) {
	fake := &provisioningFly{}
	db, manager, _, request := provisioningFixture(t, fake)
	worker, err := manager.CreateWorker(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	stale := model.ProvisioningAttempt{ID: "stale", WorkerID: worker.ID, Phase: "machine_ready", MachineID: "stale-machine", VolumeID: "stale-volume", GitSHA: worker.GitSHA, LitestreamSHA: worker.LitestreamSHA}
	for _, state := range []string{"created", "started"} {
		_, err := manager.finishWorkerProvisioning(*worker, stale, flyapi.Machine{ID: stale.MachineID, State: state}, true)
		if err == nil {
			t.Errorf("stale %s attempt accepted", state)
		}
		stored := mustWorker(t, db, worker.ID)
		if stored.FlyMachineID != worker.FlyMachineID || stored.FlyVolumeID != worker.FlyVolumeID {
			t.Fatalf("stale attempt overwrote resource identity")
		}
	}
}

func TestProvisioningRetainsSafeFailureClassification(t *testing.T) {
	fake := &provisioningFly{}
	db, manager, _, request := provisioningFixture(t, fake)
	worker, err := manager.CreateWorker(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		cause error
		want  string
	}{
		{&flyapi.APIError{StatusCode: 503, Body: "secret-provider-body"}, "HTTP 503"},
		{context.DeadlineExceeded, "timeout"}, {context.Canceled, "canceled"},
	} {
		err := manager.provisioningUnavailable(*worker, model.ProvisioningAttempt{}, "Inventory unavailable", tc.cause)
		if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "secret-provider-body") {
			t.Fatalf("unsafe or missing failure class: %v", err)
		}
	}
	events, err := db.ListEvidenceEvents("main")
	if err != nil {
		t.Fatal(err)
	}
	var found int
	for _, event := range events {
		if event.EventType == "worker_provisioning_unavailable" {
			found++
			if strings.Contains(event.Message, "secret-provider-body") {
				t.Fatal("provider body persisted")
			}
		}
	}
	if found != 3 {
		t.Fatalf("retained failures=%d", found)
	}
}

func TestReplacementPreservesUnresolvedProvisioning(t *testing.T) {
	fake := &provisioningFly{machineState: "created"}
	_, manager, _, request := provisioningFixture(t, fake)
	worker, err := manager.CreateWorker(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.GitSHA = "next"
	if _, err := manager.replaceWorkerWithRequest(context.Background(), *worker, request); err == nil {
		t.Fatal("replacement accepted unresolved provisioning")
	}
	if fake.deletes != 0 {
		t.Fatal("replacement destroyed unresolved resources")
	}
}

func TestProvisioningDiscoveryRefusesAmbiguity(t *testing.T) {
	for _, scenario := range []string{"duplicate-volume", "duplicate-machine", "missing-created-machine", "wrong-workload", "inventory-error"} {
		t.Run(scenario, func(t *testing.T) {
			fake := &provisioningFly{loseMachine: true}
			db, manager, desired, request := provisioningFixture(t, fake)
			if _, err := manager.CreateWorker(context.Background(), request); err == nil {
				t.Fatal("expected lost response")
			}
			fake.mu.Lock()
			fake.loseMachine = false
			switch scenario {
			case "duplicate-volume":
				copy := fake.volumes[0]
				copy.ID = "duplicate"
				fake.volumes = append(fake.volumes, copy)
			case "duplicate-machine":
				copy := fake.machines[0]
				copy.ID = "duplicate"
				fake.machines = append(fake.machines, copy)
			case "missing-created-machine":
				fake.machines = nil
			case "wrong-workload":
				fake.machines[0].Config.Env["SOAK_WORKLOAD_ID"] = "wrong"
			case "inventory-error":
				fake.readFailure = true
			}
			fake.mu.Unlock()
			for range 2 {
				if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, "image"); err == nil {
					t.Fatal("uncertain discovery accepted")
				}
			}
			if fake.volumePosts != 1 || fake.machinePosts != 1 || fake.deletes != 0 {
				t.Fatal("uncertainty caused resource mutation")
			}
			if mustWorker(t, db, request.WorkerID).Status != model.WorkerPending {
				t.Fatal("uncertainty completed worker")
			}
		})
	}
}

func TestProvisioningInterruptionIsUnavailableEvidence(t *testing.T) {
	if got := incidentEventClass("worker_provisioning_interrupted"); got != "unavailable" {
		t.Fatalf("interruption class=%q", got)
	}
	if got := incidentEventClass("worker_provisioning_recovered"); got != "unexpected" {
		t.Fatalf("recovery class=%q", got)
	}
}

func TestPendingAttemptCompletesBeforeNewDeployment(t *testing.T) {
	fake := &provisioningFly{loseMachine: true}
	db, manager, desired, request := provisioningFixture(t, fake)
	if _, err := manager.CreateWorker(context.Background(), request); err == nil {
		t.Fatal("expected lost response")
	}
	fake.loseMachine = false
	desired.GitSHA = "next"
	if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, "next-image"); err != nil {
		t.Fatal(err)
	}
	worker := mustWorker(t, db, request.WorkerID)
	if worker.Status != model.WorkerRunning || worker.GitSHA != request.GitSHA {
		t.Fatal("original attempt did not complete with its immutable target")
	}
	if fake.machinePosts != 1 || fake.volumePosts != 1 || fake.deletes != 0 {
		t.Fatal("recovery replaced existing resources")
	}
}

func TestResumedProvisioningRetainsOriginalDeployment(t *testing.T) {
	for _, phase := range []string{"volume_requested", "volume_ready"} {
		t.Run(phase, func(t *testing.T) {
			fake := &provisioningFly{loseVolume: true}
			db, manager, desired, request := provisioningFixture(t, fake)
			old := model.Deployment{Source: "main", GitSHA: request.GitSHA, LitestreamSHA: request.LitestreamSHA, ImageRef: request.ImageRef, WorkloadSHA: "original-workload", Status: "ready"}
			oldID, err := db.CreateDeployment(&old)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := manager.CreateWorker(context.Background(), request); err == nil {
				t.Fatal("expected volume response loss")
			}
			if phase == "volume_ready" {
				attempt, err := db.ActiveProvisioning(request.WorkerID)
				if err != nil {
					t.Fatal(err)
				}
				changed, err := db.AdvanceProvisioning(*attempt, "volume_ready", fake.volumes[0].ID, "")
				if err != nil || !changed {
					t.Fatal("failed to persist confirmed volume")
				}
			}
			newer := old
			newer.GitSHA = "next"
			newer.ImageRef = "next-image"
			newer.WorkloadSHA = "next-workload"
			newID, err := db.CreateDeployment(&newer)
			if err != nil {
				t.Fatal(err)
			}
			fake.loseVolume = false
			desired.GitSHA = newer.GitSHA
			if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, newer.ImageRef); err != nil {
				t.Fatal(err)
			}
			expected, err := db.ExpectedWorkerRun(request.WorkerID)
			if err != nil {
				t.Fatal(err)
			}
			if expected.DeploymentID != int(oldID) || expected.WorkloadSHA != old.WorkloadSHA {
				t.Fatalf("resumed identity deployment=%d workload=%s", expected.DeploymentID, expected.WorkloadSHA)
			}
			if fake.machines[0].Config.Env["SOAK_DEPLOYMENT_ID"] != fmt.Sprint(oldID) {
				t.Fatal("machine lost original deployment identity")
			}
			end := time.Now().Add(-time.Hour)
			window := model.EvidenceWindow{DeploymentID: int(oldID), Start: end.Add(-time.Hour), End: &end}
			events, err := db.ListEvidenceEvents("main", window)
			if err != nil {
				t.Fatal(err)
			}
			var interrupted, recovered bool
			for _, event := range events {
				interrupted = interrupted || event.EventType == "worker_provisioning_interrupted"
				recovered = recovered || event.EventType == "worker_provisioning_recovered"
			}
			if !interrupted || !recovered {
				t.Fatal("late old-attempt evidence disappeared from original deployment")
			}
			newerEvents, err := db.ListEvidenceEvents("main", model.EvidenceWindow{DeploymentID: int(newID), Start: time.Now().Add(-time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range newerEvents {
				if strings.HasPrefix(event.EventType, "worker_provisioning_") {
					t.Fatal("old attempt attributed to newer deployment")
				}
			}
			worker := mustWorker(t, db, request.WorkerID)
			worker.GitSHA = newer.GitSHA
			worker.Status = model.WorkerPending
			if err := db.CreateWorker(worker); err != nil {
				t.Fatal(err)
			}
			next, err := manager.beginWorkerProvisioning(*worker, newer.ImageRef)
			if err != nil || next.DeploymentID != int(newID) {
				t.Fatalf("next deployment attempt did not advance: %v", err)
			}
		})
	}
}

func TestProvisioningAdoptionRejectsKnownRevisionDrift(t *testing.T) {
	for _, field := range []string{"SOAK_DEPLOYMENT_ID", "WORKLOAD_SHA"} {
		t.Run(field, func(t *testing.T) {
			fake := &provisioningFly{loseMachine: true}
			db, manager, desired, request := provisioningFixture(t, fake)
			deployment := model.Deployment{Source: request.Source, GitSHA: request.GitSHA, LitestreamSHA: request.LitestreamSHA, ImageRef: request.ImageRef, WorkloadSHA: "original-workload", Status: "ready"}
			if _, err := db.CreateDeployment(&deployment); err != nil {
				t.Fatal(err)
			}
			if _, err := manager.CreateWorker(context.Background(), request); err == nil {
				t.Fatal("expected lost machine response")
			}
			fake.loseMachine = false
			fake.machines[0].Config.Env[field] = "999"
			if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, request.ImageRef); err == nil {
				t.Fatal("known revision drift was adopted")
			}
			if mustWorker(t, db, request.WorkerID).Status != model.WorkerPending {
				t.Fatal("mismatched resource activated worker")
			}
			if fake.machinePosts != 1 || fake.volumePosts != 1 || fake.deletes != 0 {
				t.Fatal("revision drift caused resource mutation")
			}
		})
	}
}

func TestLegacyRetiringVolumesPermitFreshRecovery(t *testing.T) {
	fake := &provisioningFly{volumes: []flyapi.Volume{
		{ID: "vol_v8ekeej110e5kgkv", Name: flyVolumeName("pending"), State: "pending_destroy", Region: "ord", SizeGB: 10},
		{ID: "older-volume", Name: flyVolumeName("pending"), State: "scheduling_destroy", Region: "ord", SizeGB: 10},
	}}
	db, manager, desired, request := provisioningFixture(t, fake)
	config, _ := marshalWorkloadConfig(normalizeWorkloadConfig(request.Workload))
	worker := manager.newWorkerRecord(request, config)
	worker.FlyMachineID = "18576451a30348"
	worker.FlyVolumeID = "vol_v8ekeej110e5kgkv"
	if err := db.CreateWorker(worker); err != nil {
		t.Fatal(err)
	}
	if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, request.ImageRef); err != nil {
		t.Fatal(err)
	}
	current := mustWorker(t, db, worker.ID)
	if current.Status != model.WorkerRunning || current.FlyVolumeID == worker.FlyVolumeID || current.FlyMachineID == worker.FlyMachineID {
		t.Fatal("fresh recovery did not replace physical identity")
	}
	if fake.volumePosts != 1 || fake.machinePosts != 1 || fake.deletes != 0 || len(fake.volumes) != 3 {
		t.Fatal("retiring records were mutated or provisioning duplicated")
	}
	if fake.volumes[2].Name == fake.volumes[0].Name || workerReplicaPath(*current) == workerReplicaPath(*worker) {
		t.Fatal("fresh resources reused old name/prefix")
	}
	events, err := db.ListEvidenceEvents("main")
	if err != nil {
		t.Fatal(err)
	}
	observed := map[string]string{}
	var interrupted, recovered bool
	for _, event := range events {
		interrupted = interrupted || event.EventType == "worker_provisioning_interrupted"
		recovered = recovered || event.EventType == "worker_provisioning_recovered"
		if event.EventType == "worker_provisioning_retiring_volume" {
			var detail struct {
				ObservedVolume flyapi.Volume `json:"observed_volume"`
				reporting.WorkerIdentity
			}
			if err := json.Unmarshal([]byte(event.Details), &detail); err != nil {
				t.Fatal(err)
			}
			observed[detail.ObservedVolume.ID] = detail.ObservedVolume.State
			if detail.MachineID != worker.FlyMachineID || detail.DeploymentID != 0 {
				t.Fatal("legacy resource evidence lost old identity or invented attribution")
			}
		}
	}
	if !interrupted || !recovered || observed["vol_v8ekeej110e5kgkv"] != "pending_destroy" || observed["older-volume"] != "scheduling_destroy" {
		t.Fatal("lost retiring resource/interruption/recovery history")
	}
}

func TestRetiringAndUnknownAttemptVolumesAreNotAdopted(t *testing.T) {
	for _, state := range []string{"pending_destroy", "scheduling_destroy", "creating", "recovering", "future_state", ""} {
		t.Run(state, func(t *testing.T) {
			fake := &provisioningFly{loseVolume: true}
			db, manager, desired, request := provisioningFixture(t, fake)
			if _, err := manager.CreateWorker(context.Background(), request); err == nil {
				t.Fatal("expected response loss")
			}
			fake.loseVolume = false
			fake.volumes[0].State = state
			if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, request.ImageRef); err == nil {
				t.Fatal("unusable attempt volume adopted")
			}
			if fake.volumePosts != 1 || fake.machinePosts != 0 || fake.deletes != 0 {
				t.Fatal("unusable volume caused mutation")
			}
			if mustWorker(t, db, request.WorkerID).Status != model.WorkerPending {
				t.Fatal("unusable volume completed attempt")
			}
		})
	}
}

func TestLegacyRecoveryKeepsLiveOrAmbiguousVolumeReferences(t *testing.T) {
	for _, scenario := range []string{"attached", "active-mount", "unknown-machine-mount", "live-duplicate", "unknown-state", "recovering", "waiting_for_detach"} {
		t.Run(scenario, func(t *testing.T) {
			volume := flyapi.Volume{ID: "old-volume", Name: flyVolumeName("pending"), State: "pending_destroy", Region: "ord", SizeGB: 10}
			fake := &provisioningFly{volumes: []flyapi.Volume{volume}}
			switch scenario {
			case "attached":
				fake.volumes[0].AttachedMachineID = "active-machine"
			case "active-mount", "unknown-machine-mount":
				state := "started"
				if scenario == "unknown-machine-mount" {
					state = "unknown"
				}
				fake.machines = []flyapi.Machine{{ID: "foreign-machine", Name: "other", State: state, Config: flyapi.MachineConfig{Mounts: []flyapi.Mount{{Volume: volume.ID, Path: "/data"}}}}}
			case "live-duplicate":
				live := volume
				live.ID = "live-volume"
				live.State = "created"
				fake.volumes = append(fake.volumes, live)
			case "unknown-state":
				fake.volumes[0].State = "future_state"
			default:
				fake.volumes[0].State = scenario
			}
			db, manager, desired, request := provisioningFixture(t, fake)
			config, _ := marshalWorkloadConfig(normalizeWorkloadConfig(request.Workload))
			worker := manager.newWorkerRecord(request, config)
			worker.FlyMachineID = "destroyed-machine"
			worker.FlyVolumeID = volume.ID
			if err := db.CreateWorker(worker); err != nil {
				t.Fatal(err)
			}
			if err := manager.ensureFleetSpec(context.Background(), FleetSpec{Workers: []DesiredWorker{desired}}, request.ImageRef); err == nil {
				t.Fatal("ambiguous resource did not block recovery")
			}
			if fake.volumePosts != 0 || fake.machinePosts != 0 || fake.deletes != 0 {
				t.Fatal("ambiguous resource caused mutation")
			}
			current := mustWorker(t, db, worker.ID)
			if current.Status != model.WorkerPending || current.FlyVolumeID != volume.ID {
				t.Fatal("ambiguous resource identity was erased")
			}
		})
	}
}

func TestMountedRetiringVolumeCannotBeAdopted(t *testing.T) {
	for _, state := range []string{"pending_destroy", "scheduling_destroy", "future_state", "created", "hydrating"} {
		t.Run(state, func(t *testing.T) {
			volume := flyapi.Volume{ID: "volume", State: state}
			machine := flyapi.Machine{ID: "machine", Config: flyapi.MachineConfig{Mounts: []flyapi.Mount{{Volume: volume.ID, Path: "/data"}}}}
			_, ok := mountedProvisioningVolume(machine, []flyapi.Volume{volume})
			if ok != (state == "created" || state == "hydrating") {
				t.Fatalf("volume state %s adoption=%v", state, ok)
			}
		})
	}
}

func TestLegacyRetiringVolumeWithAllocationRemainsAmbiguous(t *testing.T) {
	db := openTestDB(t)
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writes.Add(1)
			http.Error(w, "unexpected mutation", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apps/app/machines":
			_, _ = w.Write([]byte(`[]`))
		case "/apps/app/volumes":
			_, _ = w.Write([]byte(`[{"id":"old-volume","name":"pending","state":"pending_destroy","attached_alloc_id":"old-allocation"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	manager := NewManager(flyapi.NewClientWithBaseURL("app", "test", server.URL), db, nil, nil, "app", ReplicaConfig{}, "", "")
	worker := model.Worker{ID: "pending", Name: "pending", Source: "main", GitSHA: "sha", LitestreamSHA: "ls", Status: model.WorkerPending, FlyVolumeID: "old-volume", FlyMachineID: "destroyed", Region: "ord", ProfileName: "low-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	if err := manager.recoverPendingWorker(context.Background(), worker.ID, "image"); err == nil {
		t.Fatal("attached allocation was treated as unreferenced")
	}
	if writes.Load() != 0 {
		t.Fatal("attached allocation allowed fresh provisioning")
	}
}
