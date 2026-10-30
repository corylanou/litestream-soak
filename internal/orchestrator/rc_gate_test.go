package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	workerconfig "github.com/corylanou/litestream-soak/internal/worker"
	"github.com/corylanou/litestream-soak/internal/workload"
)

const (
	testGateCandidateSHA = "aaaaaaaa11111111111111111111111111111111"
	testGateBaselineSHA  = "bbbbbbbb22222222222222222222222222222222"
	testGateSoakSHA      = "cccccccccccc3333333333333333333333333333"
	testGateCandidate    = "rc-aaaaaaaa"
	testGateBaseline     = "rcbase-bbbbbbbb"
)

func testGateImage(source, litestreamSHA string) string {
	return "registry.fly.io/litestream-soak:sha-" + testGateSoakSHA[:12] + "-" + source + "-ls-" + litestreamSHA[:12]
}

// TestGateFleetPinsPopulateIndexRatio keeps the seed schema a profile-level
// decision: both sides of a gate read the same fleet definition, so the
// candidate and baseline always seed the same indexes. gate-large-db takes the
// heavier case with both indexes; the other profiles keep the default of none.
// TestOnlyGateFleetsStopOnPopulateFailure runs each fleet's real config path:
// desired worker, normalized workload, the machine env the control plane
// writes, and the worker's own parsing of that env. Only gate workers carry
// populate_stop_on_failure; main sentinels (worker-main-sentinel-single
// included) and PR workers keep restart-on-failure.
func TestOnlyGateFleetsStopOnPopulateFailure(t *testing.T) {
	tests := []struct {
		name string
		spec FleetSpec
		want bool
	}{
		{name: "main sentinels", spec: DefaultFleetForSource("main", testGateSoakSHA, testGateCandidateSHA)},
		{name: "pr fleet", spec: DefaultFleetForSource("pr-1514", testGateSoakSHA, testGateCandidateSHA)},
		{name: "rc gate candidate", spec: GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA), want: true},
		{name: "rc gate baseline with suffix", spec: GateFleetForSource(testGateBaseline+"-2dfdc1c3", testGateSoakSHA, testGateBaselineSHA), want: true},
		{name: "upgrade gate", spec: GateFleetForSource(testUpgradeCandidate, testGateSoakSHA, testGateCandidateSHA), want: true},
	}
	mgr := &Manager{controlBaseURL: "https://litestream-soak-ctl.fly.dev"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.spec.Workers) == 0 {
				t.Fatal("fleet has no workers")
			}
			seenSentinel := false
			for _, desired := range tt.spec.Workers {
				seenSentinel = seenSentinel || desired.WorkerID == "worker-main-sentinel-single"
				cfg := normalizeWorkloadConfig(desired.Workload)
				if cfg.PopulateStopOnFailure != tt.want {
					t.Fatalf("%s populate_stop_on_failure = %v, want %v", desired.WorkerID, cfg.PopulateStopOnFailure, tt.want)
				}
				env := mgr.workerEnv(model.Worker{ID: desired.WorkerID, Name: desired.Name, Source: desired.Source}, cfg)
				if _, set := env["POPULATE_STOP_ON_FAILURE"]; set != tt.want {
					t.Fatalf("%s POPULATE_STOP_ON_FAILURE set = %v, want %v", desired.WorkerID, set, tt.want)
				}
				effective, err := workerconfig.WorkloadFromEnvironment(env)
				if err != nil {
					t.Fatalf("%s WorkloadFromEnvironment() error = %v", desired.WorkerID, err)
				}
				if effective.PopulateStopOnFailure != tt.want {
					t.Fatalf("%s worker parsed populate_stop_on_failure = %v, want %v", desired.WorkerID, effective.PopulateStopOnFailure, tt.want)
				}
			}
			if tt.name == "main sentinels" && !seenSentinel {
				t.Fatal("main fleet no longer contains worker-main-sentinel-single; update this test")
			}
		})
	}
}

func TestGateFleetPinsPopulateIndexRatio(t *testing.T) {
	want := map[string]float64{
		gateStressProfile:    0,
		gateManyDB500Profile: 0,
		gateLargeDBProfile:   1,
		gateRemoteProfile:    0,
	}
	for _, source := range []string{testGateCandidate, testGateBaseline} {
		for _, worker := range GateFleetForSource(source, testGateSoakSHA, testGateCandidateSHA).Workers {
			wantRatio, ok := want[worker.ProfileName]
			if !ok {
				t.Fatalf("%s unexpected profile %q", source, worker.ProfileName)
			}
			if worker.Workload.PopulateIndexRatio != wantRatio {
				t.Fatalf("%s %s populate_index_ratio = %v, want %v", source, worker.ProfileName, worker.Workload.PopulateIndexRatio, wantRatio)
			}
			normalized := normalizeWorkloadConfig(worker.Workload)
			if normalized.PopulateIndexRatio != wantRatio {
				t.Fatalf("%s %s normalized populate_index_ratio = %v, want %v", source, worker.ProfileName, normalized.PopulateIndexRatio, wantRatio)
			}
			env := (&Manager{}).workerEnv(model.Worker{ID: worker.WorkerID, Name: worker.Name}, normalized)
			if got, set := env["POPULATE_INDEX_RATIO"]; (wantRatio > 0) != set || (set && got != "1") {
				t.Fatalf("%s %s POPULATE_INDEX_RATIO = %q (set %v), want ratio %v", source, worker.ProfileName, got, set, wantRatio)
			}
		}
	}
}

func TestGateFleetSpecHasFourSizedWorkersPerSource(t *testing.T) {
	type size struct {
		region   string
		cpus     int
		memoryMB int
		volumeGB int
	}
	want := map[string]size{
		gateStressProfile:    {"ord", 2, 2048, 20},
		gateManyDB500Profile: {"ord", 2, 3072, 15},
		gateLargeDBProfile:   {"ord", 2, 4096, 40},
		gateRemoteProfile:    {"ams", 1, 1024, 10},
	}
	configs := map[string]string{}
	for _, tc := range []struct {
		source        string
		litestreamSHA string
	}{
		{testGateCandidate, testGateCandidateSHA},
		{testGateBaseline, testGateBaselineSHA},
	} {
		spec := GateFleetForSource(tc.source, testGateSoakSHA, tc.litestreamSHA)
		if len(spec.Workers) != 4 {
			t.Fatalf("%s workers = %d, want 4", tc.source, len(spec.Workers))
		}
		seen := map[string]bool{}
		for _, worker := range spec.Workers {
			expected, ok := want[worker.ProfileName]
			if !ok {
				t.Fatalf("%s unexpected profile %q", tc.source, worker.ProfileName)
			}
			seen[worker.ProfileName] = true
			cfg := worker.Workload
			if worker.Source != tc.source || worker.GitSHA != testGateSoakSHA || worker.LitestreamSHA != tc.litestreamSHA || worker.PRNumber != 0 {
				t.Fatalf("%s identity = %+v", worker.Name, worker)
			}
			if worker.Name != "worker-"+tc.source+"-"+worker.ProfileName || worker.WorkerID != worker.Name {
				t.Fatalf("name = %q id = %q", worker.Name, worker.WorkerID)
			}
			if worker.Region != expected.region || cfg.CPUs != expected.cpus || cfg.MemoryMB != expected.memoryMB ||
				worker.VolumeSizeGB != expected.volumeGB || cfg.VolumeSizeGB != expected.volumeGB {
				t.Fatalf("%s resources = region %s cpus %d memory %d volume %d/%d, want %+v", worker.Name, worker.Region, cfg.CPUs, cfg.MemoryMB, worker.VolumeSizeGB, cfg.VolumeSizeGB, expected)
			}
			if previous, ok := configs[worker.ProfileName]; ok && previous != cfg.JSON() {
				t.Fatalf("%s config differs between candidate and baseline:\n%s\n%s", worker.ProfileName, previous, cfg.JSON())
			}
			configs[worker.ProfileName] = cfg.JSON()
		}
		if len(seen) != 4 {
			t.Fatalf("%s profiles = %v", tc.source, seen)
		}
	}

	large := gateWorker(t, GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA), gateLargeDBProfile).Workload
	if large.InitialSize != "6GB" {
		t.Fatalf("large-db seed = %q, want 6GB", large.InitialSize)
	}
	var total time.Duration
	for _, raw := range []string{large.VerifySourceSnapshotBudget, large.VerifyRestoreBudget, large.VerifyIntegrityCheckBudget, large.VerifyCompareBudget} {
		budget, err := time.ParseDuration(raw)
		if err != nil || budget <= 0 {
			t.Fatalf("large-db budget %q err=%v", raw, err)
		}
		total += budget
	}
	interval, err := time.ParseDuration(large.VerifyInterval)
	if err != nil || total > interval {
		t.Fatalf("large-db budgets %s exceed verify interval %q (%v)", total, large.VerifyInterval, err)
	}
	stress := gateWorker(t, GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA), gateStressProfile).Workload
	if stress.PinnedReaderHold != "" || stress.StressScenarios != "overload-truncate0,pinned-reader" || stress.StressScenarioPhase == "" {
		t.Fatalf("gate-stress must schedule separate scenarios, got %+v", stress)
	}
	manyDB := gateWorker(t, GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA), gateManyDB500Profile).Workload
	if manyDB.NumDatabases != 500 || manyDB.ActiveRotateInterval == "" || manyDB.ActiveSetSeed == 0 {
		t.Fatalf("gate-manydb-500 workload = %+v", manyDB)
	}
}

func gateWorker(t *testing.T, spec FleetSpec, profile string) DesiredWorker {
	t.Helper()
	for _, worker := range spec.Workers {
		if worker.ProfileName == profile {
			return worker
		}
	}
	t.Fatalf("profile %s missing", profile)
	return DesiredWorker{}
}

func TestGateSourcesStayOutOfMainAndPRFleets(t *testing.T) {
	t.Setenv("SOAK_ENABLE_MANY_DB_FLEET", "1")
	t.Setenv("SOAK_ENABLE_MANY_DB_500", "1")
	t.Setenv("SOAK_ENABLE_MANY_DB_1000", "1")
	gateProfiles := map[string]bool{}
	for _, worker := range GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA).Workers {
		gateProfiles[worker.ProfileName] = true
	}
	for _, spec := range []FleetSpec{DefaultMainFleet(), DefaultFleetForSource("pr-1568", "soak", "litestream")} {
		for _, worker := range spec.Workers {
			if gateProfiles[worker.ProfileName] || isGateSource(worker.Source) {
				t.Fatalf("gate worker leaked into fleet: %+v", worker)
			}
		}
	}
	for source, want := range map[string]bool{
		testGateCandidate:  true,
		testGateBaseline:   true,
		"rc-abc":           false,
		"rc-AAAAAAAA":      false,
		"rcbase-aaaaaaaa1": false,
		"main":             false,
		"pr-12":            false,
	} {
		if got := isGateSource(source); got != want {
			t.Fatalf("isGateSource(%q) = %v, want %v", source, got, want)
		}
		if want && supportsDefaultFleetSource(source) {
			t.Fatalf("gate source %s would receive the default fleet", source)
		}
	}
}

func TestPolicyLoopsSkipGateSources(t *testing.T) {
	everything := []string{"*"}
	for _, tc := range []struct {
		source   string
		prNumber int
		want     bool
	}{
		{"pr-209", 209, true},
		{testGateCandidate, 0, false},
	} {
		db := openTestDB(t)
		_, worker := createCleanSuccessCandidate(t, db, tc.source, tc.prNumber)
		addMeasuredSuccessEvidence(t, db, worker.ID)
		stored, err := db.GetLatestDeployment(tc.source)
		if err != nil {
			t.Fatal(err)
		}
		deployment := *stored
		_, ok, err := successTeardownCandidate(db, deployment, SuccessTeardownPolicy{Threshold: time.Nanosecond, SourceAllowlist: everything}, time.Now().UTC())
		if err != nil || ok != tc.want {
			t.Fatalf("%s success teardown = %v, %v; want %v", tc.source, ok, err, tc.want)
		}
		_, ok, err = prMaxAgeCandidate(db, deployment, PRMaxAgePolicy{SourceAllowlist: everything}, worker.CreatedAt.Add(30*time.Hour))
		if err != nil || ok != tc.want {
			t.Fatalf("%s pr max age = %v, %v; want %v", tc.source, ok, err, tc.want)
		}
	}
}

func TestPRStatePollerSkipsGateSources(t *testing.T) {
	db := openTestDB(t)
	for _, source := range []string{testGateCandidate, testGateBaseline} {
		createTestWorker(t, db, model.Worker{ID: "worker-" + source, Name: "worker-" + source, Status: model.WorkerRunning, Source: source, ProfileName: gateStressProfile, ProfileConfig: "{}"})
	}
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	var requests atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	t.Cleanup(api.Close)
	newTestPRStatePoller(api, manager, nil).pollOnce(context.Background())
	if requests.Load() != 0 {
		t.Fatalf("requests = %d, want 0", requests.Load())
	}
}

func TestMainAndPRReconcilersSkipGateSources(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC())
	createTestGateFleet(t, db, gate)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        strings.Repeat("d", 40),
		LitestreamSHA: testGateCandidateSHA,
		ImageRef:      "registry.fly.io/litestream-soak:newer-build",
		Source:        testGateCandidate,
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment() error = %v", err)
	}
	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	manager.reconcileFleets(context.Background(), DefaultMainFleet())

	if machines := fly.machineRequests(); len(machines) != 0 {
		t.Fatalf("machine creates = %d, want 0", len(machines))
	}
	stops, machineDeletes, volumeDeletes := fly.replacementCounts()
	if stops != 0 || machineDeletes != 0 || volumeDeletes != 0 {
		t.Fatalf("gate workers touched: stops %d machine deletes %d volume deletes %d", stops, machineDeletes, volumeDeletes)
	}
	for _, worker := range mustSourceWorkers(t, db, testGateCandidate) {
		if worker.GitSHA != testGateSoakSHA {
			t.Fatalf("%s rolled to %s", worker.Name, worker.GitSHA)
		}
	}
}

func TestGateDeploymentReadyPinsImmutableBuild(t *testing.T) {
	db := openTestDB(t)
	fly := newCreateWorkerFlyServer(t)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	deployer := NewDeployer(manager, db, "litestream-soak", false)
	ctx := context.Background()
	image := testGateImage(testGateCandidate, testGateCandidateSHA)

	if _, err := deployer.recordDeploymentReady(ctx, testGateCandidate, testGateSoakSHA, testGateCandidateSHA, image, "rc_gate", "", ""); !errors.Is(err, errRCGateNotRegistered) {
		t.Fatalf("unregistered gate err = %v, want errRCGateNotRegistered", err)
	}
	registerTestGate(t, db, false, time.Now().UTC())
	if _, err := deployer.recordDeploymentReady(ctx, testGateCandidate, testGateSoakSHA, testGateBaselineSHA, testGateImage(testGateCandidate, testGateBaselineSHA), "rc_gate", "", ""); !errors.Is(err, errRCGatePinned) {
		t.Fatalf("wrong litestream sha err = %v, want errRCGatePinned", err)
	}
	if _, err := deployer.recordDeploymentReady(ctx, testGateCandidate, testGateSoakSHA, testGateCandidateSHA, testGateImage(testGateBaseline, testGateCandidateSHA), "rc_gate", "", ""); err == nil {
		t.Fatal("image labelled for another gate source was accepted")
	}
	target, err := deployer.recordDeploymentReady(ctx, testGateCandidate, testGateSoakSHA, testGateCandidateSHA, image, "rc_gate", "", "")
	if err != nil {
		t.Fatalf("first gate deployment error = %v", err)
	}
	if _, err := deployer.recordDeploymentReady(ctx, testGateCandidate, testGateSoakSHA, testGateCandidateSHA, image, "rc_gate", "", ""); err != nil {
		t.Fatalf("repeated identical gate deployment error = %v", err)
	}
	relaunched, err := deployer.recordDeploymentReady(ctx, testGateCandidate, testGateSoakSHA, testGateCandidateSHA, image+"@sha256:"+strings.Repeat("f", 64), "rc_gate", "", "")
	if err != nil || relaunched.ImageRef != image {
		t.Fatalf("relaunched same build = %q, %v; want pinned image %q", relaunched.ImageRef, err, image)
	}
	newer := "registry.fly.io/litestream-soak:sha-" + strings.Repeat("e", 12) + "-" + testGateCandidate + "-ls-" + testGateCandidateSHA[:12]
	if _, err := deployer.recordDeploymentReady(ctx, testGateCandidate, strings.Repeat("e", 40), testGateCandidateSHA, newer, "rc_gate", "", ""); !errors.Is(err, errRCGatePinned) {
		t.Fatalf("mid-run rebuild err = %v, want errRCGatePinned", err)
	}

	if _, err := deployer.rolloutDeploymentReady(ctx, target, "rc_gate"); err != nil {
		t.Fatalf("rolloutDeploymentReady() error = %v", err)
	}
	machines := fly.machineRequests()
	if len(machines) != 4 {
		t.Fatalf("machine creates = %d, want 4", len(machines))
	}
	profiles := map[string]bool{}
	for _, machine := range machines {
		profiles[machine.Config.Env["PROFILE"]] = true
		if machine.Config.Image != image {
			t.Fatalf("machine image = %q, want %q", machine.Config.Image, image)
		}
	}
	for _, profile := range []string{gateStressProfile, gateManyDB500Profile, gateLargeDBProfile, gateRemoteProfile} {
		if !profiles[profile] {
			t.Fatalf("profiles = %v, missing %s", profiles, profile)
		}
	}
	volumes := 0
	for _, volume := range fly.volumeRequests() {
		volumes += volume.SizeGB
	}
	if volumes != 85 {
		t.Fatalf("total volume GB = %d, want 85", volumes)
	}
}

func TestRCGateTTLArchivesThenTearsDownBothSources(t *testing.T) {
	for _, tail := range []bool{false, true} {
		t.Run(map[bool]string{false: "no tail", true: "tail"}[tail], func(t *testing.T) {
			db := openTestDB(t)
			started := time.Now().UTC().Add(-time.Hour)
			gate := registerTestGate(t, db, tail, started)
			createTestGateFleet(t, db, gate)
			fly := newTeardownTestServer(t, nil)
			manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{
				Bucket: "bucket", Endpoint: fly.server.URL, AccessKey: "access", SecretKey: "secret", Region: "auto",
			}, "", "")
			ctx := context.Background()

			if err := manager.evaluateRCGates(ctx, gate.StartedAt.Add(time.Minute)); err != nil {
				t.Fatalf("evaluateRCGates(launch) error = %v", err)
			}
			if err := manager.evaluateRCGates(ctx, gate.ExpiresAt.Add(-time.Minute)); err != nil {
				t.Fatalf("evaluateRCGates(before expiry) error = %v", err)
			}
			for _, source := range gate.Sources() {
				for _, worker := range mustSourceWorkers(t, db, source) {
					if worker.Status != model.WorkerRunning {
						t.Fatalf("%s torn down before expiry", worker.Name)
					}
				}
			}

			if err := manager.evaluateRCGates(ctx, gate.ExpiresAt.Add(time.Minute)); err != nil {
				t.Fatalf("evaluateRCGates(expiry) error = %v", err)
			}
			for _, source := range gate.Sources() {
				archives, err := db.ListRunArchives(source, runArchiveTypeTeardown, 10, true)
				if err != nil || len(archives) != 1 {
					t.Fatalf("%s archives = %d, %v; want 1", source, len(archives), err)
				}
				var payload runArchivePayload
				if err := json.Unmarshal([]byte(archives[0].Payload), &payload); err != nil {
					t.Fatal(err)
				}
				if payload.Reason != "rc_gate_ttl" || len(payload.Workers) != 4 {
					t.Fatalf("archive reason %q workers %d", payload.Reason, len(payload.Workers))
				}
				for _, evidence := range payload.Workers {
					if evidence.Worker.Status != model.WorkerRunning {
						t.Fatalf("archive captured %s after teardown: %s", evidence.Worker.Name, evidence.Worker.Status)
					}
				}
				for _, worker := range mustSourceWorkers(t, db, source) {
					keep := tail && worker.ProfileName == gateStressProfile
					if keep != (worker.Status == model.WorkerRunning) {
						t.Fatalf("tail=%v %s status = %s", tail, worker.Name, worker.Status)
					}
					if keep == fly.machineDestroyed(worker.FlyMachineID) {
						t.Fatalf("tail=%v %s machine destroyed = %v", tail, worker.Name, !keep)
					}
				}
			}
			wantStatus := model.RCGateCompleted
			if tail {
				wantStatus = model.RCGateTail
			}
			assertGateStatus(t, db, gate.ID, wantStatus)
			if !tail {
				return
			}

			if err := manager.evaluateRCGates(ctx, gate.TailExpiresAt.Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
			assertGateStatus(t, db, gate.ID, model.RCGateTail)
			if err := manager.evaluateRCGates(ctx, gate.TailExpiresAt.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			for _, source := range gate.Sources() {
				archives, err := db.ListRunArchives(source, runArchiveTypeRCGateTail, 10, true)
				if err != nil || len(archives) != 1 {
					t.Fatalf("%s tail archives = %d, %v; want 1", source, len(archives), err)
				}
				for _, worker := range mustSourceWorkers(t, db, source) {
					if worker.Status != model.WorkerStopped {
						t.Fatalf("%s status after tail = %s", worker.Name, worker.Status)
					}
				}
			}
			assertGateStatus(t, db, gate.ID, model.RCGateCompleted)
		})
	}
}

func TestRCGateLedgerEndpoint(t *testing.T) {
	db := openTestDB(t)
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	api := NewAPI(db, nil, nil, nil, manager, nil)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	body, _ := json.Marshal(RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateRef: "v0.5.0-rc1",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "72h",
		Tail:         true,
	})
	for _, invalid := range []RCGateRequest{
		{Repository: "someone/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA, Duration: "72h"},
		{Repository: "benbjohnson/litestream", CandidateSHA: "aaaaaaa", BaselineSHA: testGateBaselineSHA, Duration: "72h"},
		{Repository: "benbjohnson/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA, Duration: "200h"},
		{Repository: "benbjohnson/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateCandidateSHA, Duration: "72h"},
	} {
		raw, _ := json.Marshal(invalid)
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/admin/rc-gates", bytes.NewReader(raw)))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("invalid %+v status = %d, want 400", invalid, recorder.Code)
		}
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/admin/rc-gates", bytes.NewReader(body)))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("register status = %d body %s", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/admin/rc-gates", bytes.NewReader(body)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("repeated identical register status = %d, want 200", recorder.Code)
	}
	conflicting, _ := json.Marshal(RCGateRequest{Repository: "benbjohnson/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA, Duration: "24h"})
	recorder = httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/admin/rc-gates", bytes.NewReader(conflicting)))
	if recorder.Code != http.StatusConflict {
		t.Fatalf("conflicting register status = %d, want 409", recorder.Code)
	}

	gates, err := db.ListRCGates()
	if err != nil || len(gates) != 1 {
		t.Fatalf("gates = %d, %v", len(gates), err)
	}
	createTestGateFleet(t, db, gates[0])

	recorder = httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/rc-gates", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("ledger status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var ledger RCGateLedgerResponse
	if err := json.NewDecoder(recorder.Body).Decode(&ledger); err != nil {
		t.Fatal(err)
	}
	if len(ledger.Gates) != 1 {
		t.Fatalf("ledger gates = %d", len(ledger.Gates))
	}
	entry := ledger.Gates[0]
	if entry.CandidateSource != testGateCandidate || entry.BaselineSource != testGateBaseline || entry.Duration != "72h0m0s" || !entry.Tail {
		t.Fatalf("entry = %+v", entry)
	}
	if !entry.ExpiresAt.Equal(entry.StartedAt.Add(72*time.Hour)) || entry.TailExpiresAt == nil || !entry.TailExpiresAt.Equal(entry.StartedAt.Add(rcGateTailDuration)) {
		t.Fatalf("entry times = %+v", entry)
	}
	if len(entry.Resources) != 8 || entry.TotalVolumeGB != 170 || entry.TotalMachines != 8 {
		t.Fatalf("resources = %d volume %d machines %d", len(entry.Resources), entry.TotalVolumeGB, entry.TotalMachines)
	}
	for _, resource := range entry.Resources {
		wantTail := resource.Profile == gateStressProfile
		if resource.Tail != wantTail || (wantTail && !resource.ExpiresAt.Equal(*entry.TailExpiresAt)) || (!wantTail && !resource.ExpiresAt.Equal(entry.ExpiresAt)) {
			t.Fatalf("resource = %+v", resource)
		}
	}
	if entry.Verdict != rcGateVerdictIncomplete {
		t.Fatalf("verdict = %q, want incomplete without coverage", entry.Verdict)
	}
	scenarios := map[string]RCGateScenarioResult{}
	for _, scenario := range entry.Scenarios {
		scenarios[scenario.Scenario] = scenario
	}
	if len(scenarios) != 9 {
		t.Fatalf("scenarios = %+v", entry.Scenarios)
	}
}

func TestRCGateScenarioVerdicts(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-2*time.Hour))
	createTestGateFleet(t, db, gate)
	at := gate.StartedAt.Add(time.Hour)
	recordGateVerification(t, db, testGateCandidate, gateStressProfile, "overload-truncate0", true, at)
	recordGateVerification(t, db, testGateBaseline, gateStressProfile, "overload-truncate0", true, at)
	recordGateVerification(t, db, testGateCandidate, gateStressProfile, "pinned-reader", true, at)
	recordGateVerification(t, db, testGateBaseline, gateStressProfile, "", true, at)
	recordGateVerification(t, db, testGateCandidate, gateLargeDBProfile, "", false, at)
	recordGateVerification(t, db, testGateBaseline, gateLargeDBProfile, "", true, at)
	recordGateVerification(t, db, testGateBaseline, gateLargeDBProfile, "", false, gate.StartedAt.Add(-time.Minute))
	recordGateVerification(t, db, testGateCandidate, gateRemoteProfile, "", true, at)
	unattributed := model.Verification{WorkerID: "worker-" + testGateBaseline + "-" + gateRemoteProfile, StartedAt: at, CompletedAt: &at, Status: "passed", Passed: true, CheckType: "integrity"}
	mustRecordVerification(t, db, &unattributed)

	stored, err := db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := buildRCGateLedgerEntry(db, *stored, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	verdicts := map[string]string{}
	for _, scenario := range entry.Scenarios {
		verdicts[scenario.Scenario] = scenario.Verdict
	}
	want := map[string]string{
		"overload-truncate0": rcGateVerdictPass,
		"pinned-reader":      rcGateVerdictIncomplete,
		"manydb-500":         rcGateVerdictIncomplete,
		"large-db":           rcGateVerdictRegression,
		"remote":             rcGateVerdictIncomplete,
	}
	for scenario, verdict := range want {
		if verdicts[scenario] != verdict {
			t.Fatalf("%s verdict = %q, want %q (all %v)", scenario, verdicts[scenario], verdict, verdicts)
		}
	}
	if entry.Verdict != rcGateVerdictFail {
		t.Fatalf("gate verdict = %q, want fail", entry.Verdict)
	}
}

func recordGateVerification(t *testing.T, db *model.DB, source, profile, scenario string, passed bool, at time.Time) {
	t.Helper()
	recordGateCheck(t, db, source, profile, scenario, "integrity", "", passed, at)
}

func recordGateCoverageCheck(t *testing.T, db *model.DB, source, profile, scenario string, passed bool, at time.Time) {
	t.Helper()
	recordGateCheck(t, db, source, profile, scenario, gateCheckCoverage, gateModeCoverage, passed, at)
}

func recordGateCheck(t *testing.T, db *model.DB, source, profile, scenario, checkType, mode string, passed bool, at time.Time) {
	t.Helper()
	worker, err := db.GetWorker("worker-" + source + "-" + profile)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetDeploymentByVersion(source, worker.GitSHA, worker.LitestreamSHA)
	if err != nil || deployment == nil {
		t.Fatalf("deployment for %s: %v", source, err)
	}
	run := fixtureRun(*worker, *deployment)
	run.Scenario = scenario
	status := "passed"
	if !passed {
		status = "failed"
	}
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: worker.ID, StartedAt: at, CompletedAt: &at, Status: status, Passed: passed, CheckType: checkType, RestoreMode: mode, Run: run, Attributed: true,
	})
}

func registerTestGate(t *testing.T, db *model.DB, tail bool, started time.Time) model.RCGate {
	t.Helper()
	return registerTestGateFor(t, db, "72h", tail, started)
}

func registerTestGateFor(t *testing.T, db *model.DB, duration string, tail bool, started time.Time) model.RCGate {
	t.Helper()
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	gate, _, err := manager.RegisterRCGate(RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateRef: "v0.5.0-rc1",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     duration,
		Tail:         tail,
	}, started)
	if err != nil {
		t.Fatalf("RegisterRCGate() error = %v", err)
	}
	return gate
}

func createTestGateFleet(t *testing.T, db *model.DB, gate model.RCGate) {
	t.Helper()
	createTestGateFleetWith(t, db, gate, nil)
}

func createTestGateFleetWith(t *testing.T, db *model.DB, gate model.RCGate, mutate func(*workload.Config)) {
	t.Helper()
	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		if err := db.UpsertReadyDeployment(&model.Deployment{
			GitSHA:        testGateSoakSHA,
			LitestreamSHA: litestreamSHA,
			ImageRef:      testGateImage(source, litestreamSHA),
			Source:        source,
			Status:        "ready",
		}); err != nil {
			t.Fatalf("UpsertReadyDeployment() error = %v", err)
		}
		if _, _, err := db.PinRCGateBuild(gate.ID, source, testGateImage(source, litestreamSHA), testGateSoakSHA); err != nil {
			t.Fatalf("PinRCGateBuild() error = %v", err)
		}
		for _, desired := range gateFleetForSource(gate, source, testGateSoakSHA, litestreamSHA).Workers {
			cfg := normalizeWorkloadConfig(desired.Workload)
			cfg.VolumeSizeGB = effectiveDesiredVolumeSize(desired, cfg)
			if mutate != nil {
				mutate(&cfg)
			}
			createTestWorker(t, db, model.Worker{
				ID:            desired.WorkerID,
				AppName:       "litestream-soak",
				Name:          desired.Name,
				Status:        model.WorkerRunning,
				Source:        source,
				GitSHA:        testGateSoakSHA,
				LitestreamSHA: litestreamSHA,
				ProfileName:   desired.ProfileName,
				ProfileConfig: cfg.JSON(),
				Region:        desired.Region,
				FlyMachineID:  desired.WorkerID + "-machine",
				FlyVolumeID:   desired.WorkerID + "-volume",
			})
		}
	}
}

func mustSourceWorkers(t *testing.T, db *model.DB, source string) []model.Worker {
	t.Helper()
	workers, err := db.ListWorkersFiltered("", source)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 4 {
		t.Fatalf("%s workers = %d, want 4", source, len(workers))
	}
	return workers
}

func assertGateStatus(t *testing.T, db *model.DB, id int64, want model.RCGateStatus) {
	t.Helper()
	gates, err := db.ListRCGates()
	if err != nil {
		t.Fatal(err)
	}
	for _, gate := range gates {
		if gate.ID == id {
			if gate.Status != want {
				t.Fatalf("gate status = %s, want %s", gate.Status, want)
			}
			if gate.Closed() != (gate.CompletedAt != nil) {
				t.Fatalf("gate completed_at = %v for status %s", gate.CompletedAt, gate.Status)
			}
			return
		}
	}
	t.Fatalf("gate %d missing", id)
}
