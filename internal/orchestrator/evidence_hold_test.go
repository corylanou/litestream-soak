package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func holdTestWorker(t *testing.T, db *model.DB, workerID, reason string) model.Worker {
	t.Helper()
	if err := db.SetWorkerEvidenceHold(workerID, reason); err != nil {
		t.Fatalf("SetWorkerEvidenceHold(%s) error = %v", workerID, err)
	}
	worker, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker(%s) error = %v", workerID, err)
	}
	if !worker.EvidenceHeld() || worker.EvidenceHoldReason != reason {
		t.Fatalf("worker %s hold = (%v, %q), want held with %q", workerID, worker.EvidenceHoldAt, worker.EvidenceHoldReason, reason)
	}
	return *worker
}

func assertHeldResourcesRetained(t *testing.T, fly *teardownTestServer, worker model.Worker) {
	t.Helper()
	if fly.machineDestroyed(worker.FlyMachineID) {
		t.Fatalf("held worker %s machine %s was destroyed", worker.Name, worker.FlyMachineID)
	}
	if fly.volumeDestroyed(worker.FlyVolumeID) {
		t.Fatalf("held worker %s volume %s was destroyed", worker.Name, worker.FlyVolumeID)
	}
	heldPrefix := strings.Trim(workerReplicaPath(worker), "/") + "/"
	for _, prefix := range fly.prefixes() {
		if prefix == heldPrefix {
			t.Fatalf("held worker %s replica prefix %s was cleared", worker.Name, heldPrefix)
		}
	}
}

func TestEvidenceHoldSurvivesGateClose(t *testing.T) {
	heldID := "worker-" + testGateCandidate + "-" + gateLargeDBProfile
	cases := []struct {
		name       string
		launch     bool
		evaluateAt func(gate model.RCGate) time.Time
		wantStatus model.RCGateStatus
		wantReason string
	}{
		{
			name:       "ttl expiry",
			launch:     true,
			evaluateAt: func(gate model.RCGate) time.Time { return gate.ExpiresAt.Add(time.Minute) },
			wantStatus: model.RCGateCompleted,
			wantReason: "rc_gate_ttl",
		},
		{
			name:       "unpaired abort",
			evaluateAt: func(gate model.RCGate) time.Time { return gate.ProvisionDeadline.Add(time.Minute) },
			wantStatus: model.RCGateAborted,
			wantReason: "rc_gate_unpaired",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			gate := registerTestGate(t, db, false, time.Now().UTC().Add(-time.Hour))
			createTestGateFleet(t, db, gate)
			held := holdTestWorker(t, db, heldID, "large-db restore evidence under review")
			manager, fly := teardownGateManager(t, db, nil)
			ctx := context.Background()
			if tc.launch {
				if err := manager.evaluateRCGates(ctx, gate.StartedAt.Add(time.Minute)); err != nil {
					t.Fatalf("evaluateRCGates(launch) error = %v", err)
				}
			}

			if err := manager.evaluateRCGates(ctx, tc.evaluateAt(gate)); err != nil {
				t.Fatalf("evaluateRCGates(close) error = %v", err)
			}
			assertGateStatus(t, db, gate.ID, tc.wantStatus)
			assertHeldResourcesRetained(t, fly, held)

			for _, source := range gate.Sources() {
				for _, worker := range mustSourceWorkers(t, db, source) {
					if worker.Status != model.WorkerStopped {
						t.Fatalf("%s status = %s after gate close", worker.Name, worker.Status)
					}
					if worker.ID == heldID {
						if !worker.EvidenceHeld() {
							t.Fatalf("%s lost its evidence hold during gate close", worker.Name)
						}
						continue
					}
					if !fly.machineDestroyed(worker.FlyMachineID) || !fly.volumeDestroyed(worker.FlyVolumeID) {
						t.Fatalf("%s was not destroyed", worker.Name)
					}
				}
				archives, err := db.ListRunArchives(source, runArchiveTypeTeardown, 10, true)
				if err != nil || len(archives) != 1 {
					t.Fatalf("%s archives = %d, %v; want 1", source, len(archives), err)
				}
				var payload runArchivePayload
				if err := json.Unmarshal([]byte(archives[0].Payload), &payload); err != nil {
					t.Fatal(err)
				}
				if payload.Reason != tc.wantReason {
					t.Fatalf("archive reason = %q, want %q", payload.Reason, tc.wantReason)
				}
				if source == testGateCandidate {
					archivedHold := false
					for _, evidence := range payload.Workers {
						if evidence.Worker.ID == heldID && evidence.Worker.EvidenceHeld() {
							archivedHold = true
						}
					}
					if !archivedHold {
						t.Fatalf("archive for %s does not record the evidence hold on %s", source, heldID)
					}
				}
			}
			requireWorkerEvent(t, db, heldID, eventEvidenceHoldRetained)

			stored, err := db.GetRCGate(gate.ID)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := buildRCGateLedgerEntry(db, *stored, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if len(entry.HeldWorkers) != 1 || entry.HeldWorkers[0].WorkerID != heldID || entry.HeldWorkers[0].FlyVolumeID != held.FlyVolumeID {
				t.Fatalf("ledger held workers = %+v, want %s", entry.HeldWorkers, heldID)
			}
			if entry.HeldWorkers[0].ReplicaPath != workerReplicaPath(held) {
				t.Fatalf("ledger replica path = %q, want %q", entry.HeldWorkers[0].ReplicaPath, workerReplicaPath(held))
			}
			var snapshot rcGateResults
			if err := json.Unmarshal([]byte(stored.Results), &snapshot); err != nil {
				t.Fatalf("decode closed gate results: %v", err)
			}
			if len(snapshot.HeldWorkers) != 1 || snapshot.HeldWorkers[0].WorkerID != heldID {
				t.Fatalf("closed gate snapshot held workers = %+v, want %s", snapshot.HeldWorkers, heldID)
			}
		})
	}
}

func TestEvidenceHoldSkipsSourceTeardown(t *testing.T) {
	db := openTestDB(t)
	source := "pr-158"
	createTeardownTestDeployment(t, db, source)
	for _, name := range []string{"worker-pr-158-held", "worker-pr-158-other"} {
		createTestWorker(t, db, model.Worker{
			ID:            name,
			AppName:       "litestream-soak",
			Name:          name,
			Status:        model.WorkerRunning,
			Source:        source,
			GitSHA:        "soak-sha",
			LitestreamSHA: "litestream-sha",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
			FlyMachineID:  "machine-" + name,
			FlyVolumeID:   "volume-" + name,
		})
	}
	held := holdTestWorker(t, db, "worker-pr-158-held", "operator review")

	fly := newTeardownTestServer(t, nil)
	manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{
		Bucket: "bucket", Endpoint: fly.server.URL, AccessKey: "access", SecretKey: "secret", Region: "auto",
	}, "", "")
	api := NewAPI(db, fly.client, nil, nil, manager, nil)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/admin/teardown-source?source="+source, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", recorder.Code, recorder.Body.String())
	}
	var response SourceTeardownResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.Complete || response.TotalWorkers != 2 || response.DestroyedWorkers != 1 || response.HeldWorkers != 1 || response.FailedWorkers != 0 {
		t.Fatalf("teardown response = %+v", response)
	}
	outcomes := teardownOutcomesByWorker(response.Workers)
	if outcomes["worker-pr-158-held"].Outcome != sourceTeardownOutcomeHeld {
		t.Fatalf("held outcome = %q, want %q", outcomes["worker-pr-158-held"].Outcome, sourceTeardownOutcomeHeld)
	}
	if outcomes["worker-pr-158-other"].Outcome != sourceTeardownOutcomeDestroyed {
		t.Fatalf("other outcome = %q, want %q", outcomes["worker-pr-158-other"].Outcome, sourceTeardownOutcomeDestroyed)
	}
	assertHeldResourcesRetained(t, fly, held)
	if !fly.machineDestroyed("machine-worker-pr-158-other") || !fly.volumeDestroyed("volume-worker-pr-158-other") {
		t.Fatal("unheld worker was not destroyed")
	}
	requireWorkerEvent(t, db, held.ID, eventEvidenceHoldRetained)
}

func TestEvidenceHoldRetainsAcrossDestroyPaths(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, manager *Manager, worker model.Worker)
	}{
		{
			name: "expiry loop",
			run: func(t *testing.T, manager *Manager, worker model.Worker) {
				manager.cleanExpiredWorkers(context.Background())
			},
		},
		{
			name: "direct destroy",
			run: func(t *testing.T, manager *Manager, worker model.Worker) {
				if err := manager.DestroyWorker(context.Background(), worker.ID); !errors.Is(err, errWorkerEvidenceHeld) {
					t.Fatalf("DestroyWorker() err = %v, want errWorkerEvidenceHeld", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			expired := time.Now().UTC().Add(-time.Hour)
			createTestWorker(t, db, model.Worker{
				ID:            "worker-pr-9-held",
				AppName:       "litestream-soak",
				Name:          "worker-pr-9-held",
				Status:        model.WorkerDegraded,
				Source:        "pr-9",
				GitSHA:        "soak-sha",
				LitestreamSHA: "litestream-sha",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
				FlyMachineID:  "machine-held",
				FlyVolumeID:   "volume-held",
				ExpiresAt:     &expired,
			})
			held := holdTestWorker(t, db, "worker-pr-9-held", "keep for incident")
			manager, fly := teardownGateManager(t, db, nil)

			tc.run(t, manager, held)

			assertHeldResourcesRetained(t, fly, held)
			after, err := db.GetWorker(held.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Status != model.WorkerStopped || !after.EvidenceHeld() {
				t.Fatalf("held worker after destroy attempt = (%s, held=%v), want stopped and held", after.Status, after.EvidenceHeld())
			}
			requireWorkerEvent(t, db, held.ID, eventEvidenceHoldRetained)
		})
	}
}

func TestEvidenceHoldBlocksResumeAndReplace(t *testing.T) {
	image := testGateImage(testGateCandidate, testGateCandidateSHA)
	heldID := "worker-" + testGateCandidate + "-" + gateRemoteProfile
	cases := []struct {
		name     string
		dormant  bool
		run      func(t *testing.T, manager *Manager, db *model.DB)
		wantHeld bool
	}{
		{
			name:    "operator resume",
			dormant: true,
			run: func(t *testing.T, manager *Manager, db *model.DB) {
				err := manager.ResumeDormantWorkers(context.Background(), testGateCandidate, "", "", "", "operator")
				if !errors.Is(err, errWorkerEvidenceHeld) {
					t.Fatalf("ResumeDormantWorkers() err = %v, want errWorkerEvidenceHeld", err)
				}
			},
		},
		{
			name:    "reconciler resume",
			dormant: true,
			run: func(t *testing.T, manager *Manager, db *model.DB) {
				deployment, err := db.GetLatestReadyDeployment(testGateCandidate)
				if err != nil || deployment == nil {
					t.Fatalf("GetLatestReadyDeployment() = %v, %v", deployment, err)
				}
				if err := manager.resumeDeploymentDormantWorkers(context.Background(), *deployment, "fleet_reconcile", false); err != nil {
					t.Fatalf("resumeDeploymentDormantWorkers() error = %v", err)
				}
			},
		},
		{
			name: "operator roll",
			run: func(t *testing.T, manager *Manager, db *model.DB) {
				_, err := manager.RollWorker(context.Background(), heldID, image, testGateSoakSHA, testGateCandidateSHA)
				if !errors.Is(err, errWorkerEvidenceHeld) {
					t.Fatalf("RollWorker() err = %v, want errWorkerEvidenceHeld", err)
				}
			},
		},
		{
			name: "fleet ensure",
			run: func(t *testing.T, manager *Manager, db *model.DB) {
				if err := db.UpdateWorkerStatus(heldID, model.WorkerStopped, ""); err != nil {
					t.Fatal(err)
				}
				if err := manager.EnsureSourceFleet(context.Background(), testGateCandidate, testGateSoakSHA, testGateCandidateSHA, image); err != nil {
					t.Fatalf("EnsureSourceFleet() error = %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			gate := registerTestGate(t, db, false, time.Now().UTC().Add(-time.Hour))
			createTestGateFleet(t, db, gate)
			if err := db.MarkRCGatePairedReady(gate.ID, gate.StartedAt.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if tc.dormant {
				if err := db.MarkWorkerDormant(heldID, "broken", "integrity_check_mismatch", "known_bad_source"); err != nil {
					t.Fatal(err)
				}
			}
			holdTestWorker(t, db, heldID, "retain remote evidence")
			fly := newCreateWorkerFlyServer(t)
			manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")

			tc.run(t, manager, db)

			if machines := fly.machineRequests(); len(machines) != 0 {
				t.Fatalf("held worker provisioned %d machines", len(machines))
			}
			if volumes := fly.volumeRequests(); len(volumes) != 0 {
				t.Fatalf("held worker provisioned %d volumes", len(volumes))
			}
			stopped, destroyedMachines, destroyedVolumes := fly.replacementCounts()
			if stopped != 0 || destroyedMachines != 0 || destroyedVolumes != 0 {
				t.Fatalf("held worker replacement counts = stop:%d machines:%d volumes:%d", stopped, destroyedMachines, destroyedVolumes)
			}
			after, err := db.GetWorker(heldID)
			if err != nil {
				t.Fatal(err)
			}
			if !after.EvidenceHeld() || after.FlyMachineID != heldID+"-machine" || after.FlyVolumeID != heldID+"-volume" {
				t.Fatalf("held worker changed: %+v", after)
			}
		})
	}
}

func TestEvidenceHoldAdminEndpoints(t *testing.T) {
	heldID := "worker-" + testGateCandidate + "-" + gateLargeDBProfile
	db := openTestDB(t)
	gate := registerTestGate(t, db, false, time.Now().UTC().Add(-time.Hour))
	createTestGateFleet(t, db, gate)
	manager, fly := teardownGateManager(t, db, nil)
	api := NewAPI(db, fly.client, nil, nil, manager, nil)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	post := func(t *testing.T, path, contentType, body string) (int, map[string]any) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		var decoded map[string]any
		if recorder.Body.Len() > 0 {
			_ = json.Unmarshal(recorder.Body.Bytes(), &decoded)
		}
		return recorder.Code, decoded
	}
	heldWorkerIDs := func(t *testing.T, path string) []string {
		t.Helper()
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, recorder.Code)
		}
		var ids []string
		if strings.HasPrefix(path, "/api/rc-gates") {
			var ledger RCGateLedgerResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &ledger); err != nil {
				t.Fatal(err)
			}
			for _, entry := range ledger.Gates {
				for _, held := range entry.HeldWorkers {
					ids = append(ids, held.WorkerID)
				}
			}
			return ids
		}
		var workers []model.Worker
		if err := json.Unmarshal(recorder.Body.Bytes(), &workers); err != nil {
			t.Fatal(err)
		}
		for _, worker := range workers {
			if worker.EvidenceHeld() {
				ids = append(ids, worker.ID)
			}
		}
		return ids
	}

	steps := []struct {
		name        string
		path        string
		contentType string
		body        string
		wantStatus  int
		check       func(t *testing.T, body map[string]any)
	}{
		{
			name:       "missing reason",
			path:       "/api/admin/workers/" + heldID + "/evidence-hold",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown worker",
			path:       "/api/admin/workers/missing/evidence-hold?reason=x",
			wantStatus: http.StatusNotFound,
		},
		{
			name:        "apply hold",
			path:        "/api/admin/workers/" + heldID + "/evidence-hold",
			contentType: "application/json",
			body:        `{"reason":"large-db restore evidence under review"}`,
			wantStatus:  http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				if body["held"] != true || body["evidence_hold_reason"] != "large-db restore evidence under review" || body["fly_volume_id"] != heldID+"-volume" {
					t.Fatalf("apply response = %v", body)
				}
				if body["replica_prefix"] != "s3://bucket/soak/"+heldID+"/"+heldID+"-volume/" {
					t.Fatalf("replica_prefix = %v", body["replica_prefix"])
				}
				for _, path := range []string{"/api/workers?source=" + testGateCandidate, "/api/rc-gates"} {
					if ids := heldWorkerIDs(t, path); len(ids) != 1 || ids[0] != heldID {
						t.Fatalf("%s held workers = %v, want [%s]", path, ids, heldID)
					}
				}
				requireWorkerEvent(t, db, heldID, eventEvidenceHoldApplied)
			},
		},
		{
			name:       "repeat hold is idempotent",
			path:       "/api/admin/workers/" + heldID + "/evidence-hold?reason=second",
			wantStatus: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				if body["already_held"] != true || body["evidence_hold_reason"] != "large-db restore evidence under review" {
					t.Fatalf("repeat response = %v", body)
				}
			},
		},
		{
			name:       "release",
			path:       "/api/admin/workers/" + heldID + "/evidence-hold/release",
			wantStatus: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				if body["released"] != true || body["held"] != false || body["destroyed"] != nil {
					t.Fatalf("release response = %v", body)
				}
				for _, path := range []string{"/api/workers?source=" + testGateCandidate, "/api/rc-gates"} {
					if ids := heldWorkerIDs(t, path); len(ids) != 0 {
						t.Fatalf("%s held workers after release = %v", path, ids)
					}
				}
				requireWorkerEvent(t, db, heldID, eventEvidenceHoldReleased)
				if fly.machineDestroyed(heldID+"-machine") || fly.volumeDestroyed(heldID+"-volume") {
					t.Fatal("release without destroy removed resources")
				}
			},
		},
		{
			name:       "hold again then release with destroy",
			path:       "/api/admin/workers/" + heldID + "/evidence-hold?reason=again",
			wantStatus: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				if body["held"] != true {
					t.Fatalf("hold response = %v", body)
				}
				status, release := post(t, "/api/admin/workers/"+heldID+"/evidence-hold/release?destroy=true", "", "")
				if status != http.StatusOK || release["released"] != true || release["destroyed"] != true {
					t.Fatalf("release destroy = %d %v", status, release)
				}
				if !fly.machineDestroyed(heldID+"-machine") || !fly.volumeDestroyed(heldID+"-volume") {
					t.Fatal("release with destroy kept resources")
				}
				after, err := db.GetWorker(heldID)
				if err != nil {
					t.Fatal(err)
				}
				if after.Status != model.WorkerStopped || after.EvidenceHeld() {
					t.Fatalf("worker after destroy = (%s, held=%v)", after.Status, after.EvidenceHeld())
				}
			},
		},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			status, body := post(t, step.path, step.contentType, step.body)
			if status != step.wantStatus {
				t.Fatalf("status = %d, want %d; body: %v", status, step.wantStatus, body)
			}
			if step.check != nil {
				step.check(t, body)
			}
		})
	}
}
