package model

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestUpsertReadyDeploymentMarksRolloutPendingUntilCleared(t *testing.T) {
	db := seriesTestDB(t)
	if err := db.UpsertReadyDeploymentContext(context.Background(), &Deployment{Source: "main", GitSHA: "aaaaaaa", LitestreamSHA: "bbbbbbb", ImageRef: "registry.fly.io/app:new", Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	pending, err := db.ListRolloutPendingDeployments()
	if err != nil || len(pending) != 1 || pending[0].GitSHA != "aaaaaaa" {
		t.Fatalf("pending rollouts = %+v, %v; want the recorded deployment", pending, err)
	}
	if flag, err := db.RolloutPending(pending[0].ID); err != nil || !flag {
		t.Fatalf("rollout_pending = %v, %v; want set on record", flag, err)
	}
	if pending[0].RolloutGeneration != 1 {
		t.Fatalf("generation = %d, want 1 for the first record", pending[0].RolloutGeneration)
	}
	if err := db.ClearRolloutPending(pending[0].ID, pending[0].RolloutGeneration); err != nil {
		t.Fatal(err)
	}
	if flag, err := db.RolloutPending(pending[0].ID); err != nil || flag {
		t.Fatalf("rollout_pending = %v, %v; want cleared", flag, err)
	}
	if pending, err := db.ListRolloutPendingDeployments(); err != nil || len(pending) != 0 {
		t.Fatalf("pending rollouts after clear = %+v, %v", pending, err)
	}
	if err := db.UpsertReadyDeploymentContext(context.Background(), &Deployment{Source: "main", GitSHA: "aaaaaaa", LitestreamSHA: "bbbbbbb", ImageRef: "registry.fly.io/app:new", Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	if pending, err := db.ListRolloutPendingDeployments(); err != nil || len(pending) != 1 || pending[0].RolloutGeneration != 2 {
		t.Fatalf("pending rollouts after a repeated record = %+v, %v; want the rollout pending again at generation 2", pending, err)
	}
}

func TestClearRolloutPendingIsGuardedByTheRecordedGeneration(t *testing.T) {
	db := seriesTestDB(t)
	imageA := Deployment{Source: "main", GitSHA: "aaaaaaa", LitestreamSHA: "bbbbbbb", WorkloadSHA: "cccccccccccccccccccccccccccccccccccccccc", ImageRef: "registry.fly.io/app:a", Status: "ready"}
	if err := db.UpsertReadyDeploymentContext(context.Background(), &imageA); err != nil {
		t.Fatal(err)
	}
	imageB := imageA
	imageB.ID, imageB.RolloutGeneration = 0, 0
	imageB.ImageRef = "registry.fly.io/app:b"
	if err := db.UpsertReadyDeploymentContext(context.Background(), &imageB); err != nil {
		t.Fatal(err)
	}
	if imageB.ID != imageA.ID || imageB.RolloutGeneration != imageA.RolloutGeneration+1 {
		t.Fatalf("image b recorded as row %d generation %d, want row %d at generation %d", imageB.ID, imageB.RolloutGeneration, imageA.ID, imageA.RolloutGeneration+1)
	}
	if err := db.ClearRolloutPending(imageA.ID, imageA.RolloutGeneration); err != nil {
		t.Fatal(err)
	}
	if pending, err := db.RolloutPending(imageA.ID); err != nil || !pending {
		t.Fatalf("rollout_pending after clearing the older generation = %v, %v; want image b's record kept", pending, err)
	}
	if err := db.ClearRolloutPending(imageB.ID, imageB.RolloutGeneration); err != nil {
		t.Fatal(err)
	}
	if pending, err := db.RolloutPending(imageB.ID); err != nil || pending {
		t.Fatalf("rollout_pending after clearing the current generation = %v, %v; want cleared", pending, err)
	}
	latest, err := db.GetLatestReadyDeployment("main")
	if err != nil || latest == nil || latest.ImageRef != "registry.fly.io/app:b" {
		t.Fatalf("latest ready = %+v, %v; want image b on the reused row", latest, err)
	}
}

func TestUpsertReadyDeploymentReturnsTheExactRowForEachWorkloadSHA(t *testing.T) {
	db := seriesTestDB(t)
	first := Deployment{Source: "main", GitSHA: "aaaaaaa", LitestreamSHA: "bbbbbbb", WorkloadSHA: "cccccccccccccccccccccccccccccccccccccccc", ImageRef: "registry.fly.io/app:new", Status: "ready"}
	second := first
	second.WorkloadSHA = "dddddddddddddddddddddddddddddddddddddddd"
	for _, dep := range []*Deployment{&first, &second} {
		if err := db.UpsertReadyDeploymentContext(context.Background(), dep); err != nil {
			t.Fatal(err)
		}
	}
	if first.ID == 0 || second.ID == 0 || first.ID == second.ID {
		t.Fatalf("row ids = %d and %d, want two distinct rows for the two workload shas", first.ID, second.ID)
	}
	again := first
	again.ID = 0
	if err := db.UpsertReadyDeploymentContext(context.Background(), &again); err != nil {
		t.Fatal(err)
	}
	if again.ID != first.ID {
		t.Fatalf("repeated record returned row %d, want the existing row %d", again.ID, first.ID)
	}
	if err := db.ClearRolloutPending(first.ID, first.RolloutGeneration); err != nil {
		t.Fatal(err)
	}
	if pending, err := db.RolloutPending(second.ID); err != nil || !pending {
		t.Fatalf("second row's rollout_pending = %v, %v; want untouched by clearing the first", pending, err)
	}
}

func TestRecordEventContextHonorsTheContextWhileASweepHoldsTheLock(t *testing.T) {
	db := seriesTestDB(t)
	db.blobs.sweep.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := db.RecordEventContext(ctx, "", "deploy_ready_received", "held", "")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 5*time.Second {
		t.Fatalf("RecordEventContext under a sweep = %v after %s, want the deadline honored", err, time.Since(started))
	}
	db.blobs.sweep.Unlock()
	if err := db.RecordEventContext(context.Background(), "", "deploy_ready_received", "free", ""); err != nil {
		t.Fatalf("RecordEventContext after the sweep = %v", err)
	}
	events, err := db.ListEvents(10)
	if err != nil || len(events) != 1 || events[0].Message != "free" {
		t.Fatalf("events = %+v, %v; want only the event recorded after the sweep", events, err)
	}
}

func archivedEvidenceFixture(t *testing.T, db *DB, source string) int {
	t.Helper()
	var archived int
	for _, sha := range []string{"archived", "baseline", "current"} {
		if err := db.UpsertReadyDeployment(&Deployment{Source: source, GitSHA: sha, LitestreamSHA: sha, Status: "ready"}); err != nil {
			t.Fatal(err)
		}
		deployment, err := db.GetLatestDeployment(source)
		if err != nil {
			t.Fatal(err)
		}
		if sha == "archived" {
			archived = deployment.ID
		}
	}
	if _, err := db.RecordRunArchive(&RunArchive{DeploymentID: archived, Source: source, ArchiveType: "success"}); err != nil {
		t.Fatal(err)
	}
	return archived
}

func TestPruneArchivedEvidenceRechecksEligibilityBetweenBatches(t *testing.T) {
	db := seriesTestDB(t)
	archived := archivedEvidenceFixture(t, db, "main")
	old := time.Now().UTC().AddDate(0, 0, -45)
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: archived, WorkerID: "worker", RunID: "run"}
	identityJSON, _ := json.Marshal(identity)
	for i := 0; i < 3*evidencePruneBatchSize; i++ {
		if _, err := db.writer.Exec(`INSERT INTO evidence_verifications(worker_id,started_at,completed_at,run_identity_json,evidence_source) VALUES (?,?,?,?,?)`, "worker", old, old, string(identityJSON), "main"); err != nil {
			t.Fatal(err)
		}
	}
	batches := 0
	pruneBatchHook = func() {
		batches++
		if batches != 2 {
			return
		}
		body, _ := json.Marshal(reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "observed"}})
		if err := db.RecordRuntimeEvidence(identity, body, true); err != nil {
			t.Error(err)
		}
	}
	defer func() { pruneBatchHook = nil }()
	counts, err := db.PruneArchivedEvidenceBefore(context.Background(), time.Now().UTC().AddDate(0, 0, -30))
	if err != nil || counts["verifications"] != evidencePruneBatchSize {
		t.Fatalf("prune counts=%v error=%v, want one batch deleted before the heartbeat made the deployment ineligible", counts, err)
	}
	var remaining int
	if err := db.queryRow(`SELECT count(*) FROM evidence_verifications`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 2*evidencePruneBatchSize {
		t.Fatalf("remaining verifications=%d, want the batches after the heartbeat kept", remaining)
	}
}

func TestPruneArchivedEvidenceDeletesLegacyRowsWithoutASource(t *testing.T) {
	db := seriesTestDB(t)
	archived := archivedEvidenceFixture(t, db, "main")
	old := time.Now().UTC().AddDate(0, 0, -45)
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: archived, WorkerID: "worker", RunID: "run"}
	identityJSON, _ := json.Marshal(identity)
	if _, err := db.writer.Exec(`INSERT INTO evidence_verifications(worker_id,started_at,completed_at,run_identity_json,evidence_source) VALUES (?,?,?,?,?)`, "worker", old, old, string(identityJSON), ""); err != nil {
		t.Fatal(err)
	}
	details, _ := json.Marshal(map[string]any{"deployment_id": archived})
	if _, err := db.writer.Exec(`INSERT INTO evidence_events(worker_id,event_type,message,details,created_at,evidence_source) VALUES (?,?,?,?,?,?)`, "worker", "test", "legacy", string(details), old, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.writer.Exec(`INSERT INTO evidence_runtime(source,deployment_id,identity_json,runtime_json,attributed,received_at,kind) VALUES (?,?,?,?,?,?,?)`, "", archived, string(identityJSON), "{}", true, old, "heartbeat"); err != nil {
		t.Fatal(err)
	}
	counts, err := db.PruneArchivedEvidenceBefore(context.Background(), time.Now().UTC().AddDate(0, 0, -30))
	if err != nil || counts["verifications"] != 1 || counts["events"] != 1 || counts["runtime"] != 1 {
		t.Fatalf("prune counts=%v error=%v, want the legacy rows without a source pruned with their deployment", counts, err)
	}
}
