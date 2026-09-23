package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestRetentionDisabledStillCompactsRuntimeEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := model.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	connection, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: 1, WorkerID: "worker", RunID: "run"}
	identityJSON, _ := json.Marshal(identity)
	body, _ := json.Marshal(reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "observed", ProfileIncidents: []reporting.ProfileIncident{{ID: "incident", Run: identity, Kind: "profile_capture_error"}}}})
	if _, err := connection.Exec(`INSERT INTO evidence_runtime(source,deployment_id,identity_json,runtime_json,attributed,received_at,kind) VALUES (?,?,?,?,?,?,?)`, "main", 1, string(identityJSON), string(body), true, time.Now().UTC(), "heartbeat"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	mgr := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	go func() {
		mgr.RunDBRetentionLoop(ctx, 0)
		close(done)
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var stored string
		if err := connection.QueryRow(`SELECT runtime_json FROM evidence_runtime LIMIT 1`).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if len(stored) < len(body) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("runtime evidence was not compacted with retention disabled")
		case <-ticker.C:
		}
	}
	cancel()
	<-done
}

func TestRetentionDisabledContinuesIncrementalVacuum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := model.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	connection, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if _, err := connection.Exec(`CREATE TABLE vacuum_fixture (payload BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	tx, err := connection.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 400; i++ {
		if _, err := tx.Exec(`INSERT INTO vacuum_fixture(payload) VALUES (randomblob(8192))`); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(`DELETE FROM vacuum_fixture`); err != nil {
		t.Fatal(err)
	}
	initial, err := db.EvidenceSpace()
	if err != nil || initial["freelist_count"] < 256 {
		t.Fatalf("initial space=%v error=%v", initial, err)
	}
	mgr := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	mgr.pruneDBOnce(context.Background(), 0)
	first, err := db.EvidenceSpace()
	if err != nil {
		t.Fatal(err)
	}
	mgr.pruneDBOnce(context.Background(), 0)
	second, err := db.EvidenceSpace()
	if err != nil {
		t.Fatal(err)
	}
	if first["freelist_count"] >= initial["freelist_count"] || second["freelist_count"] >= first["freelist_count"] {
		t.Fatalf("free pages did not continue shrinking: initial=%v first=%v second=%v", initial, first, second)
	}
}

func TestPruneDBOnceDropsOnlyOldHistory(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID: "w-ret", Name: "w-ret", Status: model.WorkerRunning,
		Source: "main", ProfileName: "low-volume", ProfileConfig: "{}",
	})

	oldStart := time.Now().UTC().AddDate(0, 0, -45)
	oldDone := oldStart.Add(time.Minute)
	newStart := time.Now().UTC().Add(-time.Hour)
	newDone := newStart.Add(time.Minute)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: "w-ret", StartedAt: oldStart, CompletedAt: &oldDone,
		Status: "passed", CheckType: "integrity", Passed: true,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: "w-ret", StartedAt: newStart, CompletedAt: &newDone,
		Status: "passed", CheckType: "integrity", Passed: true,
	})
	if err := db.RecordEventAt("w-ret", "test", "old event", "", oldStart); err != nil {
		t.Fatalf("RecordEventAt(old) error = %v", err)
	}
	if err := db.RecordEventAt("w-ret", "test", "new event", "", newStart); err != nil {
		t.Fatalf("RecordEventAt(new) error = %v", err)
	}

	mgr := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	mgr.pruneDBOnce(context.Background(), 30)

	stats, err := db.ListVerificationStatsSince("", time.Now().UTC().AddDate(0, 0, -90))
	if err != nil {
		t.Fatalf("ListVerificationStatsSince() error = %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("verifications after prune = %d, want 1", len(stats))
	}
	events, err := db.ListEvents(10)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].Message != "new event" {
		t.Fatalf("events after prune = %+v, want only the new event", events)
	}
}
