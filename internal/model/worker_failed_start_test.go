package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestUpsertFailedStartingWorkerLeavesTheStaleClockRunning(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{ID: "w-crashloop", Name: "w-crashloop", Status: WorkerStarting, Source: "rc-aaaaaaaa", GitSHA: "soak", LitestreamSHA: "ls", ProfileName: "gate-large-db", ProfileConfig: "{}"}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	identity := reporting.WorkerIdentity{WorkerID: worker.ID, Name: worker.Name, Source: worker.Source, GitSHA: worker.GitSHA, LitestreamSHA: worker.LitestreamSHA, ProfileName: worker.ProfileName, ProfileConfig: "{}"}
	load := func(t *testing.T) Worker {
		t.Helper()
		current, err := db.GetWorker(worker.ID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		return *current
	}

	if err := db.UpsertFailedStartingWorker(identity); err != nil {
		t.Fatalf("UpsertFailedStartingWorker() error = %v", err)
	}
	first := load(t)
	if first.Status != WorkerStarting || first.LastHeartbeatAt == nil {
		t.Fatalf("after first failure = status %s heartbeat %v, want starting with the stale clock started", first.Status, first.LastHeartbeatAt)
	}

	if _, err := db.writer.Exec("UPDATE workers SET last_heartbeat_at = datetime('now','-1 hour') WHERE id = ?", worker.ID); err != nil {
		t.Fatalf("backdate heartbeat: %v", err)
	}
	backdated := load(t)
	if err := db.UpsertFailedStartingWorker(identity); err != nil {
		t.Fatalf("UpsertFailedStartingWorker() error = %v", err)
	}
	repeated := load(t)
	if repeated.Status != WorkerStarting || repeated.LastHeartbeatAt == nil || !repeated.LastHeartbeatAt.Equal(*backdated.LastHeartbeatAt) {
		t.Fatalf("after repeated failure = status %s heartbeat %v, want starting with heartbeat still %v", repeated.Status, repeated.LastHeartbeatAt, backdated.LastHeartbeatAt)
	}
	stale, err := db.StaleWorkers(30 * time.Minute)
	if err != nil {
		t.Fatalf("StaleWorkers() error = %v", err)
	}
	if len(stale) != 1 || stale[0].ID != worker.ID {
		t.Fatalf("StaleWorkers() = %+v, want the crash-looping worker", stale)
	}

	if err := db.UpsertStartingWorker(identity); err != nil {
		t.Fatalf("UpsertStartingWorker() error = %v", err)
	}
	refreshed := load(t)
	if refreshed.Status != WorkerStarting || refreshed.LastHeartbeatAt == nil || !refreshed.LastHeartbeatAt.After(*backdated.LastHeartbeatAt) {
		t.Fatalf("after a startup heartbeat = status %s heartbeat %v, want starting with a fresh heartbeat", refreshed.Status, refreshed.LastHeartbeatAt)
	}
	if stale, err = db.StaleWorkers(30 * time.Minute); err != nil || len(stale) != 0 {
		t.Fatalf("StaleWorkers() after a real heartbeat = %+v, %v, want none", stale, err)
	}
}
