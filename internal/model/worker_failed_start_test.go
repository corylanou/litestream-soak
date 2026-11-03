package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestUpsertFailedWorkerNeverPromotesOrRefreshesTheClock(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	create := func(t *testing.T, id string, status WorkerStatus) reporting.WorkerIdentity {
		t.Helper()
		worker := &Worker{ID: id, Name: id, Status: status, Source: "rc-aaaaaaaa", GitSHA: "soak", LitestreamSHA: "ls", ProfileName: "gate-large-db", ProfileConfig: "{}"}
		if err := db.CreateWorker(worker); err != nil {
			t.Fatalf("CreateWorker(%s) error = %v", id, err)
		}
		return reporting.WorkerIdentity{WorkerID: id, Name: id, Source: worker.Source, GitSHA: worker.GitSHA, LitestreamSHA: worker.LitestreamSHA, ProfileName: worker.ProfileName, ProfileConfig: "{}"}
	}
	load := func(t *testing.T, id string) Worker {
		t.Helper()
		current, err := db.GetWorker(id)
		if err != nil {
			t.Fatalf("GetWorker(%s) error = %v", id, err)
		}
		return *current
	}
	backdate := func(t *testing.T, id string) time.Time {
		t.Helper()
		if _, err := db.writer.Exec("UPDATE workers SET last_heartbeat_at = datetime('now','-1 hour') WHERE id = ?", id); err != nil {
			t.Fatalf("backdate heartbeat: %v", err)
		}
		return *load(t, id).LastHeartbeatAt
	}
	staleIDs := func(t *testing.T) map[string]bool {
		t.Helper()
		stale, err := db.StaleWorkers(30 * time.Minute)
		if err != nil {
			t.Fatalf("StaleWorkers() error = %v", err)
		}
		ids := map[string]bool{}
		for _, worker := range stale {
			ids[worker.ID] = true
		}
		return ids
	}

	pending := create(t, "w-pending", WorkerPending)
	if err := db.UpsertFailedWorker(pending); err != nil {
		t.Fatal(err)
	}
	if got := load(t, pending.WorkerID); got.Status != WorkerStarting || got.LastHeartbeatAt == nil {
		t.Fatalf("pending after failure = %s heartbeat %v, want starting with the stale clock started", got.Status, got.LastHeartbeatAt)
	}

	starting := create(t, "w-starting", WorkerStarting)
	running := create(t, "w-running", WorkerRunning)
	degraded := create(t, "w-degraded", WorkerRunning)
	if err := db.UpdateWorkerStatus(degraded.WorkerID, WorkerDegraded, "restore failed"); err != nil {
		t.Fatal(err)
	}
	for _, identity := range []reporting.WorkerIdentity{starting, running, degraded} {
		if err := db.UpdateWorkerHeartbeat(identity.WorkerID); err != nil {
			t.Fatal(err)
		}
	}
	before := map[string]time.Time{}
	for _, identity := range []reporting.WorkerIdentity{starting, running, degraded} {
		before[identity.WorkerID] = backdate(t, identity.WorkerID)
	}
	for range 3 {
		for _, identity := range []reporting.WorkerIdentity{starting, running, degraded} {
			if err := db.UpsertFailedWorker(identity); err != nil {
				t.Fatalf("UpsertFailedWorker(%s) error = %v", identity.WorkerID, err)
			}
		}
	}
	for id, want := range map[string]struct {
		status WorkerStatus
		errMsg string
	}{
		starting.WorkerID: {status: WorkerStarting},
		running.WorkerID:  {status: WorkerRunning},
		degraded.WorkerID: {status: WorkerDegraded, errMsg: "restore failed"},
	} {
		got := load(t, id)
		if got.Status != want.status || got.ErrorMessage != want.errMsg || got.LastHeartbeatAt == nil || !got.LastHeartbeatAt.Equal(before[id]) {
			t.Fatalf("%s after repeated failures = %s %q heartbeat %v, want %s %q with heartbeat %v", id, got.Status, got.ErrorMessage, got.LastHeartbeatAt, want.status, want.errMsg, before[id])
		}
	}
	if stale := staleIDs(t); !stale[starting.WorkerID] || !stale[running.WorkerID] || stale[degraded.WorkerID] {
		t.Fatalf("StaleWorkers() = %v, want the starting and running crash-loopers (degraded is already past the deadline)", stale)
	}

	for _, identity := range []reporting.WorkerIdentity{starting, running} {
		if err := db.UpsertReportedWorker(identity); err != nil {
			t.Fatal(err)
		}
		if got := load(t, identity.WorkerID); got.Status != WorkerRunning || !got.LastHeartbeatAt.After(before[identity.WorkerID]) {
			t.Fatalf("%s after a real heartbeat = %s heartbeat %v, want running with a fresh heartbeat", identity.WorkerID, got.Status, got.LastHeartbeatAt)
		}
	}
	if stale := staleIDs(t); len(stale) != 0 {
		t.Fatalf("StaleWorkers() after real heartbeats = %v, want none", stale)
	}
}
