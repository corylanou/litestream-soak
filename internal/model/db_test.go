package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestOpenAppliesPragmas(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var journalMode string
	if err := db.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}

	var busyTimeout int
	if err := db.db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if busyTimeout != 30000 {
		t.Fatalf("busy_timeout = %d, want 30000", busyTimeout)
	}
}

func TestRecordWindowedEventAt(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	worker := &Worker{
		ID:            "worker-1",
		Name:          "worker-1",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "litestream123",
		ProfileName:   "burst-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	storedWorker, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if storedWorker.LitestreamSHA != "litestream123" {
		t.Fatalf("storedWorker.LitestreamSHA = %q, want litestream123", storedWorker.LitestreamSHA)
	}

	start := time.Date(2026, time.April, 14, 18, 0, 0, 0, time.UTC)
	created, err := db.RecordWindowedEventAt(worker.ID, "platform_disk_full", "Fly log reported disk pressure: database or disk is full", `{"line":1}`, start, 10*time.Minute)
	if err != nil {
		t.Fatalf("first RecordWindowedEventAt() error = %v", err)
	}
	if !created {
		t.Fatalf("first RecordWindowedEventAt() created = false, want true")
	}

	secondAt := start.Add(5 * time.Minute)
	created, err = db.RecordWindowedEventAt(worker.ID, "platform_disk_full", "Fly log reported disk pressure: database or disk is full", `{"line":2}`, secondAt, 10*time.Minute)
	if err != nil {
		t.Fatalf("second RecordWindowedEventAt() error = %v", err)
	}
	if created {
		t.Fatalf("second RecordWindowedEventAt() created = true, want false")
	}

	events, err := db.ListWorkerEvents(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Details != `{"line":2}` {
		t.Fatalf("events[0].Details = %q, want latest details", events[0].Details)
	}
	if !events[0].CreatedAt.Equal(secondAt) {
		t.Fatalf("events[0].CreatedAt = %s, want %s", events[0].CreatedAt, secondAt)
	}

	thirdAt := secondAt.Add(11 * time.Minute)
	created, err = db.RecordWindowedEventAt(worker.ID, "platform_disk_full", "Fly log reported disk pressure: database or disk is full", `{"line":3}`, thirdAt, 10*time.Minute)
	if err != nil {
		t.Fatalf("third RecordWindowedEventAt() error = %v", err)
	}
	if !created {
		t.Fatalf("third RecordWindowedEventAt() created = false, want true")
	}

	events, err = db.ListWorkerEvents(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents() second call error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) after second window = %d, want 2", len(events))
	}
	if !events[0].CreatedAt.Equal(thirdAt) {
		t.Fatalf("newest event CreatedAt = %s, want %s", events[0].CreatedAt, thirdAt)
	}
}

func TestUpsertReadyDeploymentKeepsLitestreamVersionsDistinct(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	for _, litestreamSHA := range []string{"litestream-a", "litestream-b"} {
		if err := db.UpsertReadyDeployment(&Deployment{
			GitSHA:        "soak-sha",
			LitestreamSHA: litestreamSHA,
			ImageRef:      "registry.fly.io/litestream-soak:sha-test",
			Source:        "main",
			Status:        "ready",
		}); err != nil {
			t.Fatalf("UpsertReadyDeployment(%s) error = %v", litestreamSHA, err)
		}
	}

	deployments, err := db.ListDeployments("main", 10)
	if err != nil {
		t.Fatalf("ListDeployments() error = %v", err)
	}
	if len(deployments) != 2 {
		t.Fatalf("len(deployments) = %d, want 2", len(deployments))
	}

	deployment, err := db.GetDeploymentByVersion("main", "soak-sha", "litestream-a")
	if err != nil {
		t.Fatalf("GetDeploymentByVersion() error = %v", err)
	}
	if deployment.LitestreamSHA != "litestream-a" {
		t.Fatalf("deployment.LitestreamSHA = %q, want litestream-a", deployment.LitestreamSHA)
	}
}

func TestCreateWorkerResetsRuntimeStateOnReuse(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	worker := &Worker{
		ID:            "worker-pr-1228-low-vol",
		Name:          "worker-pr-1228-low-vol",
		Status:        WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "old-soak",
		LitestreamSHA: "old-litestream",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker(first) error = %v", err)
	}
	if err := db.UpdateWorkerHeartbeat(worker.ID); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat() error = %v", err)
	}
	if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, reporting.RuntimePayload{
		LitestreamSnapshotHealthy: true,
		SnapshotCollectedAt:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
	}

	reused, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker(before reuse) error = %v", err)
	}
	if reused.LastHeartbeatAt == nil {
		t.Fatalf("LastHeartbeatAt before reuse = nil, want value")
	}
	if reused.LastRuntimeAt == nil {
		t.Fatalf("LastRuntimeAt before reuse = nil, want value")
	}
	createdAtBeforeReuse := reused.CreatedAt

	time.Sleep(1100 * time.Millisecond)

	worker.GitSHA = "new-soak"
	worker.LitestreamSHA = "new-litestream"
	worker.Status = WorkerPending
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker(reuse) error = %v", err)
	}

	reused, err = db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker(after reuse) error = %v", err)
	}
	if reused.LastHeartbeatAt != nil {
		t.Fatalf("LastHeartbeatAt after reuse = %v, want nil", reused.LastHeartbeatAt)
	}
	if reused.LastRuntimeAt != nil {
		t.Fatalf("LastRuntimeAt after reuse = %v, want nil", reused.LastRuntimeAt)
	}
	if reused.LastRuntimeJSON != "" {
		t.Fatalf("LastRuntimeJSON after reuse = %q, want empty", reused.LastRuntimeJSON)
	}
	if !reused.CreatedAt.After(createdAtBeforeReuse) {
		t.Fatalf("CreatedAt after reuse = %s, want reset to a newer timestamp", reused.CreatedAt)
	}
}

func TestUpsertReportedWorkerClearsStaleHeartbeatDegradation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	worker := &Worker{
		ID:            "worker-pr-1228-high-vol",
		Name:          "worker-pr-1228-high-vol",
		Status:        WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	if err := db.UpdateWorkerStatus(worker.ID, WorkerDegraded, "worker missed heartbeat deadline"); err != nil {
		t.Fatalf("UpdateWorkerStatus() error = %v", err)
	}

	if err := db.UpsertReportedWorker(reporting.WorkerIdentity{
		WorkerID:      worker.ID,
		Name:          worker.Name,
		Source:        worker.Source,
		GitSHA:        worker.GitSHA,
		LitestreamSHA: worker.LitestreamSHA,
		ProfileName:   worker.ProfileName,
		ProfileConfig: worker.ProfileConfig,
	}); err != nil {
		t.Fatalf("UpsertReportedWorker() error = %v", err)
	}

	storedWorker, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if storedWorker.Status != WorkerRunning {
		t.Fatalf("Status = %s, want %s", storedWorker.Status, WorkerRunning)
	}
	if storedWorker.ErrorMessage != "" {
		t.Fatalf("ErrorMessage = %q, want empty", storedWorker.ErrorMessage)
	}
}

func TestUpsertReportedWorkerPreservesVerificationDegradation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	worker := &Worker{
		ID:            "worker-pr-1228-taxi-mixed",
		Name:          "worker-pr-1228-taxi-mixed",
		Status:        WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ProfileName:   "taxi-mixed",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	if err := db.UpdateWorkerStatus(worker.ID, WorkerDegraded, "validation failed"); err != nil {
		t.Fatalf("UpdateWorkerStatus() error = %v", err)
	}

	if err := db.UpsertReportedWorker(reporting.WorkerIdentity{
		WorkerID:      worker.ID,
		Name:          worker.Name,
		Source:        worker.Source,
		GitSHA:        worker.GitSHA,
		LitestreamSHA: worker.LitestreamSHA,
		ProfileName:   worker.ProfileName,
		ProfileConfig: worker.ProfileConfig,
	}); err != nil {
		t.Fatalf("UpsertReportedWorker() error = %v", err)
	}

	storedWorker, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if storedWorker.Status != WorkerDegraded {
		t.Fatalf("Status = %s, want %s", storedWorker.Status, WorkerDegraded)
	}
	if storedWorker.ErrorMessage != "validation failed" {
		t.Fatalf("ErrorMessage = %q, want validation failed", storedWorker.ErrorMessage)
	}
}

func TestRecordRunArchiveIsIdempotent(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	archive := &RunArchive{
		DeploymentID:  42,
		Source:        "pr-1228",
		WorkerID:      "",
		ArchiveType:   "success",
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ImageRef:      "registry.fly.io/litestream-soak:soak-sha",
		Status:        "stable",
		Summary:       "PR #1228 completed cleanly.",
		Payload:       `{"ok":true}`,
	}

	created, err := db.RecordRunArchive(archive)
	if err != nil {
		t.Fatalf("RecordRunArchive(first) error = %v", err)
	}
	if !created {
		t.Fatalf("RecordRunArchive(first) created = false, want true")
	}
	if archive.ID == 0 {
		t.Fatalf("archive.ID = 0, want assigned id")
	}

	second := *archive
	second.Summary = "different summary should not replace existing archive"
	created, err = db.RecordRunArchive(&second)
	if err != nil {
		t.Fatalf("RecordRunArchive(second) error = %v", err)
	}
	if created {
		t.Fatalf("RecordRunArchive(second) created = true, want false")
	}
	if second.ID != archive.ID {
		t.Fatalf("second.ID = %d, want %d", second.ID, archive.ID)
	}

	archives, err := db.ListRunArchives("pr-1228", "success", 10)
	if err != nil {
		t.Fatalf("ListRunArchives() error = %v", err)
	}
	if len(archives) != 1 {
		t.Fatalf("len(archives) = %d, want 1", len(archives))
	}
	if archives[0].Summary != "PR #1228 completed cleanly." {
		t.Fatalf("Summary = %q, want original summary", archives[0].Summary)
	}

	stored, err := db.GetRunArchive(archive.ID)
	if err != nil {
		t.Fatalf("GetRunArchive() error = %v", err)
	}
	if stored.Payload != `{"ok":true}` {
		t.Fatalf("Payload = %q", stored.Payload)
	}
}

func TestStaleWorkersUTCIndependent(t *testing.T) {
	origLocal := time.Local
	t.Cleanup(func() { time.Local = origLocal })

	newWorker := func(id string) *Worker {
		return &Worker{
			ID:            id,
			Name:          id,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		}
	}

	t.Run("fresh heartbeat not stale in UTC+13", func(t *testing.T) {
		time.Local = time.FixedZone("UTC+13", 13*3600)

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		w := newWorker("worker-utc-fresh")
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}
		if err := db.UpdateWorkerHeartbeat(w.ID); err != nil {
			t.Fatalf("UpdateWorkerHeartbeat() error = %v", err)
		}

		stale, err := db.StaleWorkers(5 * time.Minute)
		if err != nil {
			t.Fatalf("StaleWorkers() error = %v", err)
		}
		if len(stale) != 0 {
			t.Fatalf("StaleWorkers() = %d workers, want 0 (fresh heartbeat should not be stale)", len(stale))
		}
	})

	t.Run("old heartbeat stale in UTC-12", func(t *testing.T) {
		time.Local = time.FixedZone("UTC-12", -12*3600)

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		w := newWorker("worker-utc-old")
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}
		if _, err := db.db.Exec(
			"UPDATE workers SET last_heartbeat_at = datetime('now','-1 hour') WHERE id = ?",
			w.ID,
		); err != nil {
			t.Fatalf("backdate heartbeat: %v", err)
		}

		stale, err := db.StaleWorkers(5 * time.Minute)
		if err != nil {
			t.Fatalf("StaleWorkers() error = %v", err)
		}
		if len(stale) != 1 {
			t.Fatalf("StaleWorkers() = %d workers, want 1 (old heartbeat should be stale)", len(stale))
		}
		if stale[0].ID != w.ID {
			t.Fatalf("stale worker ID = %q, want %q", stale[0].ID, w.ID)
		}
	})
}

func TestUpdateWorkerVerificationStateProtectedStatuses(t *testing.T) {
	t.Parallel()

	type testCase struct {
		status      WorkerStatus
		setupDormant bool
	}

	cases := []testCase{
		{status: WorkerDormant, setupDormant: true},
		{status: WorkerStopped},
		{status: WorkerFailed},
	}

	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()

			dbPath := filepath.Join(t.TempDir(), "test.db")
			db, err := Open(dbPath)
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			workerID := "worker-protected-" + string(tc.status)
			w := &Worker{
				ID:            workerID,
				Name:          workerID,
				Status:        WorkerRunning,
				Source:        "main",
				GitSHA:        "abc123",
				LitestreamSHA: "ls123",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
			}
			if err := db.CreateWorker(w); err != nil {
				t.Fatalf("CreateWorker() error = %v", err)
			}

			if tc.setupDormant {
				if err := db.MarkWorkerDormant(workerID, "probe window expired", "probe_expired", "probe"); err != nil {
					t.Fatalf("MarkWorkerDormant() error = %v", err)
				}
				if _, err := db.db.Exec(
					"UPDATE workers SET last_probe_at = datetime('now') WHERE id = ?",
					workerID,
				); err != nil {
					t.Fatalf("set last_probe_at: %v", err)
				}
			} else {
				if err := db.UpdateWorkerStatus(workerID, tc.status, "some error"); err != nil {
					t.Fatalf("UpdateWorkerStatus() error = %v", err)
				}
			}

			before, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker() before: %v", err)
			}

			if err := db.UpdateWorkerVerificationState(workerID, true, ""); err != nil {
				t.Fatalf("UpdateWorkerVerificationState(passed=true) error = %v", err)
			}

			after, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker() after passed: %v", err)
			}
			if after.Status != before.Status {
				t.Fatalf("after passed: Status = %q, want %q (unchanged)", after.Status, before.Status)
			}
			if after.ErrorMessage != before.ErrorMessage {
				t.Fatalf("after passed: ErrorMessage = %q, want %q (unchanged)", after.ErrorMessage, before.ErrorMessage)
			}
			if tc.setupDormant {
				if after.DormantAt == nil {
					t.Fatalf("after passed: DormantAt = nil, want preserved")
				}
				if after.DormantReason != before.DormantReason {
					t.Fatalf("after passed: DormantReason = %q, want %q", after.DormantReason, before.DormantReason)
				}
				if after.DormantSignature != before.DormantSignature {
					t.Fatalf("after passed: DormantSignature = %q, want %q", after.DormantSignature, before.DormantSignature)
				}
				if after.ResumeTrigger != before.ResumeTrigger {
					t.Fatalf("after passed: ResumeTrigger = %q, want %q", after.ResumeTrigger, before.ResumeTrigger)
				}
				if after.LastProbeAt == nil {
					t.Fatalf("after passed: LastProbeAt = nil, want preserved")
				}
			}

			if err := db.UpdateWorkerVerificationState(workerID, false, "boom"); err != nil {
				t.Fatalf("UpdateWorkerVerificationState(passed=false) error = %v", err)
			}

			after2, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker() after failed: %v", err)
			}
			if after2.Status != before.Status {
				t.Fatalf("after failed: Status = %q, want %q (unchanged)", after2.Status, before.Status)
			}
			if after2.ErrorMessage != before.ErrorMessage {
				t.Fatalf("after failed: ErrorMessage = %q, want %q (unchanged)", after2.ErrorMessage, before.ErrorMessage)
			}
			if tc.setupDormant {
				if after2.DormantAt == nil {
					t.Fatalf("after failed: DormantAt = nil, want preserved")
				}
				if after2.DormantReason != before.DormantReason {
					t.Fatalf("after failed: DormantReason = %q, want %q", after2.DormantReason, before.DormantReason)
				}
				if after2.DormantSignature != before.DormantSignature {
					t.Fatalf("after failed: DormantSignature = %q, want %q", after2.DormantSignature, before.DormantSignature)
				}
				if after2.ResumeTrigger != before.ResumeTrigger {
					t.Fatalf("after failed: ResumeTrigger = %q, want %q", after2.ResumeTrigger, before.ResumeTrigger)
				}
				if after2.LastProbeAt == nil {
					t.Fatalf("after failed: LastProbeAt = nil, want preserved")
				}
			}
		})
	}

	t.Run("running+failed becomes degraded", func(t *testing.T) {
		t.Parallel()

		dbPath := filepath.Join(t.TempDir(), "test.db")
		db, err := Open(dbPath)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		workerID := "worker-running-degraded"
		w := &Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		}
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}
		if err := db.UpdateWorkerVerificationState(workerID, false, "checksum mismatch"); err != nil {
			t.Fatalf("UpdateWorkerVerificationState() error = %v", err)
		}
		stored, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.Status != WorkerDegraded {
			t.Fatalf("Status = %q, want %q", stored.Status, WorkerDegraded)
		}
		if stored.ErrorMessage != "checksum mismatch" {
			t.Fatalf("ErrorMessage = %q, want checksum mismatch", stored.ErrorMessage)
		}
	})

	t.Run("probing+passed becomes running", func(t *testing.T) {
		t.Parallel()

		dbPath := filepath.Join(t.TempDir(), "test.db")
		db, err := Open(dbPath)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		workerID := "worker-probing-running"
		w := &Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		}
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}
		if err := db.MarkWorkerProbing(workerID, "probe"); err != nil {
			t.Fatalf("MarkWorkerProbing() error = %v", err)
		}
		if err := db.UpdateWorkerVerificationState(workerID, true, ""); err != nil {
			t.Fatalf("UpdateWorkerVerificationState() error = %v", err)
		}
		stored, err := db.GetWorker(workerID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.Status != WorkerRunning {
			t.Fatalf("Status = %q, want %q", stored.Status, WorkerRunning)
		}
	})
}

func TestListExpiredWorkersUTCIndependent(t *testing.T) {
	t.Parallel()

	newWorker := func(id string, expiresAt *time.Time) *Worker {
		return &Worker{
			ID:            id,
			Name:          id,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
			ExpiresAt:     expiresAt,
		}
	}

	t.Run("past expiry in negative-offset zone is classified expired", func(t *testing.T) {
		t.Parallel()

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		negZone := time.FixedZone("UTC-5", -5*3600)
		pastInNeg := time.Now().Add(-1 * time.Hour).In(negZone)
		w := newWorker("worker-past-neg", &pastInNeg)
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}

		expired, err := db.ListExpiredWorkers()
		if err != nil {
			t.Fatalf("ListExpiredWorkers() error = %v", err)
		}
		found := false
		for _, ew := range expired {
			if ew.ID == w.ID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("past expiry in UTC-5 not in ListExpiredWorkers(); got %d workers", len(expired))
		}
	})

	t.Run("future expiry in negative-offset zone is not classified expired", func(t *testing.T) {
		t.Parallel()

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		negZone := time.FixedZone("UTC-5", -5*3600)
		futureInNeg := time.Now().Add(1 * time.Hour).In(negZone)
		w := newWorker("worker-future-neg", &futureInNeg)
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}

		expired, err := db.ListExpiredWorkers()
		if err != nil {
			t.Fatalf("ListExpiredWorkers() error = %v", err)
		}
		for _, ew := range expired {
			if ew.ID == w.ID {
				t.Fatalf("future expiry in UTC-5 wrongly appears in ListExpiredWorkers()")
			}
		}
	})

	t.Run("future expiry in positive-offset zone is not classified expired", func(t *testing.T) {
		t.Parallel()

		db, err := Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		posZone := time.FixedZone("UTC+5", 5*3600)
		futureInPos := time.Now().Add(1 * time.Hour).In(posZone)
		w := newWorker("worker-future-pos", &futureInPos)
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker() error = %v", err)
		}

		expired, err := db.ListExpiredWorkers()
		if err != nil {
			t.Fatalf("ListExpiredWorkers() error = %v", err)
		}
		for _, ew := range expired {
			if ew.ID == w.ID {
				t.Fatalf("future expiry in UTC+5 wrongly appears in ListExpiredWorkers()")
			}
		}
	})
}

func TestListExpiredWorkersLegacyFormat(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	newWorker := func(id string) *Worker {
		return &Worker{
			ID:            id,
			Name:          id,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		}
	}

	pastWorker := newWorker("worker-legacy-past")
	if err := db.CreateWorker(pastWorker); err != nil {
		t.Fatalf("CreateWorker(past) error = %v", err)
	}
	if _, err := db.db.Exec(
		"UPDATE workers SET expires_at = ? WHERE id = ?",
		"2020-01-02 15:04:05.123456789 +0000 UTC",
		pastWorker.ID,
	); err != nil {
		t.Fatalf("backdate expires_at (legacy format): %v", err)
	}

	futureWorker := newWorker("worker-legacy-future")
	if err := db.CreateWorker(futureWorker); err != nil {
		t.Fatalf("CreateWorker(future) error = %v", err)
	}
	if _, err := db.db.Exec(
		"UPDATE workers SET expires_at = ? WHERE id = ?",
		"2099-01-02 15:04:05.123456789 +0000 UTC",
		futureWorker.ID,
	); err != nil {
		t.Fatalf("set future expires_at (legacy format): %v", err)
	}

	expired, err := db.ListExpiredWorkers()
	if err != nil {
		t.Fatalf("ListExpiredWorkers() error = %v", err)
	}

	foundPast := false
	foundFuture := false
	for _, w := range expired {
		if w.ID == pastWorker.ID {
			foundPast = true
		}
		if w.ID == futureWorker.ID {
			foundFuture = true
		}
	}
	if !foundPast {
		t.Fatalf("legacy past expires_at not in ListExpiredWorkers()")
	}
	if foundFuture {
		t.Fatalf("legacy future expires_at wrongly in ListExpiredWorkers()")
	}

	fetched, err := db.GetWorker(pastWorker.ID)
	if err != nil {
		t.Fatalf("GetWorker(legacy past) error = %v", err)
	}
	if fetched.ExpiresAt == nil {
		t.Fatalf("ExpiresAt after scan = nil, want value")
	}
	wantInstant := time.Date(2020, 1, 2, 15, 4, 5, 123456789, time.UTC)
	if !fetched.ExpiresAt.Equal(wantInstant) {
		t.Fatalf("ExpiresAt = %v, want %v", fetched.ExpiresAt, wantInstant)
	}
}

func TestOpenNormalizesLegacyNonUTCExpiresAt(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	newWorker := func(id string) *Worker {
		return &Worker{
			ID:            id,
			Name:          id,
			Status:        WorkerRunning,
			Source:        "main",
			GitSHA:        "abc123",
			LitestreamSHA: "ls123",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		}
	}

	legacyLayout := "2006-01-02 15:04:05.999999999 -0700 MST"
	negZone := time.FixedZone("EST", -5*3600)
	posZone := time.FixedZone("IST", 5*3600+1800)

	futureInstant := time.Now().UTC().Add(1 * time.Hour)
	pastInstant := time.Now().UTC().Add(-1 * time.Hour)

	futureWorker := newWorker("worker-legacy-neg-future")
	pastWorker := newWorker("worker-legacy-pos-past")
	for id, raw := range map[string]string{
		futureWorker.ID: futureInstant.In(negZone).Format(legacyLayout),
		pastWorker.ID:   pastInstant.In(posZone).Format(legacyLayout),
	} {
		w := newWorker(id)
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker(%s) error = %v", id, err)
		}
		if _, err := db.db.Exec("UPDATE workers SET expires_at = ? WHERE id = ?", raw, id); err != nil {
			t.Fatalf("set legacy expires_at for %s: %v", id, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	expired, err := db.ListExpiredWorkers()
	if err != nil {
		t.Fatalf("ListExpiredWorkers() error = %v", err)
	}
	foundPast := false
	for _, w := range expired {
		if w.ID == futureWorker.ID {
			t.Fatalf("legacy future expiry in EST wrongly in ListExpiredWorkers()")
		}
		if w.ID == pastWorker.ID {
			foundPast = true
		}
	}
	if !foundPast {
		t.Fatalf("legacy past expiry in IST not in ListExpiredWorkers()")
	}

	for id, want := range map[string]time.Time{
		futureWorker.ID: futureInstant,
		pastWorker.ID:   pastInstant,
	} {
		var raw string
		if err := db.db.QueryRow("SELECT expires_at FROM workers WHERE id = ?", id).Scan(&raw); err != nil {
			t.Fatalf("SELECT expires_at for %s: %v", id, err)
		}
		if !isStoredAsUTC(raw, want) {
			t.Fatalf("expires_at for %s = %q, want canonical UTC form for %v", id, raw, want)
		}
	}
}

func TestCreateWorkerStoresExpiresAtAsUTC(t *testing.T) {
	t.Parallel()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	negZone := time.FixedZone("UTC-5", -5*3600)
	expiresAt := time.Date(2030, 6, 15, 10, 0, 0, 0, negZone)
	w := &Worker{
		ID:            "worker-utc-stored",
		Name:          "worker-utc-stored",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		ExpiresAt:     &expiresAt,
	}
	if err := db.CreateWorker(w); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	var raw string
	if err := db.db.QueryRow("SELECT expires_at FROM workers WHERE id = ?", w.ID).Scan(&raw); err != nil {
		t.Fatalf("SELECT expires_at: %v", err)
	}

	wantUTCInstant := expiresAt.UTC()
	if !isStoredAsUTC(raw, wantUTCInstant) {
		t.Fatalf("expires_at raw = %q, want UTC form matching instant %v (no non-zero offset, no ` UTC` / ` MST` suffix)", raw, wantUTCInstant)
	}
}

func isStoredAsUTC(raw string, want time.Time) bool {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999-07:00", time.RFC3339Nano} {
		parsed, err := time.Parse(layout, raw)
		if err != nil {
			continue
		}
		_, offset := parsed.Zone()
		return offset == 0 && parsed.Equal(want)
	}
	return false
}

func TestUpsertReportedWorkerDoesNotResurrectStopped(t *testing.T) {
	t.Parallel()

	type testCase struct {
		status       WorkerStatus
		errorMessage string
		wantStatus   WorkerStatus
		wantError    string
	}

	cases := []testCase{
		{
			status:       WorkerStopped,
			errorMessage: "machine stopped by operator",
			wantStatus:   WorkerStopped,
			wantError:    "machine stopped by operator",
		},
		{
			status:       WorkerFailed,
			errorMessage: "disk full",
			wantStatus:   WorkerFailed,
			wantError:    "disk full",
		},
		{
			status:     WorkerPending,
			wantStatus: WorkerRunning,
			wantError:  "",
		},
		{
			status:     WorkerBuilding,
			wantStatus: WorkerRunning,
			wantError:  "",
		},
		{
			status:     WorkerStarting,
			wantStatus: WorkerRunning,
			wantError:  "",
		},
	}

	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()

			dbPath := filepath.Join(t.TempDir(), "test.db")
			db, err := Open(dbPath)
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			workerID := "worker-upsert-" + string(tc.status)
			w := &Worker{
				ID:            workerID,
				Name:          workerID,
				Status:        tc.status,
				Source:        "main",
				GitSHA:        "abc123",
				LitestreamSHA: "ls123",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
			}
			if err := db.CreateWorker(w); err != nil {
				t.Fatalf("CreateWorker() error = %v", err)
			}
			if tc.errorMessage != "" {
				if err := db.UpdateWorkerStatus(workerID, tc.status, tc.errorMessage); err != nil {
					t.Fatalf("UpdateWorkerStatus() error = %v", err)
				}
			}

			if err := db.UpsertReportedWorker(reporting.WorkerIdentity{
				WorkerID:      workerID,
				Name:          workerID,
				Source:        "main",
				GitSHA:        "abc123",
				LitestreamSHA: "ls123",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
			}); err != nil {
				t.Fatalf("UpsertReportedWorker() error = %v", err)
			}

			stored, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker() error = %v", err)
			}
			if stored.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q", stored.Status, tc.wantStatus)
			}
			if stored.ErrorMessage != tc.wantError {
				t.Fatalf("ErrorMessage = %q, want %q", stored.ErrorMessage, tc.wantError)
			}
			if stored.LastHeartbeatAt == nil {
				t.Fatalf("LastHeartbeatAt = nil, want non-nil (heartbeat should always refresh)")
			}
		})
	}
}

func TestUpdateWorkerRuntimeSnapshotHealthyPayloadSetsLastRuntimeAt(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{
		ID:            "worker-healthy",
		Name:          "worker-healthy",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-a",
		LitestreamSHA: "ls-a",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	collectedAt := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	payload := reporting.RuntimePayload{
		LitestreamSnapshotHealthy: true,
		SnapshotCollectedAt:       collectedAt,
	}
	if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, payload); err != nil {
		t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
	}

	stored, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if stored.LastRuntimeAt == nil {
		t.Fatalf("LastRuntimeAt = nil, want %v", collectedAt)
	}
	if !stored.LastRuntimeAt.Equal(collectedAt) {
		t.Fatalf("LastRuntimeAt = %v, want %v", stored.LastRuntimeAt, collectedAt)
	}
}

func TestUpdateWorkerRuntimeSnapshotUnhealthyPayloadPreservesLastRuntimeAt(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &Worker{
		ID:            "worker-unhealthy",
		Name:          "worker-unhealthy",
		Status:        WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-a",
		LitestreamSHA: "ls-a",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	unhealthyPayload := reporting.RuntimePayload{
		LitestreamSnapshotHealthy: false,
		LitestreamSnapshotError:   "snapshot failed",
		SnapshotCollectedAt:       time.Now().UTC(),
	}

	t.Run("stays nil when previously nil", func(t *testing.T) {
		if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, unhealthyPayload); err != nil {
			t.Fatalf("UpdateWorkerRuntimeSnapshot() error = %v", err)
		}
		stored, err := db.GetWorker(worker.ID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.LastRuntimeAt != nil {
			t.Fatalf("LastRuntimeAt = %v, want nil (unhealthy payload must not set timestamp)", stored.LastRuntimeAt)
		}
		if stored.LastRuntimeJSON == "" {
			t.Fatalf("LastRuntimeJSON = empty, want payload JSON (must still be written)")
		}
	})

	t.Run("prior healthy value is preserved", func(t *testing.T) {
		healthyAt := time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC)
		healthyPayload := reporting.RuntimePayload{
			LitestreamSnapshotHealthy: true,
			SnapshotCollectedAt:       healthyAt,
		}
		if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, healthyPayload); err != nil {
			t.Fatalf("UpdateWorkerRuntimeSnapshot(healthy) error = %v", err)
		}

		if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, unhealthyPayload); err != nil {
			t.Fatalf("UpdateWorkerRuntimeSnapshot(unhealthy) error = %v", err)
		}

		stored, err := db.GetWorker(worker.ID)
		if err != nil {
			t.Fatalf("GetWorker() error = %v", err)
		}
		if stored.LastRuntimeAt == nil {
			t.Fatalf("LastRuntimeAt = nil, want %v (prior healthy value must be preserved)", healthyAt)
		}
		if !stored.LastRuntimeAt.Equal(healthyAt) {
			t.Fatalf("LastRuntimeAt = %v, want %v (prior healthy value must be preserved)", stored.LastRuntimeAt, healthyAt)
		}
		if stored.LastRuntimeJSON == "" {
			t.Fatalf("LastRuntimeJSON = empty, want payload JSON (must still be written on unhealthy)")
		}
	})
}
