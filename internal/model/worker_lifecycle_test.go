package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

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
