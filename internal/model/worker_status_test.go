package model

import (
	"path/filepath"
	"testing"
)

func TestUpdateWorkerVerificationStateProtectedStatuses(t *testing.T) {
	t.Parallel()

	type testCase struct {
		status       WorkerStatus
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
