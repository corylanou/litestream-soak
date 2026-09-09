package model

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestEvidenceSurvivesRetentionDeletionAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evidence.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	worker := Worker{ID: "worker", Name: "worker", Source: "main", GitSHA: "sha", ProfileName: "low-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 600; i++ {
		v := Verification{WorkerID: worker.ID, StartedAt: at.Add(time.Duration(i) * time.Second), Status: "passed", Passed: i > 0, CheckType: "integrity"}
		if i == 0 {
			v.Status = "failed"
			v.ErrorMessage = "provider retry recovered later"
		}
		if err := db.RecordVerification(&v); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.RecordEventAt(worker.ID, "platform_restart", "unexpected restart", "{}", at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PruneVerificationsBefore(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PruneEventsBefore(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteWorker(worker.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	records, err := db.ListEvidenceVerifications("main")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 600 {
		t.Fatalf("retained %d verifications, want 600", len(records))
	}
	failures := 0
	for _, v := range records {
		if v.Failed() {
			failures++
		}
	}
	if failures != 1 {
		t.Fatalf("failures=%d", failures)
	}
	events, err := db.ListEvidenceEvents("main")
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%v err=%v", events, err)
	}
}

func TestEvidenceKeepsCoalescedObservations(t *testing.T) {
	db := seriesTestDB(t)
	worker := Worker{ID: "coalesced", Name: "coalesced", Source: "main", ProfileName: "low-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	for i := 0; i < 3; i++ {
		if _, err := db.RecordWindowedEventAt(worker.ID, "provider_retry", "retry", fmt.Sprint(i), at.Add(time.Duration(i)*time.Second), time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	events, err := db.ListEvidenceEvents("main")
	if err != nil || len(events) != 3 {
		t.Fatalf("events=%v err=%v", events, err)
	}
	other, err := db.ListEvidenceEvents("other")
	if err != nil || len(other) != 0 {
		t.Fatalf("other source=%v err=%v", other, err)
	}
}

func TestEvidenceMigrationMarksRetainedHistoryUnknown(t *testing.T) {
	db := seriesTestDB(t)
	worker := Worker{ID: "legacy", Name: "legacy", Source: "main", ProfileName: "low-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	if _, err := db.writer.Exec("DROP TRIGGER journal_verification"); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	v := Verification{WorkerID: worker.ID, StartedAt: at, CompletedAt: &at, Status: "failed", CheckType: "integrity"}
	if err := db.RecordVerification(&v); err != nil {
		t.Fatal(err)
	}
	if err := ensureEvidenceJournal(db.writer); err != nil {
		t.Fatal(err)
	}
	records, err := db.ListEvidenceVerifications("main")
	if err != nil || len(records) != 1 || records[0].HistoryComplete {
		t.Fatalf("records=%+v err=%v", records, err)
	}
}

func TestRuntimeEvidenceSurvivesWorkerRecreation(t *testing.T) {
	db := seriesTestDB(t)
	worker := Worker{ID: "runtime", Name: "runtime", Source: "main", ProfileName: "queue-churn", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	identity := reporting.WorkerIdentity{WorkerID: worker.ID, Source: "main", RunID: "old", DeploymentID: 7, ProfileName: worker.ProfileName}
	payload := json.RawMessage(`{"workload_counters_present":true,"workload_mutations_total":12,"workload_busy_total":2,"workload_errors_total":3}`)
	if err := db.RecordRuntimeEvidence(identity, payload, true); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteWorker(worker.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	identity.RunID = "new"
	if err := db.RecordRuntimeEvidence(identity, json.RawMessage(`{"workload_counters_present":true}`), true); err != nil {
		t.Fatal(err)
	}
	records, err := db.ListRuntimeEvidence("main", 7)
	if err != nil || len(records) != 2 || !bytes.Contains(records[0].RuntimeJSON, []byte(`"workload_errors_total":3`)) {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	other, err := db.ListRuntimeEvidence("main", 8)
	if err != nil || len(other) != 0 {
		t.Fatalf("other=%+v err=%v", other, err)
	}
	if err := db.RecordRuntimeEvidence(identity, json.RawMessage(`invalid`), true); err == nil {
		t.Fatal("invalid JSON accepted")
	}
}

func TestEvidenceEventUsesOriginalSourceAfterRecreation(t *testing.T) {
	db := seriesTestDB(t)
	worker := Worker{ID: "worker", Name: "worker", Source: "new-source", ProfileName: "low-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordEvent(worker.ID, "litestream_log_error", "original error", `{"source":"old-source","run_id":"original"}`); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListEvidenceEvents("old-source")
	if err != nil || len(events) != 1 {
		t.Fatalf("original evidence missing: %v %v", events, err)
	}
}

func TestEvidenceWithUnknownSourceRemainsVisible(t *testing.T) {
	db := seriesTestDB(t)
	if err := db.RecordRuntimeEvidence(reporting.WorkerIdentity{WorkerID: "orphan"}, json.RawMessage(`{"message":"legacy failure"}`), false, "event"); err != nil {
		t.Fatal(err)
	}
	records, err := db.ListRuntimeEvidence("main", 1)
	if err != nil || len(records) != 1 || records[0].Attributed {
		t.Fatalf("unknown evidence lost or credited: %+v %v", records, err)
	}
}
