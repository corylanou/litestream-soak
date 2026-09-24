package model

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func profileSnapshotFixture(incidents int) reporting.RuntimePayload {
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: 1, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	payload := reporting.RuntimePayload{
		SnapshotCollectedAt:       time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC),
		LitestreamSnapshotHealthy: true,
		DBTXID:                    42,
		ProfilingEvidence: reporting.ProfilingEvidence{
			ProfileCapability:      "observed",
			ProfileHistoryComplete: true,
			ProfileStatusCounts:    map[string]uint64{"upload-failed": 2},
			ProfileRecords: []reporting.ProfileRecordEvidence{
				{DeploymentID: 1, WorkerID: "worker", RunID: "run", MachineID: "machine", Artifact: "cpu.pprof", Status: "captured", UploadFailureCount: 2},
				{DeploymentID: 1, WorkerID: "worker", RunID: "run", MachineID: "machine", Artifact: "heap.pprof", Status: "captured", Error: "capture failed"},
			},
		},
	}
	for i := 0; i < incidents; i++ {
		payload.ProfileIncidents = append(payload.ProfileIncidents, reporting.ProfileIncident{
			ID: fmt.Sprintf("incident-%d", i), Kind: "profile_capture_error", Message: fmt.Sprintf("capture %d failed", i),
			At: time.Date(2026, 9, 23, 11, 0, i, 0, time.UTC), Run: identity,
		})
	}
	return payload
}

func decodeRuntime(t *testing.T, raw string) reporting.RuntimePayload {
	t.Helper()
	var payload reporting.RuntimePayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode runtime: %v", err)
	}
	return payload
}

func TestProfileSnapshotRoundTripsExactly(t *testing.T) {
	db := seriesTestDB(t)
	raw, err := json.Marshal(profileSnapshotFixture(50))
	if err != nil {
		t.Fatal(err)
	}
	compact, err := db.compactProfileSnapshot(string(raw))
	if err != nil {
		t.Fatalf("compactProfileSnapshot() error = %v", err)
	}
	if hasProfileSnapshotArrays(compact) || len(compact) >= len(raw)/4 {
		t.Fatalf("compact snapshot still carries profile arrays: %d of %d bytes", len(compact), len(raw))
	}

	fresh := &DB{reader: db.reader, writer: db.writer, blobs: newProfileBlobCache()}
	expanded, err := fresh.expandProfileSnapshot(compact)
	if err != nil {
		t.Fatalf("expandProfileSnapshot() error = %v", err)
	}
	var want, got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(expanded), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("expanded snapshot differs:\nwant %s\ngot  %s", raw, expanded)
	}
}

func TestProfileSnapshotLeavesPayloadsWithoutArrays(t *testing.T) {
	db := seriesTestDB(t)
	for _, raw := range []string{``, `{}`, `not json`, `{"profile_incidents":[]}`, `{"profile_records":null}`, `{"message":"no profile"}`} {
		compact, err := db.compactProfileSnapshot(raw)
		if err != nil {
			t.Fatalf("compactProfileSnapshot(%q) error = %v", raw, err)
		}
		if compact != raw {
			t.Fatalf("compactProfileSnapshot(%q) = %q, want unchanged", raw, compact)
		}
	}
}

func TestProfileSnapshotStorageStaysFlatAsHistoryGrows(t *testing.T) {
	db := seriesTestDB(t)
	var sizes []int
	for _, incidents := range []int{100, 200, 400} {
		raw, err := json.Marshal(profileSnapshotFixture(incidents))
		if err != nil {
			t.Fatal(err)
		}
		compact, err := db.compactProfileSnapshot(string(raw))
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(compact))
	}
	var blobs int
	if err := db.queryRow(`SELECT count(*) FROM evidence_profile_blobs`).Scan(&blobs); err != nil {
		t.Fatal(err)
	}
	if blobs != 400+2 {
		t.Fatalf("stored blobs = %d, want one per distinct incident and record (402)", blobs)
	}
	if sizes[2] > 8*1024 {
		t.Fatalf("compact snapshot with 400 incidents is %d bytes", sizes[2])
	}
}

func TestVerificationAndEventReadersExpandCompactSnapshots(t *testing.T) {
	db := seriesTestDB(t)
	worker := Worker{ID: "worker", Name: "worker", Source: "main", GitSHA: "sha", ProfileName: "low-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	payload := profileSnapshotFixture(25)
	if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, payload); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := db.queryRow(`SELECT last_runtime_json FROM workers WHERE id=?`, worker.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if hasProfileSnapshotArrays(stored) {
		t.Fatal("worker runtime snapshot was stored with inline profile arrays")
	}
	loaded, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeRuntime(t, loaded.LastRuntimeJSON); !reflect.DeepEqual(got.ProfilingEvidence, payload.ProfilingEvidence) {
		t.Fatalf("worker runtime profiling = %+v, want %+v", got.ProfilingEvidence, payload.ProfilingEvidence)
	}

	v := Verification{WorkerID: worker.ID, StartedAt: time.Now().UTC(), Status: "passed", Passed: true, CheckType: "integrity"}
	if err := db.RecordVerification(&v); err != nil {
		t.Fatal(err)
	}
	var verificationRuntime string
	if err := db.queryRow(`SELECT evidence_runtime FROM evidence_verifications WHERE id=?`, v.ID).Scan(&verificationRuntime); err != nil {
		t.Fatal(err)
	}
	if hasProfileSnapshotArrays(verificationRuntime) {
		t.Fatal("verification journal copied inline profile arrays")
	}
	records, err := db.ListEvidenceVerifications("main")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || !reflect.DeepEqual(records[0].Runtime.ProfilingEvidence, payload.ProfilingEvidence) {
		t.Fatalf("verification runtime profiling = %+v, want %+v", records, payload.ProfilingEvidence)
	}

	details, err := json.Marshal(struct {
		reporting.RuntimePayload
		Source     string `json:"source"`
		Attributed bool   `json:"attributed"`
	}{payload, "main", true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordEvent(worker.ID, "verification_started", "verification started", string(details)); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListEvidenceEvents("main")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !reflect.DeepEqual(decodeRuntime(t, events[0].Details).ProfilingEvidence, payload.ProfilingEvidence) {
		t.Fatalf("evidence event details = %+v", events)
	}
	workerEvents, err := db.ListWorkerEvents(worker.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(workerEvents) != 1 || !reflect.DeepEqual(decodeRuntime(t, workerEvents[0].Details).ProfilingEvidence, payload.ProfilingEvidence) {
		t.Fatalf("worker event details = %+v", workerEvents)
	}
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	if created, err := db.RecordUniqueEventAt(worker.ID, "profile_replay", "replayed", string(details), at); err != nil || !created {
		t.Fatalf("RecordUniqueEventAt(first) = %v, %v; want created", created, err)
	}
	if created, err := db.RecordUniqueEventAt(worker.ID, "profile_replay", "replayed", string(details), at); err != nil || created {
		t.Fatalf("RecordUniqueEventAt(repeat) = %v, %v; want existing compact event matched", created, err)
	}
}

func TestCompactProfileSnapshotsBackfillsExistingRows(t *testing.T) {
	db := seriesTestDB(t)
	payload := profileSnapshotFixture(30)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.writer.Exec(`INSERT INTO workers (id, name, status, source, git_sha, profile_name, profile_config, last_runtime_json) VALUES ('legacy', 'legacy', 'running', 'main', 'sha', 'low-volume', '{}', ?)`, string(raw)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := db.writer.Exec(`INSERT INTO evidence_verifications (id, worker_id, started_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message, failure_classification_json, run_identity_json, attributed, evidence_source, evidence_region, evidence_profile, evidence_runtime, history_complete) VALUES (?, 'legacy', ?, 'passed', 'integrity', '', '', 1, 0, '', '', '{}', 0, 'main', '', '', ?, 0)`, 1000+i, time.Now().UTC(), string(raw)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.writer.Exec(`INSERT INTO evidence_events (id, worker_id, event_type, message, details, created_at, evidence_source) VALUES (?, 'legacy', 'verification_started', 'started', ?, ?, 'main')`, 2000+i, string(raw), time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := db.CompactProfileSnapshots(context.Background(), 2); err != nil {
		t.Fatalf("CompactProfileSnapshots() error = %v", err)
	}
	for _, query := range []string{
		`SELECT count(*) FROM workers WHERE instr(last_runtime_json, '"profile_incidents"') > 0`,
		`SELECT count(*) FROM evidence_verifications WHERE instr(evidence_runtime, '"profile_incidents"') > 0`,
		`SELECT count(*) FROM evidence_events WHERE instr(details, '"profile_incidents"') > 0`,
	} {
		var inline int
		if err := db.queryRow(query).Scan(&inline); err != nil {
			t.Fatal(err)
		}
		if inline != 0 {
			t.Fatalf("%s = %d after compaction", query, inline)
		}
	}
	records, err := db.ListEvidenceVerifications("main")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 5 {
		t.Fatalf("verifications = %d, want 5", len(records))
	}
	for _, record := range records {
		if !reflect.DeepEqual(record.Runtime.ProfilingEvidence, payload.ProfilingEvidence) {
			t.Fatalf("compacted verification runtime = %+v", record.Runtime.ProfilingEvidence)
		}
	}
	events, err := db.ListEvidenceEvents("main")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if !reflect.DeepEqual(decodeRuntime(t, event.Details).ProfilingEvidence, payload.ProfilingEvidence) {
			t.Fatalf("compacted event details differ")
		}
	}

	again, err := db.CompactProfileSnapshots(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if again != 0 {
		t.Fatalf("second compaction processed %d rows, want 0", again)
	}
}

func TestPruneProfileBlobsKeepsOnlyReferencedBlobs(t *testing.T) {
	db := seriesTestDB(t)
	worker := Worker{ID: "worker", Name: "worker", Source: "main", GitSHA: "sha", ProfileName: "low-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	orphan, err := json.Marshal(profileSnapshotFixture(10))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.compactProfileSnapshot(string(orphan)); err != nil {
		t.Fatal(err)
	}
	kept := profileSnapshotFixture(3)
	kept.ProfileRecords = nil
	for i := range kept.ProfileIncidents {
		kept.ProfileIncidents[i].ID = fmt.Sprintf("kept-%d", i)
	}
	if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, kept); err != nil {
		t.Fatal(err)
	}

	deleted, err := db.PruneProfileBlobs(context.Background())
	if err != nil {
		t.Fatalf("PruneProfileBlobs() error = %v", err)
	}
	if deleted != 12 {
		t.Fatalf("deleted = %d, want the 12 unreferenced blobs", deleted)
	}
	loaded, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() after sweep error = %v", err)
	}
	if got := decodeRuntime(t, loaded.LastRuntimeJSON); !reflect.DeepEqual(got.ProfileIncidents, kept.ProfileIncidents) {
		t.Fatalf("referenced incidents after sweep = %+v", got.ProfileIncidents)
	}
	if again, err := db.PruneProfileBlobs(context.Background()); err != nil || again != 0 {
		t.Fatalf("second sweep inside the interval = %d, %v; want skipped", again, err)
	}
	if err := db.UpdateWorkerRuntimeSnapshot(worker.ID, profileSnapshotFixture(10)); err != nil {
		t.Fatalf("re-storing swept content error = %v", err)
	}
	loaded, err = db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() after re-store error = %v", err)
	}
	if len(decodeRuntime(t, loaded.LastRuntimeJSON).ProfileIncidents) != 10 {
		t.Fatal("re-stored snapshot lost incidents after the sweep cleared the cache")
	}
}

func TestCompactProfileSnapshotStripsReportedReferenceFields(t *testing.T) {
	db := seriesTestDB(t)
	worker := Worker{ID: "worker", Name: "worker", Source: "main", GitSHA: "sha", ProfileName: "low-volume", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	details := `{"source":"main","message":"forged","_profile_incident_blobs":[999999],"_profile_record_blobs":[888888]}`
	if err := db.RecordEvent(worker.ID, "custom", "forged refs", details); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListWorkerEvents(worker.ID, 10)
	if err != nil {
		t.Fatalf("ListWorkerEvents() error = %v", err)
	}
	if len(events) != 1 || hasProfileSnapshotRefs(events[0].Details) || hasProfileSnapshotArrays(events[0].Details) {
		t.Fatalf("events = %+v, want reference fields stripped", events)
	}
}
