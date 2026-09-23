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

func TestRuntimeIncidentDeduplicationRespectsObservationEligibility(t *testing.T) {
	db := seriesTestDB(t)
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: 1, WorkerID: "worker", RunID: "run"}
	incident := reporting.ProfileIncident{ID: "shared", Run: identity, Kind: "profile_capture_error"}
	for _, sample := range []struct {
		kind       string
		attributed bool
		capability string
	}{
		{"event", true, ""},
		{"heartbeat", false, "observed"},
		{"heartbeat", true, "observed"},
		{"heartbeat", true, "observed"},
	} {
		body, _ := json.Marshal(reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: sample.capability, ProfileIncidents: []reporting.ProfileIncident{incident}}})
		if err := db.RecordRuntimeEvidence(identity, body, sample.attributed, sample.kind); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.ListRuntimeEvidence("main", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("runtime rows=%d", len(rows))
	}
	for i, row := range rows {
		var payload reporting.RuntimePayload
		if err := json.Unmarshal(row.RuntimeJSON, &payload); err != nil {
			t.Fatal(err)
		}
		want := 1
		if i == 3 {
			want = 0
		}
		if len(payload.ProfileIncidents) != want {
			t.Fatalf("row %d incidents=%d want=%d", i, len(payload.ProfileIncidents), want)
		}
	}
}

func TestOldWorkerCumulativeHeartbeatsStayBounded(t *testing.T) {
	db := seriesTestDB(t)
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: 1, WorkerID: "worker", RunID: "run"}
	incidents := make([]reporting.ProfileIncident, 0, 100)
	for i := 0; i < 100; i++ {
		incidents = append(incidents, reporting.ProfileIncident{ID: fmt.Sprint(i), Run: identity, Kind: "profile_capture_error", Message: "collector failed"})
		body, err := json.Marshal(reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "observed", ProfileIncidents: incidents}})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordRuntimeEvidence(identity, body, true); err != nil {
			t.Fatal(err)
		}
	}
	var first, last, unique int
	if err := db.queryRow(`SELECT length(runtime_json) FROM evidence_runtime ORDER BY id LIMIT 1`).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := db.queryRow(`SELECT length(runtime_json) FROM evidence_runtime ORDER BY id DESC LIMIT 1`).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if err := db.queryRow(`SELECT count(*) FROM evidence_profile_incidents`).Scan(&unique); err != nil {
		t.Fatal(err)
	}
	if last > first+100 || unique != 100 {
		t.Fatalf("first=%d last=%d unique=%d", first, last, unique)
	}
}

func TestRuntimeIncidentUnknownFieldsSurviveNormalization(t *testing.T) {
	for _, historical := range []bool{false, true} {
		t.Run(fmt.Sprint("historical=", historical), func(t *testing.T) {
			db := seriesTestDB(t)
			identity := reporting.WorkerIdentity{Source: "main", DeploymentID: 1, WorkerID: "worker", RunID: "run"}
			body := []byte(`{"profile_capability":"observed","profile_incidents":[{"id":"incident","run":{"worker_id":"worker","source":"main","run_id":"run"},"future_field":{"version":2}}]}`)
			if historical {
				identityJSON, _ := json.Marshal(identity)
				if _, err := db.writer.Exec(`INSERT INTO evidence_runtime(source,deployment_id,identity_json,runtime_json,attributed,received_at,kind) VALUES (?,?,?,?,?,?,?)`, "main", 1, string(identityJSON), string(body), true, time.Now().UTC(), "heartbeat"); err != nil {
					t.Fatal(err)
				}
				if _, err := db.CompactRuntimeEvidence(context.Background(), 1); err != nil {
					t.Fatal(err)
				}
			} else if err := db.RecordRuntimeEvidence(identity, body, true); err != nil {
				t.Fatal(err)
			}
			rows, err := db.ListRuntimeEvidence("main", 1)
			if err != nil || len(rows) != 1 {
				t.Fatalf("rows=%d error=%v", len(rows), err)
			}
			var payload struct {
				Incidents []map[string]json.RawMessage `json:"profile_incidents"`
			}
			if err := json.Unmarshal(rows[0].RuntimeJSON, &payload); err != nil {
				t.Fatal(err)
			}
			if len(payload.Incidents) != 1 || string(payload.Incidents[0]["future_field"]) != `{"version":2}` {
				t.Fatalf("incident fields=%v", payload.Incidents)
			}
		})
	}
}

func TestRuntimeEvidenceHydrationUsesOneReaderConnection(t *testing.T) {
	db := seriesTestDB(t)
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: 1, WorkerID: "worker", RunID: "run"}
	body, _ := json.Marshal(reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{
		ProfileCapability: "observed",
		ProfileIncidents:  []reporting.ProfileIncident{{ID: "incident", Run: identity, Kind: "profile_capture_error"}},
		ProfileRecords:    []reporting.ProfileRecordEvidence{{DeploymentID: 1, WorkerID: "worker", RunID: "run", Artifact: "cpu.pprof"}},
	}})
	if err := db.RecordRuntimeEvidence(identity, body, true); err != nil {
		t.Fatal(err)
	}
	db.reader.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, compact := range []bool{true, false} {
		count := 0
		visit := func(row RuntimeEvidence) error {
			count++
			return nil
		}
		var err error
		if compact {
			err = db.WithReadContext(ctx).EachRuntimeEvidenceCompact("main", 1, nil, visit)
		} else {
			err = db.WithReadContext(ctx).EachRuntimeEvidence("main", 1, nil, visit)
		}
		if err != nil || count != 1 {
			t.Fatalf("compact=%t count=%d error=%v", compact, count, err)
		}
	}
}

func TestRuntimeEvidenceCompactionResumesAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evidence.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: 1, WorkerID: "worker", RunID: "run"}
	first := reporting.ProfileIncident{ID: "first", Run: identity, Kind: "profile_capture_error"}
	second := reporting.ProfileIncident{ID: "second", Run: identity, Kind: "profile_capture_error"}
	makeBody := func(incidents []reporting.ProfileIncident) []byte {
		body, err := json.Marshal(reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "observed", ProfileIncidents: incidents}})
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	if err := db.RecordRuntimeEvidence(identity, makeBody([]reporting.ProfileIncident{first}), true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.writer.Exec(`UPDATE evidence_compaction_progress SET last_runtime_id=(SELECT max(id) FROM evidence_runtime) WHERE id=1`); err != nil {
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
	identityJSON, _ := json.Marshal(identity)
	for i := 0; i < 2; i++ {
		if _, err := db.writer.Exec(`INSERT INTO evidence_runtime(source,deployment_id,identity_json,runtime_json,attributed,received_at,kind) VALUES (?,?,?,?,?,?,?)`, "main", 1, string(identityJSON), string(makeBody([]reporting.ProfileIncident{first, second})), true, time.Now().UTC(), "heartbeat"); err != nil {
			t.Fatal(err)
		}
	}
	processed, err := db.CompactRuntimeEvidence(context.Background(), 1)
	if err != nil || processed != 2 {
		t.Fatalf("resumed compaction processed=%d error=%v", processed, err)
	}
	processed, err = db.CompactRuntimeEvidence(context.Background(), 1)
	if err != nil || processed != 0 {
		t.Fatalf("repeat compaction processed=%d error=%v", processed, err)
	}
	rows, err := db.ListRuntimeEvidence("main", 1)
	if err != nil || len(rows) != 3 {
		t.Fatalf("runtime rows=%d error=%v", len(rows), err)
	}
	for i, row := range rows {
		var payload reporting.RuntimePayload
		if err := json.Unmarshal(row.RuntimeJSON, &payload); err != nil {
			t.Fatal(err)
		}
		want := 1
		if i == 2 {
			want = 0
		}
		if len(payload.ProfileIncidents) != want {
			t.Fatalf("row %d incidents=%d want=%d", i, len(payload.ProfileIncidents), want)
		}
	}
}

func TestPruneArchivedEvidenceKeepsComparisonBaseAndUnarchivedDeployments(t *testing.T) {
	db := seriesTestDB(t)
	old := time.Now().UTC().AddDate(0, 0, -45)
	ids := make([]int, 0, 4)
	for _, sha := range []string{"archived", "unarchived", "baseline", "current"} {
		if err := db.UpsertReadyDeployment(&Deployment{Source: "main", GitSHA: sha, LitestreamSHA: sha, Status: "ready"}); err != nil {
			t.Fatal(err)
		}
		deployment, err := db.GetLatestDeployment("main")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, deployment.ID)
		identity := reporting.WorkerIdentity{Source: "main", DeploymentID: deployment.ID, WorkerID: "worker", RunID: sha}
		body, err := json.Marshal(reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{
			ProfileCapability: "observed",
			ProfileIncidents:  []reporting.ProfileIncident{{ID: sha, Run: identity, Kind: "profile_capture_error", Message: sha}},
			ProfileRecords:    []reporting.ProfileRecordEvidence{{DeploymentID: deployment.ID, WorkerID: "worker", RunID: sha, Artifact: sha}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordRuntimeEvidence(identity, body, true); err != nil {
			t.Fatal(err)
		}
		if _, err := db.writer.Exec(`UPDATE evidence_runtime SET received_at=? WHERE deployment_id=?`, old, deployment.ID); err != nil {
			t.Fatal(err)
		}
		identityJSON, _ := json.Marshal(identity)
		if _, err := db.writer.Exec(`INSERT INTO evidence_verifications(worker_id,started_at,completed_at,run_identity_json,evidence_source) VALUES (?,?,?,?,?)`, "worker", old, old, string(identityJSON), "main"); err != nil {
			t.Fatal(err)
		}
		details, _ := json.Marshal(map[string]any{"deployment_id": deployment.ID})
		if _, err := db.writer.Exec(`INSERT INTO evidence_events(worker_id,event_type,message,details,created_at,evidence_source) VALUES (?,?,?,?,?,?)`, "worker", "test", sha, string(details), old, "main"); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int{ids[0], ids[2], ids[3]} {
		if _, err := db.RecordRunArchive(&RunArchive{DeploymentID: id, Source: "main", ArchiveType: "success"}); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := db.PruneArchivedEvidenceBefore(context.Background(), time.Now().UTC().AddDate(0, 0, -30))
	if err != nil || counts["runtime"] != 1 || counts["verifications"] != 1 || counts["events"] != 1 {
		t.Fatalf("prune counts=%v error=%v", counts, err)
	}
	var retained, incidentCount, recordCount int
	if err := db.queryRow(`SELECT count(*) FROM evidence_runtime`).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if err := db.queryRow(`SELECT count(*) FROM evidence_profile_incidents`).Scan(&incidentCount); err != nil {
		t.Fatal(err)
	}
	if err := db.queryRow(`SELECT count(*) FROM evidence_profile_records`).Scan(&recordCount); err != nil {
		t.Fatal(err)
	}
	if retained != 3 || incidentCount != 3 || recordCount != 3 {
		t.Fatalf("retained runtime=%d incidents=%d records=%d", retained, incidentCount, recordCount)
	}
	counts, err = db.PruneArchivedEvidenceBefore(context.Background(), time.Now().UTC().AddDate(0, 0, -30))
	if err != nil || counts["runtime"] != 0 {
		t.Fatalf("repeat prune counts=%v error=%v", counts, err)
	}
}

func TestPruneArchivedEvidenceWaitsForEntireRuntimeWindow(t *testing.T) {
	db := seriesTestDB(t)
	var oldDeployment int
	for _, sha := range []string{"old", "baseline", "current"} {
		if err := db.UpsertReadyDeployment(&Deployment{Source: "main", GitSHA: sha, LitestreamSHA: sha, Status: "ready"}); err != nil {
			t.Fatal(err)
		}
		deployment, err := db.GetLatestDeployment("main")
		if err != nil {
			t.Fatal(err)
		}
		if sha == "old" {
			oldDeployment = deployment.ID
		}
	}
	if _, err := db.RecordRunArchive(&RunArchive{DeploymentID: oldDeployment, Source: "main", ArchiveType: "success"}); err != nil {
		t.Fatal(err)
	}
	identity := reporting.WorkerIdentity{Source: "main", DeploymentID: oldDeployment, WorkerID: "worker", RunID: "run"}
	body, _ := json.Marshal(reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "observed", ProfileIncidents: []reporting.ProfileIncident{{ID: "first", Run: identity, Kind: "profile_capture_error"}}}})
	for i := 0; i < 2; i++ {
		if err := db.RecordRuntimeEvidence(identity, body, true); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -30)
	old := cutoff.AddDate(0, 0, -15)
	if _, err := db.writer.Exec(`UPDATE evidence_runtime SET received_at=? WHERE id=(SELECT min(id) FROM evidence_runtime)`, old); err != nil {
		t.Fatal(err)
	}
	counts, err := db.PruneArchivedEvidenceBefore(context.Background(), cutoff)
	if err != nil || counts["runtime"] != 0 {
		t.Fatalf("partial-window prune counts=%v error=%v", counts, err)
	}
	if _, err := db.writer.Exec(`UPDATE evidence_runtime SET received_at=? WHERE deployment_id=?`, old, oldDeployment); err != nil {
		t.Fatal(err)
	}
	counts, err = db.PruneArchivedEvidenceBefore(context.Background(), cutoff)
	if err != nil || counts["runtime"] != 2 {
		t.Fatalf("complete-window prune counts=%v error=%v", counts, err)
	}
	var remaining int
	if err := db.queryRow(`SELECT count(*) FROM evidence_profile_incidents`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("remaining incidents=%d", remaining)
	}
}

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

func TestCompletedEvidenceBackfillDoesNotRevisitRawRows(t *testing.T) {
	db := seriesTestDB(t)
	if _, err := db.writer.Exec(`INSERT INTO events(event_type,message,details,created_at) VALUES ('startup_failure','recovered','{}',datetime('now')); CREATE TRIGGER reject_repeated_backfill BEFORE INSERT ON evidence_events BEGIN SELECT RAISE(FAIL, 'backfill repeated'); END;`); err != nil {
		t.Fatal(err)
	}
	if err := ensureEvidenceJournal(db.writer); err != nil {
		t.Fatalf("completed backfill revisited retained rows: %v", err)
	}
}

func TestEvidenceUpgradeFailureDoesNotMarkBackfillComplete(t *testing.T) {
	db := seriesTestDB(t)
	if _, err := db.writer.Exec(`DELETE FROM evidence_migrations; INSERT INTO events(event_type,message,details,created_at) VALUES ('provider_retry','recovered','{}',datetime('now')); CREATE TRIGGER reject_upgrade BEFORE INSERT ON evidence_events BEGIN SELECT RAISE(FAIL,'interrupted upgrade'); END;`); err != nil {
		t.Fatal(err)
	}
	if err := ensureEvidenceJournal(db.writer); err == nil {
		t.Fatal("upgrade unexpectedly succeeded")
	}
	var count int
	if err := db.reader.QueryRow(`SELECT count(*) FROM evidence_migrations`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed upgrade marked complete: %d %v", count, err)
	}
	if _, err := db.writer.Exec(`DROP TRIGGER reject_upgrade`); err != nil {
		t.Fatal(err)
	}
	if err := ensureEvidenceJournal(db.writer); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListEvidenceEvents("main")
	if err != nil || len(events) != 1 || events[0].Message != "recovered" {
		t.Fatalf("lost or duplicated upgrade evidence: %+v %v", events, err)
	}
}
