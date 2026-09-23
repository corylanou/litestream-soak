package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestCumulativeRuntimeComparisonCost(t *testing.T) {
	if os.Getenv("SOAK_EVIDENCE_COMPARISON_PROOF") != "1" {
		t.Skip("set SOAK_EVIDENCE_COMPARISON_PROOF=1 to build the 13,055-heartbeat fixture")
	}
	const count = 13055
	path := filepath.Join(t.TempDir(), "cumulative.db")
	db, err := model.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, sha := range []string{"base", "head"} {
		if err := db.UpsertReadyDeployment(&model.Deployment{Source: "main", GitSHA: sha, LitestreamSHA: sha, Status: "ready"}); err != nil {
			t.Fatal(err)
		}
	}
	deployment, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	worker := model.Worker{ID: "main-worker", Name: "main-worker", Source: "main", GitSHA: deployment.GitSHA, LitestreamSHA: deployment.LitestreamSHA, ProfileName: "low-volume", ProfileConfig: "{}", FlyMachineID: "machine", Status: model.WorkerRunning}
	createTestWorker(t, db, worker)
	run := fixtureRun(worker, *deployment)
	identity, _ := json.Marshal(run)
	records := make([]reporting.ProfileRecordEvidence, 80)
	for i := range records {
		records[i] = reporting.ProfileRecordEvidence{DeploymentID: run.DeploymentID, WorkerID: run.WorkerID, MachineID: run.MachineID, RunID: run.RunID, Artifact: fmt.Sprintf("cpu-%03d.pprof", i), Status: "captured", Sampling: strings.Repeat("s", 180)}
	}
	connection, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	tx, err := connection.Begin()
	if err != nil {
		t.Fatal(err)
	}
	statement, err := tx.Prepare(`INSERT INTO evidence_runtime(source,deployment_id,identity_json,runtime_json,attributed,received_at,kind) VALUES (?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	incidents := make([]reporting.ProfileIncident, 0, count/20+1)
	for i := 0; i < count; i++ {
		if i%20 == 0 {
			incidents = append(incidents, reporting.ProfileIncident{ID: fmt.Sprintf("incident-%04d", i/20), Kind: "profile_capture_error", Message: "collector exited after transient runtime failure", At: deployment.StartedAt.Add(time.Duration(i) * time.Second), Run: run})
		}
		payload := reporting.RuntimePayload{ProfilingEvidence: reporting.ProfilingEvidence{ProfileCapability: "observed", ProfileHistoryComplete: true, ProfileIncidents: incidents, ProfileRecords: records}, SnapshotCollectedAt: deployment.StartedAt.Add(time.Duration(i+1) * time.Second), LitestreamSnapshotHealthy: true}
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := statement.Exec("main", deployment.ID, string(identity), string(body), true, deployment.StartedAt.Add(time.Duration(i+1)*time.Second), "heartbeat"); err != nil {
			t.Fatal(err)
		}
	}
	_ = statement.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var beforeBytes int64
	if err := connection.QueryRow(`SELECT sum(length(runtime_json)) FROM evidence_runtime`).Scan(&beforeBytes); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	before, err := buildLatestDeploymentComparison(db, "main")
	if err != nil {
		t.Fatal(err)
	}
	beforeTime := time.Since(started)
	started = time.Now()
	processed, err := db.CompactRuntimeEvidence(context.Background(), 8)
	if err != nil || processed != count {
		t.Fatalf("compaction processed=%d error=%v", processed, err)
	}
	t.Logf("compaction_time=%s", time.Since(started))
	var afterBytes, recordBytes, incidentBytes int64
	for _, measurement := range []struct {
		query string
		value *int64
	}{
		{`SELECT sum(length(runtime_json)) FROM evidence_runtime`, &afterBytes},
		{`SELECT coalesce(sum(length(record_json)),0) FROM evidence_profile_records`, &recordBytes},
		{`SELECT coalesce(sum(length(incident_json)),0) FROM evidence_profile_incidents`, &incidentBytes},
	} {
		if err := connection.QueryRow(measurement.query).Scan(measurement.value); err != nil {
			t.Fatal(err)
		}
	}
	started = time.Now()
	after, err := buildLatestDeploymentComparison(db, "main")
	if err != nil {
		t.Fatal(err)
	}
	afterTime := time.Since(started)
	if !reflect.DeepEqual(before.Head.Reliability, after.Head.Reliability) || before.Verdict != after.Verdict {
		t.Fatal("comparison changed after compaction")
	}
	t.Logf("comparison_before=%s runtime_bytes_before=%d comparison_after=%s runtime_bytes_after=%d profile_record_bytes_after=%d profile_incident_bytes_after=%d", beforeTime, beforeBytes, afterTime, afterBytes, recordBytes, incidentBytes)
}

func TestRetainedHistoryComparisonCost(t *testing.T) {
	count := 1000
	if os.Getenv("SOAK_LARGE_EVIDENCE_TEST") == "1" {
		count = 50000
	}
	path := filepath.Join(t.TempDir(), "retained.db")
	db, err := model.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	base := model.Deployment{Source: "main", GitSHA: "base", LitestreamSHA: "base-candidate", Status: "ready"}
	if err := db.UpsertReadyDeployment(&base); err != nil {
		t.Fatal(err)
	}
	deployment := model.Deployment{Source: "main", GitSHA: "soak", LitestreamSHA: "candidate", Status: "ready"}
	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatal(err)
	}
	current, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	worker := model.Worker{ID: "regional", Name: "regional", Source: "main", Region: "ams", ProfileName: "many-db-ams", ProfileConfig: "{}"}
	createTestWorker(t, db, worker)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	tx, err := raw.Begin()
	if err != nil {
		t.Fatal(err)
	}
	identity, _ := json.Marshal(reporting.WorkerIdentity{WorkerID: worker.ID, Source: "main", DeploymentID: current.ID + 100, Region: "ams"})
	runtime := `{"padding":"` + strings.Repeat("x", 8192) + `"}`
	details := `{"deployment_id":999999,"source":"main","padding":"` + strings.Repeat("x", 4096) + `"}`
	stmt, err := tx.Prepare(`INSERT INTO evidence_verifications (id,worker_id,started_at,completed_at,status,check_type,source_checksum,restored_checksum,passed,duration_ms,error_message,failure_classification_json,run_identity_json,attributed,evidence_source,evidence_region,evidence_profile,evidence_runtime,history_complete) VALUES (?, 'regional', ?, ?, 'failed','integrity','','',0,0,'old failure','',?,0,'main','ams','many-db-ams',?,0)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if _, err := stmt.Exec(1000+i, current.StartedAt, current.StartedAt, string(identity), runtime); err != nil {
			t.Fatal(err)
		}
	}
	_ = stmt.Close()
	stmt, err = tx.Prepare(`INSERT INTO evidence_events (id,worker_id,event_type,message,details,created_at,evidence_source) VALUES (?,'regional','provider_retry','old failure',?,?,'main')`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if _, err := stmt.Exec(1000+i, details, current.StartedAt); err != nil {
			t.Fatal(err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	at := current.StartedAt.Add(time.Hour)
	event := reporting.WorkerEventPayload{WorkerIdentity: reporting.WorkerIdentity{WorkerID: worker.ID, Source: "main", DeploymentID: current.ID, Region: "ams", ProfileName: worker.ProfileName}, EventType: "provider_retry", Message: "recovered startup failure"}
	body, _ := json.Marshal(event)
	if err := db.RecordEventAt(worker.ID, event.EventType, event.Message, string(body), at); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SOAK_LARGE_EVIDENCE_TEST") == "1" {
		started := time.Now()
		verifications, err := db.ListEvidenceVerifications("main")
		if err != nil {
			t.Fatal(err)
		}
		events, err := db.ListEvidenceEvents("main")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("previous_unscoped_read=%s verifications=%d events=%d", time.Since(started), len(verifications), len(events))
	}
	var before []byte
	for i := 0; i < 3; i++ {
		started := time.Now()
		result, err := buildLatestDeploymentComparison(db.WithReadContext(context.Background()), "main")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("retained_per_journal=%d comparison_%d=%s", count, i, time.Since(started))
		if len(result.Head.Reliability) != 1 || result.Head.Reliability[0].Eligible || len(result.Head.Reliability[0].Incidents) != 1 || result.Head.Reliability[0].Incidents[0].Message != event.Message {
			t.Fatalf("lost matching adverse evidence: %+v", result.Head.Reliability)
		}
		evidence, _ := json.Marshal(result.Head.Reliability)
		if i == 0 {
			before = evidence
		} else if string(before) != string(evidence) {
			t.Fatal("repeated reads changed evidence")
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`DELETE FROM evidence_migrations; DROP INDEX evidence_verification_window; DROP INDEX evidence_event_window; DROP TRIGGER journal_event; INSERT OR IGNORE INTO events(id,worker_id,event_type,message,details,created_at) SELECT id,worker_id,event_type,message,details,created_at FROM evidence_events;`); err != nil {
		t.Fatal(err)
	}
	upgradeStart := time.Now()
	upgraded, err := model.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("legacy_upgrade=%s", time.Since(upgradeStart))
	result, err := buildLatestDeploymentComparison(upgraded, "main")
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(result.Head.Reliability)
	if string(before) != string(after) {
		t.Fatal("upgrade changed retained evidence")
	}
	_ = upgraded.Close()
	for i := 0; i < 3; i++ {
		started := time.Now()
		reopened, err := model.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("repeat_open_%d=%s", i, time.Since(started))
		_ = reopened.Close()
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("database_bytes=%d", info.Size())
}
