package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

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
