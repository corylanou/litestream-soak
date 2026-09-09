package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const evidenceVerificationColumns = "id, worker_id, started_at, completed_at, status, check_type, source_checksum, restored_checksum, passed, duration_ms, error_message, failure_classification_json, run_identity_json, attributed"

type EvidenceVerification struct {
	Verification
	Runtime         reporting.RuntimePayload `json:"runtime"`
	HistoryComplete bool                     `json:"history_complete"`
}

func ensureEvidenceJournal(db *sql.DB) error {
	columns := strings.Split(evidenceVerificationColumns, ", ")
	qualified := "v." + strings.Join(columns, ", v.")
	_, err := db.Exec(fmt.Sprintf(`
 CREATE TABLE IF NOT EXISTS evidence_runtime (id INTEGER PRIMARY KEY, source TEXT NOT NULL, deployment_id INTEGER NOT NULL, identity_json TEXT NOT NULL, runtime_json TEXT NOT NULL, attributed BOOLEAN NOT NULL, received_at DATETIME NOT NULL);
 CREATE INDEX IF NOT EXISTS evidence_runtime_deployment ON evidence_runtime(source,deployment_id);
 CREATE TABLE IF NOT EXISTS evidence_metadata (id INTEGER PRIMARY KEY, started_at DATETIME NOT NULL);
 INSERT OR IGNORE INTO evidence_metadata VALUES (1, datetime('now'));
 CREATE TABLE IF NOT EXISTS evidence_verifications (
 id INTEGER PRIMARY KEY, worker_id TEXT, started_at DATETIME, completed_at DATETIME, status TEXT, check_type TEXT, source_checksum TEXT, restored_checksum TEXT, passed BOOLEAN, duration_ms INTEGER, error_message TEXT, failure_classification_json TEXT, run_identity_json TEXT, attributed BOOLEAN, evidence_source TEXT, evidence_region TEXT, evidence_profile TEXT, evidence_runtime TEXT, history_complete BOOLEAN
 );
 CREATE INDEX IF NOT EXISTS evidence_verification_source ON evidence_verifications(evidence_source);
 INSERT OR IGNORE INTO evidence_verifications SELECT %[1]s, COALESCE(w.source,''), w.region, w.profile_name, w.last_runtime_json, 0 FROM verifications v LEFT JOIN workers w ON w.id=v.worker_id;
 CREATE TRIGGER IF NOT EXISTS journal_verification AFTER INSERT ON verifications BEGIN
 INSERT INTO evidence_verifications SELECT %[1]s, COALESCE(NULLIF(json_extract(v.run_identity_json,'$.source'),''),w.source,''), COALESCE(NULLIF(json_extract(v.run_identity_json,'$.region'),''),w.region,''), COALESCE(NULLIF(json_extract(v.run_identity_json,'$.profile_name'),''),w.profile_name,''), COALESCE(w.last_runtime_json,''), EXISTS(SELECT 1 FROM deployments d, evidence_metadata m WHERE d.id=json_extract(v.run_identity_json,'$.deployment_id') AND julianday(d.started_at)>=julianday(m.started_at)) FROM verifications v LEFT JOIN workers w ON w.id=v.worker_id WHERE v.id=NEW.id;
 END;
 CREATE TABLE IF NOT EXISTS evidence_events (journal_id INTEGER PRIMARY KEY, id INTEGER, worker_id TEXT, event_type TEXT, message TEXT, details TEXT, created_at DATETIME, evidence_source TEXT, UNIQUE(id,created_at,details));
 CREATE INDEX IF NOT EXISTS evidence_event_source ON evidence_events(evidence_source);
 INSERT OR IGNORE INTO evidence_events (id,worker_id,event_type,message,details,created_at,evidence_source) SELECT e.id,e.worker_id,e.event_type,e.message,COALESCE(e.details,''),e.created_at,COALESCE(w.source,'') FROM events e LEFT JOIN workers w ON w.id=e.worker_id;
 CREATE TRIGGER IF NOT EXISTS journal_event AFTER INSERT ON events BEGIN
 INSERT OR IGNORE INTO evidence_events (id,worker_id,event_type,message,details,created_at,evidence_source) SELECT e.id,e.worker_id,e.event_type,e.message,COALESCE(e.details,''),e.created_at,COALESCE(w.source,'') FROM events e LEFT JOIN workers w ON w.id=e.worker_id WHERE e.id=NEW.id;
 END;
 CREATE TRIGGER IF NOT EXISTS journal_event_update AFTER UPDATE ON events BEGIN
 INSERT OR IGNORE INTO evidence_events (id,worker_id,event_type,message,details,created_at,evidence_source) SELECT e.id,e.worker_id,e.event_type,e.message,COALESCE(e.details,''),e.created_at,COALESCE(w.source,'') FROM events e LEFT JOIN workers w ON w.id=e.worker_id WHERE e.id=NEW.id;
 END;
 `, qualified))
	return err
}

func (d *DB) ListEvidenceVerifications(source string) ([]EvidenceVerification, error) {
	rows, err := d.query(`SELECT `+evidenceVerificationColumns+`, COALESCE(evidence_region,''), COALESCE(evidence_profile,''), COALESCE(evidence_runtime,''), history_complete FROM evidence_verifications WHERE evidence_source=? ORDER BY COALESCE(completed_at, started_at), id`, source)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var records []EvidenceVerification
	for rows.Next() {
		var v EvidenceVerification
		var completed sql.NullTime
		var classification, identity, region, profile, runtime string
		if err := rows.Scan(&v.ID, &v.WorkerID, &v.StartedAt, &completed, &v.Status, &v.CheckType, &v.SourceChecksum, &v.RestoredChecksum, &v.Passed, &v.DurationMS, &v.ErrorMessage, &classification, &identity, &v.Attributed, &region, &profile, &runtime, &v.HistoryComplete); err != nil {
			return nil, err
		}
		if completed.Valid {
			v.CompletedAt = &completed.Time
		}
		if err := json.Unmarshal([]byte(identity), &v.Run); err != nil {
			return nil, fmt.Errorf("decode evidence identity: %w", err)
		}
		v.FailureClassification, err = decodeFailureClassification(classification)
		if err != nil {
			return nil, err
		}
		if v.Run.Region == "" {
			v.Run.Region = region
		}
		if v.Run.ProfileName == "" {
			v.Run.ProfileName = profile
		}
		if runtime != "" {
			if err := json.Unmarshal([]byte(runtime), &v.Runtime); err != nil {
				return nil, fmt.Errorf("decode evidence runtime: %w", err)
			}
		}
		records = append(records, v)
	}
	return records, rows.Err()
}

func (d *DB) ListEvidenceEvents(source string) ([]Event, error) {
	rows, err := d.query(`SELECT id, COALESCE(worker_id,''), event_type, message, COALESCE(details,''), created_at FROM evidence_events WHERE evidence_source=? ORDER BY created_at,id`, source)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.WorkerID, &e.EventType, &e.Message, &e.Details, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

type RuntimeEvidence struct {
	ID          int                      `json:"id"`
	Run         reporting.WorkerIdentity `json:"run"`
	RuntimeJSON json.RawMessage          `json:"runtime"`
	Attributed  bool                     `json:"attributed"`
	ReceivedAt  time.Time                `json:"received_at"`
}

func (d *DB) RecordRuntimeEvidence(identity reporting.WorkerIdentity, runtimeJSON json.RawMessage, attributed bool) error {
	if !json.Valid(runtimeJSON) {
		return fmt.Errorf("invalid runtime evidence JSON")
	}
	body, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	_, err = d.exec(`INSERT INTO evidence_runtime (source,deployment_id,identity_json,runtime_json,attributed,received_at) VALUES (?,?,?,?,?,?)`, identity.Source, identity.DeploymentID, string(body), string(runtimeJSON), attributed, time.Now().UTC())
	return err
}

func (d *DB) ListRuntimeEvidence(source string, deploymentID int) ([]RuntimeEvidence, error) {
	rows, err := d.query(`SELECT id,identity_json,runtime_json,attributed,received_at FROM evidence_runtime WHERE source=? AND deployment_id IN (?,0) ORDER BY id`, source, deploymentID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var records []RuntimeEvidence
	for rows.Next() {
		var e RuntimeEvidence
		var identity, runtime string
		if err := rows.Scan(&e.ID, &identity, &runtime, &e.Attributed, &e.ReceivedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(identity), &e.Run); err != nil {
			return nil, err
		}
		e.RuntimeJSON = json.RawMessage(runtime)
		records = append(records, e)
	}
	return records, rows.Err()
}
