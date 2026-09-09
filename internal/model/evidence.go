package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
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
	started := time.Now()
	slog.Info("Evidence journal initialization started")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var triggers int
	if err := tx.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name IN ('journal_verification','journal_event','journal_event_update')`).Scan(&triggers); err != nil {
		return err
	}
	_, err = tx.Exec(fmt.Sprintf(`
 CREATE TABLE IF NOT EXISTS evidence_runtime (id INTEGER PRIMARY KEY, source TEXT NOT NULL, deployment_id INTEGER NOT NULL, identity_json TEXT NOT NULL, runtime_json TEXT NOT NULL, attributed BOOLEAN NOT NULL, received_at DATETIME NOT NULL, kind TEXT NOT NULL);
 CREATE INDEX IF NOT EXISTS evidence_runtime_deployment ON evidence_runtime(source,deployment_id);
 CREATE TABLE IF NOT EXISTS evidence_metadata (id INTEGER PRIMARY KEY, started_at DATETIME NOT NULL);
 INSERT OR IGNORE INTO evidence_metadata VALUES (1, datetime('now'));
 CREATE TABLE IF NOT EXISTS evidence_verifications (
 id INTEGER PRIMARY KEY, worker_id TEXT, started_at DATETIME, completed_at DATETIME, status TEXT, check_type TEXT, source_checksum TEXT, restored_checksum TEXT, passed BOOLEAN, duration_ms INTEGER, error_message TEXT, failure_classification_json TEXT, run_identity_json TEXT, attributed BOOLEAN, evidence_source TEXT, evidence_region TEXT, evidence_profile TEXT, evidence_runtime TEXT, history_complete BOOLEAN
 );
 CREATE INDEX IF NOT EXISTS evidence_verification_source ON evidence_verifications(evidence_source);
 CREATE TRIGGER IF NOT EXISTS journal_verification AFTER INSERT ON verifications BEGIN
 INSERT INTO evidence_verifications SELECT %[1]s, COALESCE(NULLIF(json_extract(v.run_identity_json,'$.source'),''),w.source,''), COALESCE(NULLIF(json_extract(v.run_identity_json,'$.region'),''),w.region,''), COALESCE(NULLIF(json_extract(v.run_identity_json,'$.profile_name'),''),w.profile_name,''), COALESCE(w.last_runtime_json,''), EXISTS(SELECT 1 FROM deployments d, evidence_metadata m WHERE d.id=json_extract(v.run_identity_json,'$.deployment_id') AND julianday(d.started_at)>=julianday(m.started_at)) FROM verifications v LEFT JOIN workers w ON w.id=v.worker_id WHERE v.id=NEW.id;
 END;
 CREATE TABLE IF NOT EXISTS evidence_events (journal_id INTEGER PRIMARY KEY, id INTEGER, worker_id TEXT, event_type TEXT, message TEXT, details TEXT, created_at DATETIME, evidence_source TEXT, UNIQUE(id,created_at,details));
 CREATE INDEX IF NOT EXISTS evidence_event_source ON evidence_events(evidence_source);
 CREATE TRIGGER IF NOT EXISTS journal_event AFTER INSERT ON events BEGIN
 INSERT OR IGNORE INTO evidence_events (id,worker_id,event_type,message,details,created_at,evidence_source) SELECT e.id,e.worker_id,e.event_type,e.message,COALESCE(e.details,''),e.created_at,COALESCE(NULLIF(CASE WHEN json_valid(e.details) THEN json_extract(e.details,'$.source') END,''),w.source,'') FROM events e LEFT JOIN workers w ON w.id=e.worker_id WHERE e.id=NEW.id;
 END;
 CREATE TRIGGER IF NOT EXISTS journal_event_update AFTER UPDATE ON events BEGIN
 INSERT OR IGNORE INTO evidence_events (id,worker_id,event_type,message,details,created_at,evidence_source) SELECT e.id,e.worker_id,e.event_type,e.message,COALESCE(e.details,''),e.created_at,COALESCE(NULLIF(CASE WHEN json_valid(e.details) THEN json_extract(e.details,'$.source') END,''),w.source,'') FROM events e LEFT JOIN workers w ON w.id=e.worker_id WHERE e.id=NEW.id;
 END;
 `, qualified))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS evidence_migrations (version INTEGER PRIMARY KEY, completed_at DATETIME NOT NULL)`); err != nil {
		return err
	}
	var complete bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM evidence_migrations WHERE version=1)`).Scan(&complete); err != nil {
		return err
	}
	complete = complete && triggers == 3
	if !complete {
		for _, step := range []struct{ name, query string }{
			{"verifications", fmt.Sprintf(`INSERT OR IGNORE INTO evidence_verifications SELECT %[1]s, COALESCE(NULLIF(json_extract(v.run_identity_json,'$.source'),''),w.source,''), w.region, w.profile_name, w.last_runtime_json, 0 FROM verifications v LEFT JOIN workers w ON w.id=v.worker_id;`, qualified)},
			{"events", `INSERT OR IGNORE INTO evidence_events (id,worker_id,event_type,message,details,created_at,evidence_source) SELECT e.id,e.worker_id,e.event_type,e.message,COALESCE(e.details,''),e.created_at,COALESCE(NULLIF(CASE WHEN json_valid(e.details) THEN json_extract(e.details,'$.source') END,''),w.source,'') FROM events e LEFT JOIN workers w ON w.id=e.worker_id;`},
		} {
			stepStart := time.Now()
			slog.Info("Evidence journal backfill started", "journal", step.name)
			result, err := tx.Exec(step.query)
			if err != nil {
				return fmt.Errorf("backfill %s: %w", step.name, err)
			}
			rows, _ := result.RowsAffected()
			slog.Info("Evidence journal backfill finished", "journal", step.name, "rows_inserted", rows, "duration", time.Since(stepStart))
		}
	}
	slog.Info("Evidence journal indexes started")
	if _, err := tx.Exec(evidenceWindowIndexes); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO evidence_migrations VALUES (1,datetime('now'))`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	slog.Info("Evidence journal initialization finished", "backfill_skipped", complete, "duration", time.Since(started))
	return nil
}

const verificationDeployment = "COALESCE(CASE WHEN json_valid(run_identity_json) THEN json_extract(run_identity_json,'$.deployment_id') END,0)"
const eventDeployment = "COALESCE(CASE WHEN json_valid(details) THEN json_extract(details,'$.deployment_id') END,0)"
const evidenceWindowIndexes = `CREATE INDEX IF NOT EXISTS evidence_verification_window ON evidence_verifications(evidence_source,` + verificationDeployment + `,julianday(COALESCE(completed_at,started_at)));
CREATE INDEX IF NOT EXISTS evidence_event_window ON evidence_events(evidence_source,` + eventDeployment + `,julianday(created_at));
CREATE INDEX IF NOT EXISTS evidence_runtime_window ON evidence_runtime(source,deployment_id,julianday(received_at));`

type EvidenceWindow struct {
	DeploymentID int
	Start        time.Time
	End          *time.Time
}

func evidenceWindowQuery(table, columns, deployment, at, order string, source string, windows []EvidenceWindow) (string, []any) {
	base := "SELECT " + columns + " FROM " + table + " WHERE evidence_source IN (?, '')"
	if len(windows) == 0 {
		return base + " ORDER BY " + order, []any{source}
	}
	window := windows[0]
	query := base + " AND " + deployment + "=? UNION ALL " + base + " AND " + deployment + "=0 AND julianday(" + at + ")>=julianday(?)"
	args := []any{source, window.DeploymentID, source, window.Start}
	if window.End != nil {
		query += " AND julianday(" + at + ")<=julianday(?)"
		args = append(args, *window.End)
	}
	return "SELECT * FROM (" + query + ") ORDER BY " + order, args
}

func (d *DB) ListEvidenceVerifications(source string, windows ...EvidenceWindow) ([]EvidenceVerification, error) {
	query, args := evidenceWindowQuery("evidence_verifications", evidenceVerificationColumns+", COALESCE(evidence_region,''), COALESCE(evidence_profile,''), COALESCE(evidence_runtime,''), history_complete", verificationDeployment, "COALESCE(completed_at,started_at)", "COALESCE(completed_at,started_at), id", source, windows)
	rows, err := d.query(query, args...)
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

func (d *DB) ListEvidenceEvents(source string, windows ...EvidenceWindow) ([]Event, error) {
	query, args := evidenceWindowQuery("evidence_events", "id, COALESCE(worker_id,''), event_type, message, COALESCE(details,''), created_at", eventDeployment, "created_at", "created_at,id", source, windows)
	rows, err := d.query(query, args...)
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
	Kind        string                   `json:"kind"`
	ID          int                      `json:"id"`
	Run         reporting.WorkerIdentity `json:"run"`
	RuntimeJSON json.RawMessage          `json:"runtime"`
	Attributed  bool                     `json:"attributed"`
	ReceivedAt  time.Time                `json:"received_at"`
}

func (d *DB) RecordRuntimeEvidence(identity reporting.WorkerIdentity, runtimeJSON json.RawMessage, attributed bool, kinds ...string) error {
	kind := "heartbeat"
	if len(kinds) > 0 {
		kind = kinds[0]
	}
	if !json.Valid(runtimeJSON) {
		return fmt.Errorf("invalid runtime evidence JSON")
	}
	body, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	_, err = d.exec(`INSERT INTO evidence_runtime (source,deployment_id,identity_json,runtime_json,attributed,received_at,kind) VALUES (?,?,?,?,?,?,?)`, identity.Source, identity.DeploymentID, string(body), string(runtimeJSON), attributed, time.Now().UTC(), kind)
	return err
}

func (d *DB) ListRuntimeEvidence(source string, deploymentID int, windows ...EvidenceWindow) ([]RuntimeEvidence, error) {
	query := `SELECT id,identity_json,runtime_json,attributed,received_at,kind FROM evidence_runtime WHERE (source=? OR source='') AND deployment_id IN (?,0) ORDER BY id`
	args := []any{source, deploymentID}
	if len(windows) > 0 {
		query, args = evidenceWindowQuery("evidence_runtime", "id,identity_json,runtime_json,attributed,received_at,kind", "deployment_id", "received_at", "id", source, windows)
		query = strings.ReplaceAll(query, "evidence_source", "source")
	}
	rows, err := d.query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var records []RuntimeEvidence
	for rows.Next() {
		var e RuntimeEvidence
		var identity, runtime string
		if err := rows.Scan(&e.ID, &identity, &runtime, &e.Attributed, &e.ReceivedAt, &e.Kind); err != nil {
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
