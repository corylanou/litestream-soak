package model

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const activityEventRun = "COALESCE(CASE WHEN json_valid(details) THEN json_extract(details,'$.run_id') END,'')"
const activityEventMachine = "COALESCE(CASE WHEN json_valid(details) THEN json_extract(details,'$.machine_id') END,'')"
const activityEventStart = "julianday(CASE WHEN json_valid(details) THEN json_extract(details,'$.active_verification.started_at') END)"
const activityVerificationRun = "COALESCE(CASE WHEN json_valid(run_identity_json) THEN json_extract(run_identity_json,'$.run_id') END,'')"
const activityVerificationMachine = "COALESCE(CASE WHEN json_valid(run_identity_json) THEN json_extract(run_identity_json,'$.machine_id') END,'')"

const verificationActivityIndexes = `
CREATE INDEX IF NOT EXISTS evidence_activity_start ON evidence_events(worker_id,` + activityEventRun + `,` + activityEventMachine + `,` + activityEventStart + ` DESC, journal_id DESC) WHERE event_type='verification_started';
CREATE INDEX IF NOT EXISTS evidence_activity_completion ON evidence_verifications(worker_id,` + activityVerificationRun + `,` + activityVerificationMachine + `,started_at,check_type) WHERE attributed=1;`

const latestRunVerificationStartQuery = `SELECT id,worker_id,event_type,message,details,created_at FROM evidence_events
WHERE worker_id=? AND ` + activityEventRun + `=? AND ` + activityEventMachine + `=? AND event_type='verification_started'
AND json_extract(details,'$.attributed')=1
ORDER BY ` + activityEventStart + ` DESC,journal_id DESC LIMIT 1`

func (d *DB) LatestRunVerificationStart(identity reporting.WorkerIdentity) (*Event, error) {
	var event Event
	err := d.queryRow(latestRunVerificationStartQuery, identity.WorkerID, identity.RunID, identity.MachineID).Scan(&event.ID, &event.WorkerID, &event.EventType, &event.Message, &event.Details, &event.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &event, nil
}

const runVerificationCompletionQuery = `SELECT worker_id,run_identity_json,started_at,completed_at,status,check_type,attributed FROM evidence_verifications
WHERE worker_id=? AND ` + activityVerificationRun + `=? AND ` + activityVerificationMachine + `=? AND started_at=? AND check_type=? AND attributed=1
AND (completed_at IS NOT NULL OR status<>'running') LIMIT 1`

func (d *DB) RunVerificationCompletion(identity reporting.WorkerIdentity, startedAt time.Time, checkType string) (*Verification, error) {
	var verification Verification
	var raw string
	var completed sql.NullTime
	err := d.queryRow(runVerificationCompletionQuery, identity.WorkerID, identity.RunID, identity.MachineID, startedAt, checkType).Scan(&verification.WorkerID, &raw, &verification.StartedAt, &completed, &verification.Status, &verification.CheckType, &verification.Attributed)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(raw), &verification.Run); err != nil {
		return nil, err
	}
	if completed.Valid {
		verification.CompletedAt = &completed.Time
	}
	return &verification, nil
}
