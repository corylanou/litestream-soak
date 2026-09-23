package model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const profileRecordRefsKey = "_profile_record_refs"
const profileIncidentsExternalKey = "_profile_incidents_external"

type profileRecordArrayCache struct {
	key     string
	records []reporting.ProfileRecordEvidence
}

type profileIncidentKey struct {
	source         string
	deploymentID   int
	workerID       string
	runID          string
	identityDigest string
	attributed     bool
	incidentID     string
}

func normalizeRuntimeEvidence(tx *sql.Tx, runtimeID int, identity reporting.WorkerIdentity, attributed bool, raw json.RawMessage, known map[profileIncidentKey]int, recordIDs map[string]int64, recordUses map[int64]int) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return raw, nil
	}
	changed := false
	var capability string
	_ = json.Unmarshal(fields["profile_capability"], &capability)
	if encoded, ok := fields["profile_incidents"]; ok {
		var incidents []reporting.ProfileIncident
		var rawIncidents []json.RawMessage
		if err := json.Unmarshal(encoded, &incidents); err == nil && json.Unmarshal(encoded, &rawIncidents) == nil {
			storedIncidents := 0
			identityJSON, err := json.Marshal(identity)
			if err != nil {
				return nil, err
			}
			if capability == "" {
				identityJSON = append(identityJSON, 0)
			} else {
				identityJSON = append(identityJSON, 1)
			}
			identityHash := sha256.Sum256(identityJSON)
			identityDigest := hex.EncodeToString(identityHash[:])
			for ordinal, incident := range incidents {
				key := profileIncidentKey{identity.Source, identity.DeploymentID, identity.WorkerID, incident.Run.RunID, identityDigest, attributed, incident.ID}
				if first, ok := known[key]; ok && first <= runtimeID {
					continue
				}
				if known != nil {
					known[key] = runtimeID
				}
				result, err := tx.Exec(`INSERT INTO evidence_profile_incidents (source,deployment_id,worker_id,run_id,identity_digest,attributed,incident_id,runtime_id,ordinal,incident_json)
					VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source,deployment_id,worker_id,run_id,identity_digest,attributed,incident_id) DO UPDATE SET
					runtime_id=excluded.runtime_id, ordinal=excluded.ordinal, incident_json=excluded.incident_json
					WHERE excluded.runtime_id < evidence_profile_incidents.runtime_id`,
					identity.Source, identity.DeploymentID, identity.WorkerID, incident.Run.RunID, identityDigest, attributed, incident.ID, runtimeID, ordinal, string(rawIncidents[ordinal]))
				if err != nil {
					return nil, err
				}
				affected, err := result.RowsAffected()
				if err != nil {
					return nil, err
				}
				storedIncidents += int(affected)
			}
			delete(fields, "profile_incidents")
			if storedIncidents > 0 {
				fields[profileIncidentsExternalKey] = json.RawMessage(`true`)
			}
			changed = true
		}
	}
	if encoded, ok := fields["profile_records"]; ok {
		var records []json.RawMessage
		if err := json.Unmarshal(encoded, &records); err == nil {
			if bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
				delete(fields, "profile_records")
				fields[profileRecordRefsKey] = json.RawMessage("null")
				return json.Marshal(fields)
			}
			refs := make([]int64, 0, len(records))
			for _, record := range records {
				digest := sha256.Sum256(record)
				key := hex.EncodeToString(digest[:])
				id, ok := recordIDs[key]
				if !ok {
					uses := 1
					if recordUses != nil {
						uses = 0
					}
					if err := tx.QueryRow(`INSERT INTO evidence_profile_records (digest,record_json,uses) VALUES (?,?,?) ON CONFLICT(digest) DO UPDATE SET uses=uses+? RETURNING id`, key, string(record), uses, uses).Scan(&id); err != nil {
						return nil, err
					}
					if recordIDs != nil {
						recordIDs[key] = id
					}
				} else if recordUses == nil {
					if _, err := tx.Exec(`UPDATE evidence_profile_records SET uses=uses+1 WHERE id=?`, id); err != nil {
						return nil, err
					}
				}
				if recordUses != nil {
					recordUses[id]++
				}
				refs = append(refs, id)
			}
			body, err := json.Marshal(refs)
			if err != nil {
				return nil, err
			}
			delete(fields, "profile_records")
			fields[profileRecordRefsKey] = body
			changed = true
		}
	}
	if !changed {
		return raw, nil
	}
	return json.Marshal(fields)
}

func (d *DB) attachRuntimeProfileEvidence(e *RuntimeEvidence, decoded map[int64]reporting.ProfileRecordEvidence, arrays *profileRecordArrayCache) error {
	if !bytes.Contains(e.RuntimeJSON, []byte(profileRecordRefsKey)) && !bytes.Contains(e.RuntimeJSON, []byte(profileIncidentsExternalKey)) {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(e.RuntimeJSON, &fields); err != nil {
		return err
	}
	if encoded, ok := fields[profileRecordRefsKey]; ok {
		if bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
			e.ProfileRecordsExternal = true
		} else {
			e.ProfileRecordsExternal = true
			key := string(encoded)
			records := arrays.records
			if arrays.key != key {
				var refs []int64
				if err := json.Unmarshal(encoded, &refs); err != nil {
					return err
				}
				records = make([]reporting.ProfileRecordEvidence, 0, len(refs))
				for _, ref := range refs {
					record, ok := decoded[ref]
					if !ok {
						var body string
						if err := d.queryRow(`SELECT record_json FROM evidence_profile_records WHERE id=?`, ref).Scan(&body); err != nil {
							return err
						}
						if err := json.Unmarshal([]byte(body), &record); err != nil {
							return err
						}
						decoded[ref] = record
					}
					records = append(records, record)
				}
				arrays.key = key
				arrays.records = records
				if len(decoded) > 8192 {
					clear(decoded)
				}
			}
			e.ProfileRecords = records
		}
	}
	if _, ok := fields[profileIncidentsExternalKey]; ok {
		e.ProfileIncidentsExternal = true
		rows, err := d.query(`SELECT incident_json FROM evidence_profile_incidents WHERE runtime_id=? ORDER BY ordinal`, e.ID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var body string
			if err := rows.Scan(&body); err != nil {
				_ = rows.Close()
				return err
			}
			var incident reporting.ProfileIncident
			if err := json.Unmarshal([]byte(body), &incident); err != nil {
				_ = rows.Close()
				return err
			}
			e.ProfileIncidents = append(e.ProfileIncidents, incident)
		}
		err = rows.Err()
		_ = rows.Close()
		return err
	}
	return nil
}

func (d *DB) expandRuntimeEvidence(runtimeID int, raw []byte, cache map[int64]json.RawMessage) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return raw, nil
	}
	changed := false
	if encoded, ok := fields[profileRecordRefsKey]; ok {
		if bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
			fields["profile_records"] = json.RawMessage("null")
			delete(fields, profileRecordRefsKey)
			changed = true
		} else {
			var refs []int64
			if err := json.Unmarshal(encoded, &refs); err != nil {
				return nil, err
			}
			records := make([]json.RawMessage, 0, len(refs))
			for _, ref := range refs {
				record, ok := cache[ref]
				if !ok {
					var body string
					if err := d.queryRow(`SELECT record_json FROM evidence_profile_records WHERE id=?`, ref).Scan(&body); err != nil {
						return nil, fmt.Errorf("load profile record %d: %w", ref, err)
					}
					record = json.RawMessage(body)
					cache[ref] = record
				}
				records = append(records, record)
			}
			body, err := json.Marshal(records)
			if err != nil {
				return nil, err
			}
			fields["profile_records"] = body
			delete(fields, profileRecordRefsKey)
			changed = true
		}
	}
	if _, ok := fields[profileIncidentsExternalKey]; ok {
		rows, err := d.query(`SELECT incident_json FROM evidence_profile_incidents WHERE runtime_id=? ORDER BY ordinal`, runtimeID)
		if err != nil {
			return nil, err
		}
		incidents := make([]json.RawMessage, 0)
		for rows.Next() {
			var body string
			if err := rows.Scan(&body); err != nil {
				_ = rows.Close()
				return nil, err
			}
			incidents = append(incidents, json.RawMessage(body))
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(incidents)
		if err != nil {
			return nil, err
		}
		fields["profile_incidents"] = body
		delete(fields, profileIncidentsExternalKey)
		changed = true
	}
	if !changed {
		return raw, nil
	}
	return json.Marshal(fields)
}

func (d *DB) CompactRuntimeEvidence(ctx context.Context, batchSize int) (int64, error) {
	if batchSize < 1 {
		return 0, fmt.Errorf("invalid compaction batch size %d", batchSize)
	}
	var cursor, maxID int
	if err := d.queryRow(`SELECT last_runtime_id,(SELECT coalesce(max(id),0) FROM evidence_runtime) FROM evidence_compaction_progress WHERE id=1`).Scan(&cursor, &maxID); err != nil {
		return 0, err
	}
	if cursor >= maxID {
		return 0, nil
	}
	var total int64
	var removed int64
	known := make(map[profileIncidentKey]int)
	recordIDs := make(map[string]int64)
	recordRows, err := d.query(`SELECT id,digest FROM evidence_profile_records`)
	if err != nil {
		return 0, err
	}
	for recordRows.Next() {
		var id int64
		var digest string
		if err := recordRows.Scan(&id, &digest); err != nil {
			_ = recordRows.Close()
			return 0, err
		}
		recordIDs[digest] = id
	}
	err = recordRows.Err()
	_ = recordRows.Close()
	if err != nil {
		return 0, err
	}
	incidentRows, err := d.query(`SELECT source,deployment_id,worker_id,run_id,identity_digest,attributed,incident_id,runtime_id FROM evidence_profile_incidents`)
	if err != nil {
		return 0, err
	}
	for incidentRows.Next() {
		var key profileIncidentKey
		var runtimeID int
		if err := incidentRows.Scan(&key.source, &key.deploymentID, &key.workerID, &key.runID, &key.identityDigest, &key.attributed, &key.incidentID, &runtimeID); err != nil {
			_ = incidentRows.Close()
			return 0, err
		}
		known[key] = runtimeID
	}
	err = incidentRows.Err()
	_ = incidentRows.Close()
	if err != nil {
		return 0, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if err := d.queryRow(`SELECT last_runtime_id FROM evidence_compaction_progress WHERE id=1`).Scan(&cursor); err != nil {
			return total, err
		}
		if cursor >= maxID {
			slog.Info("Runtime evidence compaction complete", "rows_processed", total, "bytes_removed", removed, "last_runtime_id", cursor)
			return total, nil
		}
		rows, err := d.query(`SELECT id,identity_json,runtime_json,attributed FROM evidence_runtime WHERE id>? AND id<=? ORDER BY id LIMIT ?`, cursor, maxID, batchSize)
		if err != nil {
			return total, err
		}
		type entry struct {
			id         int
			identity   string
			runtime    string
			attributed bool
		}
		batch := make([]entry, 0, batchSize)
		for rows.Next() {
			var item entry
			if err := rows.Scan(&item.id, &item.identity, &item.runtime, &item.attributed); err != nil {
				_ = rows.Close()
				return total, err
			}
			batch = append(batch, item)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return total, err
		}
		if len(batch) == 0 {
			if total > 0 {
				slog.Info("Runtime evidence compaction complete", "rows_processed", total, "bytes_removed", removed)
			}
			return total, nil
		}
		tx, err := d.writer.BeginTx(ctx, nil)
		if err != nil {
			return total, err
		}
		recordUses := make(map[int64]int)
		for _, item := range batch {
			var identity reporting.WorkerIdentity
			if err := json.Unmarshal([]byte(item.identity), &identity); err != nil {
				_ = tx.Rollback()
				return total, err
			}
			compact, err := normalizeRuntimeEvidence(tx, item.id, identity, item.attributed, json.RawMessage(item.runtime), known, recordIDs, recordUses)
			if err != nil {
				_ = tx.Rollback()
				return total, err
			}
			if string(compact) != item.runtime {
				removed += int64(len(item.runtime) - len(compact))
				if _, err := tx.ExecContext(ctx, `UPDATE evidence_runtime SET runtime_json=? WHERE id=?`, string(compact), item.id); err != nil {
					_ = tx.Rollback()
					return total, err
				}
			}
		}
		for id, uses := range recordUses {
			if _, err := tx.ExecContext(ctx, `UPDATE evidence_profile_records SET uses=uses+? WHERE id=?`, uses, id); err != nil {
				_ = tx.Rollback()
				return total, err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE evidence_compaction_progress SET last_runtime_id=? WHERE id=1`, batch[len(batch)-1].id); err != nil {
			_ = tx.Rollback()
			return total, err
		}
		if err := tx.Commit(); err != nil {
			return total, err
		}
		total += int64(len(batch))
		if total%1000 < int64(len(batch)) {
			slog.Info("Runtime evidence compaction progress", "rows_processed", total, "bytes_removed", removed, "last_runtime_id", batch[len(batch)-1].id)
		}
		if len(batch) < batchSize {
			slog.Info("Runtime evidence compaction complete", "rows_processed", total, "bytes_removed", removed, "last_runtime_id", batch[len(batch)-1].id)
			return total, nil
		}
	}
}
