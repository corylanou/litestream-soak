package model

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

const archivedEvidenceDeployment = `d.id NOT IN (SELECT recent.id FROM deployments recent WHERE recent.source=d.source ORDER BY recent.started_at DESC,recent.id DESC LIMIT 2)
	AND EXISTS (SELECT 1 FROM run_archives archive WHERE archive.deployment_id=d.id AND archive.worker_id='')
	AND NOT EXISTS (SELECT 1 FROM evidence_runtime newer WHERE newer.deployment_id=d.id AND julianday(newer.received_at)>=julianday(?))`

const evidencePruneBatchSize = 16

func (d *DB) PruneArchivedEvidenceBefore(ctx context.Context, cutoff time.Time) (map[string]int64, error) {
	counts := map[string]int64{"runtime": 0, "verifications": 0, "events": 0}
	for _, spec := range []struct {
		name  string
		query string
	}{
		{"verifications", `DELETE FROM evidence_verifications WHERE id IN (
			SELECT v.id FROM evidence_verifications v JOIN deployments d ON d.id=` + verificationDeployment + `
			WHERE julianday(COALESCE(v.completed_at,v.started_at))<julianday(?) AND ` + archivedEvidenceDeployment + ` LIMIT ?)`},
		{"events", `DELETE FROM evidence_events WHERE journal_id IN (
			SELECT e.journal_id FROM evidence_events e JOIN deployments d ON d.id=` + eventDeployment + `
			WHERE julianday(e.created_at)<julianday(?) AND ` + archivedEvidenceDeployment + ` LIMIT ?)`},
	} {
		for {
			if err := ctx.Err(); err != nil {
				return counts, err
			}
			result, err := d.writer.ExecContext(ctx, spec.query, cutoff, cutoff, evidencePruneBatchSize)
			if err != nil {
				return counts, fmt.Errorf("prune evidence %s: %w", spec.name, err)
			}
			n, err := result.RowsAffected()
			if err != nil {
				return counts, err
			}
			counts[spec.name] += n
			if n < evidencePruneBatchSize {
				break
			}
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return counts, err
		}
		tx, err := d.writer.BeginTx(ctx, nil)
		if err != nil {
			return counts, err
		}
		rows, err := tx.QueryContext(ctx, `SELECT er.id,er.runtime_json FROM evidence_runtime er JOIN deployments d ON d.id=er.deployment_id
			WHERE julianday(er.received_at)<julianday(?) AND `+archivedEvidenceDeployment+` ORDER BY er.id LIMIT ?`, cutoff, cutoff, evidencePruneBatchSize)
		if err != nil {
			_ = tx.Rollback()
			return counts, err
		}
		type item struct {
			id   int
			body string
		}
		batch := make([]item, 0, evidencePruneBatchSize)
		for rows.Next() {
			var current item
			if err := rows.Scan(&current.id, &current.body); err != nil {
				_ = rows.Close()
				_ = tx.Rollback()
				return counts, err
			}
			batch = append(batch, current)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			_ = tx.Rollback()
			return counts, err
		}
		if len(batch) == 0 {
			_ = tx.Rollback()
			break
		}
		for _, current := range batch {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(current.body), &fields); err != nil {
				_ = tx.Rollback()
				return counts, err
			}
			var refs []int64
			if encoded := fields[profileRecordRefsKey]; len(encoded) > 0 {
				if err := json.Unmarshal(encoded, &refs); err != nil {
					_ = tx.Rollback()
					return counts, err
				}
			}
			uses := make(map[int64]int)
			for _, ref := range refs {
				uses[ref]++
			}
			for ref, n := range uses {
				if _, err := tx.ExecContext(ctx, `UPDATE evidence_profile_records SET uses=uses-? WHERE id=?`, n, ref); err != nil {
					_ = tx.Rollback()
					return counts, err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM evidence_profile_incidents WHERE runtime_id=?`, current.id); err != nil {
				_ = tx.Rollback()
				return counts, err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM evidence_runtime WHERE id=?`, current.id); err != nil {
				_ = tx.Rollback()
				return counts, err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM evidence_profile_records WHERE uses<=0`); err != nil {
			_ = tx.Rollback()
			return counts, err
		}
		if err := tx.Commit(); err != nil {
			return counts, err
		}
		counts["runtime"] += int64(len(batch))
	}
	return counts, nil
}

func (d *DB) EvidenceSpace() (map[string]int64, error) {
	values := make(map[string]int64)
	for _, pragma := range []string{"page_count", "freelist_count", "auto_vacuum"} {
		var value int64
		if err := d.queryRow("PRAGMA " + pragma).Scan(&value); err != nil {
			return nil, err
		}
		values[pragma] = value
	}
	return values, nil
}

func (d *DB) IncrementalVacuum(pages int) error {
	if pages < 1 {
		return fmt.Errorf("invalid incremental vacuum pages %d", pages)
	}
	_, err := d.writer.Exec(`PRAGMA incremental_vacuum(` + strconv.Itoa(pages) + `)`)
	return err
}
