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

type archivedEvidenceDeploymentRef struct {
	id     int
	source string
}

// archivedEvidenceDeployments lists the deployments whose evidence may be
// pruned: archived, not one of their source's two newest, and with no
// runtime evidence newer than the cutoff. The deployments table is small, so
// deciding eligibility here once keeps the per-row scans below off the
// evidence tables, which hold millions of rows.
func (d *DB) archivedEvidenceDeployments(ctx context.Context, cutoff time.Time) ([]archivedEvidenceDeploymentRef, error) {
	rows, err := d.reader.QueryContext(ctx, `SELECT d.id, d.source FROM deployments d WHERE `+archivedEvidenceDeployment+` ORDER BY d.id`, cutoff)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var refs []archivedEvidenceDeploymentRef
	for rows.Next() {
		var ref archivedEvidenceDeploymentRef
		if err := rows.Scan(&ref.id, &ref.source); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

// archivedEvidenceDeploymentStillEligible is re-evaluated inside every batch
// statement, so a deployment that stops being eligible while the prune runs
// (a heartbeat arrives, or it becomes one of the two newest) keeps the rest
// of its evidence from that batch on.
const archivedEvidenceDeploymentStillEligible = `EXISTS (SELECT 1 FROM deployments d WHERE d.id=? AND ` + archivedEvidenceDeployment + `)`

// pruneBatchHook runs before every batch; tests use it to change the world
// between batches.
var pruneBatchHook func()

// PruneArchivedEvidenceBefore deletes the evidence of eligible deployments
// older than the cutoff in small batches. Every batch is one short statement
// or transaction driven by the evidence window indexes (source, deployment,
// time), so heartbeat and report writes interleave between batches instead
// of waiting behind a scan of the whole evidence journal, and every batch
// re-checks the deployment's eligibility. Rows journaled without a source
// but with a valid deployment id (legacy evidence) belong to the deployment
// as well and are pruned with it.
func (d *DB) PruneArchivedEvidenceBefore(ctx context.Context, cutoff time.Time) (map[string]int64, error) {
	counts := map[string]int64{"runtime": 0, "verifications": 0, "events": 0}
	deployments, err := d.archivedEvidenceDeployments(ctx, cutoff)
	if err != nil {
		return counts, fmt.Errorf("list archived evidence deployments: %w", err)
	}
	for _, deployment := range deployments {
		for _, spec := range []struct {
			name  string
			query string
		}{
			{"verifications", `DELETE FROM evidence_verifications WHERE id IN (
				SELECT id FROM evidence_verifications WHERE evidence_source IN (?, '') AND ` + verificationDeployment + `=?
				AND julianday(COALESCE(completed_at,started_at))<julianday(?) AND ` + archivedEvidenceDeploymentStillEligible + ` LIMIT ?)`},
			{"events", `DELETE FROM evidence_events WHERE journal_id IN (
				SELECT journal_id FROM evidence_events WHERE evidence_source IN (?, '') AND ` + eventDeployment + `=?
				AND julianday(created_at)<julianday(?) AND ` + archivedEvidenceDeploymentStillEligible + ` LIMIT ?)`},
		} {
			for {
				if err := ctx.Err(); err != nil {
					return counts, err
				}
				if pruneBatchHook != nil {
					pruneBatchHook()
				}
				result, err := d.writer.ExecContext(ctx, spec.query, deployment.source, deployment.id, cutoff, deployment.id, cutoff, evidencePruneBatchSize)
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
		if err := d.pruneArchivedRuntimeEvidence(ctx, deployment, cutoff, counts); err != nil {
			return counts, err
		}
	}
	return counts, nil
}

func (d *DB) pruneArchivedRuntimeEvidence(ctx context.Context, deployment archivedEvidenceDeploymentRef, cutoff time.Time, counts map[string]int64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pruneBatchHook != nil {
			pruneBatchHook()
		}
		tx, err := d.writer.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT id,runtime_json FROM evidence_runtime WHERE source IN (?, '') AND deployment_id=? AND julianday(received_at)<julianday(?) AND `+archivedEvidenceDeploymentStillEligible+` ORDER BY id LIMIT ?`, deployment.source, deployment.id, cutoff, deployment.id, cutoff, evidencePruneBatchSize)
		if err != nil {
			_ = tx.Rollback()
			return err
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
				return err
			}
			batch = append(batch, current)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if len(batch) == 0 {
			_ = tx.Rollback()
			break
		}
		for _, current := range batch {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(current.body), &fields); err != nil {
				_ = tx.Rollback()
				return err
			}
			var refs []int64
			if encoded := fields[profileRecordRefsKey]; len(encoded) > 0 {
				if err := json.Unmarshal(encoded, &refs); err != nil {
					_ = tx.Rollback()
					return err
				}
			}
			uses := make(map[int64]int)
			for _, ref := range refs {
				uses[ref]++
			}
			for ref, n := range uses {
				if _, err := tx.ExecContext(ctx, `UPDATE evidence_profile_records SET uses=uses-? WHERE id=?`, n, ref); err != nil {
					_ = tx.Rollback()
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM evidence_profile_incidents WHERE runtime_id=?`, current.id); err != nil {
				_ = tx.Rollback()
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM evidence_runtime WHERE id=?`, current.id); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM evidence_profile_records WHERE uses<=0`); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		counts["runtime"] += int64(len(batch))
	}
	return nil
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
