package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const costMeterSchema = `
CREATE TABLE IF NOT EXISTS cost_meter_state (
    worker_id TEXT PRIMARY KEY,
    source TEXT NOT NULL DEFAULT '',
    observed_at DATETIME NOT NULL,
    counters_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cost_meter_hourly (
    worker_id TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    bucket_start DATETIME NOT NULL,
    totals_json TEXT NOT NULL,
    PRIMARY KEY (worker_id, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_cost_meter_hourly_bucket ON cost_meter_hourly(bucket_start);
CREATE INDEX IF NOT EXISTS idx_cost_meter_hourly_source_bucket ON cost_meter_hourly(source, bucket_start);
CREATE INDEX IF NOT EXISTS idx_cost_meter_state_observed ON cost_meter_state(observed_at);
`

type CostMeterTotals struct {
	ReplicaRequests   map[string]uint64 `json:"replica_requests,omitempty"`
	ReplicaBytes      map[string]uint64 `json:"replica_bytes,omitempty"`
	RestoreRequests   map[string]uint64 `json:"restore_requests,omitempty"`
	Uploads           uint64            `json:"uploads"`
	UploadBytes       uint64            `json:"upload_bytes"`
	PeakDiskUsedBytes uint64            `json:"peak_disk_used_bytes"`
}

type CostMeterWorker struct {
	WorkerID string          `json:"worker_id"`
	Totals   CostMeterTotals `json:"totals"`
}

type CostMeterSource struct {
	Source                     string            `json:"source"`
	Totals                     CostMeterTotals   `json:"totals"`
	WorkerPeakDiskUsedBytesSum uint64            `json:"worker_peak_disk_used_bytes_sum"`
	Workers                    []CostMeterWorker `json:"workers"`
}

func (t CostMeterTotals) IsZero() bool {
	return len(t.ReplicaRequests) == 0 && len(t.ReplicaBytes) == 0 && len(t.RestoreRequests) == 0 && t.Uploads == 0 && t.UploadBytes == 0 && t.PeakDiskUsedBytes == 0
}

func (t *CostMeterTotals) Add(other CostMeterTotals) {
	t.ReplicaRequests = addCounterMap(t.ReplicaRequests, other.ReplicaRequests)
	t.ReplicaBytes = addCounterMap(t.ReplicaBytes, other.ReplicaBytes)
	t.RestoreRequests = addCounterMap(t.RestoreRequests, other.RestoreRequests)
	t.Uploads += other.Uploads
	t.UploadBytes += other.UploadBytes
	t.PeakDiskUsedBytes = max(t.PeakDiskUsedBytes, other.PeakDiskUsedBytes)
}

func addCounterMap(dst, src map[string]uint64) map[string]uint64 {
	for key, value := range src {
		if value == 0 {
			continue
		}
		if dst == nil {
			dst = make(map[string]uint64)
		}
		dst[key] += value
	}
	return dst
}

func counterDeltas(previous, current map[string]uint64, reset bool) map[string]uint64 {
	var deltas map[string]uint64
	for key, value := range current {
		delta := counterDelta(previous[key], value, reset)
		if delta == 0 {
			continue
		}
		if deltas == nil {
			deltas = make(map[string]uint64)
		}
		deltas[key] = delta
	}
	return deltas
}

func counterDelta(previous, current uint64, reset bool) uint64 {
	if reset || current < previous {
		return current
	}
	return current - previous
}

func mergeCounterMap(previous, current map[string]uint64, reset bool) map[string]uint64 {
	if reset {
		return maps.Clone(current)
	}
	merged := maps.Clone(previous)
	if len(current) > 0 && merged == nil {
		merged = make(map[string]uint64, len(current))
	}
	maps.Copy(merged, current)
	return merged
}

func costMeterDelta(previous *reporting.ObjectStoreCounters, current reporting.ObjectStoreCounters) (CostMeterTotals, reporting.ObjectStoreCounters) {
	if previous == nil {
		previous = &reporting.ObjectStoreCounters{}
	}
	workerReset := previous.ObjectStoreCounterEpoch != "" && current.ObjectStoreCounterEpoch != "" && previous.ObjectStoreCounterEpoch != current.ObjectStoreCounterEpoch
	delta := CostMeterTotals{
		ReplicaRequests: counterDeltas(previous.ReplicaOperationTotals, current.ReplicaOperationTotals, false),
		ReplicaBytes:    counterDeltas(previous.ReplicaOperationBytes, current.ReplicaOperationBytes, false),
		RestoreRequests: counterDeltas(previous.RestoreRequestTotals, current.RestoreRequestTotals, workerReset),
	}
	next := reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch: current.ObjectStoreCounterEpoch,
		ReplicaOperationTotals:  mergeCounterMap(previous.ReplicaOperationTotals, current.ReplicaOperationTotals, false),
		ReplicaOperationBytes:   mergeCounterMap(previous.ReplicaOperationBytes, current.ReplicaOperationBytes, false),
		RestoreRequestTotals:    mergeCounterMap(previous.RestoreRequestTotals, current.RestoreRequestTotals, workerReset),
	}
	if next.ObjectStoreCounterEpoch == "" {
		next.ObjectStoreCounterEpoch = previous.ObjectStoreCounterEpoch
	}
	if current.ObjectStoreCounterEpoch == "" {
		next.EvidenceUploadsTotal = previous.EvidenceUploadsTotal
		next.EvidenceUploadBytesTotal = previous.EvidenceUploadBytesTotal
		return delta, next
	}
	if workerReset || current.EvidenceUploadsTotal < previous.EvidenceUploadsTotal {
		delta.Uploads = current.EvidenceUploadsTotal
		delta.UploadBytes = current.EvidenceUploadBytesTotal
	} else {
		delta.Uploads = current.EvidenceUploadsTotal - previous.EvidenceUploadsTotal
		delta.UploadBytes = counterDelta(previous.EvidenceUploadBytesTotal, current.EvidenceUploadBytesTotal, false)
	}
	next.EvidenceUploadsTotal = current.EvidenceUploadsTotal
	next.EvidenceUploadBytesTotal = current.EvidenceUploadBytesTotal
	return delta, next
}

func (d *DB) RecordCostMeter(workerID, source string, observedAt time.Time, runtime reporting.RuntimePayload) (CostMeterTotals, error) {
	observedAt = observedAt.UTC()
	tx, err := d.writer.Begin()
	if err != nil {
		return CostMeterTotals{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var lastObserved time.Time
	var stateJSON string
	var previous *reporting.ObjectStoreCounters
	err = tx.QueryRow(`SELECT observed_at, counters_json FROM cost_meter_state WHERE worker_id = ?`, workerID).Scan(&lastObserved, &stateJSON)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return CostMeterTotals{}, fmt.Errorf("read cost meter state: %w", err)
	default:
		if observedAt.Before(lastObserved) {
			return CostMeterTotals{}, nil
		}
		previous = &reporting.ObjectStoreCounters{}
		if err := json.Unmarshal([]byte(stateJSON), previous); err != nil {
			return CostMeterTotals{}, fmt.Errorf("decode cost meter state: %w", err)
		}
	}

	delta, next := costMeterDelta(previous, runtime.ObjectStoreCounters)
	encodedState, err := json.Marshal(next)
	if err != nil {
		return CostMeterTotals{}, err
	}
	if _, err := tx.Exec(`
		INSERT INTO cost_meter_state (worker_id, source, observed_at, counters_json) VALUES (?, ?, ?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET source = excluded.source, observed_at = excluded.observed_at, counters_json = excluded.counters_json`,
		workerID, source, observedAt, string(encodedState)); err != nil {
		return CostMeterTotals{}, fmt.Errorf("write cost meter state: %w", err)
	}

	bucketDelta := delta
	bucketDelta.PeakDiskUsedBytes = runtime.DataDiskUsedBytes
	if !bucketDelta.IsZero() {
		bucket := observedAt.Truncate(time.Hour)
		var totals CostMeterTotals
		var totalsJSON string
		err := tx.QueryRow(`SELECT totals_json FROM cost_meter_hourly WHERE worker_id = ? AND bucket_start = ?`, workerID, bucket).Scan(&totalsJSON)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return CostMeterTotals{}, fmt.Errorf("read cost meter bucket: %w", err)
		default:
			if err := json.Unmarshal([]byte(totalsJSON), &totals); err != nil {
				return CostMeterTotals{}, fmt.Errorf("decode cost meter bucket: %w", err)
			}
		}
		before := totals.PeakDiskUsedBytes
		totals.Add(bucketDelta)
		if !delta.IsZero() || totals.PeakDiskUsedBytes != before || totalsJSON == "" {
			encoded, err := json.Marshal(totals)
			if err != nil {
				return CostMeterTotals{}, err
			}
			if _, err := tx.Exec(`
				INSERT INTO cost_meter_hourly (worker_id, source, bucket_start, totals_json) VALUES (?, ?, ?, ?)
				ON CONFLICT(worker_id, bucket_start) DO UPDATE SET source = excluded.source, totals_json = excluded.totals_json`,
				workerID, source, bucket, string(encoded)); err != nil {
				return CostMeterTotals{}, fmt.Errorf("write cost meter bucket: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return CostMeterTotals{}, err
	}
	return delta, nil
}

func (d *DB) CostMeter(source string, since time.Time) ([]CostMeterSource, error) {
	query := `SELECT worker_id, source, totals_json FROM cost_meter_hourly WHERE bucket_start >= ?`
	args := []any{since.UTC().Truncate(time.Hour)}
	if source != "" {
		query += ` AND source = ?`
		args = append(args, source)
	}
	rows, err := d.query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	bySource := make(map[string]map[string]*CostMeterTotals)
	for rows.Next() {
		var workerID, workerSource, totalsJSON string
		if err := rows.Scan(&workerID, &workerSource, &totalsJSON); err != nil {
			return nil, err
		}
		var bucket CostMeterTotals
		if err := json.Unmarshal([]byte(totalsJSON), &bucket); err != nil {
			return nil, fmt.Errorf("decode cost meter bucket: %w", err)
		}
		workers := bySource[workerSource]
		if workers == nil {
			workers = make(map[string]*CostMeterTotals)
			bySource[workerSource] = workers
		}
		totals := workers[workerID]
		if totals == nil {
			totals = &CostMeterTotals{}
			workers[workerID] = totals
		}
		totals.Add(bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	report := make([]CostMeterSource, 0, len(bySource))
	for sourceName, workers := range bySource {
		entry := CostMeterSource{Source: sourceName, Workers: make([]CostMeterWorker, 0, len(workers))}
		for workerID, totals := range workers {
			entry.Totals.Add(*totals)
			entry.WorkerPeakDiskUsedBytesSum += totals.PeakDiskUsedBytes
			entry.Workers = append(entry.Workers, CostMeterWorker{WorkerID: workerID, Totals: *totals})
		}
		sort.Slice(entry.Workers, func(i, j int) bool { return entry.Workers[i].WorkerID < entry.Workers[j].WorkerID })
		report = append(report, entry)
	}
	sort.Slice(report, func(i, j int) bool { return report[i].Source < report[j].Source })
	return report, nil
}

func (d *DB) PruneCostMeterBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	cutoff = cutoff.UTC()
	buckets, err := d.pruneBatched(ctx,
		`DELETE FROM cost_meter_hourly WHERE rowid IN (
			SELECT rowid FROM cost_meter_hourly WHERE bucket_start < ? LIMIT ?)`,
		cutoff)
	if err != nil {
		return buckets, err
	}
	states, err := d.pruneBatched(ctx,
		`DELETE FROM cost_meter_state WHERE rowid IN (
			SELECT rowid FROM cost_meter_state WHERE observed_at < ? LIMIT ?)`,
		cutoff)
	return buckets + states, err
}
