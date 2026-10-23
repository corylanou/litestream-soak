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
	LitestreamObjectOperations   map[string]uint64 `json:"litestream_object_operations,omitempty"`
	LitestreamObjectBytes        map[string]uint64 `json:"litestream_object_bytes,omitempty"`
	ReplicaRequests              map[string]uint64 `json:"replica_requests,omitempty"`
	ReplicaRequestBytesSent      map[string]uint64 `json:"replica_request_bytes_sent,omitempty"`
	ReplicaResponseBytesReceived map[string]uint64 `json:"replica_response_bytes_received,omitempty"`
	RestoreRequests              map[string]uint64 `json:"restore_requests,omitempty"`
	EvidenceObjectUploads        uint64            `json:"evidence_object_uploads"`
	EvidenceUploadPayloadBytes   uint64            `json:"evidence_upload_payload_bytes"`
	PeakDiskUsedBytes            uint64            `json:"peak_disk_used_bytes"`
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
	return len(t.LitestreamObjectOperations) == 0 && len(t.LitestreamObjectBytes) == 0 &&
		len(t.ReplicaRequests) == 0 && len(t.ReplicaRequestBytesSent) == 0 && len(t.ReplicaResponseBytesReceived) == 0 &&
		len(t.RestoreRequests) == 0 && t.EvidenceObjectUploads == 0 && t.EvidenceUploadPayloadBytes == 0 && t.PeakDiskUsedBytes == 0
}

func (t *CostMeterTotals) Add(other CostMeterTotals) {
	t.LitestreamObjectOperations = addCounterMap(t.LitestreamObjectOperations, other.LitestreamObjectOperations)
	t.LitestreamObjectBytes = addCounterMap(t.LitestreamObjectBytes, other.LitestreamObjectBytes)
	t.ReplicaRequests = addCounterMap(t.ReplicaRequests, other.ReplicaRequests)
	t.ReplicaRequestBytesSent = addCounterMap(t.ReplicaRequestBytesSent, other.ReplicaRequestBytesSent)
	t.ReplicaResponseBytesReceived = addCounterMap(t.ReplicaResponseBytesReceived, other.ReplicaResponseBytesReceived)
	t.RestoreRequests = addCounterMap(t.RestoreRequests, other.RestoreRequests)
	t.EvidenceObjectUploads += other.EvidenceObjectUploads
	t.EvidenceUploadPayloadBytes += other.EvidenceUploadPayloadBytes
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

type counterLifetime int

const (
	counterLifetimeUnknown counterLifetime = iota
	counterLifetimeContinued
	counterLifetimeRestarted
)

func classifyCounterLifetime(previousEpoch, currentEpoch string) counterLifetime {
	switch {
	case previousEpoch == "" || currentEpoch == "":
		return counterLifetimeUnknown
	case previousEpoch == currentEpoch:
		return counterLifetimeContinued
	default:
		return counterLifetimeRestarted
	}
}

func advanceCounter(previous, current uint64, lifetime counterLifetime) (uint64, uint64) {
	switch {
	case lifetime == counterLifetimeRestarted:
		return current, current
	case current >= previous:
		return current - previous, current
	case lifetime == counterLifetimeUnknown:
		return current, current
	default:
		return 0, previous
	}
}

func advanceCounterMap(previous, current map[string]uint64, lifetime counterLifetime) (map[string]uint64, map[string]uint64) {
	next := maps.Clone(previous)
	if lifetime == counterLifetimeRestarted {
		next = nil
	}
	var deltas map[string]uint64
	for key, value := range current {
		base := uint64(0)
		if lifetime != counterLifetimeRestarted {
			base = previous[key]
		}
		delta, advanced := advanceCounter(base, value, lifetime)
		if next == nil {
			next = make(map[string]uint64, len(current))
		}
		next[key] = advanced
		if delta == 0 {
			continue
		}
		if deltas == nil {
			deltas = make(map[string]uint64)
		}
		deltas[key] = delta
	}
	return deltas, next
}

func costMeterDelta(previous *reporting.ObjectStoreCounters, current reporting.ObjectStoreCounters) (CostMeterTotals, reporting.ObjectStoreCounters) {
	if previous == nil {
		previous = &reporting.ObjectStoreCounters{}
	}
	var delta CostMeterTotals
	next := *previous
	workerLifetime := classifyCounterLifetime(previous.ObjectStoreCounterEpoch, current.ObjectStoreCounterEpoch)
	if workerLifetime == counterLifetimeRestarted {
		next.LitestreamCounterEpoch = ""
		next.LitestreamObjectOperationTotals = nil
		next.LitestreamObjectOperationBytes = nil
	}

	if current.LitestreamObjectOperationTotals != nil || current.LitestreamObjectOperationBytes != nil {
		lifetime := classifyCounterLifetime(next.LitestreamCounterEpoch, current.LitestreamCounterEpoch)
		delta.LitestreamObjectOperations, next.LitestreamObjectOperationTotals = advanceCounterMap(next.LitestreamObjectOperationTotals, current.LitestreamObjectOperationTotals, lifetime)
		delta.LitestreamObjectBytes, next.LitestreamObjectOperationBytes = advanceCounterMap(next.LitestreamObjectOperationBytes, current.LitestreamObjectOperationBytes, lifetime)
		if current.LitestreamCounterEpoch != "" {
			next.LitestreamCounterEpoch = current.LitestreamCounterEpoch
		}
	}

	if current.ObjectStoreCounterEpoch == "" {
		return delta, next
	}
	next.ObjectStoreCounterEpoch = current.ObjectStoreCounterEpoch
	delta.ReplicaRequests, next.ReplicaRequestTotals = advanceCounterMap(previous.ReplicaRequestTotals, current.ReplicaRequestTotals, workerLifetime)
	delta.ReplicaRequestBytesSent, next.ReplicaRequestBytesSent = advanceCounterMap(previous.ReplicaRequestBytesSent, current.ReplicaRequestBytesSent, workerLifetime)
	delta.ReplicaResponseBytesReceived, next.ReplicaResponseBytesReceived = advanceCounterMap(previous.ReplicaResponseBytesReceived, current.ReplicaResponseBytesReceived, workerLifetime)
	delta.RestoreRequests, next.RestoreRequestTotals = advanceCounterMap(previous.RestoreRequestTotals, current.RestoreRequestTotals, workerLifetime)
	delta.EvidenceObjectUploads, next.EvidenceObjectUploadsTotal = advanceCounter(previous.EvidenceObjectUploadsTotal, current.EvidenceObjectUploadsTotal, workerLifetime)
	delta.EvidenceUploadPayloadBytes, next.EvidenceUploadPayloadBytesTotal = advanceCounter(previous.EvidenceUploadPayloadBytesTotal, current.EvidenceUploadPayloadBytesTotal, workerLifetime)
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
