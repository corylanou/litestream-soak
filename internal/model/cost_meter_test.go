package model

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func costMeterRuntime(epoch string, logical map[string]uint64, logicalBytes map[string]uint64, restore map[string]uint64, uploads, uploadBytes, diskUsed uint64) reporting.RuntimePayload {
	return reporting.RuntimePayload{
		ObjectStoreCounters: reporting.ObjectStoreCounters{
			ObjectStoreCounterEpoch:         epoch,
			LitestreamObjectOperationTotals: logical,
			LitestreamObjectOperationBytes:  logicalBytes,
			RestoreRequestTotals:            restore,
			EvidenceObjectUploadsTotal:      uploads,
			EvidenceUploadPayloadBytesTotal: uploadBytes,
		},
		DataDiskUsedBytes: diskUsed,
	}
}

type costMeterStep struct {
	at       time.Time
	counters reporting.ObjectStoreCounters
	disk     uint64
	want     CostMeterTotals
}

func runCostMeterSteps(t *testing.T, db *DB, workerID string, steps []costMeterStep) {
	t.Helper()
	for i, step := range steps {
		delta, err := db.RecordCostMeter(workerID, "main", step.at, reporting.RuntimePayload{ObjectStoreCounters: step.counters, DataDiskUsedBytes: step.disk})
		if err != nil {
			t.Fatalf("step %d: RecordCostMeter() error = %v", i, err)
		}
		if !reflect.DeepEqual(delta, step.want) {
			t.Fatalf("step %d: delta = %+v, want %+v", i, delta, step.want)
		}
	}
}

func TestRecordCostMeterSumsDeltasAcrossCounterResets(t *testing.T) {
	db := seriesTestDB(t)
	base := time.Date(2026, 10, 22, 10, 5, 0, 0, time.UTC)
	runCostMeterSteps(t, db, "w-meter", []costMeterStep{
		{
			at: base,
			counters: reporting.ObjectStoreCounters{
				ObjectStoreCounterEpoch: "worker-a", LitestreamCounterEpoch: "start:1",
				LitestreamObjectOperationTotals: map[string]uint64{"PUT": 100, "DELETE": 40}, LitestreamObjectOperationBytes: map[string]uint64{"PUT": 4096},
				ReplicaRequestTotals: map[string]uint64{"PUT": 120, "LIST": 30}, ReplicaRequestBytesSent: map[string]uint64{"PUT": 5000}, ReplicaResponseBytesReceived: map[string]uint64{"LIST": 900},
				RestoreRequestTotals: map[string]uint64{"GET": 5}, EvidenceObjectUploadsTotal: 1, EvidenceUploadPayloadBytesTotal: 512,
			},
			disk: 1000,
			want: CostMeterTotals{
				LitestreamObjectOperations: map[string]uint64{"PUT": 100, "DELETE": 40}, LitestreamObjectBytes: map[string]uint64{"PUT": 4096},
				ReplicaRequests: map[string]uint64{"PUT": 120, "LIST": 30}, ReplicaRequestBytesSent: map[string]uint64{"PUT": 5000}, ReplicaResponseBytesReceived: map[string]uint64{"LIST": 900},
				RestoreRequests: map[string]uint64{"GET": 5}, EvidenceObjectUploads: 1, EvidenceUploadPayloadBytes: 512,
			},
		},
		{
			at: base.Add(15 * time.Second),
			counters: reporting.ObjectStoreCounters{
				ObjectStoreCounterEpoch: "worker-a", LitestreamCounterEpoch: "start:1",
				LitestreamObjectOperationTotals: map[string]uint64{"PUT": 130, "DELETE": 40}, LitestreamObjectOperationBytes: map[string]uint64{"PUT": 5096},
				ReplicaRequestTotals: map[string]uint64{"PUT": 150, "LIST": 31}, ReplicaRequestBytesSent: map[string]uint64{"PUT": 6000}, ReplicaResponseBytesReceived: map[string]uint64{"LIST": 930},
				RestoreRequestTotals: map[string]uint64{"GET": 8}, EvidenceObjectUploadsTotal: 2, EvidenceUploadPayloadBytesTotal: 1024,
			},
			disk: 3000,
			want: CostMeterTotals{
				LitestreamObjectOperations: map[string]uint64{"PUT": 30}, LitestreamObjectBytes: map[string]uint64{"PUT": 1000},
				ReplicaRequests: map[string]uint64{"PUT": 30, "LIST": 1}, ReplicaRequestBytesSent: map[string]uint64{"PUT": 1000}, ReplicaResponseBytesReceived: map[string]uint64{"LIST": 30},
				RestoreRequests: map[string]uint64{"GET": 3}, EvidenceObjectUploads: 1, EvidenceUploadPayloadBytes: 512,
			},
		},
		{
			at: base.Add(30 * time.Second),
			counters: reporting.ObjectStoreCounters{
				ObjectStoreCounterEpoch: "worker-a", LitestreamCounterEpoch: "start:2",
				LitestreamObjectOperationTotals: map[string]uint64{"PUT": 140}, LitestreamObjectOperationBytes: map[string]uint64{"PUT": 7000},
				ReplicaRequestTotals: map[string]uint64{"PUT": 150, "LIST": 31}, ReplicaRequestBytesSent: map[string]uint64{"PUT": 6000}, ReplicaResponseBytesReceived: map[string]uint64{"LIST": 930},
				RestoreRequestTotals: map[string]uint64{"GET": 8}, EvidenceObjectUploadsTotal: 2, EvidenceUploadPayloadBytesTotal: 1024,
			},
			disk: 2000,
			want: CostMeterTotals{LitestreamObjectOperations: map[string]uint64{"PUT": 140}, LitestreamObjectBytes: map[string]uint64{"PUT": 7000}},
		},
		{
			at:       base.Add(10 * time.Second),
			counters: reporting.ObjectStoreCounters{ObjectStoreCounterEpoch: "worker-a", LitestreamCounterEpoch: "start:2", LitestreamObjectOperationTotals: map[string]uint64{"PUT": 500}},
			disk:     9000,
			want:     CostMeterTotals{},
		},
	})

	report, err := db.CostMeter("", base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CostMeter() error = %v", err)
	}
	if len(report) != 1 || report[0].Source != "main" || len(report[0].Workers) != 1 {
		t.Fatalf("report = %+v", report)
	}
	want := CostMeterTotals{
		LitestreamObjectOperations:   map[string]uint64{"PUT": 270, "DELETE": 40},
		LitestreamObjectBytes:        map[string]uint64{"PUT": 12096},
		ReplicaRequests:              map[string]uint64{"PUT": 150, "LIST": 31},
		ReplicaRequestBytesSent:      map[string]uint64{"PUT": 6000},
		ReplicaResponseBytesReceived: map[string]uint64{"LIST": 930},
		RestoreRequests:              map[string]uint64{"GET": 8},
		EvidenceObjectUploads:        2,
		EvidenceUploadPayloadBytes:   1024,
		PeakDiskUsedBytes:            3000,
	}
	if got := report[0].Workers[0].Totals; !reflect.DeepEqual(got, want) {
		t.Fatalf("worker totals = %+v, want %+v", got, want)
	}
	if !reflect.DeepEqual(report[0].Totals, want) || report[0].WorkerPeakDiskUsedBytesSum != 3000 {
		t.Fatalf("source totals = %+v sum=%d", report[0].Totals, report[0].WorkerPeakDiskUsedBytesSum)
	}
}

func TestRecordCostMeterResetsLitestreamBaselineOnWorkerRestart(t *testing.T) {
	base := time.Date(2026, 10, 22, 10, 5, 0, 0, time.UTC)
	first := costMeterStep{
		at: base,
		counters: reporting.ObjectStoreCounters{
			ObjectStoreCounterEpoch: "worker-a", LitestreamCounterEpoch: "pid:10",
			LitestreamObjectOperationTotals: map[string]uint64{"PUT": 100}, ReplicaRequestTotals: map[string]uint64{"PUT": 110},
			RestoreRequestTotals: map[string]uint64{"GET": 9}, EvidenceObjectUploadsTotal: 4, EvidenceUploadPayloadBytesTotal: 400,
		},
		want: CostMeterTotals{
			LitestreamObjectOperations: map[string]uint64{"PUT": 100}, ReplicaRequests: map[string]uint64{"PUT": 110},
			RestoreRequests: map[string]uint64{"GET": 9}, EvidenceObjectUploads: 4, EvidenceUploadPayloadBytes: 400,
		},
	}
	restarted := reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch: "worker-b", LitestreamCounterEpoch: "pid:10",
		LitestreamObjectOperationTotals: map[string]uint64{"PUT": 120}, ReplicaRequestTotals: map[string]uint64{"PUT": 130},
		RestoreRequestTotals: map[string]uint64{"GET": 1}, EvidenceObjectUploadsTotal: 5, EvidenceUploadPayloadBytesTotal: 500,
	}
	restartedDelta := CostMeterTotals{
		LitestreamObjectOperations: map[string]uint64{"PUT": 120}, ReplicaRequests: map[string]uint64{"PUT": 130},
		RestoreRequests: map[string]uint64{"GET": 1}, EvidenceObjectUploads: 5, EvidenceUploadPayloadBytes: 500,
	}

	t.Run("immediate first scrape", func(t *testing.T) {
		runCostMeterSteps(t, seriesTestDB(t), "w-immediate", []costMeterStep{first, {at: base.Add(15 * time.Second), counters: restarted, want: restartedDelta}})
	})
	t.Run("delayed first scrape", func(t *testing.T) {
		runCostMeterSteps(t, seriesTestDB(t), "w-delayed", []costMeterStep{
			first,
			{
				at:       base.Add(15 * time.Second),
				counters: reporting.ObjectStoreCounters{ObjectStoreCounterEpoch: "worker-b"},
				want:     CostMeterTotals{},
			},
			{
				at: base.Add(30 * time.Second),
				counters: reporting.ObjectStoreCounters{
					ObjectStoreCounterEpoch: "worker-b", LitestreamCounterEpoch: "pid:10",
					LitestreamObjectOperationTotals: map[string]uint64{"PUT": 120}, ReplicaRequestTotals: map[string]uint64{"PUT": 130},
				},
				want: CostMeterTotals{LitestreamObjectOperations: map[string]uint64{"PUT": 120}, ReplicaRequests: map[string]uint64{"PUT": 130}},
			},
		})
	})
	t.Run("litestream restart without worker restart", func(t *testing.T) {
		runCostMeterSteps(t, seriesTestDB(t), "w-litestream", []costMeterStep{
			first,
			{
				at: base.Add(15 * time.Second),
				counters: reporting.ObjectStoreCounters{
					ObjectStoreCounterEpoch: "worker-a", LitestreamCounterEpoch: "pid:11",
					LitestreamObjectOperationTotals: map[string]uint64{"PUT": 120}, ReplicaRequestTotals: map[string]uint64{"PUT": 140},
					RestoreRequestTotals: map[string]uint64{"GET": 9}, EvidenceObjectUploadsTotal: 4, EvidenceUploadPayloadBytesTotal: 400,
				},
				want: CostMeterTotals{LitestreamObjectOperations: map[string]uint64{"PUT": 120}, ReplicaRequests: map[string]uint64{"PUT": 30}},
			},
		})
	})
	t.Run("missing scrape keeps baseline", func(t *testing.T) {
		runCostMeterSteps(t, seriesTestDB(t), "w-missing", []costMeterStep{
			first,
			{at: base.Add(15 * time.Second), counters: reporting.ObjectStoreCounters{ObjectStoreCounterEpoch: "worker-a"}, want: CostMeterTotals{}},
			{
				at:       base.Add(30 * time.Second),
				counters: reporting.ObjectStoreCounters{ObjectStoreCounterEpoch: "worker-a", LitestreamCounterEpoch: "pid:10", LitestreamObjectOperationTotals: map[string]uint64{"PUT": 104}},
				want:     CostMeterTotals{LitestreamObjectOperations: map[string]uint64{"PUT": 4}},
			},
		})
	})
}

func TestCostMeterAggregatesPerSourceAndWindow(t *testing.T) {
	db := seriesTestDB(t)
	now := time.Now().UTC().Truncate(time.Hour)
	old := now.Add(-48 * time.Hour)

	record := func(worker, source string, at time.Time, puts, disk uint64) {
		t.Helper()
		if _, err := db.RecordCostMeter(worker, source, at, costMeterRuntime("e-"+worker, map[string]uint64{"PUT": puts}, nil, nil, 0, 0, disk)); err != nil {
			t.Fatalf("RecordCostMeter(%s) error = %v", worker, err)
		}
	}
	record("w-main-1", "main", old, 1000, 9000)
	record("w-main-1", "main", now.Add(time.Minute), 1010, 100)
	record("w-main-2", "main", now.Add(2*time.Minute), 20, 200)
	record("w-pr-1", "pr-1584", now.Add(3*time.Minute), 5, 50)

	report, err := db.CostMeter("", now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CostMeter() error = %v", err)
	}
	if len(report) != 2 || report[0].Source != "main" || report[1].Source != "pr-1584" {
		t.Fatalf("sources = %+v", report)
	}
	if got := report[0].Totals.LitestreamObjectOperations["PUT"]; got != 30 {
		t.Fatalf("main PUT in window = %d, want 30", got)
	}
	if report[0].Totals.PeakDiskUsedBytes != 200 || report[0].WorkerPeakDiskUsedBytesSum != 300 {
		t.Fatalf("main peak = %d sum = %d", report[0].Totals.PeakDiskUsedBytes, report[0].WorkerPeakDiskUsedBytesSum)
	}
	if len(report[0].Workers) != 2 || report[0].Workers[0].WorkerID != "w-main-1" || report[0].Workers[0].Totals.LitestreamObjectOperations["PUT"] != 10 {
		t.Fatalf("main workers = %+v", report[0].Workers)
	}

	filtered, err := db.CostMeter("pr-1584", old.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CostMeter(source) error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].Totals.LitestreamObjectOperations["PUT"] != 5 {
		t.Fatalf("filtered = %+v", filtered)
	}

	all, err := db.CostMeter("main", old.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CostMeter(all) error = %v", err)
	}
	if all[0].Totals.LitestreamObjectOperations["PUT"] != 1030 || all[0].Totals.PeakDiskUsedBytes != 9000 {
		t.Fatalf("all-time main = %+v", all[0].Totals)
	}
}

func TestCostMeterSurvivesReopenAndPrunes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meter.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := db.RecordCostMeter("w-reopen", "main", now.Add(-40*24*time.Hour), costMeterRuntime("e", map[string]uint64{"PUT": 50}, nil, nil, 0, 0, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecordCostMeter("w-reopen", "main", now.Add(-time.Minute), costMeterRuntime("e", map[string]uint64{"PUT": 80}, nil, nil, 0, 0, 10)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	delta, err := db.RecordCostMeter("w-reopen", "main", now, costMeterRuntime("e", map[string]uint64{"PUT": 85}, nil, nil, 0, 0, 10))
	if err != nil {
		t.Fatal(err)
	}
	if delta.LitestreamObjectOperations["PUT"] != 5 {
		t.Fatalf("delta after reopen = %+v, want PUT 5", delta)
	}

	deleted, err := db.PruneCostMeterBefore(context.Background(), now.Add(-30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	report, err := db.CostMeter("main", now.Add(-60*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(report) != 1 || report[0].Totals.LitestreamObjectOperations["PUT"] != 35 {
		t.Fatalf("report after prune = %+v", report)
	}
}
