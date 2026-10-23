package model

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func costMeterRuntime(epoch string, replica map[string]uint64, replicaBytes map[string]uint64, restore map[string]uint64, uploads, uploadBytes, diskUsed uint64) reporting.RuntimePayload {
	return reporting.RuntimePayload{
		ObjectStoreCounters: reporting.ObjectStoreCounters{
			ObjectStoreCounterEpoch:  epoch,
			ReplicaOperationTotals:   replica,
			ReplicaOperationBytes:    replicaBytes,
			RestoreRequestTotals:     restore,
			EvidenceUploadsTotal:     uploads,
			EvidenceUploadBytesTotal: uploadBytes,
		},
		DataDiskUsedBytes: diskUsed,
	}
}

func TestRecordCostMeterSumsDeltasAcrossCounterResets(t *testing.T) {
	db := seriesTestDB(t)
	base := time.Date(2026, 10, 22, 10, 5, 0, 0, time.UTC)

	steps := []struct {
		at      time.Time
		runtime reporting.RuntimePayload
		want    CostMeterTotals
	}{
		{
			at:      base,
			runtime: costMeterRuntime("worker-a", map[string]uint64{"PUT": 100, "LIST": 40}, map[string]uint64{"PUT": 4096}, map[string]uint64{"GET": 5}, 1, 512, 1000),
			want:    CostMeterTotals{ReplicaRequests: map[string]uint64{"PUT": 100, "LIST": 40}, ReplicaBytes: map[string]uint64{"PUT": 4096}, RestoreRequests: map[string]uint64{"GET": 5}, Uploads: 1, UploadBytes: 512},
		},
		{
			at:      base.Add(15 * time.Second),
			runtime: costMeterRuntime("worker-a", map[string]uint64{"PUT": 130, "LIST": 40}, map[string]uint64{"PUT": 5096}, map[string]uint64{"GET": 8}, 2, 1024, 3000),
			want:    CostMeterTotals{ReplicaRequests: map[string]uint64{"PUT": 30}, ReplicaBytes: map[string]uint64{"PUT": 1000}, RestoreRequests: map[string]uint64{"GET": 3}, Uploads: 1, UploadBytes: 512},
		},
		{
			at:      base.Add(30 * time.Second),
			runtime: costMeterRuntime("worker-a", map[string]uint64{"PUT": 7, "LIST": 2}, map[string]uint64{"PUT": 70}, map[string]uint64{"GET": 8}, 2, 1024, 2000),
			want:    CostMeterTotals{ReplicaRequests: map[string]uint64{"PUT": 7, "LIST": 2}, ReplicaBytes: map[string]uint64{"PUT": 70}},
		},
		{
			at:      base.Add(45 * time.Second),
			runtime: costMeterRuntime("worker-b", map[string]uint64{"PUT": 9, "LIST": 2}, map[string]uint64{"PUT": 90}, map[string]uint64{"GET": 2}, 1, 64, 1500),
			want:    CostMeterTotals{ReplicaRequests: map[string]uint64{"PUT": 2}, ReplicaBytes: map[string]uint64{"PUT": 20}, RestoreRequests: map[string]uint64{"GET": 2}, Uploads: 1, UploadBytes: 64},
		},
		{
			at:      base.Add(10 * time.Second),
			runtime: costMeterRuntime("worker-a", map[string]uint64{"PUT": 500}, nil, nil, 0, 0, 9000),
			want:    CostMeterTotals{},
		},
	}
	for i, step := range steps {
		delta, err := db.RecordCostMeter("w-meter", "main", step.at, step.runtime)
		if err != nil {
			t.Fatalf("step %d: RecordCostMeter() error = %v", i, err)
		}
		if !reflect.DeepEqual(delta, step.want) {
			t.Fatalf("step %d: delta = %+v, want %+v", i, delta, step.want)
		}
	}

	report, err := db.CostMeter("", base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CostMeter() error = %v", err)
	}
	if len(report) != 1 || report[0].Source != "main" || len(report[0].Workers) != 1 {
		t.Fatalf("report = %+v", report)
	}
	want := CostMeterTotals{
		ReplicaRequests:   map[string]uint64{"PUT": 139, "LIST": 42},
		ReplicaBytes:      map[string]uint64{"PUT": 5186},
		RestoreRequests:   map[string]uint64{"GET": 10},
		Uploads:           3,
		UploadBytes:       1088,
		PeakDiskUsedBytes: 3000,
	}
	if got := report[0].Workers[0].Totals; !reflect.DeepEqual(got, want) {
		t.Fatalf("worker totals = %+v, want %+v", got, want)
	}
	if !reflect.DeepEqual(report[0].Totals, want) || report[0].WorkerPeakDiskUsedBytesSum != 3000 {
		t.Fatalf("source totals = %+v sum=%d", report[0].Totals, report[0].WorkerPeakDiskUsedBytesSum)
	}
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
	if got := report[0].Totals.ReplicaRequests["PUT"]; got != 30 {
		t.Fatalf("main PUT in window = %d, want 30", got)
	}
	if report[0].Totals.PeakDiskUsedBytes != 200 || report[0].WorkerPeakDiskUsedBytesSum != 300 {
		t.Fatalf("main peak = %d sum = %d", report[0].Totals.PeakDiskUsedBytes, report[0].WorkerPeakDiskUsedBytesSum)
	}
	if len(report[0].Workers) != 2 || report[0].Workers[0].WorkerID != "w-main-1" || report[0].Workers[0].Totals.ReplicaRequests["PUT"] != 10 {
		t.Fatalf("main workers = %+v", report[0].Workers)
	}

	filtered, err := db.CostMeter("pr-1584", old.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CostMeter(source) error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].Totals.ReplicaRequests["PUT"] != 5 {
		t.Fatalf("filtered = %+v", filtered)
	}

	all, err := db.CostMeter("main", old.Add(-time.Hour))
	if err != nil {
		t.Fatalf("CostMeter(all) error = %v", err)
	}
	if all[0].Totals.ReplicaRequests["PUT"] != 1030 || all[0].Totals.PeakDiskUsedBytes != 9000 {
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
	if delta.ReplicaRequests["PUT"] != 5 {
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
	if len(report) != 1 || report[0].Totals.ReplicaRequests["PUT"] != 35 {
		t.Fatalf("report after prune = %+v", report)
	}
}
