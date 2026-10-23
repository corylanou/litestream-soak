package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func costMeterHeartbeat(workerID, source string, sentAt time.Time, counters reporting.ObjectStoreCounters, diskUsed uint64) reporting.HeartbeatPayload {
	return reporting.HeartbeatPayload{
		WorkerIdentity: reporting.WorkerIdentity{WorkerID: workerID, Name: workerID, Source: source, GitSHA: "sha-meter", ProfileName: "low-volume"},
		SentAt:         sentAt,
		RuntimePayload: reporting.RuntimePayload{
			ObjectStoreCounters:       counters,
			DataDiskUsedBytes:         diskUsed,
			SnapshotCollectedAt:       sentAt,
			LitestreamSnapshotHealthy: true,
		},
	}
}

func postCostMeterHeartbeat(t *testing.T, mux *http.ServeMux, payload reporting.HeartbeatPayload) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/workers/"+payload.WorkerID+"/heartbeat", bytes.NewReader(body)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("heartbeat status = %d: %s", response.Code, response.Body.String())
	}
}

func TestCostMeterHeartbeatsAggregateAndServeAPI(t *testing.T) {
	db := openTestDB(t)
	for _, worker := range []model.Worker{
		{ID: "w-meter-main-1", Name: "w-meter-main-1", Status: model.WorkerRunning, Source: "meter-main", GitSHA: "sha-meter", ProfileName: "low-volume", ProfileConfig: "{}"},
		{ID: "w-meter-main-2", Name: "w-meter-main-2", Status: model.WorkerRunning, Source: "meter-main", GitSHA: "sha-meter", ProfileName: "low-volume", ProfileConfig: "{}"},
		{ID: "w-meter-pr", Name: "w-meter-pr", Status: model.WorkerRunning, Source: "meter-pr", GitSHA: "sha-meter", ProfileName: "low-volume", ProfileConfig: "{}"},
	} {
		if err := db.CreateWorker(&worker); err != nil {
			t.Fatal(err)
		}
	}
	api := NewAPI(db, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	putSeries := controlSourceObjectStoreRequests.WithLabelValues("meter-main", "replica", "PUT")
	putBefore := testutil.ToFloat64(putSeries)
	now := time.Now().UTC()
	postCostMeterHeartbeat(t, mux, costMeterHeartbeat("w-meter-main-1", "meter-main", now.Add(-2*time.Minute), reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch:  "epoch-1",
		ReplicaOperationTotals:   map[string]uint64{"PUT": 100, "LIST": 10},
		ReplicaOperationBytes:    map[string]uint64{"PUT": 1000},
		RestoreRequestTotals:     map[string]uint64{"GET": 4, "LIST": 2},
		EvidenceUploadsTotal:     1,
		EvidenceUploadBytesTotal: 300,
	}, 5000))
	postCostMeterHeartbeat(t, mux, costMeterHeartbeat("w-meter-main-1", "meter-main", now.Add(-time.Minute), reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch:  "epoch-1",
		ReplicaOperationTotals:   map[string]uint64{"PUT": 12, "LIST": 1},
		ReplicaOperationBytes:    map[string]uint64{"PUT": 120},
		RestoreRequestTotals:     map[string]uint64{"GET": 4, "LIST": 2},
		EvidenceUploadsTotal:     1,
		EvidenceUploadBytesTotal: 300,
	}, 7000))
	postCostMeterHeartbeat(t, mux, costMeterHeartbeat("w-meter-main-2", "meter-main", now.Add(-time.Minute), reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch: "epoch-2",
		ReplicaOperationTotals:  map[string]uint64{"PUT": 8},
	}, 1000))
	postCostMeterHeartbeat(t, mux, costMeterHeartbeat("w-meter-pr", "meter-pr", now.Add(-time.Minute), reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch: "epoch-3",
		ReplicaOperationTotals:  map[string]uint64{"GET": 3},
	}, 100))
	legacy := costMeterHeartbeat("w-meter-pr", "meter-pr", now, reporting.ObjectStoreCounters{}, 200)
	postCostMeterHeartbeat(t, mux, legacy)

	if got := testutil.ToFloat64(putSeries) - putBefore; got != 120 {
		t.Fatalf("per-source PUT counter delta = %v, want 120", got)
	}

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/cost-meter?source=meter-main&since=1h", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var report CostMeterResponse
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Source != "meter-main" || report.Since.IsZero() || report.WindowStart.After(report.Since) {
		t.Fatalf("window = %+v", report)
	}
	if len(report.Sources) != 1 || len(report.Sources[0].Workers) != 2 {
		t.Fatalf("sources = %+v", report.Sources)
	}
	main := report.Sources[0]
	if main.Totals.ReplicaRequests["PUT"] != 120 || main.Totals.ReplicaRequests["LIST"] != 11 || main.Totals.ReplicaBytes["PUT"] != 1120 {
		t.Fatalf("main replica totals = %+v", main.Totals)
	}
	if main.Totals.RestoreRequests["GET"] != 4 || main.Totals.Uploads != 1 || main.Totals.UploadBytes != 300 {
		t.Fatalf("main restore/upload totals = %+v", main.Totals)
	}
	if main.Totals.PeakDiskUsedBytes != 7000 || main.WorkerPeakDiskUsedBytesSum != 8000 {
		t.Fatalf("main peak = %d sum = %d", main.Totals.PeakDiskUsedBytes, main.WorkerPeakDiskUsedBytesSum)
	}
	if main.Workers[0].WorkerID != "w-meter-main-1" || main.Workers[0].Totals.ReplicaRequests["PUT"] != 112 {
		t.Fatalf("main worker = %+v", main.Workers[0])
	}

	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/cost-meter", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	report = CostMeterResponse{}
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Sources) != 2 || report.Sources[1].Source != "meter-pr" || report.Sources[1].Totals.PeakDiskUsedBytes != 200 {
		t.Fatalf("all sources = %+v", report.Sources)
	}

	for _, since := range []string{"yesterday", "-1h"} {
		response = httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/cost-meter?since="+since, nil))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("since=%s status = %d, want 400", since, response.Code)
		}
	}
}

func TestPruneDBOnceBoundsCostMeterEvenWithRetentionDisabled(t *testing.T) {
	db := openTestDB(t)
	now := time.Now().UTC()
	counters := func(puts uint64) reporting.RuntimePayload {
		return reporting.RuntimePayload{ObjectStoreCounters: reporting.ObjectStoreCounters{ObjectStoreCounterEpoch: "e", ReplicaOperationTotals: map[string]uint64{"PUT": puts}}}
	}
	if _, err := db.RecordCostMeter("w-meter-retention", "main", now.Add(-costMeterRetention-48*time.Hour), counters(10)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecordCostMeter("w-meter-retention", "main", now, counters(15)); err != nil {
		t.Fatal(err)
	}
	NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "").pruneDBOnce(context.Background(), 0)
	report, err := db.CostMeter("main", now.Add(-2*costMeterRetention))
	if err != nil {
		t.Fatal(err)
	}
	if len(report) != 1 || report[0].Totals.ReplicaRequests["PUT"] != 5 {
		t.Fatalf("report after prune = %+v", report)
	}
}

func TestParseCostMeterSince(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	for raw, want := range map[string]time.Time{
		"":                     now.Add(-24 * time.Hour),
		"6h":                   now.Add(-6 * time.Hour),
		"7d":                   now.AddDate(0, 0, -7),
		"2026-10-20T00:00:00Z": time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC),
	} {
		got, err := parseCostMeterSince(raw, now)
		if err != nil || !got.Equal(want) {
			t.Fatalf("parseCostMeterSince(%q) = %v, %v; want %v", raw, got, err, want)
		}
	}
}

func workerSeriesCount(t *testing.T, workerID string) int {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			if metricLabels(metric)["worker_id"] == workerID {
				count++
			}
		}
	}
	return count
}

func TestControlMetricsRemoveSeriesForStoppedWorker(t *testing.T) {
	db := openTestDB(t)
	metrics := NewControlMetrics(db)
	completed := time.Now().UTC()
	worker := model.Worker{
		ID:              "worker-metrics-stopped",
		Name:            "worker-metrics-stopped",
		Status:          model.WorkerRunning,
		Source:          "main",
		GitSHA:          "sha-stopped",
		ProfileName:     "provider-tigris",
		ProfileConfig:   "{}",
		LastHeartbeatAt: &completed,
		LastRuntimeJSON: mustJSON(reporting.RuntimePayload{DataDiskUsedBytes: 4096, LitestreamSnapshotHealthy: true}),
	}
	failed := model.Verification{WorkerID: worker.ID, StartedAt: completed, CompletedAt: &completed, Status: "failed", CheckType: "integrity", ErrorMessage: "restore failed"}
	metrics.observeWorker(worker)
	metrics.observeVerification(worker, failed)
	metrics.observePlatformEvent(worker, &model.Event{EventType: reporting.WorkerEventDiskFullNoProgress, Message: "disk full", CreatedAt: completed})
	if workerSeriesCount(t, worker.ID) == 0 {
		t.Fatal("expected live worker series")
	}

	worker.Status = model.WorkerStopped
	metrics.observeWorker(worker)
	metrics.observeVerification(worker, failed)
	metrics.observeLastFailure(worker, failed)
	metrics.observePlatformEvent(worker, &model.Event{EventType: reporting.WorkerEventDiskFullNoProgress, Message: "disk full", CreatedAt: completed})
	if got := workerSeriesCount(t, worker.ID); got != 0 {
		t.Fatalf("stopped worker still exports %d series", got)
	}

	stopped := model.Worker{ID: "worker-metrics-stopped-db", Name: "worker-metrics-stopped-db", Status: model.WorkerStopped, Source: "main", GitSHA: "sha-stopped", ProfileName: "s3-flap", ProfileConfig: "{}"}
	if err := db.CreateWorker(&stopped); err != nil {
		t.Fatal(err)
	}
	if err := NewControlMetrics(db).syncFromDB(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if got := workerSeriesCount(t, stopped.ID); got != 0 {
		t.Fatalf("synced stopped worker exports %d series", got)
	}

	worker.Status = model.WorkerRunning
	metrics.observeWorker(worker)
	if workerSeriesCount(t, worker.ID) == 0 {
		t.Fatal("restarted worker did not export series")
	}
}
