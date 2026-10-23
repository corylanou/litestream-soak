package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
	logicalSeries := controlSourceLitestreamObjectOperations.WithLabelValues("meter-main", "PUT")
	logicalBefore := testutil.ToFloat64(logicalSeries)
	now := time.Now().UTC()
	postCostMeterHeartbeat(t, mux, costMeterHeartbeat("w-meter-main-1", "meter-main", now.Add(-2*time.Minute), reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch:         "epoch-1",
		LitestreamCounterEpoch:          "start:1",
		LitestreamObjectOperationTotals: map[string]uint64{"PUT": 100, "DELETE": 10},
		LitestreamObjectOperationBytes:  map[string]uint64{"PUT": 1000},
		ReplicaRequestTotals:            map[string]uint64{"PUT": 150, "LIST": 10},
		ReplicaRequestBytesSent:         map[string]uint64{"PUT": 2000},
		RestoreRequestTotals:            map[string]uint64{"GET": 4, "LIST": 2},
		EvidenceObjectUploadsTotal:      1,
		EvidenceUploadPayloadBytesTotal: 300,
	}, 5000))
	postCostMeterHeartbeat(t, mux, costMeterHeartbeat("w-meter-main-1", "meter-main", now.Add(-time.Minute), reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch:         "epoch-1",
		LitestreamCounterEpoch:          "start:2",
		LitestreamObjectOperationTotals: map[string]uint64{"PUT": 12, "DELETE": 1},
		LitestreamObjectOperationBytes:  map[string]uint64{"PUT": 120},
		ReplicaRequestTotals:            map[string]uint64{"PUT": 170, "LIST": 12},
		ReplicaRequestBytesSent:         map[string]uint64{"PUT": 2400},
		RestoreRequestTotals:            map[string]uint64{"GET": 4, "LIST": 2},
		EvidenceObjectUploadsTotal:      1,
		EvidenceUploadPayloadBytesTotal: 300,
	}, 7000))
	postCostMeterHeartbeat(t, mux, costMeterHeartbeat("w-meter-main-2", "meter-main", now.Add(-time.Minute), reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch:         "epoch-2",
		LitestreamObjectOperationTotals: map[string]uint64{"PUT": 8},
		ReplicaRequestTotals:            map[string]uint64{"PUT": 9},
	}, 1000))
	postCostMeterHeartbeat(t, mux, costMeterHeartbeat("w-meter-pr", "meter-pr", now.Add(-time.Minute), reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch:         "epoch-3",
		LitestreamObjectOperationTotals: map[string]uint64{"PUT": 3},
	}, 100))
	legacy := costMeterHeartbeat("w-meter-pr", "meter-pr", now, reporting.ObjectStoreCounters{}, 200)
	postCostMeterHeartbeat(t, mux, legacy)

	if got := testutil.ToFloat64(putSeries) - putBefore; got != 179 {
		t.Fatalf("per-source replica PUT request delta = %v, want 179", got)
	}
	if got := testutil.ToFloat64(logicalSeries) - logicalBefore; got != 120 {
		t.Fatalf("per-source logical PUT operation delta = %v, want 120", got)
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
	if main.Totals.LitestreamObjectOperations["PUT"] != 120 || main.Totals.LitestreamObjectOperations["DELETE"] != 11 || main.Totals.LitestreamObjectBytes["PUT"] != 1120 {
		t.Fatalf("main logical totals = %+v", main.Totals)
	}
	if main.Totals.ReplicaRequests["PUT"] != 179 || main.Totals.ReplicaRequests["LIST"] != 12 || main.Totals.ReplicaRequestBytesSent["PUT"] != 2400 {
		t.Fatalf("main replica request totals = %+v", main.Totals)
	}
	if main.Totals.RestoreRequests["GET"] != 4 || main.Totals.EvidenceObjectUploads != 1 || main.Totals.EvidenceUploadPayloadBytes != 300 {
		t.Fatalf("main restore/upload totals = %+v", main.Totals)
	}
	if main.Totals.PeakDiskUsedBytes != 7000 || main.WorkerPeakDiskUsedBytesSum != 8000 {
		t.Fatalf("main peak = %d sum = %d", main.Totals.PeakDiskUsedBytes, main.WorkerPeakDiskUsedBytesSum)
	}
	if main.Workers[0].WorkerID != "w-meter-main-1" || main.Workers[0].Totals.LitestreamObjectOperations["PUT"] != 112 {
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
		return reporting.RuntimePayload{ObjectStoreCounters: reporting.ObjectStoreCounters{ObjectStoreCounterEpoch: "e", LitestreamObjectOperationTotals: map[string]uint64{"PUT": puts}}}
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
	if len(report) != 1 || report[0].Totals.LitestreamObjectOperations["PUT"] != 5 {
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

func workerSeriesByMetric(t *testing.T, workerID string) map[string][]map[string]string {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	series := make(map[string][]map[string]string)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			labels := metricLabels(metric)
			if labels["worker_id"] == workerID {
				series[family.GetName()] = append(series[family.GetName()], labels)
			}
		}
	}
	return series
}

func dashboardExpressions(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "grafana", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("dashboards = %v, %v", paths, err)
	}
	var exprs []string
	var walk func(any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			for key, child := range value {
				if text, ok := child.(string); ok && key == "expr" {
					exprs = append(exprs, text)
					continue
				}
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var dashboard any
		if err := json.Unmarshal(body, &dashboard); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		walk(dashboard)
	}
	return exprs
}

var workerMetricNamePattern = regexp.MustCompile(`soak_control_worker_[a-z0-9_]+`)

func observeStoppedMetricsWorker(t *testing.T, metrics *controlMetrics, worker model.Worker) {
	t.Helper()
	completed := time.Now().UTC()
	failed := model.Verification{WorkerID: worker.ID, StartedAt: completed, CompletedAt: &completed, Status: "failed", CheckType: "integrity", ErrorMessage: "restore failed"}
	metrics.observeWorker(worker)
	metrics.observeVerification(worker, failed)
	metrics.observePlatformEvent(worker, &model.Event{EventType: reporting.WorkerEventDiskFullNoProgress, Message: "disk full", CreatedAt: completed})
}

func TestControlMetricsKeepLifecycleButDropMeasurementsForStoppedWorker(t *testing.T) {
	db := openTestDB(t)
	metrics := NewControlMetrics(db)
	heartbeat := time.Now().UTC()
	worker := model.Worker{
		ID:              "worker-metrics-stopped",
		Name:            "worker-metrics-stopped",
		Status:          model.WorkerRunning,
		Source:          "main",
		GitSHA:          "sha-stopped",
		ProfileName:     "provider-tigris",
		ProfileConfig:   "{}",
		LastHeartbeatAt: &heartbeat,
		LastRuntimeJSON: mustJSON(reporting.RuntimePayload{DataDiskUsedBytes: 4096, DataDiskUsedPercent: 40, LocalStateStatus: "fresh", LitestreamLTXSizeBytes: 64, LitestreamSnapshotHealthy: true}),
	}
	observeStoppedMetricsWorker(t, metrics, worker)
	running := workerSeriesByMetric(t, worker.ID)

	worker.Status = model.WorkerStopped
	observeStoppedMetricsWorker(t, metrics, worker)
	stopped := workerSeriesByMetric(t, worker.ID)

	measurementNames := make(map[string]bool)
	for _, collector := range workerMeasurementCollectors() {
		descs := make(chan *prometheus.Desc, 1)
		collector.Describe(descs)
		desc := (<-descs).String()
		name := desc[strings.Index(desc, `fqName: "`)+len(`fqName: "`):]
		measurementNames[name[:strings.Index(name, `"`)]] = true
	}
	for name := range running {
		_, kept := stopped[name]
		if measurementNames[name] && kept {
			t.Errorf("stopped worker still exports measurement %s", name)
		}
		if !measurementNames[name] && !kept {
			t.Errorf("stopped worker lost lifecycle series %s", name)
		}
	}
	statusSeries := stopped["soak_control_worker_status"]
	foundStopped := false
	for _, labels := range statusSeries {
		if labels["status"] == string(model.WorkerStopped) {
			foundStopped = true
		}
	}
	if !foundStopped {
		t.Fatalf("status series for stopped worker = %v", statusSeries)
	}
	assertGaugeVecValue(t, controlWorkerStatus, metricLabelsWith(workerMetricLabels(worker), string(model.WorkerStopped)), 1)

	stoppedDB := model.Worker{ID: "worker-metrics-stopped-db", Name: "worker-metrics-stopped-db", Status: model.WorkerStopped, Source: "main", GitSHA: "sha-stopped", ProfileName: "s3-flap", ProfileConfig: "{}", LastRuntimeJSON: mustJSON(reporting.RuntimePayload{DataDiskUsedBytes: 9, LitestreamSnapshotHealthy: true})}
	if err := db.CreateWorker(&stoppedDB); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateWorkerRuntimeSnapshot(stoppedDB.ID, reporting.RuntimePayload{DataDiskUsedBytes: 9, LitestreamSnapshotHealthy: true}); err != nil {
		t.Fatal(err)
	}
	if err := NewControlMetrics(db).syncFromDB(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	synced := workerSeriesByMetric(t, stoppedDB.ID)
	if _, ok := synced["soak_control_worker_data_disk_used_bytes"]; ok {
		t.Fatal("synced stopped worker exports data disk usage")
	}
	if _, ok := synced["soak_control_worker_status"]; !ok {
		t.Fatal("synced stopped worker lost its status series")
	}

	for _, expr := range dashboardExpressions(t) {
		if !strings.Contains(expr, `status=\"stopped\"`) && !strings.Contains(expr, `status="stopped"`) {
			continue
		}
		for _, name := range workerMetricNamePattern.FindAllString(expr, -1) {
			if _, ok := stopped[name]; !ok {
				t.Errorf("dashboard query needs %s for stopped workers: %s", name, expr)
			}
		}
	}
}
