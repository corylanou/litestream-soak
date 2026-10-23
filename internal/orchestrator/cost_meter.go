package orchestrator

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	costMeterDefaultWindow = 24 * time.Hour
	costMeterRetention     = 30 * 24 * time.Hour
)

var (
	controlSourceObjectStoreRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_control_source_object_store_requests_total",
		Help: "Object-store requests reported by workers, summed per source from counter deltas.",
	}, []string{"source", "origin", "operation"})

	controlSourceObjectStoreBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_control_source_object_store_bytes_total",
		Help: "Object-store bytes reported by workers, summed per source from counter deltas.",
	}, []string{"source", "origin", "operation"})
)

type CostMeterResponse struct {
	Source      string                  `json:"source,omitempty"`
	Since       time.Time               `json:"since"`
	WindowStart time.Time               `json:"window_start"`
	Sources     []model.CostMeterSource `json:"sources"`
}

func (a *API) handleCostMeter(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	since, err := parseCostMeterSince(r.URL.Query().Get("since"), now)
	if err != nil {
		respondError(w, r, http.StatusBadRequest, nil, err.Error())
		return
	}
	source := strings.TrimSpace(r.URL.Query().Get("source"))
	sources, err := a.db.WithReadContext(r.Context()).CostMeter(source, since)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to load cost meter")
		return
	}
	writeAPIJSON(w, CostMeterResponse{
		Source:      source,
		Since:       since,
		WindowStart: since.Truncate(time.Hour),
		Sources:     sources,
	})
}

func parseCostMeterSince(raw string, now time.Time) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return now.Add(-costMeterDefaultWindow), nil
	}
	if days, ok := strings.CutSuffix(raw, "d"); ok {
		if n, err := strconv.Atoi(days); err == nil && n > 0 {
			return now.AddDate(0, 0, -n), nil
		}
	}
	if window, err := time.ParseDuration(raw); err == nil {
		if window <= 0 {
			return time.Time{}, errors.New("since must be a positive window")
		}
		return now.Add(-window), nil
	}
	if at, err := time.Parse(time.RFC3339, raw); err == nil {
		return at.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid since %q: use a duration such as 24h or 7d, or an RFC3339 timestamp", raw)
}

func (a *API) recordCostMeter(worker model.Worker, observedAt time.Time, runtime reporting.RuntimePayload) {
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	delta, err := a.db.RecordCostMeter(worker.ID, worker.Source, observedAt, runtime)
	if err != nil {
		slog.Warn("Failed to record cost meter", "worker_id", worker.ID, "error", err)
		return
	}
	observeCostMeterDelta(worker.Source, delta)
}

func observeCostMeterDelta(source string, delta model.CostMeterTotals) {
	source = metricValueOrUnknown(source)
	for operation, count := range delta.ReplicaRequests {
		controlSourceObjectStoreRequests.WithLabelValues(source, "replica", operation).Add(float64(count))
	}
	for operation, bytes := range delta.ReplicaBytes {
		controlSourceObjectStoreBytes.WithLabelValues(source, "replica", operation).Add(float64(bytes))
	}
	for operation, count := range delta.RestoreRequests {
		controlSourceObjectStoreRequests.WithLabelValues(source, "restore", operation).Add(float64(count))
	}
	if delta.Uploads > 0 {
		controlSourceObjectStoreRequests.WithLabelValues(source, "evidence", "PUT").Add(float64(delta.Uploads))
	}
	if delta.UploadBytes > 0 {
		controlSourceObjectStoreBytes.WithLabelValues(source, "evidence", "PUT").Add(float64(delta.UploadBytes))
	}
}

func workerMetricsRetired(worker model.Worker) bool {
	return worker.Status == model.WorkerStopped
}

func workerSeriesCollectors() []interface {
	DeletePartialMatch(prometheus.Labels) int
} {
	return []interface {
		DeletePartialMatch(prometheus.Labels) int
	}{
		controlWorkerInfo,
		controlWorkerVersionInfo,
		controlWorkerStatus,
		controlWorkerWorkloadInfo,
		controlWorkerLastHeartbeat,
		controlWorkerLastVerificationResult,
		controlWorkerLastVerificationDuration,
		controlWorkerLastVerificationCompleted,
		controlWorkerRuntimeSnapshotStatus,
		controlWorkerLitestreamMetricsStatus,
		controlWorkerDataDiskTotalSize,
		controlWorkerDataDiskUsedSize,
		controlWorkerDataDiskFreeSize,
		controlWorkerDataDiskUsedPercent,
		controlWorkerDBSize,
		controlWorkerWALSize,
		controlWorkerDBCount,
		controlWorkerDBTotalSize,
		controlWorkerWALTotalSize,
		controlWorkerLastSyncAgeP50,
		controlWorkerLastSyncAgeP95,
		controlWorkerLastSyncAgeMax,
		controlWorkerReplicationLagP95,
		controlWorkerReplicationLagMax,
		controlWorkerReplicationLagOverThreshold,
		controlWorkerLitestreamRSSBytes,
		controlWorkerLitestreamCPUSeconds,
		controlWorkerLitestreamGoroutines,
		controlWorkerLitestreamFDs,
		controlWorkerS3ListRequestsTotal,
		controlWorkerLitestreamHeapInuseBytes,
		controlWorkerLitestreamStackInuseBytes,
		controlWorkerLitestreamAllocBytesTotal,
		controlWorkerLitestreamAllocRateBytesPerSecond,
		controlWorkerRSSBytes,
		controlWorkerFDs,
		controlWorkerLitestreamLocalStateSize,
		controlWorkerLitestreamLocalLTXSize,
		controlWorkerLatestPlatformEventInfo,
		controlWorkerLatestPlatformEventUnixTime,
		controlWorkerLatestPlatformEventWindowCount,
		controlWorkerFailureInfo,
		controlWorkerLastFailureInfo,
		controlWorkerLastFailure,
		controlResourceObservationStatus,
		controlResourceObservationTime,
		controlWorkerLastAlert,
		controlAlertTotal,
	}
}

func (m *controlMetrics) forgetWorker(workerID string) {
	m.mu.Lock()
	delete(m.statusByWorker, workerID)
	delete(m.infoByWorker, workerID)
	delete(m.workloadByWorker, workerID)
	delete(m.runtimeByWorker, workerID)
	delete(m.litestreamMetricsByWorker, workerID)
	delete(m.platformByWorker, workerID)
	delete(m.failureByWorker, workerID)
	delete(m.lastFailureByWorker, workerID)
	labels := prometheus.Labels{"worker_id": workerID}
	for _, collector := range workerSeriesCollectors() {
		collector.DeletePartialMatch(labels)
	}
	m.mu.Unlock()
}
