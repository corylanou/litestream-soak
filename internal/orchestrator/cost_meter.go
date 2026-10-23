package orchestrator

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sort"
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
		Help: "Object-store HTTP requests measured by the worker observe proxy (origin=replica) or the verifier restore log (origin=restore), summed per source from counter deltas.",
	}, []string{"source", "origin", "operation"})

	controlSourceObjectStoreRequestBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_control_source_object_store_request_bytes_total",
		Help: "Object-store HTTP body bytes measured by the worker observe proxy, by direction, summed per source from counter deltas.",
	}, []string{"source", "origin", "operation", "direction"})

	controlSourceLitestreamObjectOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_control_source_litestream_object_operations_total",
		Help: "Logical object operations from litestream_replica_operation_total (uploaded and deleted objects, not HTTP requests), summed per source.",
	}, []string{"source", "operation"})

	controlSourceLitestreamObjectBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_control_source_litestream_object_bytes_total",
		Help: "Logical object bytes from litestream_replica_operation_bytes, summed per source.",
	}, []string{"source", "operation"})

	controlSourceEvidenceObjectUploads = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_control_source_evidence_object_uploads_total",
		Help: "Successful pprof and evidence object uploads (one per s3cmd put, not per HTTP request), summed per source.",
	}, []string{"source"})

	controlSourceEvidenceUploadPayloadBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_control_source_evidence_upload_payload_bytes_total",
		Help: "Payload bytes of successful pprof and evidence object uploads, summed per source.",
	}, []string{"source"})
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
	db := a.db.WithReadContext(r.Context())
	sources, err := db.CostMeter(source, since)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to load cost meter")
		return
	}
	windows, err := db.ListMeteringWindows(source)
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to load metering windows")
		return
	}
	sources = attachMeteringWindows(sources, windows, since)
	writeAPIJSON(w, CostMeterResponse{
		Source:      source,
		Since:       since,
		WindowStart: since.Truncate(time.Hour),
		Sources:     sources,
	})
}

func attachMeteringWindows(sources []model.CostMeterSource, windows []model.MeteringWindow, since time.Time) []model.CostMeterSource {
	for _, window := range windows {
		if window.EndedAt != nil && !window.EndedAt.After(since) {
			continue
		}
		index := slices.IndexFunc(sources, func(entry model.CostMeterSource) bool { return entry.Source == window.Source })
		if index < 0 {
			sources = append(sources, model.CostMeterSource{Source: window.Source, Workers: []model.CostMeterWorker{}})
			index = len(sources) - 1
		}
		current := sources[index].MeteringWindow
		if current == nil || window.StartedAt.After(current.StartedAt) {
			sources[index].MeteringWindow = &window
		}
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Source < sources[j].Source })
	return sources
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
	for operation, count := range delta.LitestreamObjectOperations {
		controlSourceLitestreamObjectOperations.WithLabelValues(source, operation).Add(float64(count))
	}
	for operation, bytes := range delta.LitestreamObjectBytes {
		controlSourceLitestreamObjectBytes.WithLabelValues(source, operation).Add(float64(bytes))
	}
	for operation, count := range delta.ReplicaRequests {
		controlSourceObjectStoreRequests.WithLabelValues(source, "replica", operation).Add(float64(count))
	}
	for operation, bytes := range delta.ReplicaRequestBytesSent {
		controlSourceObjectStoreRequestBytes.WithLabelValues(source, "replica", operation, "sent").Add(float64(bytes))
	}
	for operation, bytes := range delta.ReplicaResponseBytesReceived {
		controlSourceObjectStoreRequestBytes.WithLabelValues(source, "replica", operation, "received").Add(float64(bytes))
	}
	for operation, count := range delta.RestoreRequests {
		controlSourceObjectStoreRequests.WithLabelValues(source, "restore", operation).Add(float64(count))
	}
	if delta.EvidenceObjectUploads > 0 {
		controlSourceEvidenceObjectUploads.WithLabelValues(source).Add(float64(delta.EvidenceObjectUploads))
	}
	if delta.EvidenceUploadPayloadBytes > 0 {
		controlSourceEvidenceUploadPayloadBytes.WithLabelValues(source).Add(float64(delta.EvidenceUploadPayloadBytes))
	}
}

func workerMeasurementsRetired(worker model.Worker) bool {
	return worker.Status == model.WorkerStopped
}

func workerMeasurementCollectors() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{
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
		controlResourceObservationStatus,
		controlResourceObservationTime,
	}
}

func forgetWorkerMeasurements(workerID string) {
	labels := prometheus.Labels{"worker_id": workerID}
	for _, collector := range workerMeasurementCollectors() {
		collector.DeletePartialMatch(labels)
	}
}
