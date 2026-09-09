package orchestrator

import (
	"math"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestResourceMetricsRespectObservationStatus(t *testing.T) {
	metrics := NewControlMetrics(openTestDB(t))
	worker := model.Worker{ID: "resource-status", ProfileName: "test", Source: "test"}
	labels := workerMetricLabels(worker)
	for _, status := range []string{"fresh", "stale", "unavailable", "unsupported", ""} {
		t.Run(status, func(t *testing.T) {
			worker.LastRuntimeJSON = mustJSON(reporting.RuntimePayload{
				LitestreamProcess:  reporting.ProcessObservation{Status: status, CollectedAt: time.Now()},
				WorkerProcess:      reporting.ProcessObservation{Status: status, CollectedAt: time.Now()},
				LocalStateStatus:   status,
				LitestreamRSSBytes: 10, WorkerRSSBytes: 20, LitestreamDirSizeBytes: 30,
			})
			metrics.observeWorker(worker)
			for _, gauge := range []float64{
				testutil.ToFloat64(controlWorkerLitestreamRSSBytes.WithLabelValues(labels...)),
				testutil.ToFloat64(controlWorkerRSSBytes.WithLabelValues(labels...)),
				testutil.ToFloat64(controlWorkerLitestreamLocalStateSize.WithLabelValues(labels...)),
			} {
				if status == "fresh" && math.IsNaN(gauge) || status != "fresh" && !math.IsNaN(gauge) {
					t.Fatalf("status=%q value=%v", status, gauge)
				}
			}
		})
	}
}

func TestMissingResourceRuntimeClearsMetrics(t *testing.T) {
	worker := model.Worker{ID: "missing-resource", ProfileName: "test", Source: "test"}
	labels := workerMetricLabels(worker)
	publishControlResourceMetrics(labels, nil)
	if !math.IsNaN(testutil.ToFloat64(controlWorkerLitestreamRSSBytes.WithLabelValues(labels...))) {
		t.Fatal("missing runtime appears measured")
	}
	for _, resource := range []string{"litestream", "worker", "local_state"} {
		resourceLabels := append(append([]string(nil), labels...), resource)
		if testutil.ToFloat64(controlResourceObservationStatus.WithLabelValues(resourceLabels...)) != 0 || !math.IsNaN(testutil.ToFloat64(controlResourceObservationTime.WithLabelValues(resourceLabels...))) {
			t.Fatal("missing validity metadata")
		}
	}
}
