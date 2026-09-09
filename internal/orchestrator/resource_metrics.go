package orchestrator

import (
	"math"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var controlResourceObservationStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "soak_control_worker_resource_observation_status",
	Help: "Resource observation status: 0 unavailable or unknown, 1 fresh, 2 stale, 3 unsupported.",
}, []string{"worker_id", "profile", "source", "app_name", "region", "resource"})

var controlResourceObservationTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "soak_control_worker_resource_observation_unixtime",
	Help: "Unix timestamp of the last complete resource observation; NaN until observed.",
}, []string{"worker_id", "profile", "source", "app_name", "region", "resource"})

func publishControlResourceMetrics(labels []string, runtime *reporting.RuntimePayload) {
	if runtime == nil {
		runtime = &reporting.RuntimePayload{}
	}
	publish := func(resource, status string, at time.Time) {
		code := 0.0
		switch status {
		case "fresh":
			code = 1
		case "stale":
			code = 2
		case "unsupported":
			code = 3
		}
		timestamp := math.NaN()
		if !at.IsZero() {
			timestamp = float64(at.Unix())
		}
		resourceLabels := append(append([]string(nil), labels...), resource)
		controlResourceObservationStatus.WithLabelValues(resourceLabels...).Set(code)
		controlResourceObservationTime.WithLabelValues(resourceLabels...).Set(timestamp)
	}
	value := func(status string, n float64) float64 {
		if status != "fresh" {
			return math.NaN()
		}
		return n
	}
	publish("litestream", runtime.LitestreamProcess.Status, runtime.LitestreamProcess.CollectedAt)
	publish("worker", runtime.WorkerProcess.Status, runtime.WorkerProcess.CollectedAt)
	publish("local_state", runtime.LocalStateStatus, runtime.LocalStateCollectedAt)
	controlWorkerLitestreamRSSBytes.WithLabelValues(labels...).Set(value(runtime.LitestreamProcess.Status, float64(runtime.LitestreamRSSBytes)))
	controlWorkerLitestreamCPUSeconds.WithLabelValues(labels...).Set(value(runtime.LitestreamProcess.Status, runtime.LitestreamCPUSecondsTotal))
	controlWorkerLitestreamFDs.WithLabelValues(labels...).Set(value(runtime.LitestreamProcess.Status, float64(runtime.LitestreamFDs)))
	controlWorkerRSSBytes.WithLabelValues(labels...).Set(value(runtime.WorkerProcess.Status, float64(runtime.WorkerRSSBytes)))
	controlWorkerFDs.WithLabelValues(labels...).Set(value(runtime.WorkerProcess.Status, float64(runtime.WorkerFDs)))
	controlWorkerLitestreamLocalStateSize.WithLabelValues(labels...).Set(value(runtime.LocalStateStatus, float64(runtime.LitestreamDirSizeBytes)))
	controlWorkerLitestreamLocalLTXSize.WithLabelValues(labels...).Set(value(runtime.LocalStateStatus, float64(runtime.LitestreamLTXSizeBytes)))
}
