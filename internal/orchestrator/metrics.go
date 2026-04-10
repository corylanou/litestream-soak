package orchestrator

import (
	"sync"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type controlMetrics struct {
	mu              sync.Mutex
	statusByWorker  map[string]string
	failureByWorker map[string]failureMetricState
}

type failureMetricState struct {
	labels []string
}

var (
	controlWorkerInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_info",
		Help: "Static control-plane info about a soak worker.",
	}, []string{"worker_id", "git_sha", "profile", "source", "app_name", "region"})

	controlWorkerStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_status",
		Help: "Current control-plane worker status.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "status"})

	controlWorkerLastHeartbeat = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_last_heartbeat_unixtime",
		Help: "Unix timestamp of the last worker heartbeat seen by the control plane.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerLastVerificationResult = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_last_verification_result",
		Help: "Most recent verification result recorded by the control plane (1=pass, 0=fail).",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerLastVerificationDuration = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_last_verification_duration_seconds",
		Help: "Duration of the most recent verification recorded by the control plane.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerLastVerificationCompleted = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_last_verification_completed_unixtime",
		Help: "Unix timestamp when the most recent verification completed.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerFailureInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_failure_info",
		Help: "Current failure classification tracked by the control plane.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "failure_stage", "failure_signature"})
)

func newControlMetrics(db *model.DB) *controlMetrics {
	m := &controlMetrics{
		statusByWorker:  make(map[string]string),
		failureByWorker: make(map[string]failureMetricState),
	}
	m.syncFromDB(db)
	return m
}

func (m *controlMetrics) syncFromDB(db *model.DB) {
	workers, err := db.ListWorkers("")
	if err != nil {
		return
	}

	for _, worker := range workers {
		m.observeWorker(worker)

		verifications, err := db.ListVerifications(worker.ID, 1)
		if err != nil || len(verifications) == 0 {
			continue
		}
		m.observeVerification(worker, verifications[0])
	}
}

func (m *controlMetrics) observeWorker(worker model.Worker) {
	labels := workerMetricLabels(worker)

	controlWorkerInfo.WithLabelValues(worker.ID, worker.GitSHA, worker.ProfileName, worker.Source, workerAppName(worker), workerRegion(worker)).Set(1)

	m.mu.Lock()
	previousStatus := m.statusByWorker[worker.ID]
	m.statusByWorker[worker.ID] = string(worker.Status)
	m.mu.Unlock()

	if previousStatus != "" && previousStatus != string(worker.Status) {
		controlWorkerStatus.WithLabelValues(append(labels, previousStatus)...).Set(0)
	}
	controlWorkerStatus.WithLabelValues(append(labels, string(worker.Status))...).Set(1)

	if worker.LastHeartbeatAt != nil && !worker.LastHeartbeatAt.IsZero() {
		controlWorkerLastHeartbeat.WithLabelValues(labels...).Set(float64(worker.LastHeartbeatAt.Unix()))
	}
}

func (m *controlMetrics) observeVerification(worker model.Worker, verification model.Verification) {
	labels := workerMetricLabels(worker)

	if verification.Passed {
		controlWorkerLastVerificationResult.WithLabelValues(labels...).Set(1)
		m.clearFailure(worker.ID)
	} else {
		controlWorkerLastVerificationResult.WithLabelValues(labels...).Set(0)

		stage := inferFailureStage(&verification)
		signature := inferFailureSignature(&verification)
		failureLabels := append(labels, metricValueOrUnknown(stage), metricValueOrUnknown(signature))

		m.mu.Lock()
		previous := m.failureByWorker[worker.ID]
		m.failureByWorker[worker.ID] = failureMetricState{labels: failureLabels}
		m.mu.Unlock()

		if len(previous.labels) > 0 && !sameMetricLabels(previous.labels, failureLabels) {
			controlWorkerFailureInfo.WithLabelValues(previous.labels...).Set(0)
		}
		controlWorkerFailureInfo.WithLabelValues(failureLabels...).Set(1)
	}

	controlWorkerLastVerificationDuration.WithLabelValues(labels...).Set(float64(verification.DurationMS) / 1000)

	if verification.CompletedAt != nil && !verification.CompletedAt.IsZero() {
		controlWorkerLastVerificationCompleted.WithLabelValues(labels...).Set(float64(verification.CompletedAt.Unix()))
		return
	}
	if !verification.StartedAt.IsZero() {
		controlWorkerLastVerificationCompleted.WithLabelValues(labels...).Set(float64(verification.StartedAt.Unix()))
	}
}

func (m *controlMetrics) clearFailure(workerID string) {
	m.mu.Lock()
	previous := m.failureByWorker[workerID]
	delete(m.failureByWorker, workerID)
	m.mu.Unlock()

	if len(previous.labels) > 0 {
		controlWorkerFailureInfo.WithLabelValues(previous.labels...).Set(0)
	}
}

func workerMetricLabels(worker model.Worker) []string {
	return []string{
		worker.ID,
		worker.ProfileName,
		worker.Source,
		workerAppName(worker),
		workerRegion(worker),
	}
}

func workerAppName(worker model.Worker) string {
	if worker.AppName == "" {
		return "unknown"
	}
	return worker.AppName
}

func workerRegion(worker model.Worker) string {
	if worker.Region == "" {
		return "unknown"
	}
	return worker.Region
}

func metricValueOrUnknown(v string) string {
	if v == "" {
		return "unknown"
	}
	return v
}

func sameMetricLabels(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
