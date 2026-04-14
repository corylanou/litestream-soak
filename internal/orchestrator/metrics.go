package orchestrator

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type controlMetrics struct {
	mu                  sync.Mutex
	statusByWorker      map[string]string
	workloadByWorker    map[string]labelMetricState
	runtimeByWorker     map[string]labelMetricState
	platformByWorker    map[string]labelMetricState
	failureByWorker     map[string]failureMetricState
	lastFailureByWorker map[string]failureMetricState
	latestDeployment    labelMetricState
	rolloutByState      map[string]labelMetricState
}

type labelMetricState struct {
	labels []string
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

	controlWorkerWorkloadInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_workload_info",
		Help: "Configured workload shape for a control-plane worker.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "load_mode", "replay_dataset", "pattern", "write_rate", "payload_size", "load_workers", "replay_speed", "memory_mb", "cpus"})

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

	controlWorkerRuntimeSnapshotStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_runtime_snapshot_status",
		Help: "Current runtime snapshot health classification tracked by the control plane.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "runtime_status"})

	controlWorkerLatestPlatformEventInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_latest_platform_event_info",
		Help: "Latest normalized platform signal tracked by the control plane.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "event_type", "event_summary"})

	controlWorkerLatestPlatformEventUnixTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_latest_platform_event_unixtime",
		Help: "Unix timestamp of the latest normalized platform signal tracked by the control plane.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "event_type", "event_summary"})

	controlWorkerLatestPlatformEventWindowCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_latest_platform_event_window_count",
		Help: "Number of similar platform log lines represented by the latest displayed platform incident row.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "event_type", "event_summary"})

	controlWorkerFailureInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_failure_info",
		Help: "Current failure classification tracked by the control plane.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "failure_stage", "failure_signature"})

	controlWorkerLastFailureInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_last_failure_info",
		Help: "Most recent failure classification seen for a worker.",
	}, []string{"worker_id", "profile", "source", "app_name", "region", "failure_stage", "failure_signature"})

	controlWorkerLastFailure = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_last_failure_unixtime",
		Help: "Unix timestamp of the most recent failed verification seen for a worker.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlLatestDeploymentInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_info",
		Help: "Latest deployment tracked by the control plane.",
	}, []string{"source", "git_sha", "status"})

	controlLatestDeploymentWorkers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_workers",
		Help: "Worker counts for the latest deployment tracked by the control plane.",
	}, []string{"source", "git_sha", "status", "worker_state"})

	controlLatestDeploymentAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_age_seconds",
		Help: "Age of the latest deployment tracked by the control plane.",
	}, []string{"source", "git_sha", "status"})

	controlLatestDeploymentGraceExceeded = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_grace_exceeded",
		Help: "Whether the latest deployment has exceeded the rollout grace window (1=yes, 0=no).",
	}, []string{"source", "git_sha", "status"})
)

func NewControlMetrics(db *model.DB) *controlMetrics {
	m := &controlMetrics{
		statusByWorker:      make(map[string]string),
		workloadByWorker:    make(map[string]labelMetricState),
		runtimeByWorker:     make(map[string]labelMetricState),
		platformByWorker:    make(map[string]labelMetricState),
		failureByWorker:     make(map[string]failureMetricState),
		lastFailureByWorker: make(map[string]failureMetricState),
		rolloutByState:      make(map[string]labelMetricState),
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
		events, err := db.ListWorkerEvents(worker.ID, 20)
		if err == nil {
			m.observePlatformEvent(worker, latestPlatformEvent(coalesceEventFeed(events)))
		}

		verifications, err := db.ListVerifications(worker.ID, 1)
		if err != nil || len(verifications) == 0 {
			latestFailed, err := db.GetLatestFailedVerification(worker.ID)
			if err == nil && latestFailed != nil {
				m.observeLastFailure(worker, *latestFailed)
			}
			continue
		}
		m.observeVerification(worker, verifications[0])

		if verifications[0].Passed {
			latestFailed, err := db.GetLatestFailedVerification(worker.ID)
			if err == nil && latestFailed != nil {
				m.observeLastFailure(worker, *latestFailed)
			}
		}
	}

	m.observeLatestDeployment(db)
}

func (m *controlMetrics) observeWorker(worker model.Worker) {
	labels := workerMetricLabels(worker)
	workloadCfg := resolveWorkerWorkload(worker)
	runtimeStatus := reporting.SnapshotStatus(extractReportedRuntime(worker, nil))
	workloadLabels := []string{
		worker.ID,
		worker.ProfileName,
		worker.Source,
		workerAppName(worker),
		workerRegion(worker),
		workloadCfg.MetricLoadMode(),
		workloadCfg.MetricReplayDataset(),
		workloadCfg.MetricPattern(),
		metricIntLabel(workloadCfg.WriteRate),
		metricIntLabel(workloadCfg.PayloadSize),
		metricIntLabel(workloadCfg.Workers),
		metricFloatLabel(workloadCfg.ReplaySpeed),
		metricIntLabel(workloadCfg.MemoryMB),
		metricIntLabel(workloadCfg.CPUs),
	}
	runtimeLabels := append(labels, metricValueOrUnknown(runtimeStatus))

	controlWorkerInfo.WithLabelValues(worker.ID, worker.GitSHA, worker.ProfileName, worker.Source, workerAppName(worker), workerRegion(worker)).Set(1)

	m.mu.Lock()
	previousStatus := m.statusByWorker[worker.ID]
	m.statusByWorker[worker.ID] = string(worker.Status)
	previousWorkload := m.workloadByWorker[worker.ID]
	m.workloadByWorker[worker.ID] = labelMetricState{labels: workloadLabels}
	previousRuntime := m.runtimeByWorker[worker.ID]
	m.runtimeByWorker[worker.ID] = labelMetricState{labels: runtimeLabels}
	m.mu.Unlock()

	if len(previousWorkload.labels) > 0 && !sameMetricLabels(previousWorkload.labels, workloadLabels) {
		controlWorkerWorkloadInfo.WithLabelValues(previousWorkload.labels...).Set(0)
	}
	controlWorkerWorkloadInfo.WithLabelValues(workloadLabels...).Set(1)

	if len(previousRuntime.labels) > 0 && !sameMetricLabels(previousRuntime.labels, runtimeLabels) {
		controlWorkerRuntimeSnapshotStatus.WithLabelValues(previousRuntime.labels...).Set(0)
	}
	controlWorkerRuntimeSnapshotStatus.WithLabelValues(runtimeLabels...).Set(1)

	if previousStatus != "" && previousStatus != string(worker.Status) {
		controlWorkerStatus.WithLabelValues(append(labels, previousStatus)...).Set(0)
	}
	controlWorkerStatus.WithLabelValues(append(labels, string(worker.Status))...).Set(1)

	if worker.LastHeartbeatAt != nil && !worker.LastHeartbeatAt.IsZero() {
		controlWorkerLastHeartbeat.WithLabelValues(labels...).Set(float64(worker.LastHeartbeatAt.Unix()))
	}
}

func (m *controlMetrics) observePlatformEvent(worker model.Worker, event *model.Event) {
	baseLabels := workerMetricLabels(worker)

	m.mu.Lock()
	previous := m.platformByWorker[worker.ID]
	if event == nil {
		delete(m.platformByWorker, worker.ID)
	} else {
		m.platformByWorker[worker.ID] = labelMetricState{labels: append(baseLabels, metricValueOrUnknown(strings.TrimSpace(event.EventType)), metricValueOrUnknown(strings.TrimSpace(event.Message)))}
	}
	m.mu.Unlock()

	if len(previous.labels) > 0 {
		controlWorkerLatestPlatformEventInfo.WithLabelValues(previous.labels...).Set(0)
		controlWorkerLatestPlatformEventUnixTime.WithLabelValues(previous.labels...).Set(0)
		controlWorkerLatestPlatformEventWindowCount.WithLabelValues(previous.labels...).Set(0)
	}
	if event == nil {
		return
	}

	labels := append(baseLabels, metricValueOrUnknown(strings.TrimSpace(event.EventType)), metricValueOrUnknown(strings.TrimSpace(event.Message)))
	controlWorkerLatestPlatformEventInfo.WithLabelValues(labels...).Set(1)
	if !event.CreatedAt.IsZero() {
		controlWorkerLatestPlatformEventUnixTime.WithLabelValues(labels...).Set(float64(event.CreatedAt.Unix()))
	}
	windowCount := 1
	if event.CollapsedCount > 1 {
		windowCount = event.CollapsedCount
	}
	controlWorkerLatestPlatformEventWindowCount.WithLabelValues(labels...).Set(float64(windowCount))
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
		m.observeLastFailure(worker, verification)
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

func (m *controlMetrics) observeLastFailure(worker model.Worker, verification model.Verification) {
	labels := workerMetricLabels(worker)
	stage := inferFailureStage(&verification)
	signature := inferFailureSignature(&verification)
	lastFailureLabels := append(labels, metricValueOrUnknown(stage), metricValueOrUnknown(signature))

	m.mu.Lock()
	previous := m.lastFailureByWorker[worker.ID]
	m.lastFailureByWorker[worker.ID] = failureMetricState{labels: lastFailureLabels}
	m.mu.Unlock()

	if len(previous.labels) > 0 && !sameMetricLabels(previous.labels, lastFailureLabels) {
		controlWorkerLastFailureInfo.WithLabelValues(previous.labels...).Set(0)
	}
	controlWorkerLastFailureInfo.WithLabelValues(lastFailureLabels...).Set(1)

	if verification.CompletedAt != nil && !verification.CompletedAt.IsZero() {
		controlWorkerLastFailure.WithLabelValues(labels...).Set(float64(verification.CompletedAt.Unix()))
		return
	}
	if !verification.StartedAt.IsZero() {
		controlWorkerLastFailure.WithLabelValues(labels...).Set(float64(verification.StartedAt.Unix()))
	}
}

func (m *controlMetrics) observeLatestDeployment(db *model.DB) {
	deployment, err := db.GetLatestDeployment("main")
	if err != nil || deployment == nil {
		return
	}

	workers, err := db.ListWorkersForSource(valueOrUnknown(deployment.Source))
	if err != nil {
		return
	}

	rollout := DeploymentRolloutResponse{
		Deployment: *deployment,
	}
	for _, worker := range workers {
		rollout.TotalWorkers++
		if strings.TrimSpace(worker.GitSHA) == strings.TrimSpace(deployment.GitSHA) {
			rollout.UpdatedWorkers++
		} else {
			rollout.OutdatedWorkers++
		}

		switch worker.Status {
		case model.WorkerRunning:
			rollout.RunningWorkers++
		case model.WorkerDegraded:
			rollout.DegradedWorkers++
		case model.WorkerDormant:
			rollout.DormantWorkers++
		case model.WorkerProbing:
			rollout.ProbingWorkers++
		}
		if worker.Status != model.WorkerRunning {
			rollout.AttentionWorkers++
		}
	}
	rollout.Status = inferDeploymentRolloutStatus(rollout)
	applyDeploymentRolloutGuidance(&rollout, time.Now().UTC())

	deploymentLabels := []string{
		valueOrUnknown(deployment.Source),
		valueOrUnknown(deployment.GitSHA),
		valueOrUnknown(rollout.Status),
	}
	rolloutStates := map[string]float64{
		"total":     float64(rollout.TotalWorkers),
		"updated":   float64(rollout.UpdatedWorkers),
		"outdated":  float64(rollout.OutdatedWorkers),
		"running":   float64(rollout.RunningWorkers),
		"degraded":  float64(rollout.DegradedWorkers),
		"dormant":   float64(rollout.DormantWorkers),
		"probing":   float64(rollout.ProbingWorkers),
		"attention": float64(rollout.AttentionWorkers),
	}

	m.mu.Lock()
	previousDeployment := m.latestDeployment
	m.latestDeployment = labelMetricState{labels: deploymentLabels}
	previousRolloutStates := make(map[string]labelMetricState, len(m.rolloutByState))
	for key, state := range m.rolloutByState {
		previousRolloutStates[key] = state
	}
	m.rolloutByState = make(map[string]labelMetricState, len(rolloutStates))
	for state := range rolloutStates {
		m.rolloutByState[state] = labelMetricState{labels: append(append([]string{}, deploymentLabels...), state)}
	}
	m.mu.Unlock()

	if len(previousDeployment.labels) > 0 && !sameMetricLabels(previousDeployment.labels, deploymentLabels) {
		controlLatestDeploymentInfo.WithLabelValues(previousDeployment.labels...).Set(0)
		controlLatestDeploymentAge.WithLabelValues(previousDeployment.labels...).Set(0)
		controlLatestDeploymentGraceExceeded.WithLabelValues(previousDeployment.labels...).Set(0)
	}
	controlLatestDeploymentInfo.WithLabelValues(deploymentLabels...).Set(deploymentMetricValue(rollout.Status))
	if !deployment.StartedAt.IsZero() {
		controlLatestDeploymentAge.WithLabelValues(deploymentLabels...).Set(time.Since(deployment.StartedAt).Seconds())
	}
	if rollout.GraceWindowExceeded {
		controlLatestDeploymentGraceExceeded.WithLabelValues(deploymentLabels...).Set(1)
	} else {
		controlLatestDeploymentGraceExceeded.WithLabelValues(deploymentLabels...).Set(0)
	}

	for state, previous := range previousRolloutStates {
		currentLabels := append(append([]string{}, deploymentLabels...), state)
		if len(previous.labels) > 0 && !sameMetricLabels(previous.labels, currentLabels) {
			controlLatestDeploymentWorkers.WithLabelValues(previous.labels...).Set(0)
		}
	}
	for state, value := range rolloutStates {
		controlLatestDeploymentWorkers.WithLabelValues(append(append([]string{}, deploymentLabels...), state)...).Set(value)
	}
}

func deploymentMetricValue(status string) float64 {
	switch status {
	case "stable":
		return 4
	case "needs_attention":
		return 3
	case "probing":
		return 2
	case "rolling_out":
		return 1
	default:
		return 0
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

func metricIntLabel(v int) string {
	if v <= 0 {
		return "0"
	}
	return fmt.Sprintf("%d", v)
}

func metricFloatLabel(v float64) string {
	if v <= 0 {
		return "0"
	}
	label := fmt.Sprintf("%.2f", v)
	label = strings.TrimRight(label, "0")
	label = strings.TrimRight(label, ".")
	if label == "" {
		return "0"
	}
	return label
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
