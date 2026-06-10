package orchestrator

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type controlMetrics struct {
	mu                      sync.Mutex
	statusByWorker          map[string]string
	infoByWorker            map[string]labelMetricState
	workloadByWorker        map[string]labelMetricState
	runtimeByWorker         map[string]labelMetricState
	platformByWorker        map[string]labelMetricState
	failureByWorker         map[string]failureMetricState
	lastFailureByWorker     map[string]failureMetricState
	latestDeployment        labelMetricState
	latestDeploymentVersion labelMetricState
	rolloutByState          map[string]labelMetricState
	comparisonInfo          labelMetricState
	comparisonWorkers       map[string]labelMetricState
	comparisonDeltas        map[string]labelMetricState
	comparisonFailures      map[string]labelMetricState
	sourceComparisonInfo    map[string]labelMetricState
	sourceComparisonWorkers map[string]labelMetricState
	sourceComparisonDeltas  map[string]labelMetricState
	sourceComparisonFailure map[string]labelMetricState
	volumeInventory         map[string]volumeMetricState
}

type labelMetricState struct {
	labels []string
}

type failureMetricState struct {
	labels []string
}

type volumeMetricState struct {
	labels  []string
	count   float64
	totalGB float64
}

var (
	controlWorkerInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_info",
		Help: "Static control-plane info about a soak worker.",
	}, []string{"worker_id", "git_sha", "profile", "source", "app_name", "region"})

	controlWorkerVersionInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_version_info",
		Help: "Static version info about a soak worker and the Litestream commit under test.",
	}, []string{"worker_id", "git_sha", "litestream_sha", "profile", "source", "app_name", "region"})

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

	controlWorkerDataDiskTotalSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_data_disk_total_bytes",
		Help: "Total size of the worker data filesystem in bytes from the latest runtime snapshot.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerDataDiskUsedSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_data_disk_used_bytes",
		Help: "Used size of the worker data filesystem in bytes from the latest runtime snapshot.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerDataDiskFreeSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_data_disk_free_bytes",
		Help: "Free size of the worker data filesystem in bytes from the latest runtime snapshot.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerDataDiskUsedPercent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_data_disk_used_percent",
		Help: "Used percentage of the worker data filesystem from the latest runtime snapshot.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerDBSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_db_size_bytes",
		Help: "Current database file size in bytes from the latest runtime snapshot.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerWALSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_wal_size_bytes",
		Help: "Current WAL file size in bytes from the latest runtime snapshot.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerLitestreamLocalStateSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_litestream_local_state_bytes",
		Help: "Recursive size of the local Litestream state directory in bytes from the latest runtime snapshot.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

	controlWorkerLitestreamLocalLTXSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_worker_litestream_local_ltx_bytes",
		Help: "Recursive size of the local Litestream LTX directory in bytes from the latest runtime snapshot.",
	}, []string{"worker_id", "profile", "source", "app_name", "region"})

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

	controlLatestDeploymentVersionInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_version_info",
		Help: "Latest deployment tracked by the control plane, including the Litestream commit under test.",
	}, []string{"source", "git_sha", "litestream_sha", "status"})

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

	controlLatestDeploymentComparisonInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_comparison_info",
		Help: "Latest release-over-release comparison tracked by the control plane.",
	}, []string{"source", "head_git_sha", "head_litestream_sha", "base_git_sha", "base_litestream_sha", "verdict"})

	controlLatestDeploymentComparisonWorkers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_comparison_workers",
		Help: "Worker counts for the latest release comparison, split by head/base deployment.",
	}, []string{"source", "comparison_role", "git_sha", "litestream_sha", "worker_state"})

	controlLatestDeploymentComparisonDelta = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_comparison_delta",
		Help: "Derived release-over-release deltas for the latest deployment comparison.",
	}, []string{"source", "head_git_sha", "head_litestream_sha", "base_git_sha", "base_litestream_sha", "delta_type", "verdict"})

	controlLatestDeploymentComparisonFailure = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_latest_deployment_comparison_failure",
		Help: "Failure signature counts for the latest release comparison.",
	}, []string{"source", "comparison_role", "git_sha", "litestream_sha", "failure_stage", "failure_signature"})

	controlSourceComparisonInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_source_comparison_info",
		Help: "Latest cross-source comparison tracked by the control plane.",
	}, []string{"base_source", "head_source", "head_git_sha", "head_litestream_sha", "base_git_sha", "base_litestream_sha", "verdict"})

	controlSourceComparisonWorkers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_source_comparison_workers",
		Help: "Worker counts for the latest cross-source comparison, split by head/base deployment.",
	}, []string{"base_source", "head_source", "comparison_role", "git_sha", "litestream_sha", "worker_state"})

	controlSourceComparisonDelta = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_source_comparison_delta",
		Help: "Derived deltas for the latest cross-source comparison.",
	}, []string{"base_source", "head_source", "head_git_sha", "head_litestream_sha", "base_git_sha", "base_litestream_sha", "delta_type", "verdict"})

	controlSourceComparisonFailure = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_source_comparison_failure",
		Help: "Failure signature counts for the latest cross-source comparison.",
	}, []string{"base_source", "head_source", "comparison_role", "git_sha", "litestream_sha", "failure_stage", "failure_signature"})

	controlAppVolumeCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_app_volume_count",
		Help: "Fly volume count grouped by app, region, attachment state, and configured size.",
	}, []string{"app_name", "region", "attachment_state", "size_gb"})

	controlAppVolumeSizeGB = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_control_app_volume_size_gb",
		Help: "Total Fly volume size in gigabytes grouped by app, region, attachment state, and configured size.",
	}, []string{"app_name", "region", "attachment_state", "size_gb"})
)

func NewControlMetrics(db *model.DB) *controlMetrics {
	m := &controlMetrics{
		statusByWorker:          make(map[string]string),
		infoByWorker:            make(map[string]labelMetricState),
		workloadByWorker:        make(map[string]labelMetricState),
		runtimeByWorker:         make(map[string]labelMetricState),
		platformByWorker:        make(map[string]labelMetricState),
		failureByWorker:         make(map[string]failureMetricState),
		lastFailureByWorker:     make(map[string]failureMetricState),
		rolloutByState:          make(map[string]labelMetricState),
		comparisonWorkers:       make(map[string]labelMetricState),
		comparisonDeltas:        make(map[string]labelMetricState),
		comparisonFailures:      make(map[string]labelMetricState),
		sourceComparisonInfo:    make(map[string]labelMetricState),
		sourceComparisonWorkers: make(map[string]labelMetricState),
		sourceComparisonDeltas:  make(map[string]labelMetricState),
		sourceComparisonFailure: make(map[string]labelMetricState),
		volumeInventory:         make(map[string]volumeMetricState),
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
	m.observeLatestDeploymentComparison(db)
	m.observeSourceComparisons(db)
}

func (m *controlMetrics) observeWorker(worker model.Worker) {
	labels := workerMetricLabels(worker)
	infoLabels := []string{
		worker.ID,
		worker.GitSHA,
		worker.ProfileName,
		worker.Source,
		workerAppName(worker),
		workerRegion(worker),
	}
	versionLabels := []string{
		worker.ID,
		worker.GitSHA,
		worker.LitestreamSHA,
		worker.ProfileName,
		worker.Source,
		workerAppName(worker),
		workerRegion(worker),
	}
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

	m.mu.Lock()
	previousStatus := m.statusByWorker[worker.ID]
	m.statusByWorker[worker.ID] = string(worker.Status)
	previousInfo := m.infoByWorker[worker.ID]
	m.infoByWorker[worker.ID] = labelMetricState{labels: versionLabels}
	previousWorkload := m.workloadByWorker[worker.ID]
	m.workloadByWorker[worker.ID] = labelMetricState{labels: workloadLabels}
	previousRuntime := m.runtimeByWorker[worker.ID]
	m.runtimeByWorker[worker.ID] = labelMetricState{labels: runtimeLabels}
	m.mu.Unlock()

	if len(previousInfo.labels) > 0 {
		previousInfoLabels := []string{previousInfo.labels[0], previousInfo.labels[1], previousInfo.labels[3], previousInfo.labels[4], previousInfo.labels[5], previousInfo.labels[6]}
		if !sameMetricLabels(previousInfoLabels, infoLabels) {
			controlWorkerInfo.WithLabelValues(previousInfoLabels...).Set(0)
		}
		if !sameMetricLabels(previousInfo.labels, versionLabels) {
			controlWorkerVersionInfo.WithLabelValues(previousInfo.labels...).Set(0)
		}
	}
	controlWorkerInfo.WithLabelValues(infoLabels...).Set(1)
	controlWorkerVersionInfo.WithLabelValues(versionLabels...).Set(1)

	if len(previousWorkload.labels) > 0 && !sameMetricLabels(previousWorkload.labels, workloadLabels) {
		controlWorkerWorkloadInfo.WithLabelValues(previousWorkload.labels...).Set(0)
	}
	controlWorkerWorkloadInfo.WithLabelValues(workloadLabels...).Set(1)

	if len(previousRuntime.labels) > 0 && !sameMetricLabels(previousRuntime.labels, runtimeLabels) {
		controlWorkerRuntimeSnapshotStatus.WithLabelValues(previousRuntime.labels...).Set(0)
	}
	controlWorkerRuntimeSnapshotStatus.WithLabelValues(runtimeLabels...).Set(1)
	if runtime := extractReportedRuntime(worker, nil); runtime != nil {
		controlWorkerDataDiskTotalSize.WithLabelValues(labels...).Set(float64(runtime.DataDiskTotalBytes))
		controlWorkerDataDiskUsedSize.WithLabelValues(labels...).Set(float64(runtime.DataDiskUsedBytes))
		controlWorkerDataDiskFreeSize.WithLabelValues(labels...).Set(float64(runtime.DataDiskFreeBytes))
		controlWorkerDataDiskUsedPercent.WithLabelValues(labels...).Set(runtime.DataDiskUsedPercent)
		controlWorkerDBSize.WithLabelValues(labels...).Set(float64(runtime.DBSizeBytes))
		controlWorkerWALSize.WithLabelValues(labels...).Set(float64(runtime.WALSizeBytes))
		controlWorkerLitestreamLocalStateSize.WithLabelValues(labels...).Set(float64(runtime.LitestreamDirSizeBytes))
		controlWorkerLitestreamLocalLTXSize.WithLabelValues(labels...).Set(float64(runtime.LitestreamLTXSizeBytes))
	}

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

	rollout, err := buildDeploymentRollout(db, *deployment)
	if err != nil {
		return
	}

	deploymentLabels := []string{
		valueOrUnknown(rollout.Deployment.Source),
		valueOrUnknown(rollout.Deployment.GitSHA),
		valueOrUnknown(rollout.Status),
	}
	deploymentVersionLabels := []string{
		valueOrUnknown(rollout.Deployment.Source),
		valueOrUnknown(rollout.Deployment.GitSHA),
		valueOrUnknown(rollout.Deployment.LitestreamSHA),
		valueOrUnknown(rollout.Status),
	}
	rolloutStates := map[string]float64{
		"total":                 float64(rollout.TotalWorkers),
		"updated":               float64(rollout.UpdatedWorkers),
		"outdated":              float64(rollout.OutdatedWorkers),
		"running":               float64(rollout.RunningWorkers),
		"degraded":              float64(rollout.DegradedWorkers),
		"dormant":               float64(rollout.DormantWorkers),
		"probing":               float64(rollout.ProbingWorkers),
		"runtime_unhealthy":     float64(rollout.RuntimeUnhealthyWorkers),
		"attention":             float64(rollout.AttentionWorkers),
		"verified_since_deploy": float64(rollout.VerifiedSinceDeploy),
		"awaiting_verification": float64(rollout.AwaitingVerification),
	}

	m.mu.Lock()
	previousDeployment := m.latestDeployment
	previousDeploymentVersion := m.latestDeploymentVersion
	m.latestDeployment = labelMetricState{labels: deploymentLabels}
	m.latestDeploymentVersion = labelMetricState{labels: deploymentVersionLabels}
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
	if len(previousDeploymentVersion.labels) > 0 && !sameMetricLabels(previousDeploymentVersion.labels, deploymentVersionLabels) {
		controlLatestDeploymentVersionInfo.WithLabelValues(previousDeploymentVersion.labels...).Set(0)
	}
	controlLatestDeploymentInfo.WithLabelValues(deploymentLabels...).Set(deploymentMetricValue(rollout.Status))
	controlLatestDeploymentVersionInfo.WithLabelValues(deploymentVersionLabels...).Set(deploymentMetricValue(rollout.Status))
	if !rollout.Deployment.StartedAt.IsZero() {
		controlLatestDeploymentAge.WithLabelValues(deploymentLabels...).Set(time.Since(rollout.Deployment.StartedAt).Seconds())
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

func (m *controlMetrics) observeLatestDeploymentComparison(db *model.DB) {
	comparison, err := buildLatestDeploymentComparison(db, "main")
	if err != nil || comparison == nil {
		return
	}

	headDeployment := comparison.Head.Deployment
	baseDeployment := model.Deployment{}
	if comparison.Base != nil {
		baseDeployment = comparison.Base.Deployment
	}

	infoLabels := []string{
		valueOrUnknown(headDeployment.Source),
		valueOrUnknown(headDeployment.GitSHA),
		valueOrUnknown(headDeployment.LitestreamSHA),
		valueOrUnknown(baseDeployment.GitSHA),
		valueOrUnknown(baseDeployment.LitestreamSHA),
		valueOrUnknown(comparison.Verdict),
	}

	workerStates := make(map[string]comparisonMetricValue)
	for state, value := range deploymentScorecardWorkerStates(comparison.Head) {
		workerStates["head:"+state] = comparisonMetricValue{
			labels: []string{
				valueOrUnknown(headDeployment.Source),
				"head",
				valueOrUnknown(headDeployment.GitSHA),
				valueOrUnknown(headDeployment.LitestreamSHA),
				state,
			},
			value: value,
		}
	}
	if comparison.Base != nil {
		for state, value := range deploymentScorecardWorkerStates(*comparison.Base) {
			workerStates["base:"+state] = comparisonMetricValue{
				labels: []string{
					valueOrUnknown(baseDeployment.Source),
					"base",
					valueOrUnknown(baseDeployment.GitSHA),
					valueOrUnknown(baseDeployment.LitestreamSHA),
					state,
				},
				value: value,
			}
		}
	}

	deltaLabels := []string{
		valueOrUnknown(headDeployment.Source),
		valueOrUnknown(headDeployment.GitSHA),
		valueOrUnknown(headDeployment.LitestreamSHA),
		valueOrUnknown(baseDeployment.GitSHA),
		valueOrUnknown(baseDeployment.LitestreamSHA),
	}
	deltaStates := map[string]comparisonMetricValue{
		"pass_delta": {
			labels: append(append([]string{}, deltaLabels...), "pass_delta", valueOrUnknown(comparison.Verdict)),
			value:  float64(comparison.PassDelta),
		},
		"fail_delta": {
			labels: append(append([]string{}, deltaLabels...), "fail_delta", valueOrUnknown(comparison.Verdict)),
			value:  float64(comparison.FailDelta),
		},
		"awaiting_delta": {
			labels: append(append([]string{}, deltaLabels...), "awaiting_delta", valueOrUnknown(comparison.Verdict)),
			value:  float64(comparison.AwaitingDelta),
		},
		"improved_workers": {
			labels: append(append([]string{}, deltaLabels...), "improved_workers", valueOrUnknown(comparison.Verdict)),
			value:  float64(len(comparison.ImprovedWorkers)),
		},
		"regressed_workers": {
			labels: append(append([]string{}, deltaLabels...), "regressed_workers", valueOrUnknown(comparison.Verdict)),
			value:  float64(len(comparison.RegressedWorkers)),
		},
		"new_failures": {
			labels: append(append([]string{}, deltaLabels...), "new_failures", valueOrUnknown(comparison.Verdict)),
			value:  float64(len(comparison.NewFailures)),
		},
		"resolved_failures": {
			labels: append(append([]string{}, deltaLabels...), "resolved_failures", valueOrUnknown(comparison.Verdict)),
			value:  float64(len(comparison.ResolvedFailures)),
		},
	}

	failureStates := make(map[string]comparisonMetricValue)
	for _, failure := range comparison.Head.Failures {
		key := strings.Join([]string{"head", failure.Stage, failure.Signature}, ":")
		failureStates[key] = comparisonMetricValue{
			labels: []string{
				valueOrUnknown(headDeployment.Source),
				"head",
				valueOrUnknown(headDeployment.GitSHA),
				valueOrUnknown(headDeployment.LitestreamSHA),
				metricValueOrUnknown(failure.Stage),
				metricValueOrUnknown(failure.Signature),
			},
			value: float64(failure.Count),
		}
	}
	if comparison.Base != nil {
		for _, failure := range comparison.Base.Failures {
			key := strings.Join([]string{"base", failure.Stage, failure.Signature}, ":")
			failureStates[key] = comparisonMetricValue{
				labels: []string{
					valueOrUnknown(baseDeployment.Source),
					"base",
					valueOrUnknown(baseDeployment.GitSHA),
					valueOrUnknown(baseDeployment.LitestreamSHA),
					metricValueOrUnknown(failure.Stage),
					metricValueOrUnknown(failure.Signature),
				},
				value: float64(failure.Count),
			}
		}
	}
	for _, failure := range comparison.NewFailures {
		key := strings.Join([]string{"new", failure.Stage, failure.Signature}, ":")
		failureStates[key] = comparisonMetricValue{
			labels: []string{
				valueOrUnknown(headDeployment.Source),
				"new",
				valueOrUnknown(headDeployment.GitSHA),
				valueOrUnknown(headDeployment.LitestreamSHA),
				metricValueOrUnknown(failure.Stage),
				metricValueOrUnknown(failure.Signature),
			},
			value: float64(failure.Count),
		}
	}
	for _, failure := range comparison.ResolvedFailures {
		key := strings.Join([]string{"resolved", failure.Stage, failure.Signature}, ":")
		failureStates[key] = comparisonMetricValue{
			labels: []string{
				valueOrUnknown(baseDeployment.Source),
				"resolved",
				valueOrUnknown(baseDeployment.GitSHA),
				valueOrUnknown(baseDeployment.LitestreamSHA),
				metricValueOrUnknown(failure.Stage),
				metricValueOrUnknown(failure.Signature),
			},
			value: float64(failure.Count),
		}
	}

	m.mu.Lock()
	previousInfo := m.comparisonInfo
	previousWorkers := cloneLabelMetricStates(m.comparisonWorkers)
	previousDeltas := cloneLabelMetricStates(m.comparisonDeltas)
	previousFailures := cloneLabelMetricStates(m.comparisonFailures)
	m.comparisonInfo = labelMetricState{labels: infoLabels}
	m.comparisonWorkers = make(map[string]labelMetricState, len(workerStates))
	for key, metric := range workerStates {
		m.comparisonWorkers[key] = labelMetricState{labels: metric.labels}
	}
	m.comparisonDeltas = make(map[string]labelMetricState, len(deltaStates))
	for key, metric := range deltaStates {
		m.comparisonDeltas[key] = labelMetricState{labels: metric.labels}
	}
	m.comparisonFailures = make(map[string]labelMetricState, len(failureStates))
	for key, metric := range failureStates {
		m.comparisonFailures[key] = labelMetricState{labels: metric.labels}
	}
	m.mu.Unlock()

	if len(previousInfo.labels) > 0 && !sameMetricLabels(previousInfo.labels, infoLabels) {
		controlLatestDeploymentComparisonInfo.WithLabelValues(previousInfo.labels...).Set(0)
	}
	controlLatestDeploymentComparisonInfo.WithLabelValues(infoLabels...).Set(1)

	for _, previous := range previousWorkers {
		if len(previous.labels) > 0 {
			controlLatestDeploymentComparisonWorkers.WithLabelValues(previous.labels...).Set(0)
		}
	}
	for _, metric := range workerStates {
		controlLatestDeploymentComparisonWorkers.WithLabelValues(metric.labels...).Set(metric.value)
	}

	for _, previous := range previousDeltas {
		if len(previous.labels) > 0 {
			controlLatestDeploymentComparisonDelta.WithLabelValues(previous.labels...).Set(0)
		}
	}
	for _, metric := range deltaStates {
		controlLatestDeploymentComparisonDelta.WithLabelValues(metric.labels...).Set(metric.value)
	}

	for _, previous := range previousFailures {
		if len(previous.labels) > 0 {
			controlLatestDeploymentComparisonFailure.WithLabelValues(previous.labels...).Set(0)
		}
	}
	for _, metric := range failureStates {
		controlLatestDeploymentComparisonFailure.WithLabelValues(metric.labels...).Set(metric.value)
	}
}

func (m *controlMetrics) observeSourceComparisons(db *model.DB) {
	workers, err := db.ListWorkers("")
	if err != nil {
		return
	}

	headSources := make([]string, 0)
	seen := make(map[string]struct{})
	for _, worker := range workers {
		source := strings.TrimSpace(worker.Source)
		if source == "" || source == "main" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		headSources = append(headSources, source)
	}
	sort.Strings(headSources)

	infoStates := make(map[string]labelMetricState)
	workerStates := make(map[string]labelMetricState)
	deltaStates := make(map[string]labelMetricState)
	failureStates := make(map[string]labelMetricState)

	for _, headSource := range headSources {
		comparison, err := buildLatestCrossSourceDeploymentComparison(db, "main", headSource)
		if err != nil || comparison == nil || comparison.Base == nil {
			continue
		}

		headDeployment := comparison.Head.Deployment
		baseDeployment := comparison.Base.Deployment
		infoLabels := []string{
			valueOrUnknown(comparison.BaseSource),
			valueOrUnknown(comparison.HeadSource),
			valueOrUnknown(headDeployment.GitSHA),
			valueOrUnknown(headDeployment.LitestreamSHA),
			valueOrUnknown(baseDeployment.GitSHA),
			valueOrUnknown(baseDeployment.LitestreamSHA),
			valueOrUnknown(comparison.Verdict),
		}
		infoStates[headSource] = labelMetricState{labels: infoLabels}

		for state, value := range deploymentScorecardWorkerStates(comparison.Head) {
			key := strings.Join([]string{headSource, "head", state}, ":")
			workerStates[key] = labelMetricState{labels: []string{
				valueOrUnknown(comparison.BaseSource),
				valueOrUnknown(comparison.HeadSource),
				"head",
				valueOrUnknown(headDeployment.GitSHA),
				valueOrUnknown(headDeployment.LitestreamSHA),
				state,
			}}
			controlSourceComparisonWorkers.WithLabelValues(workerStates[key].labels...).Set(value)
		}
		for state, value := range deploymentScorecardWorkerStates(*comparison.Base) {
			key := strings.Join([]string{headSource, "base", state}, ":")
			workerStates[key] = labelMetricState{labels: []string{
				valueOrUnknown(comparison.BaseSource),
				valueOrUnknown(comparison.HeadSource),
				"base",
				valueOrUnknown(baseDeployment.GitSHA),
				valueOrUnknown(baseDeployment.LitestreamSHA),
				state,
			}}
			controlSourceComparisonWorkers.WithLabelValues(workerStates[key].labels...).Set(value)
		}

		deltaLabels := []string{
			valueOrUnknown(comparison.BaseSource),
			valueOrUnknown(comparison.HeadSource),
			valueOrUnknown(headDeployment.GitSHA),
			valueOrUnknown(headDeployment.LitestreamSHA),
			valueOrUnknown(baseDeployment.GitSHA),
			valueOrUnknown(baseDeployment.LitestreamSHA),
		}
		deltas := map[string]float64{
			"pass_delta":        float64(comparison.PassDelta),
			"fail_delta":        float64(comparison.FailDelta),
			"awaiting_delta":    float64(comparison.AwaitingDelta),
			"improved_workers":  float64(len(comparison.ImprovedWorkers)),
			"regressed_workers": float64(len(comparison.RegressedWorkers)),
			"new_failures":      float64(len(comparison.NewFailures)),
			"resolved_failures": float64(len(comparison.ResolvedFailures)),
		}
		for deltaType, value := range deltas {
			key := strings.Join([]string{headSource, deltaType}, ":")
			deltaStates[key] = labelMetricState{labels: append(append([]string{}, deltaLabels...), deltaType, valueOrUnknown(comparison.Verdict))}
			controlSourceComparisonDelta.WithLabelValues(deltaStates[key].labels...).Set(value)
		}

		for _, failure := range comparison.Head.Failures {
			key := strings.Join([]string{headSource, "head", failure.Stage, failure.Signature}, ":")
			failureStates[key] = labelMetricState{labels: []string{
				valueOrUnknown(comparison.BaseSource),
				valueOrUnknown(comparison.HeadSource),
				"head",
				valueOrUnknown(headDeployment.GitSHA),
				valueOrUnknown(headDeployment.LitestreamSHA),
				metricValueOrUnknown(failure.Stage),
				metricValueOrUnknown(failure.Signature),
			}}
			controlSourceComparisonFailure.WithLabelValues(failureStates[key].labels...).Set(float64(failure.Count))
		}
		for _, failure := range comparison.Base.Failures {
			key := strings.Join([]string{headSource, "base", failure.Stage, failure.Signature}, ":")
			failureStates[key] = labelMetricState{labels: []string{
				valueOrUnknown(comparison.BaseSource),
				valueOrUnknown(comparison.HeadSource),
				"base",
				valueOrUnknown(baseDeployment.GitSHA),
				valueOrUnknown(baseDeployment.LitestreamSHA),
				metricValueOrUnknown(failure.Stage),
				metricValueOrUnknown(failure.Signature),
			}}
			controlSourceComparisonFailure.WithLabelValues(failureStates[key].labels...).Set(float64(failure.Count))
		}
		for _, failure := range comparison.NewFailures {
			key := strings.Join([]string{headSource, "new", failure.Stage, failure.Signature}, ":")
			failureStates[key] = labelMetricState{labels: []string{
				valueOrUnknown(comparison.BaseSource),
				valueOrUnknown(comparison.HeadSource),
				"new",
				valueOrUnknown(headDeployment.GitSHA),
				valueOrUnknown(headDeployment.LitestreamSHA),
				metricValueOrUnknown(failure.Stage),
				metricValueOrUnknown(failure.Signature),
			}}
			controlSourceComparisonFailure.WithLabelValues(failureStates[key].labels...).Set(float64(failure.Count))
		}
		for _, failure := range comparison.ResolvedFailures {
			key := strings.Join([]string{headSource, "resolved", failure.Stage, failure.Signature}, ":")
			failureStates[key] = labelMetricState{labels: []string{
				valueOrUnknown(comparison.BaseSource),
				valueOrUnknown(comparison.HeadSource),
				"resolved",
				valueOrUnknown(baseDeployment.GitSHA),
				valueOrUnknown(baseDeployment.LitestreamSHA),
				metricValueOrUnknown(failure.Stage),
				metricValueOrUnknown(failure.Signature),
			}}
			controlSourceComparisonFailure.WithLabelValues(failureStates[key].labels...).Set(float64(failure.Count))
		}

		controlSourceComparisonInfo.WithLabelValues(infoLabels...).Set(1)
	}

	m.mu.Lock()
	previousInfo := cloneLabelMetricStates(m.sourceComparisonInfo)
	previousWorkers := cloneLabelMetricStates(m.sourceComparisonWorkers)
	previousDeltas := cloneLabelMetricStates(m.sourceComparisonDeltas)
	previousFailures := cloneLabelMetricStates(m.sourceComparisonFailure)
	m.sourceComparisonInfo = infoStates
	m.sourceComparisonWorkers = workerStates
	m.sourceComparisonDeltas = deltaStates
	m.sourceComparisonFailure = failureStates
	m.mu.Unlock()

	for key, previous := range previousInfo {
		current, ok := infoStates[key]
		if !ok || !sameMetricLabels(previous.labels, current.labels) {
			controlSourceComparisonInfo.WithLabelValues(previous.labels...).Set(0)
		}
	}
	for key, previous := range previousWorkers {
		current, ok := workerStates[key]
		if !ok || !sameMetricLabels(previous.labels, current.labels) {
			controlSourceComparisonWorkers.WithLabelValues(previous.labels...).Set(0)
		}
	}
	for key, previous := range previousDeltas {
		current, ok := deltaStates[key]
		if !ok || !sameMetricLabels(previous.labels, current.labels) {
			controlSourceComparisonDelta.WithLabelValues(previous.labels...).Set(0)
		}
	}
	for key, previous := range previousFailures {
		current, ok := failureStates[key]
		if !ok || !sameMetricLabels(previous.labels, current.labels) {
			controlSourceComparisonFailure.WithLabelValues(previous.labels...).Set(0)
		}
	}
}

type comparisonMetricValue struct {
	labels []string
	value  float64
}

func deploymentScorecardWorkerStates(scorecard DeploymentScorecard) map[string]float64 {
	return map[string]float64{
		"total":    float64(scorecard.TotalWorkers),
		"verified": float64(scorecard.VerifiedWorkers),
		"passed":   float64(scorecard.PassedWorkers),
		"failed":   float64(scorecard.FailedWorkers),
		"awaiting": float64(scorecard.AwaitingWorkers),
	}
}

func cloneLabelMetricStates(source map[string]labelMetricState) map[string]labelMetricState {
	cloned := make(map[string]labelMetricState, len(source))
	for key, state := range source {
		cloned[key] = state
	}
	return cloned
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

func (m *controlMetrics) observeVolumes(appName string, volumes []flyapi.Volume) {
	appName = metricValueOrUnknown(appName)
	next := make(map[string]volumeMetricState)
	for _, volume := range volumes {
		if skipVolumeInventoryState(volume.State) {
			continue
		}
		attachmentState := "unattached"
		if strings.TrimSpace(volume.AttachedMachineID) != "" {
			attachmentState = "attached"
		}
		labels := []string{
			appName,
			metricValueOrUnknown(volume.Region),
			attachmentState,
			metricIntLabel(volume.SizeGB),
		}
		key := strings.Join(labels, ":")
		state := next[key]
		if len(state.labels) == 0 {
			state.labels = labels
		}
		state.count++
		state.totalGB += float64(volume.SizeGB)
		next[key] = state
	}

	m.mu.Lock()
	previous := make([]volumeMetricState, 0, len(m.volumeInventory))
	for key, state := range m.volumeInventory {
		if _, ok := next[key]; !ok {
			previous = append(previous, state)
		}
	}
	m.volumeInventory = next
	m.mu.Unlock()

	for _, state := range previous {
		if len(state.labels) > 0 {
			controlAppVolumeCount.WithLabelValues(state.labels...).Set(0)
			controlAppVolumeSizeGB.WithLabelValues(state.labels...).Set(0)
		}
	}
	for _, state := range next {
		controlAppVolumeCount.WithLabelValues(state.labels...).Set(state.count)
		controlAppVolumeSizeGB.WithLabelValues(state.labels...).Set(state.totalGB)
	}
}

func skipVolumeInventoryState(state string) bool {
	switch strings.TrimSpace(strings.ToLower(state)) {
	case "destroyed", "deleting", "pending_destroy":
		return true
	default:
		return false
	}
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
