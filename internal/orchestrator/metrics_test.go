package orchestrator

import (
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestControlMetricsExposeLatestDeploymentComparison(t *testing.T) {
	db := openTestDB(t)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-base-metrics",
		LitestreamSHA: "litestream-base-metrics",
		ImageRef:      "registry.fly.io/litestream-soak:sha-base-metrics",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(base) error = %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:        "sha-head-metrics",
		LitestreamSHA: "litestream-head-metrics",
		ImageRef:      "registry.fly.io/litestream-soak:sha-head-metrics",
		Source:        "main",
		Status:        "ready",
	}); err != nil {
		t.Fatalf("UpsertReadyDeployment(head) error = %v", err)
	}

	deployments, err := db.ListDeployments("main", 2)
	if err != nil {
		t.Fatalf("ListDeployments() error = %v", err)
	}
	head := deployments[0]
	base := deployments[1]

	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-metrics-one",
		Name:          "worker-main-metrics-one",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        head.GitSHA,
		LitestreamSHA: head.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-metrics-two",
		Name:          "worker-main-metrics-two",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        head.GitSHA,
		LitestreamSHA: head.LitestreamSHA,
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-metrics-three",
		Name:          "worker-main-metrics-three",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        head.GitSHA,
		LitestreamSHA: head.LitestreamSHA,
		ProfileName:   "burst-volume",
		ProfileConfig: "{}",
	})

	basePassAt := base.StartedAt.Add(200 * time.Millisecond).UTC()
	baseFailAt := base.StartedAt.Add(400 * time.Millisecond).UTC()
	headPassAt := head.StartedAt.Add(200 * time.Millisecond).UTC()
	headFailAt := head.StartedAt.Add(400 * time.Millisecond).UTC()

	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-metrics-one",
		StartedAt:   basePassAt.Add(-15 * time.Second),
		CompletedAt: &basePassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-main-metrics-two",
		StartedAt:    baseFailAt.Add(-15 * time.Second),
		CompletedAt:  &baseFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wrong # of entries in index idx_load_test_timestamp`,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-metrics-three",
		StartedAt:   basePassAt.Add(-30 * time.Second),
		CompletedAt: &basePassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-metrics-one",
		StartedAt:   headPassAt.Add(-15 * time.Second),
		CompletedAt: &headPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-main-metrics-two",
		StartedAt:   headPassAt.Add(-30 * time.Second),
		CompletedAt: &headPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  15000,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-main-metrics-three",
		StartedAt:    headFailAt.Add(-15 * time.Second),
		CompletedAt:  &headFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   15000,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": context deadline exceeded`,
	})

	NewControlMetrics(db)

	assertGatheredGaugeValue(t, "soak_control_latest_deployment_comparison_info", map[string]string{
		"source":              "main",
		"head_git_sha":        "sha-head-metrics",
		"head_litestream_sha": "litestream-head-metrics",
		"base_git_sha":        "sha-base-metrics",
		"base_litestream_sha": "litestream-base-metrics",
		"verdict":             "mixed",
	}, 1)
	assertGatheredGaugeValue(t, "soak_control_latest_deployment_comparison_workers", map[string]string{
		"source":          "main",
		"comparison_role": "head",
		"git_sha":         "sha-head-metrics",
		"litestream_sha":  "litestream-head-metrics",
		"worker_state":    "passed",
	}, 2)
	assertGatheredGaugeValue(t, "soak_control_latest_deployment_comparison_workers", map[string]string{
		"source":          "main",
		"comparison_role": "base",
		"git_sha":         "sha-base-metrics",
		"litestream_sha":  "litestream-base-metrics",
		"worker_state":    "failed",
	}, 1)
	assertGatheredGaugeValue(t, "soak_control_latest_deployment_comparison_delta", map[string]string{
		"source":              "main",
		"head_git_sha":        "sha-head-metrics",
		"head_litestream_sha": "litestream-head-metrics",
		"base_git_sha":        "sha-base-metrics",
		"base_litestream_sha": "litestream-base-metrics",
		"delta_type":          "improved_workers",
		"verdict":             "mixed",
	}, 1)
	assertGatheredGaugeValue(t, "soak_control_latest_deployment_comparison_delta", map[string]string{
		"source":              "main",
		"head_git_sha":        "sha-head-metrics",
		"head_litestream_sha": "litestream-head-metrics",
		"base_git_sha":        "sha-base-metrics",
		"base_litestream_sha": "litestream-base-metrics",
		"delta_type":          "regressed_workers",
		"verdict":             "mixed",
	}, 1)
	assertGatheredGaugeValue(t, "soak_control_latest_deployment_comparison_failure", map[string]string{
		"source":            "main",
		"comparison_role":   "new",
		"git_sha":           "sha-head-metrics",
		"litestream_sha":    "litestream-head-metrics",
		"failure_stage":     "sync",
		"failure_signature": "litestream_sync_timeout",
	}, 1)
	assertGatheredGaugeValue(t, "soak_control_latest_deployment_comparison_failure", map[string]string{
		"source":            "main",
		"comparison_role":   "resolved",
		"git_sha":           "sha-base-metrics",
		"litestream_sha":    "litestream-base-metrics",
		"failure_stage":     "integrity_check",
		"failure_signature": "sqlite_index_mismatch",
	}, 1)
}

func TestControlMetricsObserveWorkerZeroesStaleInfoSeries(t *testing.T) {
	db := openTestDB(t)
	metrics := NewControlMetrics(db)

	worker := model.Worker{
		ID:            "worker-info-stale-series",
		Name:          "worker-info-stale-series",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-info-old",
		LitestreamSHA: "litestream-info-old",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		AppName:       "metrics-info-app",
		Region:        "ord",
	}
	metrics.observeWorker(worker)

	assertGatheredGaugeValue(t, "soak_control_worker_info", map[string]string{
		"worker_id": "worker-info-stale-series",
		"git_sha":   "sha-info-old",
		"profile":   "low-volume",
		"source":    "main",
		"app_name":  "metrics-info-app",
		"region":    "ord",
	}, 1)
	assertGatheredGaugeValue(t, "soak_control_worker_version_info", map[string]string{
		"worker_id":      "worker-info-stale-series",
		"git_sha":        "sha-info-old",
		"litestream_sha": "litestream-info-old",
		"profile":        "low-volume",
		"source":         "main",
		"app_name":       "metrics-info-app",
		"region":         "ord",
	}, 1)

	worker.GitSHA = "sha-info-new"
	worker.LitestreamSHA = "litestream-info-new"
	metrics.observeWorker(worker)

	assertGatheredGaugeValue(t, "soak_control_worker_info", map[string]string{
		"worker_id": "worker-info-stale-series",
		"git_sha":   "sha-info-old",
		"profile":   "low-volume",
		"source":    "main",
		"app_name":  "metrics-info-app",
		"region":    "ord",
	}, 0)
	assertGatheredGaugeValue(t, "soak_control_worker_version_info", map[string]string{
		"worker_id":      "worker-info-stale-series",
		"git_sha":        "sha-info-old",
		"litestream_sha": "litestream-info-old",
		"profile":        "low-volume",
		"source":         "main",
		"app_name":       "metrics-info-app",
		"region":         "ord",
	}, 0)
	assertGatheredGaugeValue(t, "soak_control_worker_info", map[string]string{
		"worker_id": "worker-info-stale-series",
		"git_sha":   "sha-info-new",
		"profile":   "low-volume",
		"source":    "main",
		"app_name":  "metrics-info-app",
		"region":    "ord",
	}, 1)
	assertGatheredGaugeValue(t, "soak_control_worker_version_info", map[string]string{
		"worker_id":      "worker-info-stale-series",
		"git_sha":        "sha-info-new",
		"litestream_sha": "litestream-info-new",
		"profile":        "low-volume",
		"source":         "main",
		"app_name":       "metrics-info-app",
		"region":         "ord",
	}, 1)
}

func TestControlMetricsObserveWorkerZeroesStaleStatusAndRuntimeSeries(t *testing.T) {
	db := openTestDB(t)
	metrics := NewControlMetrics(db)

	worker := model.Worker{
		ID:            "worker-runtime-stale-series",
		Name:          "worker-runtime-stale-series",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-runtime-series",
		LitestreamSHA: "litestream-runtime-series",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		AppName:       "metrics-runtime-app",
		Region:        "iad",
	}
	labels := workerMetricLabels(worker)
	metrics.observeWorker(worker)

	assertGaugeVecValue(t, controlWorkerStatus, metricLabelsWith(labels, string(model.WorkerRunning)), 1)
	assertGaugeVecValue(t, controlWorkerRuntimeSnapshotStatus, metricLabelsWith(labels, reporting.RuntimeSnapshotStatusMissing), 1)

	worker.Status = model.WorkerDegraded
	worker.LastRuntimeJSON = mustJSON(reporting.RuntimePayload{
		DataDiskTotalBytes:        8192,
		DataDiskUsedBytes:         4096,
		DataDiskFreeBytes:         4096,
		DataDiskUsedPercent:       50,
		DBSizeBytes:               2048,
		WALSizeBytes:              256,
		LitestreamDirSizeBytes:    512,
		LitestreamLTXSizeBytes:    128,
		LitestreamSnapshotHealthy: true,
	})
	metrics.observeWorker(worker)

	assertGaugeVecValue(t, controlWorkerStatus, metricLabelsWith(labels, string(model.WorkerRunning)), 0)
	assertGaugeVecValue(t, controlWorkerStatus, metricLabelsWith(labels, string(model.WorkerDegraded)), 1)
	assertGaugeVecValue(t, controlWorkerRuntimeSnapshotStatus, metricLabelsWith(labels, reporting.RuntimeSnapshotStatusMissing), 0)
	assertGaugeVecValue(t, controlWorkerRuntimeSnapshotStatus, metricLabelsWith(labels, reporting.RuntimeSnapshotStatusHealthy), 1)
	assertGaugeVecValue(t, controlWorkerDataDiskTotalSize, labels, 8192)
	assertGaugeVecValue(t, controlWorkerDataDiskUsedSize, labels, 4096)
	assertGaugeVecValue(t, controlWorkerDataDiskFreeSize, labels, 4096)
	assertGaugeVecValue(t, controlWorkerDataDiskUsedPercent, labels, 50)
	assertGaugeVecValue(t, controlWorkerDBSize, labels, 2048)
	assertGaugeVecValue(t, controlWorkerWALSize, labels, 256)
	assertGaugeVecValue(t, controlWorkerLitestreamLocalStateSize, labels, 512)
	assertGaugeVecValue(t, controlWorkerLitestreamLocalLTXSize, labels, 128)
}

func TestControlMetricsObserveVerificationSetsResultAndClearsCurrentFailure(t *testing.T) {
	db := openTestDB(t)
	metrics := NewControlMetrics(db)
	worker := model.Worker{
		ID:            "worker-verification-metrics",
		Name:          "worker-verification-metrics",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-verification-metrics",
		LitestreamSHA: "litestream-verification-metrics",
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
		AppName:       "metrics-verification-app",
		Region:        "ord",
	}
	labels := workerMetricLabels(worker)
	failureLabels := metricLabelsWith(labels, "integrity_check", "sqlite_index_mismatch")
	failedAt := time.Date(2026, 6, 10, 15, 4, 5, 0, time.UTC)

	metrics.observeVerification(worker, model.Verification{
		WorkerID:     worker.ID,
		StartedAt:    failedAt.Add(-2500 * time.Millisecond),
		CompletedAt:  &failedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   2500,
		ErrorMessage: `wrong # of entries in index idx_load_test_timestamp`,
	})

	assertGaugeVecValue(t, controlWorkerLastVerificationResult, labels, 0)
	assertGaugeVecValue(t, controlWorkerLastVerificationDuration, labels, 2.5)
	assertGaugeVecValue(t, controlWorkerLastVerificationCompleted, labels, float64(failedAt.Unix()))
	assertGaugeVecValue(t, controlWorkerFailureInfo, failureLabels, 1)
	assertGaugeVecValue(t, controlWorkerLastFailureInfo, failureLabels, 1)
	assertGaugeVecValue(t, controlWorkerLastFailure, labels, float64(failedAt.Unix()))

	passedAt := failedAt.Add(time.Minute)
	metrics.observeVerification(worker, model.Verification{
		WorkerID:    worker.ID,
		StartedAt:   passedAt.Add(-500 * time.Millisecond),
		CompletedAt: &passedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  500,
	})

	assertGaugeVecValue(t, controlWorkerLastVerificationResult, labels, 1)
	assertGaugeVecValue(t, controlWorkerLastVerificationDuration, labels, 0.5)
	assertGaugeVecValue(t, controlWorkerLastVerificationCompleted, labels, float64(passedAt.Unix()))
	assertGaugeVecValue(t, controlWorkerFailureInfo, failureLabels, 0)
	assertGaugeVecValue(t, controlWorkerLastFailureInfo, failureLabels, 1)
}

func TestControlMetricsObserveLatestDeploymentEmitsRolloutAndZeroesPreviousDeployment(t *testing.T) {
	db := openTestDB(t)
	metrics := NewControlMetrics(db)
	mustUpsertReadyDeployment(t, db, model.Deployment{
		GitSHA:        "sha-rollout-old",
		LitestreamSHA: "litestream-rollout-old",
		ImageRef:      "registry.fly.io/litestream-soak:sha-rollout-old",
		Source:        "main",
		Status:        "ready",
	})
	oldDeployment := mustLatestDeployment(t, db, "main")

	createTestWorker(t, db, model.Worker{
		ID:            "worker-rollout-metrics",
		Name:          "worker-rollout-metrics",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        oldDeployment.GitSHA,
		LitestreamSHA: oldDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	verifiedAt := time.Now().UTC().Add(time.Second)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-rollout-metrics",
		StartedAt:   verifiedAt.Add(-time.Second),
		CompletedAt: &verifiedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  1000,
	})
	metrics.observeLatestDeployment(db)

	oldLabels := []string{"main", "sha-rollout-old", "stable"}
	oldVersionLabels := []string{"main", "sha-rollout-old", "litestream-rollout-old", "stable"}
	assertGaugeVecValue(t, controlLatestDeploymentInfo, oldLabels, 4)
	assertGaugeVecValue(t, controlLatestDeploymentVersionInfo, oldVersionLabels, 4)
	assertGaugeVecValue(t, controlLatestDeploymentWorkers, metricLabelsWith(oldLabels, "total"), 1)
	assertGaugeVecValue(t, controlLatestDeploymentWorkers, metricLabelsWith(oldLabels, "verified_since_deploy"), 1)

	mustUpsertReadyDeployment(t, db, model.Deployment{
		GitSHA:        "sha-rollout-new",
		LitestreamSHA: "litestream-rollout-new",
		ImageRef:      "registry.fly.io/litestream-soak:sha-rollout-new",
		Source:        "main",
		Status:        "ready",
	})
	metrics.observeLatestDeployment(db)

	newLabels := []string{"main", "sha-rollout-new", "rolling_out"}
	newVersionLabels := []string{"main", "sha-rollout-new", "litestream-rollout-new", "rolling_out"}
	assertGaugeVecValue(t, controlLatestDeploymentInfo, oldLabels, 0)
	assertGaugeVecValue(t, controlLatestDeploymentVersionInfo, oldVersionLabels, 0)
	assertGaugeVecValue(t, controlLatestDeploymentWorkers, metricLabelsWith(oldLabels, "total"), 0)
	assertGaugeVecValue(t, controlLatestDeploymentInfo, newLabels, 1)
	assertGaugeVecValue(t, controlLatestDeploymentVersionInfo, newVersionLabels, 1)
	assertGaugeVecValue(t, controlLatestDeploymentWorkers, metricLabelsWith(newLabels, "total"), 1)
	assertGaugeVecValue(t, controlLatestDeploymentWorkers, metricLabelsWith(newLabels, "updated"), 0)
	assertGaugeVecValue(t, controlLatestDeploymentWorkers, metricLabelsWith(newLabels, "outdated"), 1)
}

func TestControlMetricsObserveSourceComparisonsEmitsMetricsAndZeroesRemovedSource(t *testing.T) {
	db := openTestDB(t)
	metrics := NewControlMetrics(db)
	mustUpsertReadyDeployment(t, db, model.Deployment{
		GitSHA:        "sha-source-main",
		LitestreamSHA: "litestream-source-main",
		ImageRef:      "registry.fly.io/litestream-soak:sha-source-main",
		Source:        "main",
		Status:        "ready",
	})
	mustUpsertReadyDeployment(t, db, model.Deployment{
		GitSHA:        "sha-source-pr",
		LitestreamSHA: "litestream-source-pr",
		ImageRef:      "registry.fly.io/litestream-soak:sha-source-pr",
		Source:        "pr-45-source",
		PRNumber:      45,
		Status:        "ready",
	})
	mainDeployment := mustLatestDeployment(t, db, "main")
	prDeployment := mustLatestDeployment(t, db, "pr-45-source")

	createTestWorker(t, db, model.Worker{
		ID:            "worker-source-main",
		Name:          "worker-source-main",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        mainDeployment.GitSHA,
		LitestreamSHA: mainDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-source-pr",
		Name:          "worker-source-pr",
		Status:        model.WorkerRunning,
		Source:        "pr-45-source",
		GitSHA:        prDeployment.GitSHA,
		LitestreamSHA: prDeployment.LitestreamSHA,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})

	baseFailedAt := time.Now().UTC().Add(3 * time.Second)
	basePassedAt := baseFailedAt.Add(-1500 * time.Millisecond)
	headPassedAt := baseFailedAt.Add(time.Second)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-source-main",
		StartedAt:   basePassedAt.Add(-100 * time.Millisecond),
		CompletedAt: &basePassedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  100,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     "worker-source-main",
		StartedAt:    baseFailedAt.Add(-time.Second),
		CompletedAt:  &baseFailedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   1000,
		ErrorMessage: `wrong # of entries in index idx_load_test_timestamp`,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-source-pr",
		StartedAt:   headPassedAt.Add(-time.Second),
		CompletedAt: &headPassedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
		DurationMS:  1000,
	})
	metrics.observeSourceComparisons(db)

	infoLabels := []string{"main", "pr-45-source", "sha-source-pr", "litestream-source-pr", "sha-source-main", "litestream-source-main", "better"}
	headPassedLabels := []string{"main", "pr-45-source", "head", "sha-source-pr", "litestream-source-pr", "passed"}
	baseFailedLabels := []string{"main", "pr-45-source", "base", "sha-source-main", "litestream-source-main", "failed"}
	passDeltaLabels := []string{"main", "pr-45-source", "sha-source-pr", "litestream-source-pr", "sha-source-main", "litestream-source-main", "pass_delta", "better"}
	failDeltaLabels := []string{"main", "pr-45-source", "sha-source-pr", "litestream-source-pr", "sha-source-main", "litestream-source-main", "fail_delta", "better"}
	improvedLabels := []string{"main", "pr-45-source", "sha-source-pr", "litestream-source-pr", "sha-source-main", "litestream-source-main", "improved_workers", "better"}
	resolvedFailureLabels := []string{"main", "pr-45-source", "resolved", "sha-source-main", "litestream-source-main", "integrity_check", "sqlite_index_mismatch"}

	assertGaugeVecValue(t, controlSourceComparisonInfo, infoLabels, 1)
	assertGaugeVecValue(t, controlSourceComparisonWorkers, headPassedLabels, 1)
	assertGaugeVecValue(t, controlSourceComparisonWorkers, baseFailedLabels, 1)
	assertGaugeVecValue(t, controlSourceComparisonDelta, passDeltaLabels, 1)
	assertGaugeVecValue(t, controlSourceComparisonDelta, failDeltaLabels, -1)
	assertGaugeVecValue(t, controlSourceComparisonDelta, improvedLabels, 1)
	assertGaugeVecValue(t, controlSourceComparisonFailure, resolvedFailureLabels, 1)

	if err := db.DeleteWorker("worker-source-pr"); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}
	metrics.observeSourceComparisons(db)

	assertGaugeVecValue(t, controlSourceComparisonInfo, infoLabels, 0)
	assertGaugeVecValue(t, controlSourceComparisonWorkers, headPassedLabels, 0)
	assertGaugeVecValue(t, controlSourceComparisonWorkers, baseFailedLabels, 0)
	assertGaugeVecValue(t, controlSourceComparisonDelta, passDeltaLabels, 0)
	assertGaugeVecValue(t, controlSourceComparisonDelta, failDeltaLabels, 0)
	assertGaugeVecValue(t, controlSourceComparisonDelta, improvedLabels, 0)
	assertGaugeVecValue(t, controlSourceComparisonFailure, resolvedFailureLabels, 0)
}

func TestControlMetricsSyncFromDBRebuildsWorkerVerificationStateAndHandlesEmptyDB(t *testing.T) {
	NewControlMetrics(openTestDB(t))

	db := openTestDB(t)
	worker := model.Worker{
		ID:            "worker-sync-rebuild",
		Name:          "worker-sync-rebuild",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-sync-rebuild",
		LitestreamSHA: "litestream-sync-rebuild",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
		AppName:       "metrics-sync-app",
		Region:        "ord",
	}
	createTestWorker(t, db, worker)
	completedAt := time.Date(2026, 6, 10, 15, 7, 9, 0, time.UTC)
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     worker.ID,
		StartedAt:    completedAt.Add(-3 * time.Second),
		CompletedAt:  &completedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		DurationMS:   3000,
		ErrorMessage: `wrong # of entries in index idx_load_test_timestamp`,
	})

	NewControlMetrics(db)

	labels := workerMetricLabels(worker)
	failureLabels := metricLabelsWith(labels, "integrity_check", "sqlite_index_mismatch")
	assertGaugeVecValue(t, controlWorkerInfo, []string{
		worker.ID,
		worker.GitSHA,
		worker.ProfileName,
		worker.Source,
		worker.AppName,
		worker.Region,
	}, 1)
	assertGaugeVecValue(t, controlWorkerVersionInfo, []string{
		worker.ID,
		worker.GitSHA,
		worker.LitestreamSHA,
		worker.ProfileName,
		worker.Source,
		worker.AppName,
		worker.Region,
	}, 1)
	assertGaugeVecValue(t, controlWorkerStatus, metricLabelsWith(labels, string(model.WorkerRunning)), 1)
	assertGaugeVecValue(t, controlWorkerLastVerificationResult, labels, 0)
	assertGaugeVecValue(t, controlWorkerLastVerificationDuration, labels, 3)
	assertGaugeVecValue(t, controlWorkerFailureInfo, failureLabels, 1)
	assertGaugeVecValue(t, controlWorkerLastFailureInfo, failureLabels, 1)
	assertGaugeVecValue(t, controlWorkerLastFailure, labels, float64(completedAt.Unix()))
}

func TestControlMetricsExposeVolumeInventory(t *testing.T) {
	db := openTestDB(t)
	metrics := NewControlMetrics(db)
	metrics.observeVolumes("metrics-volume-app", []flyapi.Volume{
		{Region: "ord", SizeGB: 100, AttachedMachineID: "machine-one"},
		{Region: "ord", SizeGB: 10},
		{Region: "ord", SizeGB: 10},
		{Region: "ord", State: "pending_destroy", SizeGB: 100},
	})

	assertGatheredGaugeValue(t, "soak_control_app_volume_count", map[string]string{
		"app_name":         "metrics-volume-app",
		"region":           "ord",
		"attachment_state": "attached",
		"size_gb":          "100",
	}, 1)
	assertGatheredGaugeValue(t, "soak_control_app_volume_size_gb", map[string]string{
		"app_name":         "metrics-volume-app",
		"region":           "ord",
		"attachment_state": "unattached",
		"size_gb":          "10",
	}, 20)

	metrics.observeVolumes("metrics-volume-app", []flyapi.Volume{
		{Region: "ord", SizeGB: 10, AttachedMachineID: "machine-two"},
	})
	assertGatheredGaugeValue(t, "soak_control_app_volume_count", map[string]string{
		"app_name":         "metrics-volume-app",
		"region":           "ord",
		"attachment_state": "unattached",
		"size_gb":          "10",
	}, 0)
	assertGatheredGaugeValue(t, "soak_control_app_volume_size_gb", map[string]string{
		"app_name":         "metrics-volume-app",
		"region":           "ord",
		"attachment_state": "attached",
		"size_gb":          "10",
	}, 10)
}

func mustUpsertReadyDeployment(t *testing.T, db *model.DB, deployment model.Deployment) {
	t.Helper()

	if err := db.UpsertReadyDeployment(&deployment); err != nil {
		t.Fatalf("UpsertReadyDeployment(%s) error = %v", deployment.GitSHA, err)
	}
}

func mustLatestDeployment(t *testing.T, db *model.DB, source string) model.Deployment {
	t.Helper()

	deployment, err := db.GetLatestDeployment(source)
	if err != nil {
		t.Fatalf("GetLatestDeployment(%s) error = %v", source, err)
	}
	if deployment == nil {
		t.Fatalf("GetLatestDeployment(%s) = nil, want deployment", source)
	}
	return *deployment
}

func metricLabelsWith(labels []string, extra ...string) []string {
	combined := append([]string{}, labels...)
	return append(combined, extra...)
}

func assertGaugeVecValue(t *testing.T, gauge *prometheus.GaugeVec, labels []string, expectedValue float64) {
	t.Helper()

	if got := testutil.ToFloat64(gauge.WithLabelValues(labels...)); got != expectedValue {
		t.Fatalf("gauge value = %v, want %v for labels %v", got, expectedValue, labels)
	}
}

func assertGatheredGaugeValue(t *testing.T, metricName string, expectedLabels map[string]string, expectedValue float64) {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	available := make([]map[string]string, 0)
	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.Metric {
			available = append(available, metricLabels(metric))
			if metricLabelsMatch(metric, expectedLabels) {
				if got := metric.GetGauge().GetValue(); got != expectedValue {
					t.Fatalf("%s value = %v, want %v for labels %+v", metricName, got, expectedValue, expectedLabels)
				}
				return
			}
		}
	}

	t.Fatalf("%s with labels %+v not found; available=%+v", metricName, expectedLabels, available)
}

func metricLabelsMatch(metric *dto.Metric, expected map[string]string) bool {
	if len(metric.Label) != len(expected) {
		return false
	}
	for _, label := range metric.Label {
		if expected[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}

func metricLabels(metric *dto.Metric) map[string]string {
	labels := make(map[string]string, len(metric.Label))
	for _, label := range metric.Label {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}
