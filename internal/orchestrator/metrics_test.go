package orchestrator

import (
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/prometheus/client_golang/prometheus"
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
		"failure_stage":     "integrity",
		"failure_signature": "sqlite_index_mismatch",
	}, 1)
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
