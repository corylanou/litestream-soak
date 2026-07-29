package worker

import (
	"testing"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestWorkerReplicationMetricsIncludeRegionLabel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WorkerID = "worker-region-metrics"
	cfg.ProfileName = "low-vol-syd"
	cfg.Source = "main"
	cfg.Region = "syd"

	SetWorkerInfo(cfg)
	SetReplicationLag(7)
	SetLastSyncAge(12)

	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source, cfg.Region}
	if got := testutil.ToFloat64(replicationLag.WithLabelValues(labels...)); got != 7 {
		t.Fatalf("soak_replication_lag_txids = %v, want 7", got)
	}
	if got := testutil.ToFloat64(lastSyncAge.WithLabelValues(labels...)); got != 12 {
		t.Fatalf("soak_last_sync_age_seconds = %v, want 12", got)
	}
}

func TestLitestreamMetricsGaugesPreserveThreeWayState(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WorkerID = "worker-litestream-metrics-state"
	cfg.ProfileName = "low-volume"
	cfg.Source = "main"
	cfg.Region = "ord"
	SetWorkerInfo(cfg)
	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source, cfg.Region}

	SetLitestreamMetricsState(reporting.LitestreamMetricsScrapeStatusHealthy, true, true)
	if got := testutil.ToFloat64(litestreamMetricsScrapeHealthy.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("soak_litestream_metrics_scrape_healthy = %v, want 1", got)
	}
	if got := testutil.ToFloat64(litestreamDiskFullMetricPresent.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("soak_litestream_disk_full_metric_present = %v, want 1", got)
	}
	if got := testutil.ToFloat64(litestreamMemStatsMetricsPresent.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("soak_litestream_memstats_metrics_present = %v, want 1", got)
	}

	SetLitestreamMetricsState(reporting.LitestreamMetricsScrapeStatusHealthy, false, false)
	if got := testutil.ToFloat64(litestreamMetricsScrapeHealthy.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("healthy scrape with absent metrics = %v, want 1", got)
	}
	if got := testutil.ToFloat64(litestreamDiskFullMetricPresent.WithLabelValues(labels...)); got != 0 {
		t.Fatalf("absent disk-full metric = %v, want 0", got)
	}

	SetLitestreamMetricsState(reporting.LitestreamMetricsScrapeStatusFailed, false, false)
	if got := testutil.ToFloat64(litestreamMetricsScrapeHealthy.WithLabelValues(labels...)); got != 0 {
		t.Fatalf("failed scrape = %v, want 0", got)
	}
}
