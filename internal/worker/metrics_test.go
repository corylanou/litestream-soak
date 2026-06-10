package worker

import (
	"testing"

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
