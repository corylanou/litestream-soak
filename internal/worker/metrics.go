package worker

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricStateMu sync.RWMutex
	metricLabels  = []string{"unknown", "unknown", "unknown", "unknown"}
	lastDBState   string

	workerInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_worker_info",
		Help: "Static info about the soak worker.",
	}, []string{"worker_id", "git_sha", "profile", "source", "region"})

	workerVersionInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_worker_version_info",
		Help: "Static version info about the soak worker and the Litestream build under test.",
	}, []string{"worker_id", "git_sha", "litestream_sha", "profile", "source", "region"})

	workerUptime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_worker_uptime_seconds",
		Help: "Time since worker started.",
	}, []string{"worker_id", "profile", "source", "region"})

	verificationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_verification_total",
		Help: "Total number of verification cycles by result.",
	}, []string{"worker_id", "profile", "source", "region", "result"})

	verificationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "soak_verification_duration_seconds",
		Help:    "Duration of verification cycles.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"worker_id", "profile", "source", "region"})

	verificationLastResult = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_verification_last_result",
		Help: "Result of the last verification cycle (1=pass, 0=fail).",
	}, []string{"worker_id", "profile", "source", "region"})

	loadRunning = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_load_running",
		Help: "Whether the load generator is currently running (1=yes, 0=paused).",
	}, []string{"worker_id", "profile", "source", "region"})

	loadRestarts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_load_restarts_total",
		Help: "Total number of load generator restarts by kind.",
	}, []string{"worker_id", "profile", "source", "region", "kind"})

	dbSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_db_size_bytes",
		Help: "Current database file size in bytes.",
	}, []string{"worker_id", "profile", "source", "region"})

	walSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_wal_size_bytes",
		Help: "Current WAL file size in bytes.",
	}, []string{"worker_id", "profile", "source", "region"})

	dataDiskTotalSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_data_disk_total_bytes",
		Help: "Total size of the worker data filesystem in bytes.",
	}, []string{"worker_id", "profile", "source", "region"})

	dataDiskUsedSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_data_disk_used_bytes",
		Help: "Used size of the worker data filesystem in bytes.",
	}, []string{"worker_id", "profile", "source", "region"})

	dataDiskFreeSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_data_disk_free_bytes",
		Help: "Free size of the worker data filesystem in bytes.",
	}, []string{"worker_id", "profile", "source", "region"})

	dataDiskUsedPercent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_data_disk_used_percent",
		Help: "Used percentage of the worker data filesystem.",
	}, []string{"worker_id", "profile", "source", "region"})

	litestreamDirSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_litestream_local_state_bytes",
		Help: "Recursive size of the local Litestream state directory in bytes.",
	}, []string{"worker_id", "profile", "source", "region"})

	litestreamLTXSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_litestream_local_ltx_bytes",
		Help: "Recursive size of the local Litestream LTX directory in bytes.",
	}, []string{"worker_id", "profile", "source", "region"})

	dbTXID = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_db_txid",
		Help: "Current transaction ID from litestream.",
	}, []string{"worker_id", "profile", "source", "region"})

	replicatedTXID = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_replicated_txid",
		Help: "Last replicated transaction ID.",
	}, []string{"worker_id", "profile", "source", "region"})

	replicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_replication_lag_txids",
		Help: "Replication lag in transaction IDs (db_txid - replicated_txid).",
	}, []string{"worker_id", "profile", "source", "region"})

	lastSyncAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_last_sync_age_seconds",
		Help: "Seconds since last successful replica sync.",
	}, []string{"worker_id", "profile", "source", "region"})

	litestreamUptime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_litestream_uptime_seconds",
		Help: "Litestream process uptime.",
	}, []string{"worker_id", "profile", "source", "region"})

	litestreamSnapshotHealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_litestream_snapshot_healthy",
		Help: "Whether the worker could refresh Litestream runtime stats on the last poll (1=yes, 0=no).",
	}, []string{"worker_id", "profile", "source", "region"})

	dbStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_db_status",
		Help: "Database status (1=active, 0=inactive).",
	}, []string{"worker_id", "profile", "source", "region", "status"})
)

func SetWorkerInfo(cfg Config) {
	region := metricRegion(cfg.Region)
	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source, region}

	metricStateMu.Lock()
	metricLabels = labels
	lastDBState = ""
	metricStateMu.Unlock()

	workerInfo.WithLabelValues(cfg.WorkerID, cfg.GitSHA, cfg.ProfileName, cfg.Source, region).Set(1)
	workerVersionInfo.WithLabelValues(cfg.WorkerID, cfg.GitSHA, cfg.LitestreamSHA, cfg.ProfileName, cfg.Source, region).Set(1)
}

func SetUptime(seconds float64) {
	workerUptime.WithLabelValues(currentMetricLabels()...).Set(seconds)
}

func RecordVerificationOutcome(status string, durationSec float64) {
	labels := currentMetricLabels()
	verificationTotal.WithLabelValues(append(labels, status)...).Inc()
	switch status {
	case "passed":
		verificationLastResult.WithLabelValues(labels...).Set(1)
	case "failed":
		verificationLastResult.WithLabelValues(labels...).Set(0)
	}
	verificationDuration.WithLabelValues(labels...).Observe(durationSec)
}

func SetLoadRunning(running bool) {
	value := 0.0
	if running {
		value = 1
	}
	loadRunning.WithLabelValues(currentMetricLabels()...).Set(value)
}

func IncLoadRestart(kind string) {
	loadRestarts.WithLabelValues(append(currentMetricLabels(), kind)...).Inc()
}

func SetDBSize(bytes int64) {
	dbSize.WithLabelValues(currentMetricLabels()...).Set(float64(bytes))
}

func SetWALSize(bytes int64) {
	walSize.WithLabelValues(currentMetricLabels()...).Set(float64(bytes))
}

func SetDataDiskStats(total, used, free uint64, usedPercent float64) {
	labels := currentMetricLabels()
	dataDiskTotalSize.WithLabelValues(labels...).Set(float64(total))
	dataDiskUsedSize.WithLabelValues(labels...).Set(float64(used))
	dataDiskFreeSize.WithLabelValues(labels...).Set(float64(free))
	dataDiskUsedPercent.WithLabelValues(labels...).Set(usedPercent)
}

func SetLitestreamLocalStateSize(dirBytes, ltxBytes int64) {
	labels := currentMetricLabels()
	litestreamDirSize.WithLabelValues(labels...).Set(float64(dirBytes))
	litestreamLTXSize.WithLabelValues(labels...).Set(float64(ltxBytes))
}

func SetDBTXID(txid float64) {
	dbTXID.WithLabelValues(currentMetricLabels()...).Set(txid)
}

func SetReplicatedTXID(txid float64) {
	replicatedTXID.WithLabelValues(currentMetricLabels()...).Set(txid)
}

func SetReplicationLag(lag float64) {
	replicationLag.WithLabelValues(currentMetricLabels()...).Set(lag)
}

func SetLastSyncAge(seconds float64) {
	lastSyncAge.WithLabelValues(currentMetricLabels()...).Set(seconds)
}

func SetLitestreamUptime(seconds float64) {
	litestreamUptime.WithLabelValues(currentMetricLabels()...).Set(seconds)
}

func SetLitestreamSnapshotHealthy(healthy bool) {
	value := 0.0
	if healthy {
		value = 1
	}
	litestreamSnapshotHealthy.WithLabelValues(currentMetricLabels()...).Set(value)
}

func SetDBStatus(status string) {
	metricStateMu.Lock()
	labels := append([]string(nil), metricLabels...)
	previous := lastDBState
	lastDBState = status
	metricStateMu.Unlock()

	if previous != "" && previous != status {
		dbStatus.WithLabelValues(append(labels, previous)...).Set(0)
	}
	dbStatus.WithLabelValues(append(labels, status)...).Set(1)
}

func currentMetricLabels() []string {
	metricStateMu.RLock()
	defer metricStateMu.RUnlock()
	return append([]string(nil), metricLabels...)
}

func metricRegion(region string) string {
	region = strings.TrimSpace(region)
	if region == "" {
		return "unknown"
	}
	return region
}
