package worker

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricStateMu sync.RWMutex
	metricLabels  = []string{"unknown", "unknown", "unknown"}
	lastDBState   string

	workerInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_worker_info",
		Help: "Static info about the soak worker.",
	}, []string{"worker_id", "git_sha", "profile", "source"})

	workerUptime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_worker_uptime_seconds",
		Help: "Time since worker started.",
	}, []string{"worker_id", "profile", "source"})

	verificationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_verification_total",
		Help: "Total number of verification cycles by result.",
	}, []string{"worker_id", "profile", "source", "result"})

	verificationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "soak_verification_duration_seconds",
		Help:    "Duration of verification cycles.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"worker_id", "profile", "source"})

	verificationLastResult = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_verification_last_result",
		Help: "Result of the last verification cycle (1=pass, 0=fail).",
	}, []string{"worker_id", "profile", "source"})

	loadRunning = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_load_running",
		Help: "Whether the load generator is currently running (1=yes, 0=paused).",
	}, []string{"worker_id", "profile", "source"})

	dbSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_db_size_bytes",
		Help: "Current database file size in bytes.",
	}, []string{"worker_id", "profile", "source"})

	walSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_wal_size_bytes",
		Help: "Current WAL file size in bytes.",
	}, []string{"worker_id", "profile", "source"})

	dbTXID = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_db_txid",
		Help: "Current transaction ID from litestream.",
	}, []string{"worker_id", "profile", "source"})

	replicatedTXID = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_replicated_txid",
		Help: "Last replicated transaction ID.",
	}, []string{"worker_id", "profile", "source"})

	replicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_replication_lag_txids",
		Help: "Replication lag in transaction IDs (db_txid - replicated_txid).",
	}, []string{"worker_id", "profile", "source"})

	lastSyncAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_last_sync_age_seconds",
		Help: "Seconds since last successful replica sync.",
	}, []string{"worker_id", "profile", "source"})

	litestreamUptime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_litestream_uptime_seconds",
		Help: "Litestream process uptime.",
	}, []string{"worker_id", "profile", "source"})

	dbStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_db_status",
		Help: "Database status (1=active, 0=inactive).",
	}, []string{"worker_id", "profile", "source", "status"})
)

func SetWorkerInfo(cfg Config) {
	labels := []string{cfg.WorkerID, cfg.ProfileName, cfg.Source}

	metricStateMu.Lock()
	metricLabels = labels
	lastDBState = ""
	metricStateMu.Unlock()

	workerInfo.WithLabelValues(cfg.WorkerID, cfg.GitSHA, cfg.ProfileName, cfg.Source).Set(1)
}

func SetUptime(seconds float64) {
	workerUptime.WithLabelValues(currentMetricLabels()...).Set(seconds)
}

func RecordVerification(passed bool, durationSec float64) {
	labels := currentMetricLabels()
	if passed {
		verificationTotal.WithLabelValues(append(labels, "passed")...).Inc()
		verificationLastResult.WithLabelValues(labels...).Set(1)
	} else {
		verificationTotal.WithLabelValues(append(labels, "failed")...).Inc()
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

func SetDBSize(bytes int64) {
	dbSize.WithLabelValues(currentMetricLabels()...).Set(float64(bytes))
}

func SetWALSize(bytes int64) {
	walSize.WithLabelValues(currentMetricLabels()...).Set(float64(bytes))
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
