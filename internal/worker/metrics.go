package worker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	workerInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_worker_info",
		Help: "Static info about the soak worker.",
	}, []string{"worker_id", "git_sha", "profile", "source"})

	workerUptime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_worker_uptime_seconds",
		Help: "Time since worker started.",
	})

	verificationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_verification_total",
		Help: "Total number of verification cycles by result.",
	}, []string{"result"})

	verificationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "soak_verification_duration_seconds",
		Help:    "Duration of verification cycles.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s to ~68min
	})

	verificationLastResult = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_verification_last_result",
		Help: "Result of the last verification cycle (1=pass, 0=fail).",
	})

	loadRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_load_running",
		Help: "Whether the load generator is currently running (1=yes, 0=paused).",
	})

	dbSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_db_size_bytes",
		Help: "Current database file size in bytes.",
	})

	walSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_wal_size_bytes",
		Help: "Current WAL file size in bytes.",
	})

	dbTXID = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_db_txid",
		Help: "Current transaction ID from litestream.",
	})

	replicatedTXID = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_replicated_txid",
		Help: "Last replicated transaction ID.",
	})

	replicationLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_replication_lag_txids",
		Help: "Replication lag in transaction IDs (db_txid - replicated_txid).",
	})

	lastSyncAge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_last_sync_age_seconds",
		Help: "Seconds since last successful replica sync.",
	})

	litestreamUptime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_litestream_uptime_seconds",
		Help: "Litestream process uptime.",
	})

	dbStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "soak_db_status",
		Help: "Database status (1=replicating, 0=other).",
	}, []string{"status"})
)

func SetWorkerInfo(cfg Config) {
	workerInfo.WithLabelValues(cfg.WorkerID, cfg.GitSHA, cfg.ProfileName, cfg.Source).Set(1)
}

func SetUptime(seconds float64) {
	workerUptime.Set(seconds)
}

func RecordVerification(passed bool, durationSec float64) {
	if passed {
		verificationTotal.WithLabelValues("passed").Inc()
		verificationLastResult.Set(1)
	} else {
		verificationTotal.WithLabelValues("failed").Inc()
		verificationLastResult.Set(0)
	}
	verificationDuration.Observe(durationSec)
}

func SetLoadRunning(running bool) {
	if running {
		loadRunning.Set(1)
	} else {
		loadRunning.Set(0)
	}
}

func SetDBSize(bytes int64) {
	dbSize.Set(float64(bytes))
}

func SetWALSize(bytes int64) {
	walSize.Set(float64(bytes))
}

func SetDBTXID(txid float64) {
	dbTXID.Set(txid)
}

func SetReplicatedTXID(txid float64) {
	replicatedTXID.Set(txid)
}

func SetReplicationLag(lag float64) {
	replicationLag.Set(lag)
}

func SetLastSyncAge(seconds float64) {
	lastSyncAge.Set(seconds)
}

func SetLitestreamUptime(seconds float64) {
	litestreamUptime.Set(seconds)
}

func SetDBStatus(status string) {
	dbStatus.Reset()
	dbStatus.WithLabelValues(status).Set(1)
}
