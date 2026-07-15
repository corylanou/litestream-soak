package worker

import (
	"strings"
	"testing"
	"time"
)

func TestConfigFromEnvRejectsUnsafeRuntimeValues(t *testing.T) {
	tests := []struct {
		key   string
		value string
	}{
		{key: "WRITE_RATE", value: "-1"},
		{key: "PAYLOAD_SIZE", value: "0"},
		{key: "READ_RATIO", value: "1.1"},
		{key: "LOAD_WORKERS", value: "0"},
		{key: "VERIFY_INTERVAL", value: "0s"},
		{key: "SNAPSHOT_INTERVAL", value: "0s"},
		{key: "SYNC_INTERVAL", value: "-1s"},
		{key: "REPLAY_SPEED", value: "0"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Setenv(tt.key, tt.value)
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatalf("ConfigFromEnv() error = nil for %s=%s", tt.key, tt.value)
			}
		})
	}
}

func TestConfigFromEnvReadsS3UploadTuning(t *testing.T) {
	t.Setenv("REPLICA_TYPE", "s3")
	t.Setenv("S3_BUCKET", "bucket")
	t.Setenv("LITESTREAM_S3_PART_SIZE", "16MB")
	t.Setenv("LITESTREAM_S3_CONCURRENCY", "8")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.S3PartSize != "16MB" {
		t.Fatalf("S3PartSize = %q, want 16MB", cfg.S3PartSize)
	}
	if cfg.S3Concurrency != 8 {
		t.Fatalf("S3Concurrency = %d, want 8", cfg.S3Concurrency)
	}
}

func TestConfigFromEnvReadsS3FaultProxyConfig(t *testing.T) {
	t.Setenv("REPLICA_TYPE", "s3")
	t.Setenv("S3_BUCKET", "bucket")
	t.Setenv("S3_ENDPOINT", "https://fly.storage.tigris.dev")
	t.Setenv("S3_FAULT_PROXY_ENABLED", "true")
	t.Setenv("S3_FAULT_PROXY_TARGET_ENDPOINT", "https://target.example.com")
	t.Setenv("S3_FAULT_PROXY_MODE", "source-get-reset")
	t.Setenv("S3_FAULT_PROXY_LISTEN_ADDR", "127.0.0.1:19000")
	t.Setenv("S3_FAULT_PROXY_MIN_CONTENT_LENGTH", "8388608")
	t.Setenv("S3_FAULT_PROXY_RESET_AFTER_BYTES", "2097152")
	t.Setenv("S3_FAULT_PROXY_FAIL_FIRST_ATTEMPTS", "1")
	t.Setenv("S3_FAULT_PROXY_MAX_FAILURES", "6")
	t.Setenv("S3_FAULT_PROXY_SOURCE_LEVEL", "0001")
	t.Setenv("S3_FAULT_PROXY_REQUIRE_OBSERVED_SOURCE_GET", "true")
	t.Setenv("S3_FAULT_PROXY_REQUIRE_OBSERVED_SOURCE_RANGE_GET", "true")
	t.Setenv("REPLICA_LEVEL_REPORTING", "true")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if !cfg.S3FaultProxyEnabled {
		t.Fatal("S3FaultProxyEnabled = false, want true")
	}
	if cfg.S3FaultProxyTargetEndpoint != "https://target.example.com" {
		t.Fatalf("S3FaultProxyTargetEndpoint = %q, want target endpoint", cfg.S3FaultProxyTargetEndpoint)
	}
	if cfg.S3FaultProxyMode != "source-get-reset" {
		t.Fatalf("S3FaultProxyMode = %q, want source-get-reset", cfg.S3FaultProxyMode)
	}
	if cfg.S3FaultProxyListenAddr != "127.0.0.1:19000" {
		t.Fatalf("S3FaultProxyListenAddr = %q, want 127.0.0.1:19000", cfg.S3FaultProxyListenAddr)
	}
	if cfg.S3FaultProxyMinContentLength != 8*1024*1024 {
		t.Fatalf("S3FaultProxyMinContentLength = %d, want 8MiB", cfg.S3FaultProxyMinContentLength)
	}
	if cfg.S3FaultProxyResetAfterBytes != 2*1024*1024 {
		t.Fatalf("S3FaultProxyResetAfterBytes = %d, want 2MiB", cfg.S3FaultProxyResetAfterBytes)
	}
	if cfg.S3FaultProxyFailFirstAttempts != 1 {
		t.Fatalf("S3FaultProxyFailFirstAttempts = %d, want 1", cfg.S3FaultProxyFailFirstAttempts)
	}
	if cfg.S3FaultProxyMaxFailures != 6 {
		t.Fatalf("S3FaultProxyMaxFailures = %d, want 6", cfg.S3FaultProxyMaxFailures)
	}
	if cfg.S3FaultProxySourceLevel != "0001" {
		t.Fatalf("S3FaultProxySourceLevel = %q, want 0001", cfg.S3FaultProxySourceLevel)
	}
	if !cfg.S3FaultProxyRequireObservedSourceGet {
		t.Fatal("S3FaultProxyRequireObservedSourceGet = false, want true")
	}
	if !cfg.S3FaultProxyRequireObservedSourceRangeGet {
		t.Fatal("S3FaultProxyRequireObservedSourceRangeGet = false, want true")
	}
	if !cfg.ReplicaLevelReporting {
		t.Fatal("ReplicaLevelReporting = false, want true")
	}
}

func TestWorkloadConfigOmitsDisabledS3FaultProxyDefaults(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	got := cfg.WorkloadConfig().JSON()

	if strings.Contains(got, "s3_fault_proxy") {
		t.Fatalf("WorkloadConfig().JSON() = %s, want no disabled fault proxy defaults", got)
	}
}

func TestConfigFromEnvReadsCompactionKnobs(t *testing.T) {
	t.Setenv("L1_COMPACTION_INTERVAL", "5m")
	t.Setenv("L2_COMPACTION_INTERVAL", "30m")
	t.Setenv("L3_COMPACTION_INTERVAL", "6h")
	t.Setenv("L0_RETENTION", "1h")
	t.Setenv("L0_RETENTION_CHECK_INTERVAL", "2m")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.L1CompactionInterval != 5*time.Minute {
		t.Fatalf("L1CompactionInterval = %s, want 5m", cfg.L1CompactionInterval)
	}
	if cfg.L2CompactionInterval != 30*time.Minute {
		t.Fatalf("L2CompactionInterval = %s, want 30m", cfg.L2CompactionInterval)
	}
	if cfg.L3CompactionInterval != 6*time.Hour {
		t.Fatalf("L3CompactionInterval = %s, want 6h", cfg.L3CompactionInterval)
	}
	if cfg.L0Retention != time.Hour {
		t.Fatalf("L0Retention = %s, want 1h", cfg.L0Retention)
	}
	if cfg.L0RetentionCheckInterval != 2*time.Minute {
		t.Fatalf("L0RetentionCheckInterval = %s, want 2m", cfg.L0RetentionCheckInterval)
	}
}

func TestConfigFromEnvRejectsInvalidCompactionKnobs(t *testing.T) {
	for _, key := range []string{
		"L1_COMPACTION_INTERVAL",
		"L2_COMPACTION_INTERVAL",
		"L3_COMPACTION_INTERVAL",
		"L0_RETENTION",
		"L0_RETENTION_CHECK_INTERVAL",
	} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("L1_COMPACTION_INTERVAL", "5m")
			t.Setenv("L2_COMPACTION_INTERVAL", "30m")
			t.Setenv("L3_COMPACTION_INTERVAL", "6h")
			t.Setenv(key, "not-a-duration")

			if _, err := ConfigFromEnv(); err == nil {
				t.Fatalf("ConfigFromEnv() error = nil, want non-nil for invalid %s", key)
			}
		})
	}
}

func TestConfigFromEnvLeavesCompactionKnobsUnset(t *testing.T) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.L1CompactionInterval != 0 || cfg.L2CompactionInterval != 0 || cfg.L3CompactionInterval != 0 {
		t.Fatalf("compaction intervals = %s/%s/%s, want all zero",
			cfg.L1CompactionInterval, cfg.L2CompactionInterval, cfg.L3CompactionInterval)
	}
	if cfg.L0Retention != 0 || cfg.L0RetentionCheckInterval != 0 {
		t.Fatalf("L0 retention knobs = %s/%s, want zero", cfg.L0Retention, cfg.L0RetentionCheckInterval)
	}
}

func TestConfigFromEnvRejectsPartialCompactionLevels(t *testing.T) {
	t.Setenv("L1_COMPACTION_INTERVAL", "5m")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("ConfigFromEnv() error = nil, want non-nil for partial compaction levels")
	}
}

func TestWorkloadConfigOmitsZeroCompactionKnobs(t *testing.T) {
	t.Parallel()

	got := DefaultConfig().WorkloadConfig().JSON()
	for _, key := range []string{
		"l1_compaction_interval",
		"l2_compaction_interval",
		"l3_compaction_interval",
		"l0_retention",
		"l0_retention_check_interval",
	} {
		if strings.Contains(got, key) {
			t.Fatalf("WorkloadConfig().JSON() = %s, want no %q when unset", got, key)
		}
	}
}

func TestWorkloadConfigIncludesCompactionKnobsWhenSet(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.L1CompactionInterval = 5 * time.Minute
	cfg.L2CompactionInterval = 30 * time.Minute
	cfg.L3CompactionInterval = 6 * time.Hour
	cfg.L0Retention = time.Hour
	cfg.L0RetentionCheckInterval = 2 * time.Minute

	got := cfg.WorkloadConfig()
	if got.L1CompactionInterval != "5m0s" {
		t.Fatalf("L1CompactionInterval = %q, want 5m0s", got.L1CompactionInterval)
	}
	if got.L2CompactionInterval != "30m0s" {
		t.Fatalf("L2CompactionInterval = %q, want 30m0s", got.L2CompactionInterval)
	}
	if got.L3CompactionInterval != "6h0m0s" {
		t.Fatalf("L3CompactionInterval = %q, want 6h0m0s", got.L3CompactionInterval)
	}
	if got.L0Retention != "1h0m0s" {
		t.Fatalf("L0Retention = %q, want 1h0m0s", got.L0Retention)
	}
	if got.L0RetentionCheckInterval != "2m0s" {
		t.Fatalf("L0RetentionCheckInterval = %q, want 2m0s", got.L0RetentionCheckInterval)
	}
}

func TestConfigFromEnvReadsManyDBConfig(t *testing.T) {
	t.Setenv("NUM_DATABASES", "100")
	t.Setenv("ACTIVE_PERCENT", "2.5")
	t.Setenv("ACTIVE_ROTATE_INTERVAL", "10m")
	t.Setenv("ACTIVE_SET_SEED", "42")
	t.Setenv("CONFIG_MODE", "dir")
	t.Setenv("VERIFY_SAMPLE_SIZE", "7")
	t.Setenv("VERIFY_CHANGED_LIMIT", "40")
	t.Setenv("REPLICATION_LAG_THRESHOLD", "3")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.NumDatabases != 100 {
		t.Fatalf("NumDatabases = %d, want 100", cfg.NumDatabases)
	}
	if cfg.ActivePercent != 2.5 {
		t.Fatalf("ActivePercent = %v, want 2.5", cfg.ActivePercent)
	}
	if cfg.ActiveRotateInterval != 10*time.Minute {
		t.Fatalf("ActiveRotateInterval = %s, want 10m", cfg.ActiveRotateInterval)
	}
	if cfg.ActiveSetSeed != 42 {
		t.Fatalf("ActiveSetSeed = %d, want 42", cfg.ActiveSetSeed)
	}
	if cfg.ConfigMode != "dir" {
		t.Fatalf("ConfigMode = %q, want dir", cfg.ConfigMode)
	}
	if cfg.VerifySampleSize != 7 {
		t.Fatalf("VerifySampleSize = %d, want 7", cfg.VerifySampleSize)
	}
	if cfg.VerifyChangedLimit != 40 {
		t.Fatalf("VerifyChangedLimit = %d, want 40", cfg.VerifyChangedLimit)
	}
	if cfg.ReplicationLagThreshold != 3 {
		t.Fatalf("ReplicationLagThreshold = %d, want 3", cfg.ReplicationLagThreshold)
	}
}

func TestConfigFromEnvReadsManyDBProfiles(t *testing.T) {
	tests := []struct {
		profile      string
		numDatabases int
		workers      int
		configMode   string
	}{
		{profile: "many-dbs-100-list", numDatabases: 100, workers: 2, configMode: "list"},
		{profile: "many-dbs-100-dir", numDatabases: 100, workers: 2, configMode: "dir"},
		{profile: "many-dbs-500-list", numDatabases: 500, workers: 3, configMode: "list"},
		{profile: "many-dbs-500-dir", numDatabases: 500, workers: 3, configMode: "dir"},
		{profile: "many-dbs-500-dir-lowfreq", numDatabases: 500, workers: 3, configMode: "dir"},
		{profile: "many-dbs-1000-dir", numDatabases: 1000, workers: 4, configMode: "dir"},
	}

	for _, tc := range tests {
		t.Run(tc.profile, func(t *testing.T) {
			t.Setenv("PROFILE", tc.profile)

			cfg, err := ConfigFromEnv()
			if err != nil {
				t.Fatalf("ConfigFromEnv() error = %v", err)
			}

			if cfg.LoadMode != "many-db" {
				t.Fatalf("LoadMode = %q, want many-db", cfg.LoadMode)
			}
			if cfg.NumDatabases != tc.numDatabases {
				t.Fatalf("NumDatabases = %d, want %d", cfg.NumDatabases, tc.numDatabases)
			}
			if cfg.Workers != tc.workers {
				t.Fatalf("Workers = %d, want %d", cfg.Workers, tc.workers)
			}
			if cfg.ConfigMode != tc.configMode {
				t.Fatalf("ConfigMode = %q, want %q", cfg.ConfigMode, tc.configMode)
			}
			if cfg.S3FaultProxyEnabled {
				t.Fatal("S3FaultProxyEnabled = true, want false by default until the proxy re-signs requests (issue #146)")
			}
			if cfg.S3FaultProxyFailFirstAttempts != 0 {
				t.Fatalf("S3FaultProxyFailFirstAttempts = %d, want 0", cfg.S3FaultProxyFailFirstAttempts)
			}
		})
	}
}

func TestConfigFromEnvReadsManyDB500LowFreqProfileKnobs(t *testing.T) {
	t.Setenv("PROFILE", "many-dbs-500-dir-lowfreq")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.SnapshotInterval != time.Hour {
		t.Fatalf("SnapshotInterval = %s, want 1h", cfg.SnapshotInterval)
	}
	if cfg.L1CompactionInterval != 5*time.Minute {
		t.Fatalf("L1CompactionInterval = %s, want 5m", cfg.L1CompactionInterval)
	}
	if cfg.L2CompactionInterval != 30*time.Minute {
		t.Fatalf("L2CompactionInterval = %s, want 30m", cfg.L2CompactionInterval)
	}
	if cfg.L3CompactionInterval != 6*time.Hour {
		t.Fatalf("L3CompactionInterval = %s, want 6h", cfg.L3CompactionInterval)
	}
	if cfg.L0Retention != time.Hour {
		t.Fatalf("L0Retention = %s, want 1h", cfg.L0Retention)
	}
	if cfg.L0RetentionCheckInterval != 2*time.Minute {
		t.Fatalf("L0RetentionCheckInterval = %s, want 2m", cfg.L0RetentionCheckInterval)
	}
}

func TestConfigFromEnvReadsDiskFullNoProgressWindow(t *testing.T) {
	t.Setenv("MONITOR_INTERVAL", "1s")
	t.Setenv("DISK_FULL_NO_PROGRESS_WINDOW", "2m")
	t.Setenv("DISK_FULL_RECOVERY_RESERVE_BYTES", "314572800")
	t.Setenv("DISK_FULL_RECOVERY_TIMEOUT", "5m")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.DiskFullNoProgressWindow != 2*time.Minute {
		t.Fatalf("DiskFullNoProgressWindow = %s, want 2m", cfg.DiskFullNoProgressWindow)
	}
	if cfg.MonitorInterval != time.Second {
		t.Fatalf("MonitorInterval = %s, want 1s", cfg.MonitorInterval)
	}
	if cfg.DiskFullRecoveryReserve != 314572800 {
		t.Fatalf("DiskFullRecoveryReserve = %d, want 314572800", cfg.DiskFullRecoveryReserve)
	}
	if cfg.DiskFullRecoveryTimeout != 5*time.Minute {
		t.Fatalf("DiskFullRecoveryTimeout = %s, want 5m", cfg.DiskFullRecoveryTimeout)
	}
}

func TestConfigFromEnvReadsConstrainedDiskProfile(t *testing.T) {
	t.Setenv("PROFILE", "constrained-disk")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.InitialSize != "420MB" {
		t.Fatalf("InitialSize = %q, want 420MB", cfg.InitialSize)
	}
	if cfg.MonitorInterval != time.Second {
		t.Fatalf("MonitorInterval = %s, want 1s", cfg.MonitorInterval)
	}
	if cfg.DiskFullRecoveryReserve != 300*1024*1024 {
		t.Fatalf("DiskFullRecoveryReserve = %d, want 314572800", cfg.DiskFullRecoveryReserve)
	}
	if cfg.DiskFullNoProgressWindow != 7*time.Second {
		t.Fatalf("DiskFullNoProgressWindow = %s, want 7s", cfg.DiskFullNoProgressWindow)
	}
	if cfg.DiskFullRecoveryTimeout != 5*time.Minute {
		t.Fatalf("DiskFullRecoveryTimeout = %s, want 5m", cfg.DiskFullRecoveryTimeout)
	}
	if cfg.SnapshotInterval != 2*time.Minute {
		t.Fatalf("SnapshotInterval = %s, want 2m", cfg.SnapshotInterval)
	}
}

func TestConfigFromEnvReadsZeroActivePercent(t *testing.T) {
	t.Setenv("NUM_DATABASES", "100")
	t.Setenv("ACTIVE_PERCENT", "0")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.ActivePercent != 0 {
		t.Fatalf("ActivePercent = %v, want 0", cfg.ActivePercent)
	}
	if got := len(cfg.ManyDBActivePaths()); got != 0 {
		t.Fatalf("ManyDBActivePaths() len = %d, want 0", got)
	}
}

func TestConfigFromEnvRejectsTrailingGarbageForManyDBConfig(t *testing.T) {
	t.Setenv("NUM_DATABASES", "100abc")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("ConfigFromEnv() error = nil, want non-nil")
	}
}

func TestConfigFromEnvRejectsNaNActivePercent(t *testing.T) {
	t.Setenv("NUM_DATABASES", "100")
	t.Setenv("ACTIVE_PERCENT", "NaN")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("ConfigFromEnv() error = nil, want non-nil")
	}
}

func TestConfigFromEnvNormalizesObserveModeFaultKnobs(t *testing.T) {
	t.Setenv("S3_FAULT_PROXY_ENABLED", "true")
	t.Setenv("S3_FAULT_PROXY_MODE", "observe")
	t.Setenv("S3_FAULT_PROXY_FAIL_FIRST_ATTEMPTS", "3")
	t.Setenv("S3_FAULT_PROXY_MAX_FAILURES", "5")
	t.Setenv("S3_FAULT_PROXY_REQUIRE_OBSERVED_SOURCE_GET", "true")
	t.Setenv("S3_FAULT_PROXY_REQUIRE_OBSERVED_SOURCE_RANGE_GET", "true")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}

	if cfg.S3FaultProxyFailFirstAttempts != 0 {
		t.Fatalf("S3FaultProxyFailFirstAttempts = %d, want 0 in observe mode", cfg.S3FaultProxyFailFirstAttempts)
	}
	if cfg.S3FaultProxyMaxFailures != 0 {
		t.Fatalf("S3FaultProxyMaxFailures = %d, want 0 in observe mode", cfg.S3FaultProxyMaxFailures)
	}
	if cfg.S3FaultProxyRequireObservedSourceGet {
		t.Fatal("S3FaultProxyRequireObservedSourceGet = true, want false in observe mode")
	}
	if cfg.S3FaultProxyRequireObservedSourceRangeGet {
		t.Fatal("S3FaultProxyRequireObservedSourceRangeGet = true, want false in observe mode")
	}
}

func TestConfigFromEnvReadsTruncatePageNAndPinnedReader(t *testing.T) {
	t.Setenv("REPLICA_TYPE", "s3")
	t.Setenv("S3_BUCKET", "bucket")
	t.Setenv("TRUNCATE_PAGE_N", "0")
	t.Setenv("PINNED_READER_HOLD", "4m")
	t.Setenv("PINNED_READER_PAUSE", "45s")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.TruncatePageN == nil || *cfg.TruncatePageN != 0 {
		t.Fatalf("TruncatePageN = %v, want pointer to 0", cfg.TruncatePageN)
	}
	if got, want := cfg.PinnedReaderHold, 4*time.Minute; got != want {
		t.Fatalf("PinnedReaderHold = %v, want %v", got, want)
	}
	if got, want := cfg.PinnedReaderPause, 45*time.Second; got != want {
		t.Fatalf("PinnedReaderPause = %v, want %v", got, want)
	}

	wl := cfg.WorkloadConfig()
	if wl.TruncatePageN == nil || *wl.TruncatePageN != 0 {
		t.Fatalf("WorkloadConfig().TruncatePageN = %v, want pointer to 0", wl.TruncatePageN)
	}
	if wl.PinnedReaderHold != "4m0s" || wl.PinnedReaderPause != "45s" {
		t.Fatalf("WorkloadConfig() pinned reader = %q/%q, want 4m0s/45s", wl.PinnedReaderHold, wl.PinnedReaderPause)
	}
}

func TestConfigFromEnvRejectsNegativeTruncatePageN(t *testing.T) {
	t.Setenv("REPLICA_TYPE", "s3")
	t.Setenv("S3_BUCKET", "bucket")
	t.Setenv("TRUNCATE_PAGE_N", "-1")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("ConfigFromEnv() error = nil, want error for negative TRUNCATE_PAGE_N")
	}
}

func TestObserveProxyOptInViaEnv(t *testing.T) {
	t.Setenv("REPLICA_TYPE", "s3")
	t.Setenv("S3_BUCKET", "bucket")
	t.Setenv("S3_OBSERVE_PROXY_ENABLED", "true")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if !cfg.S3FaultProxyEnabled {
		t.Fatal("S3FaultProxyEnabled = false, want true when S3_OBSERVE_PROXY_ENABLED is set")
	}
	if cfg.S3FaultProxyMode != "observe" {
		t.Fatalf("S3FaultProxyMode = %q, want observe", cfg.S3FaultProxyMode)
	}
}
