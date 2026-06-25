package worker

import (
	"strings"
	"testing"
	"time"
)

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
