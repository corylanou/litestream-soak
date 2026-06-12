package worker

import "testing"

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

func TestConfigFromEnvReadsManyDBConfig(t *testing.T) {
	t.Setenv("NUM_DATABASES", "100")
	t.Setenv("ACTIVE_PERCENT", "2.5")
	t.Setenv("CONFIG_MODE", "dir")
	t.Setenv("VERIFY_SAMPLE_SIZE", "7")
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
	if cfg.ConfigMode != "dir" {
		t.Fatalf("ConfigMode = %q, want dir", cfg.ConfigMode)
	}
	if cfg.VerifySampleSize != 7 {
		t.Fatalf("VerifySampleSize = %d, want 7", cfg.VerifySampleSize)
	}
	if cfg.ReplicationLagThreshold != 3 {
		t.Fatalf("ReplicationLagThreshold = %d, want 3", cfg.ReplicationLagThreshold)
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
