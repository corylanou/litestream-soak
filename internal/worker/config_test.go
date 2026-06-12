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
