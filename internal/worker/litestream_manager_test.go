package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteLitestreamConfigOmitsCompactionKnobsByDefault(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-1"

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}

	body, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	config := string(body)
	for _, unwanted := range []string{"levels:", "l0-retention:", "l0-retention-check-interval:"} {
		if strings.Contains(config, unwanted) {
			t.Fatalf("config contains %q when knobs unset:\n%s", unwanted, config)
		}
	}
}

func TestWriteLitestreamConfigRendersCompactionKnobs(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-1"
	cfg.L1CompactionInterval = 5 * time.Minute
	cfg.L2CompactionInterval = 30 * time.Minute
	cfg.L3CompactionInterval = 6 * time.Hour
	cfg.L0Retention = time.Hour
	cfg.L0RetentionCheckInterval = 2 * time.Minute

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}

	body, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	config := string(body)
	for _, want := range []string{
		"levels:",
		"l0-retention: 1h0m0s",
		"l0-retention-check-interval: 2m0s",
		"dbs:",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("config missing %q:\n%s", want, config)
		}
	}

	levelsIdx := strings.Index(config, "levels:")
	l1Idx := strings.Index(config, "- interval: 5m0s")
	l2Idx := strings.Index(config, "- interval: 30m0s")
	l3Idx := strings.Index(config, "- interval: 6h0m0s")
	if l1Idx < 0 || l2Idx < 0 || l3Idx < 0 {
		t.Fatalf("config missing level intervals:\n%s", config)
	}
	if levelsIdx >= l1Idx || l1Idx >= l2Idx || l2Idx >= l3Idx {
		t.Fatalf("level intervals out of L1->L3 order:\n%s", config)
	}
	if got := strings.Count(config, "- interval:"); got != 3 {
		t.Fatalf("config has %d level interval entries, want 3:\n%s", got, config)
	}
}

func TestWriteLitestreamConfigRendersTruncatePageN(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-1"
	zero := 0
	cfg.TruncatePageN = &zero

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}

	body, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(string(body), "truncate-page-n: 0") {
		t.Fatalf("config missing truncate-page-n: 0:\n%s", string(body))
	}
}

func TestWriteLitestreamConfigOmitsTruncatePageNByDefault(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-1"

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}

	body, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if strings.Contains(string(body), "truncate-page-n") {
		t.Fatalf("config contains truncate-page-n when unset:\n%s", string(body))
	}
}
