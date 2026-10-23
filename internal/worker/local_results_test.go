package worker

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestLocalResultsAppendsVerificationCycles(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "results", "verifications.jsonl")
	cfg := DefaultConfig()
	cfg.LocalResultsPath = path
	cfg.ReplicaBackend = workload.ReplicaBackendSFTP
	cfg.ReplicaType = "sftp"
	cfg.ProfileName = "coverage-sftp"
	runner := NewRunner(cfg)

	runner.recordLocalResult(VerificationResult{CheckType: "integrity", Status: "passed", Passed: true})
	runner.recordLocalResult(VerificationResult{CheckType: "integrity", Status: "failed", ErrorMessage: "boom"})

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var records []localResult
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var record localResult
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	if records[0].Backend != "sftp" || records[0].ReplicaType != "sftp" || records[0].Profile != "coverage-sftp" || !records[0].Result.Passed {
		t.Fatalf("first record = %+v", records[0])
	}
	if records[1].Result.Passed || records[1].Result.ErrorMessage != "boom" {
		t.Fatalf("second record = %+v", records[1])
	}
}

func TestLocalResultsDisabledWithoutPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	NewRunner(cfg).recordLocalResult(VerificationResult{Passed: true})
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("recordLocalResult wrote %d files without SOAK_LOCAL_RESULTS", len(entries))
	}
}

func TestConfigFromEnvReadsSocketPathOverride(t *testing.T) {
	t.Parallel()

	env := map[string]string{"DATA_DIR": "/very/long/data/dir", "LITESTREAM_SOCKET_PATH": "/tmp/ls.sock"}
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SocketPath != "/tmp/ls.sock" {
		t.Fatalf("SocketPath = %q, want override", cfg.SocketPath)
	}
}

func TestConfigFromEnvReadsLocalResultsPath(t *testing.T) {
	t.Parallel()

	cfg, err := configFromLookup(func(key string) string {
		if key == "SOAK_LOCAL_RESULTS" {
			return "/tmp/results.jsonl"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LocalResultsPath != "/tmp/results.jsonl" {
		t.Fatalf("LocalResultsPath = %q", cfg.LocalResultsPath)
	}
}
