package worker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func observeProxyRoutingConfig(t *testing.T, dir string) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.LogicalTimeout = 30 * time.Second
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-single"
	cfg.S3Region = "auto"
	cfg.S3AccessKey = "example-key"
	cfg.S3SecretKey = "example-secret"
	cfg.S3Endpoint = "http://127.0.0.1:9999"
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyMode = "observe"
	cfg.S3FaultProxyEndpoint = "http://127.0.0.1:9999"
	cfg.S3FaultProxyTargetEndpoint = "https://real.example.com"
	return cfg
}

func TestObserveModeSingleDBRestoreBypassesProxy(t *testing.T) {
	dir := t.TempDir()
	cfg := observeProxyRoutingConfig(t, dir)
	if err := os.WriteFile(cfg.ConfigPath, []byte("dbs: []\n"), 0600); err != nil {
		t.Fatal(err)
	}
	logicalTestDB(t, cfg.DBPath, "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
	logicalTestDB(t, cfg.DBPath+".restored", "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
	startBoundarySyncFixture(t, &cfg, 42)
	script := "#!/bin/sh\ncat \"$3\"\necho \"https_proxy=${HTTPS_PROXY:-unset} http_proxy=${HTTP_PROXY:-unset}\"\n"
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	v := NewVerifier(cfg)
	if passed, err := v.validate(context.Background(), 42); !passed || err != nil {
		t.Fatalf("passed=%v err=%v", passed, err)
	}
	var output string
	for _, step := range v.validationSteps {
		if step.Name == "restore" {
			output = step.OutputTail
		}
	}
	if !strings.Contains(output, "endpoint: https://real.example.com") || strings.Contains(output, "127.0.0.1:9999") {
		t.Fatalf("single-DB observe restore did not bypass the proxy:\n%s", output)
	}
	if !strings.Contains(output, "https_proxy=unset http_proxy=unset") {
		t.Fatalf("single-DB observe restore tunnels through the proxy:\n%s", output)
	}
}

func TestObserveModeEvidenceAndDiagnosticsBypassProxy(t *testing.T) {
	cfg := observeProxyRoutingConfig(t, t.TempDir())
	if got := cfg.directS3Endpoint(); got != "https://real.example.com" {
		t.Fatalf("directS3Endpoint() = %q", got)
	}
	cfg.S3FaultProxyMode = "uploadpart-reset"
	if got := cfg.directS3Endpoint(); got != cfg.S3Endpoint {
		t.Fatalf("fault-mode directS3Endpoint() = %q, want unchanged %q", got, cfg.S3Endpoint)
	}

	cfg = observeProxyRoutingConfig(t, t.TempDir())
	dir := t.TempDir()
	output := filepath.Join(dir, "args")
	t.Setenv("PROFILE_TEST_ARGS", output)
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	if err := os.WriteFile(filepath.Join(dir, "s3cmd"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$PROFILE_TEST_ARGS\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := newPprofCapturer(&cfg).upload(context.Background(), "/tmp/example.pprof", "example.pprof"); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "--host=real.example.com") || strings.Contains(string(body), "127.0.0.1:9999") {
		t.Fatalf("evidence upload went through the proxy: %s", body)
	}
	if host := endpointHost(cfg.directS3Endpoint()); host != "real.example.com" {
		t.Fatalf("diagnostic listing host = %q", host)
	}
}

func TestRealLitestreamSingleDBRestoreCountsEachRequestOnce(t *testing.T) {
	if os.Getenv("SOAK_TEST_LITESTREAM_BINARY") != "1" {
		t.Skip("set SOAK_TEST_LITESTREAM_BINARY=1 to run against the installed litestream binary")
	}
	if _, err := exec.LookPath("litestream"); err != nil {
		t.Fatalf("litestream binary: %v", err)
	}
	fixture := &recordingS3Fixture{requests: make(map[string]uint64), hosts: make(map[string]bool)}
	upstream := newRecordingUpstream(t, fixture)

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.LogicalTimeout = 30 * time.Second
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-single"
	cfg.S3Endpoint = upstream
	cfg.S3AccessKey = testS3AccessKey
	cfg.S3SecretKey = testS3SecretKey
	cfg.S3SessionToken = testS3SessionToken
	cfg.S3Region = testS3Region
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyMode = "observe"
	cfg.S3FaultProxyListenAddr = "127.0.0.1:0"
	logicalTestDB(t, cfg.DBPath, "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
	startBoundarySyncFixture(t, &cfg, 42)

	runner := NewRunner(cfg)
	if err := runner.startS3FaultProxy(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.stopS3FaultProxy)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatal(err)
	}
	verifier := NewVerifier(runner.cfg)
	verifier.objectStore = runner.objectStore
	_, _ = verifier.validate(context.Background(), 42)

	served, _, _ := fixture.snapshot()
	if served["LIST"] == 0 {
		t.Fatalf("fixture saw no restore LIST requests: %v", served)
	}
	snapshot := runner.currentSnapshot()
	combined := make(map[string]uint64)
	for _, counts := range []map[string]uint64{snapshot.RestoreRequestTotals, snapshot.ReplicaRequestTotals} {
		for operation, count := range counts {
			combined[operation] += count
		}
	}
	if !reflect.DeepEqual(combined, served) {
		t.Fatalf("restore %v + replica %v = %v, upstream served %v", snapshot.RestoreRequestTotals, snapshot.ReplicaRequestTotals, combined, served)
	}
	if len(snapshot.ReplicaRequestTotals) != 0 {
		t.Fatalf("restore traffic attributed to replica: %v", snapshot.ReplicaRequestTotals)
	}
}
