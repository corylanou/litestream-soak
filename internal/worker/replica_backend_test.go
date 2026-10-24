package worker

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/workload"
)

func replicaBackendEnv(dataDir, backend string, extra map[string]string) map[string]string {
	env := map[string]string{
		"DATA_DIR":               dataDir,
		"REPLICA_BACKEND":        backend,
		"REPLICA_TYPE":           workload.ReplicaTypeForBackend(backend),
		"S3_BUCKET":              "tigris-bucket",
		"S3_ENDPOINT":            "https://fly.storage.tigris.dev",
		"S3_PATH":                "soak/worker-main/vol_1",
		"AWS_ACCESS_KEY_ID":      "tigris-access",
		"AWS_SECRET_ACCESS_KEY":  "tigris-secret",
		"AWS_REGION":             "auto",
		"S3_FAULT_PROXY_ENABLED": "true",
	}
	for key, value := range extra {
		env[key] = value
	}
	return env
}

func loadReplicaBackendConfig(t *testing.T, env map[string]string) Config {
	t.Helper()
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func renderLitestreamConfig(t *testing.T, cfg Config) string {
	t.Helper()
	manager := newLitestreamManager(&cfg)
	var out bytes.Buffer
	if err := litestreamConfigTmpl.Execute(&out, manager.litestreamConfigData()); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func assertConfigContains(t *testing.T, config string, want ...string) {
	t.Helper()
	for _, line := range want {
		if !strings.Contains(config, line) {
			t.Errorf("litestream config missing %q:\n%s", line, config)
		}
	}
}

func assertConfigOmits(t *testing.T, config string, unwanted ...string) {
	t.Helper()
	for _, line := range unwanted {
		if strings.Contains(config, line) {
			t.Errorf("litestream config must not contain %q:\n%s", line, config)
		}
	}
}

func assertTigrisArtifactStore(t *testing.T, cfg Config) {
	t.Helper()
	store, ok := cfg.artifactStore()
	if !ok || store.Bucket != "tigris-bucket" || store.Endpoint != "https://fly.storage.tigris.dev" || store.AccessKey != "tigris-access" || store.SecretKey != "tigris-secret" {
		t.Fatalf("artifactStore() = bucket %q endpoint %q ok %v; want Tigris store for profiles", store.Bucket, store.Endpoint, ok)
	}
}

func assertWorkloadBackend(t *testing.T, env map[string]string, want string) {
	t.Helper()
	workloadCfg, err := WorkloadFromEnvironment(env)
	if err != nil {
		t.Fatal(err)
	}
	if workloadCfg.ReplicaBackend != want {
		t.Fatalf("WorkloadFromEnvironment ReplicaBackend = %q, want %q", workloadCfg.ReplicaBackend, want)
	}
	if workloadCfg.S3FaultProxyEnabled {
		t.Fatal("WorkloadFromEnvironment must not report a fault proxy for a non-Tigris backend")
	}
}

func TestMinIOBackendTargetsPathStyleEndpoint(t *testing.T) {
	t.Parallel()

	env := replicaBackendEnv(t.TempDir(), workload.ReplicaBackendMinIO, map[string]string{
		"REPLICA_URL":               "http://127.0.0.1:9000",
		"REPLICA_BUCKET":            "coverage",
		"REPLICA_ACCESS_KEY_ID":     "minio-access",
		"REPLICA_SECRET_ACCESS_KEY": "minio-secret",
	})
	cfg := loadReplicaBackendConfig(t, env)
	if cfg.ReplicaType != "s3" || cfg.S3Endpoint != "http://127.0.0.1:9000" || cfg.S3Bucket != "coverage" {
		t.Fatalf("MinIO replica = type %q endpoint %q bucket %q", cfg.ReplicaType, cfg.S3Endpoint, cfg.S3Bucket)
	}
	if cfg.S3AccessKey != "minio-access" || cfg.S3SecretKey != "minio-secret" {
		t.Fatal("MinIO replica must use the MinIO credentials, not Tigris")
	}
	if cfg.S3FaultProxyEnabled {
		t.Fatal("S3 fault proxy must stay off for the MinIO backend")
	}
	assertTigrisArtifactStore(t, cfg)

	config := renderLitestreamConfig(t, cfg)
	assertConfigContains(t, config, "- url: s3://coverage/soak/worker-main/vol_1", "endpoint: http://127.0.0.1:9000", "force-path-style: true")
	assertConfigOmits(t, config, "tigris")
	assertWorkloadBackend(t, env, workload.ReplicaBackendMinIO)
}

func TestFileBackendReplicatesToWorkerVolume(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	env := replicaBackendEnv(dataDir, workload.ReplicaBackendFile, nil)
	cfg := loadReplicaBackendConfig(t, env)
	if cfg.ReplicaType != "file" || cfg.ReplicaPath != filepath.Join(dataDir, "replicas") {
		t.Fatalf("file replica = type %q path %q, want file on the worker volume", cfg.ReplicaType, cfg.ReplicaPath)
	}
	if cfg.S3FaultProxyEnabled {
		t.Fatal("S3 fault proxy must stay off for the file backend")
	}
	assertTigrisArtifactStore(t, cfg)

	config := renderLitestreamConfig(t, cfg)
	assertConfigContains(t, config, "- path: "+filepath.Join(dataDir, "replicas"))
	assertConfigOmits(t, config, "endpoint:", "url:")
	assertWorkloadBackend(t, env, workload.ReplicaBackendFile)
}

func TestSFTPBackendGeneratesSFTPReplica(t *testing.T) {
	t.Parallel()

	env := replicaBackendEnv(t.TempDir(), workload.ReplicaBackendSFTP, map[string]string{
		"REPLICA_URL":      "sftp://soak@127.0.0.1:2222/upload",
		"REPLICA_PASSWORD": "sftp-secret",
		"REPLICA_KEY_PATH": "/keys/id_ed25519",
	})
	cfg := loadReplicaBackendConfig(t, env)
	if cfg.ReplicaType != "sftp" {
		t.Fatalf("ReplicaType = %q, want sftp", cfg.ReplicaType)
	}
	assertTigrisArtifactStore(t, cfg)

	config := renderLitestreamConfig(t, cfg)
	assertConfigContains(t, config,
		"- url: sftp://soak@127.0.0.1:2222/upload/soak/worker-main/vol_1",
		"type: sftp",
		"password: ${REPLICA_PASSWORD}",
		"key-path: /keys/id_ed25519",
	)
	assertConfigOmits(t, config, "sftp-secret", "endpoint:", "force-path-style")
	assertWorkloadBackend(t, env, workload.ReplicaBackendSFTP)

	target, ok, err := litestreamConfigReplicaTarget(writeTestConfig(t, config))
	if err != nil || !ok || target != "sftp://soak@127.0.0.1:2222/upload/soak/worker-main/vol_1" {
		t.Fatalf("replica target = %q, %v, %v", target, ok, err)
	}
}

func TestNATSBackendGeneratesObjectStoreReplica(t *testing.T) {
	t.Parallel()

	env := replicaBackendEnv(t.TempDir(), workload.ReplicaBackendNATS, map[string]string{
		"REPLICA_URL": "nats://127.0.0.1:4222",
	})
	cfg := loadReplicaBackendConfig(t, env)
	if cfg.ReplicaType != "nats" {
		t.Fatalf("ReplicaType = %q, want nats", cfg.ReplicaType)
	}
	if got := cfg.NATSBuckets(); len(got) != 1 || got[0] != "soak-worker-main-vol_1" {
		t.Fatalf("NATSBuckets() = %v, want the single DB bucket", got)
	}

	config := renderLitestreamConfig(t, cfg)
	assertConfigContains(t, config, "- url: nats://127.0.0.1:4222/soak-worker-main-vol_1", "type: nats")
	assertConfigOmits(t, config, "password:", "endpoint:")
	assertWorkloadBackend(t, env, workload.ReplicaBackendNATS)
}

func TestWebDAVBackendGeneratesWebDAVReplica(t *testing.T) {
	t.Parallel()

	env := replicaBackendEnv(t.TempDir(), workload.ReplicaBackendWebDAV, map[string]string{
		"REPLICA_URL":      "webdav://soak@127.0.0.1:8080",
		"REPLICA_PASSWORD": "webdav-secret",
	})
	cfg := loadReplicaBackendConfig(t, env)
	if cfg.ReplicaType != "webdav" {
		t.Fatalf("ReplicaType = %q, want webdav", cfg.ReplicaType)
	}

	config := renderLitestreamConfig(t, cfg)
	assertConfigContains(t, config,
		"- url: webdav://soak@127.0.0.1:8080/soak/worker-main/vol_1",
		"type: webdav",
		"webdav-password: ${REPLICA_PASSWORD}",
	)
	assertConfigOmits(t, config, "webdav-secret", "endpoint:")
	assertWorkloadBackend(t, env, workload.ReplicaBackendWebDAV)
}

func TestRemoteBackendsSplitManyDBReplicas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		backend string
		url     string
		want    string
	}{
		{backend: workload.ReplicaBackendSFTP, url: "sftp://soak@127.0.0.1:2222", want: "- url: sftp://soak@127.0.0.1:2222/soak/worker-main/vol_1/db-00002.db"},
		{backend: workload.ReplicaBackendWebDAV, url: "webdav://127.0.0.1:8080", want: "- url: webdav://127.0.0.1:8080/soak/worker-main/vol_1/db-00002.db"},
		{backend: workload.ReplicaBackendNATS, url: "nats://127.0.0.1:4222", want: "- url: nats://127.0.0.1:4222/soak-worker-main-vol_1-db-00002-db"},
	}
	for _, tt := range tests {
		env := replicaBackendEnv(t.TempDir(), tt.backend, map[string]string{
			"REPLICA_URL":   tt.url,
			"NUM_DATABASES": "2",
			"CONFIG_MODE":   "list",
		})
		cfg := loadReplicaBackendConfig(t, env)
		assertConfigContains(t, renderLitestreamConfig(t, cfg), tt.want)
	}
}

func TestReplicaBackendRequiresRemoteURL(t *testing.T) {
	t.Parallel()

	for _, backend := range []string{workload.ReplicaBackendMinIO, workload.ReplicaBackendSFTP, workload.ReplicaBackendNATS, workload.ReplicaBackendWebDAV} {
		env := replicaBackendEnv(t.TempDir(), backend, nil)
		if _, err := configFromLookup(func(key string) string { return env[key] }); err == nil || !strings.Contains(err.Error(), "REPLICA_URL") {
			t.Errorf("%s configFromLookup() error = %v, want REPLICA_URL required", backend, err)
		}
	}
}

func TestReplicaBackendRejectsEmbeddedPassword(t *testing.T) {
	t.Parallel()

	for backend, replicaURL := range map[string]string{
		workload.ReplicaBackendSFTP:   "sftp://soak:sftp-secret@127.0.0.1:2222/upload",
		workload.ReplicaBackendWebDAV: "webdav://soak:webdav-secret@127.0.0.1:8080",
		workload.ReplicaBackendNATS:   "nats://soak:nats-secret@127.0.0.1:4222",
		workload.ReplicaBackendMinIO:  "http://minio:minio-secret@127.0.0.1:9000",
	} {
		env := replicaBackendEnv(t.TempDir(), backend, map[string]string{"REPLICA_URL": replicaURL})
		_, err := configFromLookup(func(key string) string { return env[key] })
		if err == nil || !strings.Contains(err.Error(), "REPLICA_PASSWORD") {
			t.Errorf("%s configFromLookup() error = %v, want embedded password rejected", backend, err)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("%s error leaks the embedded password: %v", backend, err)
		}
	}
}

func TestTigrisBackendKeepsReplicaAndArtifactStoreTogether(t *testing.T) {
	t.Parallel()

	env := replicaBackendEnv(t.TempDir(), "", nil)
	cfg := loadReplicaBackendConfig(t, env)
	if cfg.S3Endpoint != "https://fly.storage.tigris.dev" || cfg.S3Bucket != "tigris-bucket" || !cfg.S3FaultProxyEnabled {
		t.Fatalf("tigris backend changed replica config: endpoint %q bucket %q", cfg.S3Endpoint, cfg.S3Bucket)
	}
	store, ok := cfg.artifactStore()
	if !ok || store.Bucket != "tigris-bucket" {
		t.Fatalf("artifactStore() = %q, %v; want replica bucket", store.Bucket, ok)
	}
	workloadCfg, err := WorkloadFromEnvironment(env)
	if err != nil {
		t.Fatal(err)
	}
	if workloadCfg.ReplicaBackend != "" {
		t.Fatalf("tigris ReplicaBackend = %q, want empty so existing workers do not drift", workloadCfg.ReplicaBackend)
	}
}

func TestUnknownReplicaBackendIsRejected(t *testing.T) {
	t.Parallel()

	env := replicaBackendEnv(t.TempDir(), "carrier-pigeon", nil)
	if _, err := configFromLookup(func(key string) string { return env[key] }); err == nil || !strings.Contains(err.Error(), "REPLICA_BACKEND") {
		t.Fatalf("configFromLookup() error = %v, want unsupported REPLICA_BACKEND", err)
	}
}

func TestProfileUploadUsesArtifactStoreForNonTigrisBackend(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaBackend = workload.ReplicaBackendMinIO
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "coverage"
	cfg.S3Endpoint = "http://127.0.0.1:9000"
	cfg.S3AccessKey = "minio-access"
	cfg.S3SecretKey = "minio-secret"
	cfg.ArtifactS3 = s3Store{Bucket: "tigris-bucket", Endpoint: "https://fly.storage.tigris.dev", AccessKey: "tigris-access", SecretKey: "tigris-secret"}
	dir := t.TempDir()
	output := filepath.Join(dir, "args")
	t.Setenv("PROFILE_TEST_ARGS", output)
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$PROFILE_TEST_ARGS\"\nenv | grep '^AWS_ACCESS_KEY_ID=' >> \"$PROFILE_TEST_ARGS\"\n"
	if err := os.WriteFile(filepath.Join(dir, "s3cmd"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := newPprofCapturer(&cfg).upload(t.Context(), "/tmp/example.pprof", "example.pprof"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	for _, want := range []string{"--host=fly.storage.tigris.dev", "s3://tigris-bucket/", "AWS_ACCESS_KEY_ID=tigris-access"} {
		if !strings.Contains(body, want) {
			t.Errorf("upload args missing %q", want)
		}
	}
	if strings.Contains(body, "127.0.0.1:9000") || strings.Contains(body, "minio-access") {
		t.Fatal("profile upload must not target MinIO")
	}
}

func writeTestConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "litestream.yml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
