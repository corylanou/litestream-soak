package worker

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/workload"
)

func replicaBackendEnv(dataDir, backend string) map[string]string {
	return map[string]string{
		"DATA_DIR":               dataDir,
		"REPLICA_BACKEND":        backend,
		"REPLICA_TYPE":           workload.ReplicaTypeForBackend(backend),
		"S3_BUCKET":              "tigris-bucket",
		"S3_ENDPOINT":            "https://fly.storage.tigris.dev",
		"S3_PATH":                "soak/worker-main-minio/vol_1",
		"AWS_ACCESS_KEY_ID":      "tigris-access",
		"AWS_SECRET_ACCESS_KEY":  "tigris-secret",
		"AWS_REGION":             "auto",
		"S3_FAULT_PROXY_ENABLED": "true",
	}
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

func TestMinIOSidecarBackendTargetsLoopbackPathStyleReplica(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	env := replicaBackendEnv(dataDir, workload.ReplicaBackendMinIOSidecar)
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}

	if cfg.ReplicaType != "s3" {
		t.Fatalf("ReplicaType = %q, want s3", cfg.ReplicaType)
	}
	if cfg.S3Endpoint != "http://127.0.0.1:9100" {
		t.Fatalf("S3Endpoint = %q, want loopback MinIO endpoint", cfg.S3Endpoint)
	}
	if cfg.S3Bucket != minioSidecarBucket {
		t.Fatalf("S3Bucket = %q, want %q", cfg.S3Bucket, minioSidecarBucket)
	}
	if cfg.S3AccessKey != "" || cfg.S3SecretKey != "" {
		t.Fatal("MinIO replica must not reuse Tigris credentials")
	}
	if cfg.S3FaultProxyEnabled {
		t.Fatal("S3 fault proxy must stay off for the MinIO sidecar backend")
	}
	if cfg.MinIODataDir != filepath.Join(dataDir, "minio") {
		t.Fatalf("MinIODataDir = %q, want it on the worker volume", cfg.MinIODataDir)
	}

	store, ok := cfg.artifactStore()
	if !ok || store.Bucket != "tigris-bucket" || store.Endpoint != "https://fly.storage.tigris.dev" || store.AccessKey != "tigris-access" || store.SecretKey != "tigris-secret" {
		t.Fatalf("artifactStore() = %+v, %v; want Tigris store for profiles", store, ok)
	}

	config := renderLitestreamConfig(t, cfg)
	for _, want := range []string{
		"url: s3://" + minioSidecarBucket + "/soak/worker-main-minio/vol_1",
		"endpoint: http://127.0.0.1:9100",
		"force-path-style: true",
	} {
		if !strings.Contains(config, want) {
			t.Errorf("litestream config missing %q:\n%s", want, config)
		}
	}
	if strings.Contains(config, "tigris") {
		t.Fatalf("litestream config must not reference Tigris:\n%s", config)
	}

	workloadCfg, err := WorkloadFromEnvironment(env)
	if err != nil {
		t.Fatal(err)
	}
	if workloadCfg.ReplicaBackend != workload.ReplicaBackendMinIOSidecar {
		t.Fatalf("WorkloadFromEnvironment ReplicaBackend = %q, want minio-sidecar", workloadCfg.ReplicaBackend)
	}
	if workloadCfg.S3FaultProxyEnabled {
		t.Fatal("WorkloadFromEnvironment must not report a fault proxy for the MinIO backend")
	}
}

func TestFileBackendReplicatesToWorkerVolume(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	env := replicaBackendEnv(dataDir, workload.ReplicaBackendFile)
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReplicaType != "file" {
		t.Fatalf("ReplicaType = %q, want file", cfg.ReplicaType)
	}
	if cfg.ReplicaPath != filepath.Join(dataDir, "replicas") {
		t.Fatalf("ReplicaPath = %q, want it on the worker volume", cfg.ReplicaPath)
	}
	if cfg.S3FaultProxyEnabled {
		t.Fatal("S3 fault proxy must stay off for the file backend")
	}
	store, ok := cfg.artifactStore()
	if !ok || store.Bucket != "tigris-bucket" || store.AccessKey != "tigris-access" {
		t.Fatalf("artifactStore() = %+v, %v; want Tigris store for profiles", store, ok)
	}

	config := renderLitestreamConfig(t, cfg)
	if !strings.Contains(config, "- path: "+filepath.Join(dataDir, "replicas")) {
		t.Fatalf("litestream config missing file replica path:\n%s", config)
	}
	if strings.Contains(config, "endpoint:") {
		t.Fatalf("file backend config must not have an S3 endpoint:\n%s", config)
	}

	workloadCfg, err := WorkloadFromEnvironment(env)
	if err != nil {
		t.Fatal(err)
	}
	if workloadCfg.ReplicaBackend != workload.ReplicaBackendFile {
		t.Fatalf("WorkloadFromEnvironment ReplicaBackend = %q, want file", workloadCfg.ReplicaBackend)
	}
}

func TestTigrisBackendKeepsReplicaAndArtifactStoreTogether(t *testing.T) {
	t.Parallel()

	env := replicaBackendEnv(t.TempDir(), "")
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.S3Endpoint != "https://fly.storage.tigris.dev" || cfg.S3Bucket != "tigris-bucket" || !cfg.S3FaultProxyEnabled {
		t.Fatalf("tigris backend changed replica config: %+v", cfg)
	}
	store, ok := cfg.artifactStore()
	if !ok || store.Bucket != "tigris-bucket" {
		t.Fatalf("artifactStore() = %+v, %v; want replica bucket", store, ok)
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

	env := replicaBackendEnv(t.TempDir(), "carrier-pigeon")
	if _, err := configFromLookup(func(key string) string { return env[key] }); err == nil || !strings.Contains(err.Error(), "REPLICA_BACKEND") {
		t.Fatalf("configFromLookup() error = %v, want unsupported REPLICA_BACKEND", err)
	}
}

func TestProfileUploadUsesArtifactStoreForNonTigrisBackend(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaBackend = workload.ReplicaBackendMinIOSidecar
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = minioSidecarBucket
	cfg.S3Endpoint = "http://127.0.0.1:9100"
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
			t.Errorf("upload args missing %q: %s", want, body)
		}
	}
	if strings.Contains(body, "127.0.0.1:9100") || strings.Contains(body, "minio-access") {
		t.Fatalf("profile upload must not target MinIO: %s", body)
	}
}
