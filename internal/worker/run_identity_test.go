package worker

import (
	"testing"
)

func TestReporterCapturesDistinctRunVersions(t *testing.T) {
	t.Setenv("SOAK_DEPLOYMENT_ID", "42")
	t.Setenv("SOAK_RUN_ID", "run")
	t.Setenv("SOAK_WORKLOAD_ID", "configuration-hash")
	t.Setenv("WORKLOAD_SHA", "generator-source")
	t.Setenv("GIT_SHA", "harness-source")
	t.Setenv("LITESTREAM_SHA", "candidate-source")
	t.Setenv("REPLICA_TYPE", "file")
	t.Setenv("CONTROL_BASE_URL", "http://control.example")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	reporter := NewReporter(cfg)
	if reporter.identity.DeploymentID != 42 || reporter.identity.RunID != "run" || reporter.identity.WorkloadID != "configuration-hash" || reporter.identity.WorkloadSHA != "generator-source" || reporter.identity.LitestreamSHA != "candidate-source" || reporter.identity.ValidatorID != "soak-verifier:harness-source" {
		t.Fatalf("identity = %+v", reporter.identity)
	}
	cfg.GitSHA = "changed"
	if reporter.identity.GitSHA != "harness-source" {
		t.Fatal("reporter identity changed after construction")
	}
}

func TestExpectedWorkloadUsesWorkerDefaultsAndOverrides(t *testing.T) {
	env := map[string]string{"PROFILE": "high-volume", "WRITE_RATE": "987", "VERIFY_INTERVAL": "7m", "REPLICA_TYPE": "s3"}
	expected, err := WorkloadFromEnvironment(env)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range env {
		t.Setenv(key, value)
	}
	t.Setenv("S3_BUCKET", "test-bucket")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if expected.JSON() != cfg.WorkloadConfig().JSON() {
		t.Fatalf("expected workload differs from running worker:\n%s\n%s", expected.JSON(), cfg.WorkloadConfig().JSON())
	}
	env["WRITE_RATE"] = "invalid"
	if _, err := WorkloadFromEnvironment(env); err == nil {
		t.Fatal("invalid workload accepted")
	}
}

func TestDeploymentIdentityRejectsInvalidEnvironment(t *testing.T) {
	for _, value := range []string{"invalid", "0", "-1"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("SOAK_DEPLOYMENT_ID", value)
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid deployment identity accepted")
			}
		})
	}
}

func TestLegacyImageReportsSharedGeneratorSource(t *testing.T) {
	t.Setenv("WORKLOAD_SHA", "")
	t.Setenv("LITESTREAM_SHA", "shared-source")
	t.Setenv("REPLICA_TYPE", "file")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkloadSHA != cfg.LitestreamSHA {
		t.Fatalf("legacy generator source = %q, want %q", cfg.WorkloadSHA, cfg.LitestreamSHA)
	}
}
