package orchestrator

import (
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestManagerWorkerEnvIncludesReplicaConfig(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica: ReplicaConfig{
			Bucket:    "litestream-soak-replicas-shared",
			Endpoint:  "https://fly.storage.tigris.dev",
			AccessKey: "access-key",
			SecretKey: "secret-key",
			Region:    "auto",
		},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:            "worker-main-gharchive",
		Name:          "worker-main-gharchive",
		LitestreamSHA: "abc123",
	}, workload.Config{
		InitialSize:      "10MB",
		VerifyInterval:   "30m",
		VerifyType:       "integrity",
		SnapshotInterval: "10m",
		SyncInterval:     "1s",
		LoadMode:         "replay",
	})

	if got, want := env["S3_BUCKET"], "litestream-soak-replicas-shared"; got != want {
		t.Fatalf("S3_BUCKET=%q, want %q", got, want)
	}
	if got, want := env["BUCKET_NAME"], "litestream-soak-replicas-shared"; got != want {
		t.Fatalf("BUCKET_NAME=%q, want %q", got, want)
	}
	if got, want := env["S3_ENDPOINT"], "https://fly.storage.tigris.dev"; got != want {
		t.Fatalf("S3_ENDPOINT=%q, want %q", got, want)
	}
	if got, want := env["AWS_ENDPOINT_URL_S3"], "https://fly.storage.tigris.dev"; got != want {
		t.Fatalf("AWS_ENDPOINT_URL_S3=%q, want %q", got, want)
	}
	if got, want := env["AWS_ACCESS_KEY_ID"], "access-key"; got != want {
		t.Fatalf("AWS_ACCESS_KEY_ID=%q, want %q", got, want)
	}
	if got, want := env["AWS_SECRET_ACCESS_KEY"], "secret-key"; got != want {
		t.Fatalf("AWS_SECRET_ACCESS_KEY=%q, want %q", got, want)
	}
	if got, want := env["AWS_REGION"], "auto"; got != want {
		t.Fatalf("AWS_REGION=%q, want %q", got, want)
	}
}

func TestManagerWorkerEnvOmitsEmptyReplicaCredentials(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica: ReplicaConfig{
			Bucket:   "litestream-soak-replicas-shared",
			Endpoint: "https://fly.storage.tigris.dev",
		},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-low-vol",
		Name: "worker-main-low-vol",
	}, workload.Config{
		InitialSize:      "5MB",
		VerifyInterval:   "30m",
		VerifyType:       "integrity",
		SnapshotInterval: "10m",
		SyncInterval:     "1s",
		LoadMode:         "synthetic",
	})

	if _, ok := env["AWS_ACCESS_KEY_ID"]; ok {
		t.Fatal("AWS_ACCESS_KEY_ID should be omitted when unset")
	}
	if _, ok := env["AWS_SECRET_ACCESS_KEY"]; ok {
		t.Fatal("AWS_SECRET_ACCESS_KEY should be omitted when unset")
	}
	if _, ok := env["AWS_REGION"]; ok {
		t.Fatal("AWS_REGION should be omitted when unset")
	}
}
