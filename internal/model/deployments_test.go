package model

import (
	"path/filepath"
	"testing"
)

func TestUpsertReadyDeploymentKeepsLitestreamVersionsDistinct(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	for _, litestreamSHA := range []string{"litestream-a", "litestream-b"} {
		if err := db.UpsertReadyDeployment(&Deployment{
			GitSHA:        "soak-sha",
			LitestreamSHA: litestreamSHA,
			ImageRef:      "registry.fly.io/litestream-soak:sha-test",
			Source:        "main",
			Status:        "ready",
		}); err != nil {
			t.Fatalf("UpsertReadyDeployment(%s) error = %v", litestreamSHA, err)
		}
	}

	deployments, err := db.ListDeployments("main", 10)
	if err != nil {
		t.Fatalf("ListDeployments() error = %v", err)
	}
	if len(deployments) != 2 {
		t.Fatalf("len(deployments) = %d, want 2", len(deployments))
	}

	deployment, err := db.GetDeploymentByVersion("main", "soak-sha", "litestream-a")
	if err != nil {
		t.Fatalf("GetDeploymentByVersion() error = %v", err)
	}
	if deployment.LitestreamSHA != "litestream-a" {
		t.Fatalf("deployment.LitestreamSHA = %q, want litestream-a", deployment.LitestreamSHA)
	}
}
