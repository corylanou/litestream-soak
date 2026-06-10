package model

import (
	"path/filepath"
	"testing"
)

func TestUpsertReadyDeploymentKeepsLitestreamVersionsDistinct(t *testing.T) {
	t.Parallel()

	db := openDeploymentTestDB(t)

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

func TestDeploymentReadsHandleNullErrorMessage(t *testing.T) {
	t.Parallel()

	db := openDeploymentTestDB(t)

	mustCreateDeployment(t, db, &Deployment{
		GitSHA:        "building-sha",
		LitestreamSHA: "litestream-sha",
		ImageRef:      "",
		Source:        "main",
		Status:        "building",
	})

	latest, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestDeployment() error = %v", err)
	}
	if latest == nil {
		t.Fatal("GetLatestDeployment() = nil, want deployment")
	}
	if latest.ErrorMessage != "" {
		t.Fatalf("latest.ErrorMessage = %q, want empty", latest.ErrorMessage)
	}

	deployments, err := db.ListDeployments("main", 10)
	if err != nil {
		t.Fatalf("ListDeployments() error = %v", err)
	}
	if len(deployments) != 1 {
		t.Fatalf("len(deployments) = %d, want 1", len(deployments))
	}
	if deployments[0].ErrorMessage != "" {
		t.Fatalf("deployments[0].ErrorMessage = %q, want empty", deployments[0].ErrorMessage)
	}
}

func TestGetLatestReadyDeploymentSkipsBuildingRows(t *testing.T) {
	t.Parallel()

	db := openDeploymentTestDB(t)

	oldID := mustCreateDeployment(t, db, &Deployment{
		GitSHA:        "old-sha",
		LitestreamSHA: "old-litestream",
		ImageRef:      "",
		Source:        "main",
		Status:        "building",
	})
	newID := mustCreateDeployment(t, db, &Deployment{
		GitSHA:        "new-sha",
		LitestreamSHA: "new-litestream",
		ImageRef:      "",
		Source:        "main",
		Status:        "building",
	})
	mustCreateDeployment(t, db, &Deployment{
		GitSHA:        "building-sha",
		LitestreamSHA: "building-litestream",
		ImageRef:      "",
		Source:        "main",
		Status:        "building",
	})

	if err := db.UpdateDeployment(oldID, "ready", "registry.fly.io/app:old", ""); err != nil {
		t.Fatalf("UpdateDeployment(old) error = %v", err)
	}
	if err := db.UpdateDeployment(newID, "ready", "registry.fly.io/app:new", ""); err != nil {
		t.Fatalf("UpdateDeployment(new) error = %v", err)
	}

	latest, err := db.GetLatestReadyDeployment("main")
	if err != nil {
		t.Fatalf("GetLatestReadyDeployment() error = %v", err)
	}
	if latest == nil {
		t.Fatal("GetLatestReadyDeployment() = nil, want deployment")
	}
	if latest.GitSHA != "new-sha" {
		t.Fatalf("latest.GitSHA = %q, want new-sha", latest.GitSHA)
	}
	if latest.LitestreamSHA != "new-litestream" {
		t.Fatalf("latest.LitestreamSHA = %q, want new-litestream", latest.LitestreamSHA)
	}

	missing, err := db.GetLatestReadyDeployment("missing")
	if err != nil {
		t.Fatalf("GetLatestReadyDeployment(missing) error = %v", err)
	}
	if missing != nil {
		t.Fatalf("GetLatestReadyDeployment(missing) = %#v, want nil", missing)
	}
}

func openDeploymentTestDB(t *testing.T) *DB {
	t.Helper()

	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func mustCreateDeployment(t *testing.T, db *DB, deployment *Deployment) int64 {
	t.Helper()

	id, err := db.CreateDeployment(deployment)
	if err != nil {
		t.Fatalf("CreateDeployment(%s) error = %v", deployment.GitSHA, err)
	}
	return id
}
