package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

type Deployer struct {
	manager *Manager
	db      *model.DB
	appName string
}

func NewDeployer(manager *Manager, db *model.DB, appName string) *Deployer {
	return &Deployer{
		manager: manager,
		db:      db,
		appName: appName,
	}
}

func (d *Deployer) DeployNewSHA(sha string) error {
	existing, err := d.db.GetDeploymentBySHA(sha)
	if err == nil && existing.Status == "ready" {
		slog.Info("Deployment already exists for SHA, triggering rolling update", "sha", sha, "image", existing.ImageRef)
		return d.manager.RollingUpdate(context.Background(), existing.ImageRef, sha)
	}

	slog.Info("Building new image for SHA", "sha", sha)

	dep := &model.Deployment{
		GitSHA:   sha,
		ImageRef: "",
		Source:   "main",
		Status:   "building",
	}
	depID, err := d.db.CreateDeployment(dep)
	if err != nil {
		return fmt.Errorf("create deployment record: %w", err)
	}

	d.db.RecordEvent("", "deploy_started", fmt.Sprintf("Building image for %s", sha[:12]), "")

	imageRef, err := d.buildImage(sha)
	if err != nil {
		d.db.UpdateDeployment(depID, "failed", "", err.Error())
		d.db.RecordEvent("", "deploy_failed", fmt.Sprintf("Build failed for %s: %v", sha[:12], err), "")
		return fmt.Errorf("build image: %w", err)
	}

	d.db.UpdateDeployment(depID, "ready", imageRef, "")
	d.db.RecordEvent("", "deploy_completed", fmt.Sprintf("Image ready for %s: %s", sha[:12], imageRef), "")

	slog.Info("Image built, starting rolling update", "sha", sha, "image", imageRef)
	return d.manager.RollingUpdate(context.Background(), imageRef, sha)
}

func (d *Deployer) buildImage(sha string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	imageTag := fmt.Sprintf("registry.fly.io/%s:sha-%s", d.appName, sha[:12])

	cmd := exec.CommandContext(ctx, "fly", "deploy",
		"--app", d.appName,
		"--build-arg", fmt.Sprintf("LITESTREAM_SHA=%s", sha),
		"--image-label", fmt.Sprintf("sha-%s", sha[:12]),
		"--build-only",
		"--push",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("fly deploy --build-only failed: %w\n%s", err, string(output))
	}

	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "image:") {
			imageTag = strings.TrimSpace(strings.TrimPrefix(line, "image:"))
			break
		}
	}

	slog.Info("Image built successfully", "sha", sha, "image", imageTag)
	return imageTag, nil
}
