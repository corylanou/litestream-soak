package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

type Deployer struct {
	manager           *Manager
	db                *model.DB
	appName           string
	allowRuntimeBuild bool
}

func NewDeployer(manager *Manager, db *model.DB, appName string, allowRuntimeBuild bool) *Deployer {
	return &Deployer{
		manager:           manager,
		db:                db,
		appName:           appName,
		allowRuntimeBuild: allowRuntimeBuild,
	}
}

func (d *Deployer) DeployNewSHA(sha string) error {
	if !d.allowRuntimeBuild {
		return fmt.Errorf("runtime builds are disabled; build in CI and notify /api/admin/deployments/ready")
	}

	existing, err := d.db.GetDeploymentBySHA(sha)
	if err == nil && existing.Status == "ready" {
		slog.Info("Deployment already exists for SHA, triggering rolling update", "sha", sha, "image", existing.ImageRef)
		_, err := d.NotifyDeploymentReady(context.Background(), "main", sha, existing.ImageRef, "github_webhook_ready")
		return err
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
	_, err = d.NotifyDeploymentReady(context.Background(), "main", sha, imageRef, "github_webhook_build")
	return err
}

func (d *Deployer) NotifyDeploymentReady(ctx context.Context, source, sha, imageRef, trigger string) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		source = "main"
	}
	sha = strings.TrimSpace(sha)
	if sha == "" {
		return "", fmt.Errorf("deployment sha is required")
	}
	trigger = strings.TrimSpace(trigger)
	if trigger == "" {
		trigger = "deploy_ready"
	}
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		var err error
		imageRef, err = d.manager.currentWorkerImage(ctx)
		if err != nil {
			return "", fmt.Errorf("resolve worker image: %w", err)
		}
	}

	if err := d.db.UpsertReadyDeployment(&model.Deployment{
		GitSHA:   sha,
		ImageRef: imageRef,
		Source:   source,
		Status:   "ready",
	}); err != nil {
		return "", fmt.Errorf("record ready deployment: %w", err)
	}

	shortSHA := sha
	if len(shortSHA) > 12 {
		shortSHA = shortSHA[:12]
	}
	message := fmt.Sprintf("Image ready for %s via %s", shortSHA, trigger)
	if err := d.db.RecordEvent("", "deploy_ready_received", message, imageRef); err != nil {
		return "", fmt.Errorf("record deploy event: %w", err)
	}

	slog.Info("Deployment ready, starting rolling update", "sha", sha, "image", imageRef, "trigger", trigger)
	if err := d.manager.RollingUpdate(ctx, imageRef, sha); err != nil {
		return "", err
	}
	if err := d.manager.ResumeDormantWorkers(ctx, source, imageRef, sha, trigger); err != nil {
		return "", err
	}

	return imageRef, nil
}

func (d *Deployer) buildImage(sha string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	imageTag := fmt.Sprintf("registry.fly.io/%s:sha-%s", d.appName, sha[:12])

	args := []string{
		"deploy",
		"--app", d.appName,
		"--image-label", fmt.Sprintf("sha-%s", sha[:12]),
		"--build-only",
		"--push",
	}
	if litestreamSHA := strings.TrimSpace(os.Getenv("LITESTREAM_SHA")); litestreamSHA != "" {
		args = append(args, "--build-arg", fmt.Sprintf("LITESTREAM_SHA=%s", litestreamSHA))
	}

	cmd := exec.CommandContext(ctx, "fly", args...)

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
