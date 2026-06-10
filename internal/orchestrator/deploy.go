package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

var validSHARe = regexp.MustCompile(`^[0-9a-fA-F]{7,40}$`)

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
	sha = strings.TrimSpace(sha)
	if !validSHARe.MatchString(sha) {
		return fmt.Errorf("invalid deploy sha %q: must be 7-40 hex characters", sha)
	}

	if !d.allowRuntimeBuild {
		return fmt.Errorf("runtime builds are disabled; build in CI and notify /api/admin/deployments/ready")
	}

	litestreamSHA, err := resolveLitestreamBuildSHA(context.Background(), strings.TrimSpace(os.Getenv("LITESTREAM_SHA")))
	if err != nil {
		return fmt.Errorf("resolve litestream sha: %w", err)
	}

	existing, err := d.db.GetDeploymentByVersion("main", sha, litestreamSHA)
	if err == nil && existing.Status == "ready" {
		slog.Info("Deployment already exists for SHA, triggering rolling update", "sha", sha, "image", existing.ImageRef)
		_, err := d.NotifyDeploymentReady(context.Background(), "main", sha, litestreamSHA, existing.ImageRef, "github_webhook_ready")
		return err
	}

	slog.Info("Building new image for SHA", "sha", sha)

	dep := &model.Deployment{
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ImageRef:      "",
		Source:        "main",
		Status:        "building",
	}
	depID, err := d.db.CreateDeployment(dep)
	if err != nil {
		return fmt.Errorf("create deployment record: %w", err)
	}

	d.db.RecordEvent("", "deploy_started", fmt.Sprintf("Building image for %s", trimSHA(sha)), "")

	imageRef, err := d.buildImage(sha)
	if err != nil {
		d.db.UpdateDeployment(depID, "failed", "", err.Error())
		d.db.RecordEvent("", "deploy_failed", fmt.Sprintf("Build failed for %s: %v", trimSHA(sha), err), "")
		return fmt.Errorf("build image: %w", err)
	}

	d.db.UpdateDeployment(depID, "ready", imageRef, "")
	_, err = d.NotifyDeploymentReady(context.Background(), "main", sha, litestreamSHA, imageRef, "github_webhook_build")
	return err
}

func (d *Deployer) NotifyDeploymentReady(ctx context.Context, source, sha, litestreamSHA, imageRef, trigger string) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		source = "main"
	}
	sha = strings.TrimSpace(sha)
	if !validSHARe.MatchString(sha) {
		return "", fmt.Errorf("invalid deployment sha %q: must be 7-40 hex characters", sha)
	}
	litestreamSHA = strings.TrimSpace(litestreamSHA)
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
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ImageRef:      imageRef,
		Source:        source,
		PRNumber:      sourcePRNumber(source),
		Status:        "ready",
	}); err != nil {
		return "", fmt.Errorf("record ready deployment: %w", err)
	}

	message := fmt.Sprintf("Image ready for soak %s / litestream %s via %s", trimSHA(sha), shortVersionValue(litestreamSHA), trigger)
	if err := d.db.RecordEvent("", "deploy_ready_received", message, imageRef); err != nil {
		return "", fmt.Errorf("record deploy event: %w", err)
	}

	slog.Info("Deployment ready, starting rolling update", "sha", sha, "litestream_sha", litestreamSHA, "image", imageRef, "trigger", trigger)
	if err := d.manager.EnsureSourceFleet(ctx, source, sha, litestreamSHA, imageRef); err != nil {
		return "", err
	}
	if err := d.manager.RollingUpdateSource(ctx, source, imageRef, sha, litestreamSHA); err != nil {
		return "", err
	}
	if err := d.manager.ResumeDormantWorkers(ctx, source, imageRef, sha, litestreamSHA, trigger); err != nil {
		return "", err
	}

	return imageRef, nil
}

func (d *Deployer) buildImage(sha string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	imageTag := fmt.Sprintf("registry.fly.io/%s:sha-%s", d.appName, trimSHA(sha))

	args := []string{
		"deploy",
		"--app", d.appName,
		"--image-label", fmt.Sprintf("sha-%s", trimSHA(sha)),
		"--build-only",
		"--push",
	}
	if litestreamSHA, err := resolveLitestreamBuildSHA(ctx, strings.TrimSpace(os.Getenv("LITESTREAM_SHA"))); err == nil && litestreamSHA != "" {
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

func resolveLitestreamBuildSHA(ctx context.Context, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		ref = "main"
	}
	if len(ref) == 40 && !strings.ContainsAny(ref, "/ \t\n") {
		return ref, nil
	}

	pattern := ref
	if ref == "main" {
		pattern = "refs/heads/main"
	}

	cmd := exec.CommandContext(ctx, "git", "ls-remote", "https://github.com/benbjohnson/litestream.git", pattern)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git ls-remote %s: %w", pattern, err)
	}

	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		return "", fmt.Errorf("no upstream Litestream ref matched %q", ref)
	}

	return fields[0], nil
}
