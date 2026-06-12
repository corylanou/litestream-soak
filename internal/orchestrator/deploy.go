package orchestrator

import (
	"context"
	"errors"
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
var validImageRefRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$`)

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

	if err := d.db.RecordEvent("", "deploy_started", fmt.Sprintf("Building image for %s", trimSHA(sha)), ""); err != nil {
		return fmt.Errorf("record deploy started event: %w", err)
	}

	imageRef, err := d.buildImage(sha)
	if err != nil {
		resultErr := err
		if updateErr := d.db.UpdateDeployment(depID, "failed", "", err.Error()); updateErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("mark deployment failed: %w", updateErr))
		}
		if eventErr := d.db.RecordEvent("", "deploy_failed", fmt.Sprintf("Build failed for %s: %v", trimSHA(sha), err), ""); eventErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("record deploy failed event: %w", eventErr))
		}
		return fmt.Errorf("build image: %w", resultErr)
	}

	if err := d.db.UpdateDeployment(depID, "ready", imageRef, ""); err != nil {
		return fmt.Errorf("mark deployment ready: %w", err)
	}
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
		if d.manager == nil {
			return "", fmt.Errorf("deployment manager unavailable")
		}
		var err error
		imageRef, err = d.manager.currentWorkerImage(ctx)
		if err != nil {
			return "", fmt.Errorf("resolve worker image: %w", err)
		}
	}
	if err := validateDeploymentImageRef(imageRef); err != nil {
		return "", err
	}
	if d.manager == nil {
		return "", fmt.Errorf("deployment manager unavailable")
	}

	unlockSource := d.manager.lockSource(source)
	defer unlockSource()

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

	current, latest, err := d.manager.latestReadyDeploymentMatches(source, imageRef, sha, litestreamSHA)
	if err != nil {
		return "", err
	}
	if !current {
		slog.Info("Deployment ready superseded, skipping rollout", "source", source, "sha", sha, "litestream_sha", litestreamSHA, "image", imageRef, "latest_sha", latest.GitSHA, "latest_litestream_sha", latest.LitestreamSHA, "latest_image", latest.ImageRef, "trigger", trigger)
		return imageRef, nil
	}

	slog.Info("Deployment ready, starting rolling update", "sha", sha, "litestream_sha", litestreamSHA, "image", imageRef, "trigger", trigger)
	if err := d.manager.EnsureSourceFleet(ctx, source, sha, litestreamSHA, imageRef); err != nil {
		return "", err
	}
	if err := d.manager.rollingUpdateSourceLocked(ctx, source, imageRef, sha, litestreamSHA); err != nil {
		return "", err
	}
	current, latest, err = d.manager.latestReadyDeploymentMatches(source, imageRef, sha, litestreamSHA)
	if err != nil {
		return "", err
	}
	if !current {
		slog.Info("Deployment ready superseded, skipping dormant resume", "source", source, "sha", sha, "litestream_sha", litestreamSHA, "image", imageRef, "latest_sha", latest.GitSHA, "latest_litestream_sha", latest.LitestreamSHA, "latest_image", latest.ImageRef, "trigger", trigger)
		return imageRef, nil
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
	if err := validateDeploymentImageRef(imageTag); err != nil {
		return "", err
	}

	slog.Info("Image built successfully", "sha", sha, "image", imageTag)
	return imageTag, nil
}

func validateDeploymentImageRef(imageRef string) error {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return fmt.Errorf("invalid deployment image ref: empty")
	}
	if !validImageRefRe.MatchString(imageRef) {
		return fmt.Errorf("invalid deployment image ref %q", imageRef)
	}
	return nil
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

	cmd := exec.CommandContext(ctx, "git", "ls-remote", "https://github.com/benbjohnson/litestream.git", pattern, pattern+"^{}")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git ls-remote %s: %w", pattern, err)
	}

	// annotated tags list both the tag object and the peeled commit (ref^{});
	// prefer the peeled line so deployments record the commit under test
	first := ""
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if first == "" {
			first = fields[0]
		}
		if strings.HasSuffix(fields[1], "^{}") {
			return fields[0], nil
		}
	}
	if first == "" {
		return "", fmt.Errorf("no upstream Litestream ref matched %q", ref)
	}

	return first, nil
}
