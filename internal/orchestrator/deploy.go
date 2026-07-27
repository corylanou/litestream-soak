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
var versionedWorkerImageRe = regexp.MustCompile(`(^|:)sha-([0-9a-fA-F]{7,40})(-pr-([1-9][0-9]*))?-ls-([0-9a-fA-F]{7,40})(@sha256:[0-9a-fA-F]{32,})?$`)

const deploymentBuildTimeout = 10 * time.Minute

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
	if litestreamSHA == "" {
		return "", fmt.Errorf("litestream sha is required")
	}
	trigger = strings.TrimSpace(trigger)
	if trigger == "" {
		trigger = "deploy_ready"
	}
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return "", fmt.Errorf("deployment image ref is required")
	}
	target := model.Deployment{
		GitSHA:        sha,
		LitestreamSHA: litestreamSHA,
		ImageRef:      imageRef,
		Source:        source,
		PRNumber:      sourcePRNumber(source),
		Status:        "ready",
	}
	if err := validateReadyDeploymentTarget(target); err != nil {
		return "", err
	}
	if d.manager == nil {
		return "", fmt.Errorf("deployment manager unavailable")
	}

	unlockSource, err := d.manager.lockSource(ctx, source)
	if err != nil {
		return "", fmt.Errorf("lock deployment source: %w", err)
	}
	defer unlockSource()
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("start deployment ready rollout: %w", err)
	}

	if err := d.db.UpsertReadyDeployment(&target); err != nil {
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
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("start deployment rollout: %w", err)
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
	ctx, cancel := context.WithTimeout(context.Background(), deploymentBuildTimeout)
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

func validateReadyDeploymentTarget(deployment model.Deployment) error {
	source := firstNonEmpty(strings.TrimSpace(deployment.Source), "main")
	if !strings.EqualFold(strings.TrimSpace(deployment.Status), "ready") {
		return fmt.Errorf("%s deployment is not ready", source)
	}

	gitSHA := strings.TrimSpace(deployment.GitSHA)
	if gitSHA == "" {
		return fmt.Errorf("%s ready deployment has no git sha", source)
	}
	litestreamSHA := strings.TrimSpace(deployment.LitestreamSHA)
	if litestreamSHA == "" {
		return fmt.Errorf("%s ready deployment has no litestream sha", source)
	}
	imageRef := strings.TrimSpace(deployment.ImageRef)
	if err := validateDeploymentImageRef(imageRef); err != nil {
		return err
	}

	match := versionedWorkerImageRe.FindStringSubmatch(imageRef)
	if match == nil {
		return nil
	}
	if !strings.HasPrefix(strings.ToLower(gitSHA), strings.ToLower(match[2])) {
		return fmt.Errorf("deployment image git sha %s does not match recorded git sha %s", match[2], gitSHA)
	}
	if !strings.HasPrefix(strings.ToLower(litestreamSHA), strings.ToLower(match[5])) {
		return fmt.Errorf("deployment image litestream sha %s does not match recorded litestream sha %s", match[5], litestreamSHA)
	}

	imageSource := "main"
	if match[4] != "" {
		imageSource = "pr-" + match[4]
	}
	if source != imageSource {
		return fmt.Errorf("deployment image source %s does not match requested source %s", imageSource, source)
	}
	return nil
}

func resolveReadyDeploymentTarget(db *model.DB, source, imageRef, gitSHA, litestreamSHA string) (*model.Deployment, error) {
	source = firstNonEmpty(strings.TrimSpace(source), "main")
	imageRef = strings.TrimSpace(imageRef)
	gitSHA = strings.TrimSpace(gitSHA)
	litestreamSHA = strings.TrimSpace(litestreamSHA)
	if (gitSHA == "") != (litestreamSHA == "") {
		return nil, fmt.Errorf("git sha and litestream sha must be provided together")
	}

	var (
		deployment *model.Deployment
		err        error
	)
	if gitSHA == "" {
		deployment, err = db.GetLatestReadyDeployment(source)
	} else {
		deployment, err = db.GetDeploymentByVersion(source, gitSHA, litestreamSHA)
	}
	if err != nil {
		return nil, fmt.Errorf("get ready deployment for %s: %w", source, err)
	}
	if deployment == nil {
		return nil, fmt.Errorf("no ready deployment exists for %s", source)
	}
	if err := validateReadyDeploymentTarget(*deployment); err != nil {
		return nil, err
	}
	if imageRef != "" && imageRef != strings.TrimSpace(deployment.ImageRef) {
		return nil, fmt.Errorf("image override %s does not match ready %s deployment image %s", imageRef, source, deployment.ImageRef)
	}
	return deployment, nil
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
