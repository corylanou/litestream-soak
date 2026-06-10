package orchestrator

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func formatUITime(value time.Time) string {
	return value.Local().Format("2006-01-02 15:04:05 MST")
}

func formatUITimePtr(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "never"
	}
	return formatUITime(*value)
}

func formatTimeAgo(value time.Time) string {
	if value.IsZero() {
		return "never"
	}

	delta := time.Since(value)
	if delta < 0 {
		delta = -delta
	}

	switch {
	case delta < time.Minute:
		return fmt.Sprintf("%ds ago", int(delta.Seconds()))
	case delta < time.Hour:
		return fmt.Sprintf("%dm ago", int(delta.Minutes()))
	case delta < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(delta.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(delta.Hours()/24))
	}
}

func formatTimeAgoPtr(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "never"
	}
	return formatTimeAgo(*value)
}

func maxTime(current *time.Time, candidate time.Time) *time.Time {
	candidate = candidate.UTC()
	if current == nil || candidate.After(current.UTC()) {
		return &candidate
	}
	return current
}

func minTime(current *time.Time, candidate time.Time) *time.Time {
	candidate = candidate.UTC()
	if current == nil || candidate.Before(current.UTC()) {
		return &candidate
	}
	return current
}

func trimSHA(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

const (
	soakGitHubRepo       = "https://github.com/corylanou/litestream-soak"
	litestreamGitHubRepo = "https://github.com/benbjohnson/litestream"
)

func sourceLabel(source string) string {
	prNumber := sourcePRNumber(strings.TrimSpace(source))
	if prNumber > 0 {
		return fmt.Sprintf("PR #%d", prNumber)
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return "main"
	}
	return source
}

func sourceURL(source string) string {
	prNumber := sourcePRNumber(strings.TrimSpace(source))
	if prNumber > 0 {
		return fmt.Sprintf("%s/pull/%d", litestreamGitHubRepo, prNumber)
	}
	source = firstNonEmpty(strings.TrimSpace(source), "main")
	return fmt.Sprintf("%s/tree/%s", litestreamGitHubRepo, url.PathEscape(source))
}

func deploymentSourceLabel(dep model.Deployment) string {
	if dep.PRNumber > 0 {
		return fmt.Sprintf("PR #%d", dep.PRNumber)
	}
	return sourceLabel(dep.Source)
}

func deploymentSourceURL(dep model.Deployment) string {
	if dep.PRNumber > 0 {
		return fmt.Sprintf("%s/pull/%d", litestreamGitHubRepo, dep.PRNumber)
	}
	return sourceURL(dep.Source)
}

func deploymentSourceSummary(dep model.Deployment) string {
	if dep.PRNumber > 0 {
		return fmt.Sprintf("%s from benbjohnson/litestream", deploymentSourceLabel(dep))
	}
	return fmt.Sprintf("%s branch from benbjohnson/litestream", deploymentSourceLabel(dep))
}

func soakCommitURL(sha string) string {
	sha = strings.TrimSpace(sha)
	if sha == "" {
		return ""
	}
	return fmt.Sprintf("%s/commit/%s", soakGitHubRepo, sha)
}

func litestreamCommitURL(sha string) string {
	sha = strings.TrimSpace(sha)
	if sha == "" {
		return ""
	}
	return fmt.Sprintf("%s/commit/%s", litestreamGitHubRepo, sha)
}

func copyLabel(text string) string {
	if sourcePRNumber(strings.TrimSpace(text)) > 0 {
		return "PR"
	}
	if strings.TrimSpace(text) == "" {
		return "Source"
	}
	return strings.TrimSpace(text)
}

func deploymentCopyText(dep model.Deployment) string {
	parts := []string{
		fmt.Sprintf("source=%s", firstNonEmpty(strings.TrimSpace(dep.Source), "main")),
		fmt.Sprintf("source_label=%s", deploymentSourceLabel(dep)),
	}
	if dep.PRNumber > 0 {
		parts = append(parts, fmt.Sprintf("pr_url=%s", deploymentSourceURL(dep)))
	} else {
		parts = append(parts, fmt.Sprintf("branch_url=%s", deploymentSourceURL(dep)))
	}
	if sha := strings.TrimSpace(dep.GitSHA); sha != "" {
		parts = append(parts,
			fmt.Sprintf("soak_sha=%s", sha),
			fmt.Sprintf("soak_commit_url=%s", soakCommitURL(sha)),
		)
	}
	if sha := strings.TrimSpace(dep.LitestreamSHA); sha != "" {
		parts = append(parts,
			fmt.Sprintf("litestream_sha=%s", sha),
			fmt.Sprintf("litestream_commit_url=%s", litestreamCommitURL(sha)),
		)
	}
	return strings.Join(parts, "\n")
}

func comparisonCopyText(comparison *DeploymentComparisonResponse) string {
	if comparison == nil {
		return ""
	}
	parts := []string{
		fmt.Sprintf("verdict=%s", comparison.Verdict),
		fmt.Sprintf("summary=%s", comparison.Summary),
		fmt.Sprintf("base_source=%s", comparison.BaseSource),
		fmt.Sprintf("head_source=%s", comparison.HeadSource),
		fmt.Sprintf("pass_delta=%d", comparison.PassDelta),
		fmt.Sprintf("fail_delta=%d", comparison.FailDelta),
		fmt.Sprintf("awaiting_delta=%d", comparison.AwaitingDelta),
	}
	if comparison.Base != nil {
		parts = append(parts, deploymentCopyText(comparison.Base.Deployment))
	}
	parts = append(parts, deploymentCopyText(comparison.Head.Deployment))
	return strings.Join(parts, "\n")
}

func comparisonTitle(comparison *DeploymentComparisonResponse) string {
	if comparison == nil {
		return "Release Comparison"
	}
	if comparison.ComparisonKind == "cross_source" {
		return "Source Comparison"
	}
	return "Release Comparison"
}

func comparisonModeSummary(comparison *DeploymentComparisonResponse) string {
	if comparison == nil {
		return ""
	}
	if comparison.ComparisonKind == "cross_source" {
		return fmt.Sprintf("Compare mode: %s versus %s.", sourceHumanLabel(comparison.HeadSource), sourceHumanLabel(comparison.BaseSource))
	}
	return fmt.Sprintf("History mode: current %s rollout versus the previous %s rollout.", sourceHumanLabel(comparison.HeadSource), sourceHumanLabel(comparison.BaseSource))
}

func comparisonBaseLabel(comparison *DeploymentComparisonResponse) string {
	if comparison != nil && comparison.ComparisonKind == "source_history" {
		return fmt.Sprintf("Previous %s rollout", sourceHumanLabel(comparison.BaseSource))
	}
	return "Baseline"
}

func comparisonHeadLabel(comparison *DeploymentComparisonResponse) string {
	if comparison != nil && comparison.ComparisonKind == "source_history" {
		return fmt.Sprintf("Current %s rollout", sourceHumanLabel(comparison.HeadSource))
	}
	return "Candidate"
}

func shortenText(value string, limit int) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) <= limit {
		return value
	}
	return strings.TrimSpace(value[:limit-1]) + "..."
}

func statusClass(value any) string {
	switch strings.ToLower(fmt.Sprint(value)) {
	case "running":
		return "status-good"
	case "degraded", "dormant", "probing", "starting", "building", "pending":
		return "status-warn"
	case "failed", "stopped":
		return "status-bad"
	default:
		return "status-neutral"
	}
}

func confidenceClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "high":
		return "status-good"
	case "medium":
		return "status-warn"
	default:
		return "status-neutral"
	}
}

func deploymentStatusClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "stable":
		return "status-good"
	case "rolling_out", "probing":
		return "status-warn"
	case "needs_attention":
		return "status-bad"
	default:
		return "status-neutral"
	}
}

func heartbeatClass(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "status-neutral"
	}
	age := time.Since(*value)
	switch {
	case age <= 45*time.Second:
		return "status-good"
	case age <= 2*time.Minute:
		return "status-warn"
	default:
		return "status-bad"
	}
}

func runtimeSnapshotClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case reporting.RuntimeSnapshotStatusHealthy:
		return "status-good"
	case reporting.RuntimeSnapshotStatusLegacy:
		return "status-warn"
	case reporting.RuntimeSnapshotStatusUnhealthy:
		return "status-bad"
	default:
		return "status-neutral"
	}
}

func runtimeSnapshotLabel(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case reporting.RuntimeSnapshotStatusHealthy:
		return "snapshot ok"
	case reporting.RuntimeSnapshotStatusLegacy:
		return "legacy telemetry"
	case reporting.RuntimeSnapshotStatusUnhealthy:
		return "snapshot unhealthy"
	default:
		return "snapshot missing"
	}
}

func eventClass(value string) string {
	switch {
	case strings.HasPrefix(strings.TrimSpace(value), "platform_oom"), strings.HasPrefix(strings.TrimSpace(value), "platform_disk_full"), strings.HasPrefix(strings.TrimSpace(value), "platform_killed"):
		return "status-bad"
	case strings.HasPrefix(strings.TrimSpace(value), "platform_restart"):
		return "status-warn"
	case strings.Contains(value, "failed"):
		return "status-bad"
	case strings.Contains(value, "passed"):
		return "status-good"
	default:
		return "status-neutral"
	}
}

func verificationClass(value any) string {
	verification := coerceVerification(value)
	if verification == nil {
		return "status-neutral"
	}
	if verification.Aborted() {
		return "status-neutral"
	}
	if verification.Succeeded() {
		return "status-good"
	}
	return "status-bad"
}

func verificationLabel(value any) string {
	verification := coerceVerification(value)
	if verification == nil {
		return "no data"
	}
	if verification.Aborted() {
		return "aborted"
	}
	if verification.Succeeded() {
		return "pass"
	}
	return "fail"
}

func failureText(value any) string {
	verification := coerceVerification(value)
	if verification == nil {
		return "verification failed"
	}
	if strings.TrimSpace(verification.ErrorMessage) != "" {
		return verification.ErrorMessage
	}
	if strings.TrimSpace(verification.Status) != "" {
		return verification.Status
	}
	return "verification failed"
}

func workerName(worker *model.Worker, workerID string) string {
	if worker == nil {
		return workerID
	}
	if worker.Name != "" {
		return worker.Name
	}
	return worker.ID
}

func formatDurationMS(ms int) string {
	if ms <= 0 {
		return "n/a"
	}

	d := time.Duration(ms) * time.Millisecond
	if d < time.Second {
		return d.String()
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.Round(time.Second).String()
}

func coerceVerification(value any) *model.Verification {
	switch verification := value.(type) {
	case model.Verification:
		return &verification
	case *model.Verification:
		return verification
	default:
		return nil
	}
}

func workerRank(status model.WorkerStatus) int {
	switch status {
	case model.WorkerFailed:
		return 0
	case model.WorkerDegraded, model.WorkerProbing:
		return 1
	case model.WorkerDormant:
		return 2
	case model.WorkerStarting, model.WorkerBuilding, model.WorkerPending:
		return 3
	case model.WorkerRunning:
		return 4
	default:
		return 5
	}
}

func homeWorkerRank(worker homeWorker) int {
	if worker.Worker.Status == model.WorkerRunning && runtimeSnapshotNeedsAttention(worker.RuntimeSnapshotStatus) {
		return workerRank(model.WorkerDegraded)
	}
	return workerRank(worker.Worker.Status)
}

func heartbeatUnix(value *time.Time) int64 {
	if value == nil {
		return 0
	}
	return value.Unix()
}
