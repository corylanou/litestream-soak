package orchestrator

import (
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestDeploymentSourceLabelAndURL(t *testing.T) {
	t.Parallel()

	prDeployment := model.Deployment{Source: "pr-1228", PRNumber: 1228}
	if got := deploymentSourceLabel(prDeployment); got != "PR #1228" {
		t.Fatalf("deploymentSourceLabel(pr) = %q, want %q", got, "PR #1228")
	}
	if got := deploymentSourceURL(prDeployment); got != "https://github.com/benbjohnson/litestream/pull/1228" {
		t.Fatalf("deploymentSourceURL(pr) = %q", got)
	}

	mainDeployment := model.Deployment{Source: "main"}
	if got := deploymentSourceLabel(mainDeployment); got != "main" {
		t.Fatalf("deploymentSourceLabel(main) = %q, want %q", got, "main")
	}
	if got := deploymentSourceURL(mainDeployment); got != "https://github.com/benbjohnson/litestream/tree/main" {
		t.Fatalf("deploymentSourceURL(main) = %q", got)
	}
}

func TestCommitURLs(t *testing.T) {
	t.Parallel()

	if got := soakCommitURL("abc123"); got != "https://github.com/corylanou/litestream-soak/commit/abc123" {
		t.Fatalf("soakCommitURL() = %q", got)
	}
	if got := litestreamCommitURL("def456"); got != "https://github.com/benbjohnson/litestream/commit/def456" {
		t.Fatalf("litestreamCommitURL() = %q", got)
	}
}

func TestDeploymentSourceSummaryAndCopyText(t *testing.T) {
	t.Parallel()

	deployment := model.Deployment{
		Source:        "pr-1228",
		PRNumber:      1228,
		GitSHA:        "abc123",
		LitestreamSHA: "def456",
	}
	if got := deploymentSourceSummary(deployment); got != "PR #1228 from benbjohnson/litestream" {
		t.Fatalf("deploymentSourceSummary() = %q", got)
	}

	copyText := deploymentCopyText(deployment)
	for _, want := range []string{
		"source=pr-1228",
		"source_label=PR #1228",
		"pr_url=https://github.com/benbjohnson/litestream/pull/1228",
		"soak_sha=abc123",
		"litestream_sha=def456",
	} {
		if !strings.Contains(copyText, want) {
			t.Fatalf("deploymentCopyText() missing %q in %q", want, copyText)
		}
	}
}

func TestComparisonCopyText(t *testing.T) {
	t.Parallel()

	comparison := &DeploymentComparisonResponse{
		Verdict:       "better",
		Summary:       "candidate looks better",
		BaseSource:    "main",
		HeadSource:    "pr-1228",
		PassDelta:     5,
		FailDelta:     -4,
		AwaitingDelta: -1,
		Base: &DeploymentScorecard{
			Deployment: model.Deployment{Source: "main", GitSHA: "base-soak", LitestreamSHA: "base-ls"},
		},
		Head: DeploymentScorecard{
			Deployment: model.Deployment{Source: "pr-1228", PRNumber: 1228, GitSHA: "head-soak", LitestreamSHA: "head-ls"},
		},
	}

	copyText := comparisonCopyText(comparison)
	for _, want := range []string{
		"verdict=better",
		"summary=candidate looks better",
		"base_source=main",
		"head_source=pr-1228",
		"pass_delta=5",
		"fail_delta=-4",
		"awaiting_delta=-1",
		"source_label=PR #1228",
		"litestream_sha=head-ls",
	} {
		if !strings.Contains(copyText, want) {
			t.Fatalf("comparisonCopyText() missing %q in %q", want, copyText)
		}
	}
}
