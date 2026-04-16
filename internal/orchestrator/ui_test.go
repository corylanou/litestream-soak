package orchestrator

import (
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
