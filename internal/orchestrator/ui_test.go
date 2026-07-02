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

func TestComparisonPresentationHelpers(t *testing.T) {
	t.Parallel()

	history := &DeploymentComparisonResponse{
		BaseSource:     "main",
		HeadSource:     "main",
		ComparisonKind: "source_history",
	}
	if got := comparisonTitle(history); got != "Release Comparison" {
		t.Fatalf("comparisonTitle(history) = %q", got)
	}
	if got := comparisonModeSummary(history); got != "History mode: current main branch rollout versus the previous main branch rollout." {
		t.Fatalf("comparisonModeSummary(history) = %q", got)
	}
	if got := comparisonBaseLabel(history); got != "Previous main branch rollout" {
		t.Fatalf("comparisonBaseLabel(history) = %q", got)
	}
	if got := comparisonHeadLabel(history); got != "Current main branch rollout" {
		t.Fatalf("comparisonHeadLabel(history) = %q", got)
	}

	crossSource := &DeploymentComparisonResponse{
		BaseSource:     "main",
		HeadSource:     "pr-1228",
		ComparisonKind: "cross_source",
	}
	if got := comparisonTitle(crossSource); got != "Source Comparison" {
		t.Fatalf("comparisonTitle(crossSource) = %q", got)
	}
	if got := comparisonModeSummary(crossSource); got != "Compare mode: PR #1228 versus main branch." {
		t.Fatalf("comparisonModeSummary(crossSource) = %q", got)
	}
	if got := comparisonBaseLabel(crossSource); got != "Baseline" {
		t.Fatalf("comparisonBaseLabel(crossSource) = %q", got)
	}
	if got := comparisonHeadLabel(crossSource); got != "Candidate" {
		t.Fatalf("comparisonHeadLabel(crossSource) = %q", got)
	}
}

func TestBuildHomeScopeSummary(t *testing.T) {
	t.Parallel()

	history := &DeploymentComparisonResponse{
		BaseSource:     "main",
		HeadSource:     "main",
		ComparisonKind: "source_history",
	}
	if got := buildHomeScopeSummary("main", "main", history); got != "You are viewing main branch. Latest Rollout below is the latest main branch rollout. Release Comparison shows the current main branch rollout versus the previous main branch rollout." {
		t.Fatalf("buildHomeScopeSummary(history) = %q", got)
	}

	crossSource := &DeploymentComparisonResponse{
		BaseSource:     "main",
		HeadSource:     "pr-1228",
		ComparisonKind: "cross_source",
	}
	if got := buildHomeScopeSummary("pr-1228", "pr-1228", crossSource); got != "You are viewing PR #1228. Latest Rollout below is the latest PR #1228 rollout. Source Comparison is comparing PR #1228 against main branch." {
		t.Fatalf("buildHomeScopeSummary(crossSource) = %q", got)
	}
}

func TestSortHomeSourceCardsStableOrder(t *testing.T) {
	t.Parallel()

	cards := []homeSourceCard{
		{Source: "pr-1306", Label: "PR #1306"},
		{Source: "experimental", Label: "experimental branch"},
		{Source: "main", Label: "main branch"},
		{Source: "pr-1302", Label: "PR #1302", Selected: true},
		{Source: "pr-99", Label: "PR #99"},
	}

	sortHomeSourceCards(cards)

	got := make([]string, len(cards))
	for i, card := range cards {
		got[i] = card.Source
	}
	want := []string{"main", "pr-99", "pr-1302", "pr-1306", "experimental"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v (main first, PRs numeric ascending, others last; selection must not affect order)", got, want)
		}
	}
}

func TestActiveWorkerNeedsAttentionTreatsStoppedAsNeutral(t *testing.T) {
	t.Parallel()

	if activeWorkerNeedsAttention(model.WorkerStopped, "") {
		t.Fatal("stopped worker should never need attention")
	}
	if activeWorkerNeedsAttention(model.WorkerStopped, "unhealthy") {
		t.Fatal("stopped worker with stale unhealthy runtime should never need attention")
	}
	if !activeWorkerNeedsAttention(model.WorkerDormant, "") {
		t.Fatal("dormant worker should need attention")
	}
	if !activeWorkerNeedsAttention(model.WorkerDegraded, "") {
		t.Fatal("degraded worker should need attention")
	}
	if activeWorkerNeedsAttention(model.WorkerRunning, "") {
		t.Fatal("healthy running worker should not need attention")
	}
}

func TestAssembleHomeSourceCardsRetiresAllStoppedSources(t *testing.T) {
	t.Parallel()

	workers := []model.Worker{
		{ID: "w1", Name: "worker-main-low-vol", Source: "main", Status: model.WorkerRunning},
		{ID: "w2", Name: "worker-main-old", Source: "main", Status: model.WorkerStopped},
		{ID: "w3", Name: "worker-pr-1305-a", Source: "pr-1305", Status: model.WorkerStopped},
		{ID: "w4", Name: "worker-pr-1305-b", Source: "pr-1305", Status: model.WorkerStopped},
		{ID: "w5", Name: "worker-pr-77-a", Source: "pr-77", Status: model.WorkerStopped},
		{ID: "w6", Name: "worker-pr-1322-a", Source: "pr-1322", Status: model.WorkerRunning},
		{ID: "w7", Name: "worker-pr-1322-b", Source: "pr-1322", Status: model.WorkerDormant},
	}
	archives := map[string]model.RunArchive{"pr-77": {}}

	cards := assembleHomeSourceCards("main", workers, archives)

	bySource := map[string]homeSourceCard{}
	for _, card := range cards {
		bySource[card.Source] = card
	}

	if _, ok := bySource["pr-1305"]; ok {
		t.Fatal("all-stopped source without success archive should have no tab")
	}
	if _, ok := bySource["pr-77"]; ok {
		t.Fatal("passed (all-stopped) source should have no tab either — the bar is active-only")
	}
	mainCard := bySource["main"]
	if mainCard.Total != 1 {
		t.Fatalf("main Total = %d, want 1 (stopped workers excluded)", mainCard.Total)
	}
	if mainCard.Attention != 0 {
		t.Fatalf("main Attention = %d, want 0", mainCard.Attention)
	}
	pr1322 := bySource["pr-1322"]
	if pr1322.Total != 2 || pr1322.Attention != 1 {
		t.Fatalf("pr-1322 counts = %d total / %d attention, want 2/1", pr1322.Total, pr1322.Attention)
	}
}

func TestAssembleHomeSourceCardsKeepsSelectedRetiredSourceVisible(t *testing.T) {
	t.Parallel()

	workers := []model.Worker{
		{ID: "w1", Name: "worker-main-low-vol", Source: "main", Status: model.WorkerRunning},
		{ID: "w2", Name: "worker-pr-1324-a", Source: "pr-1324", Status: model.WorkerStopped},
	}

	cards := assembleHomeSourceCards("pr-1324", workers, nil)

	var retired *homeSourceCard
	for i := range cards {
		if cards[i].Source == "pr-1324" {
			retired = &cards[i]
		}
	}
	if retired == nil {
		t.Fatal("selected retired source should stay visible")
	}
	if !retired.Retired {
		t.Fatal("selected all-stopped source should be marked retired")
	}
	if retired.Attention != 0 {
		t.Fatalf("retired card Attention = %d, want 0", retired.Attention)
	}
	if !strings.Contains(retired.Summary, "retired") {
		t.Fatalf("retired card summary = %q, want mention of retired", retired.Summary)
	}
}

func TestStatusClassTreatsStoppedAsNeutral(t *testing.T) {
	t.Parallel()

	if got := statusClass("stopped"); got != "status-neutral" {
		t.Fatalf("statusClass(stopped) = %q, want status-neutral", got)
	}
	if got := statusClass("failed"); got != "status-bad" {
		t.Fatalf("statusClass(failed) = %q, want status-bad", got)
	}
}

func TestWorkerPromptURLUsesHealthyModeForStoppedWorkers(t *testing.T) {
	t.Parallel()

	worker := model.Worker{ID: "worker-pr-9-low", Status: model.WorkerStopped}
	if got := workerPromptURL(worker, "", ""); !strings.Contains(got, "mode=healthy") {
		t.Fatalf("workerPromptURL(stopped) = %q, want healthy mode", got)
	}
	if got := workerPromptURL(worker, "restore_decode_error", ""); !strings.Contains(got, "mode=triage") {
		t.Fatalf("workerPromptURL(stopped with failure signature) = %q, want triage mode", got)
	}
}

func TestFilterStatsToActiveWorkersDropsRetiredHistory(t *testing.T) {
	t.Parallel()

	summaries := []WorkerSummaryResponse{
		{Worker: model.Worker{ID: "w-live", Status: model.WorkerRunning}},
		{Worker: model.Worker{ID: "w-retired", Status: model.WorkerStopped}},
		{Worker: model.Worker{ID: "w-dead", Status: model.WorkerFailed}},
	}
	stats := []model.VerificationStat{
		{WorkerID: "w-live", Status: "passed", Passed: true},
		{WorkerID: "w-retired", Status: "failed"},
		{WorkerID: "w-dead", Status: "failed"},
	}

	filtered := filterStatsToActiveWorkers(stats, summaries)

	if len(filtered) != 1 {
		t.Fatalf("filtered len = %d, want 1 (only the live worker), got %+v", len(filtered), filtered)
	}
	if filtered[0].WorkerID != "w-live" {
		t.Fatalf("filtered worker = %q, want w-live", filtered[0].WorkerID)
	}
}

func TestActiveWorkerSummariesDropsRetiredRows(t *testing.T) {
	t.Parallel()

	summaries := []WorkerSummaryResponse{
		{Worker: model.Worker{ID: "w-live", Status: model.WorkerRunning}},
		{Worker: model.Worker{ID: "w-stopped", Status: model.WorkerStopped}},
		{Worker: model.Worker{ID: "w-failed", Status: model.WorkerFailed}},
		{Worker: model.Worker{ID: "w-dormant", Status: model.WorkerDormant}},
	}

	got := activeWorkerSummaries(summaries)

	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (running + dormant)", len(got))
	}
	for _, summary := range got {
		if summary.Worker.Status == model.WorkerStopped || summary.Worker.Status == model.WorkerFailed {
			t.Fatalf("retired worker %s leaked through", summary.Worker.ID)
		}
	}
}
