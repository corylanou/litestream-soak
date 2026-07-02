package orchestrator

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func renderHomeBody(t *testing.T, data homePageData) string {
	t.Helper()

	var body bytes.Buffer
	if err := uiTemplates.ExecuteTemplate(&body, "home_body", data); err != nil {
		t.Fatalf("ExecuteTemplate(home_body) error = %v", err)
	}
	return body.String()
}

func detailsTagWithID(t *testing.T, rendered, id string) string {
	t.Helper()

	idAttr := `id="` + id + `"`
	idx := strings.Index(rendered, idAttr)
	if idx < 0 {
		t.Fatalf("rendered home body missing %s", idAttr)
	}
	start := strings.LastIndex(rendered[:idx], "<details")
	if start < 0 {
		t.Fatalf("no <details tag before %s", idAttr)
	}
	end := strings.Index(rendered[start:], ">")
	if end < 0 {
		t.Fatalf("unterminated <details tag before %s", idAttr)
	}
	return rendered[start : start+end+1]
}

func TestHomeBodyAnchorIDs(t *testing.T) {
	t.Parallel()

	rendered := renderHomeBody(t, homePageData{})
	for _, id := range []string{"overview", "fleet", "failures", "rollout", "comparison", "diagnosis", "guide"} {
		if !strings.Contains(rendered, `id="`+id+`"`) {
			t.Errorf("rendered home body missing anchor id=%q", id)
		}
	}
}

func TestHomeBodyDiagnosisDetailsAutoOpen(t *testing.T) {
	t.Parallel()

	clusters := []diagnosisCluster{{
		Headline:             "checksum mismatch cluster",
		Summary:              "2 workers share a checksum failure",
		WorkerCount:          2,
		RepresentativeWorker: diagnosisWorkerRef{ID: "w-1", Name: "w-1"},
	}}

	cases := []struct {
		name     string
		data     homePageData
		wantOpen bool
	}{
		{
			name: "clusters with recent failures auto-open",
			data: homePageData{
				Diagnosis: diagnosisSnapshot{Clusters: clusters},
				Summary:   homeSummary{RecentFailures: 3},
			},
			wantOpen: true,
		},
		{
			name: "no clusters stays closed",
			data: homePageData{
				Summary: homeSummary{RecentFailures: 3},
			},
			wantOpen: false,
		},
		{
			name: "clusters without recent failures stays closed",
			data: homePageData{
				Diagnosis: diagnosisSnapshot{Clusters: clusters},
			},
			wantOpen: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tag := detailsTagWithID(t, renderHomeBody(t, tc.data), "diagnosis")
			gotOpen := strings.Contains(tag, " open")
			if gotOpen != tc.wantOpen {
				t.Fatalf("diagnosis details tag = %q, open = %v, want %v", tag, gotOpen, tc.wantOpen)
			}
		})
	}
}

func TestRolloutDegradedBadgeLinksFirstDegradedWorker(t *testing.T) {
	t.Parallel()

	rollout := &DeploymentRolloutResponse{
		DegradedWorkers: 1,
		Workers: []DeploymentWorkerProgress{
			{WorkerID: "w-ok", Name: "w-ok", Status: model.WorkerRunning},
			{WorkerID: "w-degraded", Name: "w-degraded", Status: model.WorkerDegraded},
		},
	}

	rendered := renderHomeBody(t, homePageData{LatestDeployment: rollout})
	if !strings.Contains(rendered, `<a class="badge badge-bad badge-link" href="/ui/workers/w-degraded">1 degraded</a>`) {
		t.Errorf("rollout degraded badge does not link to the first degraded worker")
	}

	rollout.Workers = nil
	rendered = renderHomeBody(t, homePageData{LatestDeployment: rollout})
	if !strings.Contains(rendered, `<span class="badge badge-bad">1 degraded</span>`) {
		t.Errorf("rollout degraded badge without worker detail should stay a plain badge")
	}
}

func TestHomeTemplateContainsSectionNav(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	if err := uiTemplates.ExecuteTemplate(&buf, "home", homePageData{}); err != nil {
		t.Fatalf("ExecuteTemplate(home) error = %v", err)
	}
	rendered := buf.String()
	if !strings.Contains(rendered, "section-nav") {
		t.Errorf("rendered home template missing section-nav")
	}
	if !strings.Contains(rendered, `href="#fleet"`) {
		t.Errorf("rendered home template missing #fleet nav link")
	}
	navIdx := strings.Index(rendered, "section-nav")
	rootIdx := strings.Index(rendered, `id="home-live-root"`)
	if navIdx < 0 || rootIdx < 0 || navIdx > rootIdx {
		t.Errorf("section-nav must appear before home-live-root (nav at %d, root at %d)", navIdx, rootIdx)
	}
}

func TestBuildAttentionItemsAttentionWorkersLinkFirstWorker(t *testing.T) {
	t.Parallel()

	freshAt := time.Now().UTC().Add(-10 * time.Second)
	workers := []homeWorker{
		{Worker: model.Worker{ID: "w-degraded", Name: "w-degraded", Status: model.WorkerDegraded, LastHeartbeatAt: &freshAt}},
		{Worker: model.Worker{ID: "w-fresh", Name: "w-fresh", Status: model.WorkerRunning, LastHeartbeatAt: &freshAt}},
	}
	summary := homeSummary{TotalWorkers: 2, HealthyWorkers: 1, AttentionWorkers: 1}

	items := buildAttentionItems("main", diagnosisSnapshot{}, summary, workers, nil, nil, "", "", failureClassificationContext{})

	item := findAttentionItem(t, items, "need attention")
	action := findWorkerAction(t, item)
	if action.URL != "/ui/workers/w-degraded" {
		t.Fatalf("attention-workers action URL = %q, want /ui/workers/w-degraded", action.URL)
	}
	if action.Label != "Open w-degraded" {
		t.Fatalf("attention-workers action label = %q, want %q", action.Label, "Open w-degraded")
	}
}

func TestBuildAttentionItemsStaleHeartbeatLinksFirstWorker(t *testing.T) {
	t.Parallel()

	staleAt := time.Now().UTC().Add(-30 * time.Minute)
	freshAt := time.Now().UTC().Add(-10 * time.Second)
	workers := []homeWorker{
		{Worker: model.Worker{ID: "w-stale", Name: "w-stale", Status: model.WorkerRunning, LastHeartbeatAt: &staleAt}},
		{Worker: model.Worker{ID: "w-fresh", Name: "w-fresh", Status: model.WorkerRunning, LastHeartbeatAt: &freshAt}},
	}

	items := buildAttentionItems("main", diagnosisSnapshot{}, homeSummary{TotalWorkers: 2, HealthyWorkers: 2}, workers, nil, nil, "", "", failureClassificationContext{})

	item := findAttentionItem(t, items, "stale heartbeat")
	action := findWorkerAction(t, item)
	if action.URL != "/ui/workers/w-stale" {
		t.Fatalf("stale-heartbeat action URL = %q, want /ui/workers/w-stale", action.URL)
	}
	if action.Label != "Open w-stale" {
		t.Fatalf("stale-heartbeat action label = %q, want %q", action.Label, "Open w-stale")
	}
}

func findAttentionItem(t *testing.T, items []attentionItem, titleSubstr string) attentionItem {
	t.Helper()

	for _, item := range items {
		if strings.Contains(item.Title, titleSubstr) {
			return item
		}
	}
	t.Fatalf("no attention item with title containing %q in %+v", titleSubstr, items)
	return attentionItem{}
}

func findWorkerAction(t *testing.T, item attentionItem) attentionAction {
	t.Helper()

	for _, action := range item.Actions {
		if strings.Contains(action.URL, "/ui/workers/") {
			return action
		}
	}
	t.Fatalf("attention item %q has no /ui/workers/ action: %+v", item.Title, item.Actions)
	return attentionAction{}
}
