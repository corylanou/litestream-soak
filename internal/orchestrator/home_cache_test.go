package orchestrator

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestHomePageServesFreshSnapshot(t *testing.T) {
	api := NewAPI(openTestDB(t), nil, nil, nil, nil, nil)
	r := httptest.NewRequest("GET", "/ui", nil)
	key := homeCacheKey(r)
	snapshot := homePageData{SelectedSource: "cached"}
	api.home.entries = map[string]*homeCacheEntry{key: {data: snapshot, builtAt: time.Now()}}

	got, err := api.homePageData(r)
	if err != nil {
		t.Fatalf("homePageData() error = %v", err)
	}
	if got.SelectedSource != "cached" {
		t.Fatalf("homePageData() = %q, want cached snapshot", got.SelectedSource)
	}
}

func TestHomePageServesStaleSnapshotWhileRefreshing(t *testing.T) {
	api := NewAPI(openTestDB(t), nil, nil, nil, nil, nil)
	r := httptest.NewRequest("GET", "/ui?source=main", nil)
	key := homeCacheKey(r)
	entry := &homeCacheEntry{data: homePageData{SelectedSource: "stale"}, builtAt: time.Now().Add(-2 * homeCacheFresh)}
	api.home.entries = map[string]*homeCacheEntry{key: entry}

	got, err := api.homePageData(r)
	if err != nil {
		t.Fatalf("homePageData() error = %v", err)
	}
	if got.SelectedSource != "stale" {
		t.Fatalf("homePageData() = %q, want stale snapshot served immediately", got.SelectedSource)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		api.home.mu.Lock()
		current := api.home.entries[key]
		refreshing := current != nil && current.refreshing
		api.home.mu.Unlock()
		if !refreshing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background refresh did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHomePageBuildsSynchronouslyWithoutSnapshot(t *testing.T) {
	api := NewAPI(openTestDB(t), nil, nil, nil, nil, nil)
	got, err := api.homePageData(httptest.NewRequest("GET", "/ui", nil))
	if err != nil {
		t.Fatalf("homePageData() error = %v", err)
	}
	if got.SelectedSource != "main" {
		t.Fatalf("SelectedSource = %q, want main", got.SelectedSource)
	}
	api.home.mu.Lock()
	defer api.home.mu.Unlock()
	if len(api.home.entries) != 0 {
		t.Fatalf("fast build was cached: %d entries", len(api.home.entries))
	}
}

func TestComparisonSnapshotSurvivesRestart(t *testing.T) {
	db := openTestDB(t)
	first := NewAPI(db, nil, nil, nil, nil, nil)
	key := comparisonCacheKey{source: "main", deployments: "main=none"}
	at := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	first.storeComparisonSnapshot(key, &DeploymentComparisonResponse{HeadSource: "persisted", Verdict: "passed"}, at)

	second := NewAPI(db, nil, nil, nil, nil, nil)
	got, gotAt, ok := second.loadComparisonSnapshot(key)
	if !ok || got.HeadSource != "persisted" || got.Verdict != "passed" || !gotAt.Equal(at) {
		t.Fatalf("loadComparisonSnapshot() = %+v, %v, %v", got, gotAt, ok)
	}
	if _, _, ok := second.loadComparisonSnapshot(comparisonCacheKey{source: "main", deployments: "main=1/ready/"}); ok {
		t.Fatal("snapshot for a different deployment state was loaded")
	}
}

func TestOnlyComparisonPending(t *testing.T) {
	other := errors.New("alert read failed")
	for _, tt := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errComparisonPending, true},
		{errors.Join(errComparisonPending, nil), true},
		{errors.Join(errComparisonPending, other), false},
		{other, false},
	} {
		if got := onlyComparisonPending(tt.err); got != tt.want {
			t.Errorf("onlyComparisonPending(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestWarmDashboardCachesSlowBuildBeforeFirstRequest(t *testing.T) {
	previous := homeCacheMinCost
	homeCacheMinCost = 0
	t.Cleanup(func() { homeCacheMinCost = previous })
	api := NewAPI(openTestDB(t), nil, nil, nil, nil, nil)

	api.WarmDashboard()

	api.home.mu.Lock()
	defer api.home.mu.Unlock()
	if len(api.home.entries) != 1 {
		t.Fatalf("home cache entries = %d, want the warmed default dashboard", len(api.home.entries))
	}
}

func TestSourceComparisonMetricsSkipRetiredSources(t *testing.T) {
	db := openTestDB(t)
	for _, worker := range []model.Worker{
		{ID: "worker-pr-1-active", Name: "active", Source: "pr-1", GitSHA: "sha", ProfileName: "low-volume", ProfileConfig: "{}", Status: model.WorkerRunning},
		{ID: "worker-pr-2-retired", Name: "retired", Source: "pr-2", GitSHA: "sha", ProfileName: "low-volume", ProfileConfig: "{}", Status: model.WorkerStopped},
	} {
		createTestWorker(t, db, worker)
	}
	metrics := NewControlMetrics(db)
	var requested []string
	metrics.comparisons = func(source, baseSource, headSource string) (*DeploymentComparisonResponse, bool, error) {
		requested = append(requested, headSource)
		return nil, true, nil
	}
	if _, err := metrics.prepareSourceComparisons(db); err != nil {
		t.Fatalf("prepareSourceComparisons() error = %v", err)
	}
	if len(requested) != 1 || requested[0] != "pr-1" {
		t.Fatalf("requested comparisons = %v, want only the active pr-1 source", requested)
	}
}

func TestRecentRunIncidentsCapsDashboardList(t *testing.T) {
	incidents := make([]RunIncident, 0, 50)
	for i := 0; i < 50; i++ {
		incidents = append(incidents, RunIncident{Kind: fmt.Sprintf("incident-%d", i)})
	}
	shown := recentRunIncidents(incidents)
	if len(shown) != dashboardIncidentLimit || shown[len(shown)-1].Kind != "incident-49" || shown[0].Kind != "incident-30" {
		t.Fatalf("recentRunIncidents() = %d incidents from %q to %q", len(shown), shown[0].Kind, shown[len(shown)-1].Kind)
	}
	if few := recentRunIncidents(incidents[:3]); len(few) != 3 {
		t.Fatalf("recentRunIncidents(3) = %d, want all 3", len(few))
	}
}
