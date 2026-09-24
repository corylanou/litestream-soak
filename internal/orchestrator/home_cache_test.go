package orchestrator

import (
	"net/http/httptest"
	"testing"
	"time"
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
