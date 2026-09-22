package orchestrator

import (
	"context"
	"testing"
	"time"
)

func TestDeploymentComparisonServesFreshCache(t *testing.T) {
	api := NewAPI(openTestDB(t), nil, nil, nil, nil, nil)
	want := &DeploymentComparisonResponse{HeadSource: "cached"}
	key := comparisonCacheKey{source: "main"}
	api.comparisons.entries = map[comparisonCacheKey]*comparisonCacheEntry{key: {cached: want, cachedAt: time.Now()}}

	got, err := api.deploymentComparison(context.Background(), "main", "", "")
	if err != nil {
		t.Fatalf("deploymentComparison() error = %v", err)
	}
	if got != want {
		t.Fatalf("deploymentComparison() = %+v, want cached %+v", got, want)
	}
}

func TestDeploymentComparisonServesStaleWhileRefreshing(t *testing.T) {
	api := NewAPI(openTestDB(t), nil, nil, nil, nil, nil)
	stale := &DeploymentComparisonResponse{HeadSource: "stale"}
	key := comparisonCacheKey{source: "main"}
	entry := &comparisonCacheEntry{cached: stale, cachedAt: time.Now().Add(-2 * comparisonCacheTTL)}
	api.comparisons.entries = map[comparisonCacheKey]*comparisonCacheEntry{key: entry}

	got, err := api.deploymentComparison(context.Background(), "main", "", "")
	if err != nil {
		t.Fatalf("deploymentComparison() error = %v", err)
	}
	if got != stale {
		t.Fatalf("deploymentComparison() = %+v, want stale %+v", got, stale)
	}

	api.comparisons.mu.Lock()
	flight := entry.flight
	api.comparisons.mu.Unlock()
	if flight == nil {
		t.Fatal("expected a background refresh to start")
	}
	<-flight.done

	api.comparisons.mu.Lock()
	defer api.comparisons.mu.Unlock()
	if entry.flight != nil {
		t.Fatal("refresh flight was not cleared")
	}
	if entry.cached != nil {
		t.Fatalf("fast refresh should drop the cache, got %+v", entry.cached)
	}
	if _, ok := api.comparisons.entries[key]; ok {
		t.Fatal("uncached entry was not evicted")
	}
}

func TestDeploymentComparisonDropsResultsPastMaxStale(t *testing.T) {
	api := NewAPI(openTestDB(t), nil, nil, nil, nil, nil)
	expired := &DeploymentComparisonResponse{HeadSource: "expired"}
	key := comparisonCacheKey{source: "main"}
	api.comparisons.entries = map[comparisonCacheKey]*comparisonCacheEntry{key: {cached: expired, cachedAt: time.Now().Add(-2 * comparisonCacheMaxStale)}}

	got, err := api.deploymentComparison(context.Background(), "main", "", "")
	if err != nil {
		t.Fatalf("deploymentComparison() error = %v", err)
	}
	if got == expired {
		t.Fatal("deploymentComparison() served a result past the max stale age")
	}
}

func TestDeploymentComparisonWithoutCacheReturnsBuiltResult(t *testing.T) {
	api := NewAPI(openTestDB(t), nil, nil, nil, nil, nil)

	got, err := api.deploymentComparison(context.Background(), "main", "", "")
	if err != nil {
		t.Fatalf("deploymentComparison() error = %v", err)
	}
	want, err := buildRequestedDeploymentComparison(api.db, "main", "", "")
	if err != nil {
		t.Fatalf("buildRequestedDeploymentComparison() error = %v", err)
	}
	if (got == nil) != (want == nil) {
		t.Fatalf("deploymentComparison() = %+v, want %+v", got, want)
	}
	api.comparisons.mu.Lock()
	defer api.comparisons.mu.Unlock()
	if len(api.comparisons.entries) != 0 {
		t.Fatalf("uncached comparisons retained %d entries", len(api.comparisons.entries))
	}
}

func TestDeploymentComparisonKeepsStaleAfterFailedRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	api := NewAPIWithContext(ctx, openTestDB(t), nil, nil, nil, nil, nil)
	stale := &DeploymentComparisonResponse{HeadSource: "stale"}
	key := comparisonCacheKey{source: "main"}
	entry := &comparisonCacheEntry{cached: stale, cachedAt: time.Now().Add(-2 * comparisonCacheTTL)}
	api.comparisons.entries = map[comparisonCacheKey]*comparisonCacheEntry{key: entry}

	if got, err := api.deploymentComparison(context.Background(), "main", "", ""); err != nil || got != stale {
		t.Fatalf("deploymentComparison() = %+v, %v; want stale", got, err)
	}
	api.comparisons.mu.Lock()
	flight := entry.flight
	api.comparisons.mu.Unlock()
	<-flight.done
	if flight.err == nil {
		t.Fatal("expected refresh to fail under a canceled context")
	}

	api.comparisons.mu.Lock()
	defer api.comparisons.mu.Unlock()
	if entry.cached != stale {
		t.Fatalf("failed refresh dropped stale result, cached = %+v", entry.cached)
	}
}

func TestCanonicalComparisonKey(t *testing.T) {
	tests := []struct {
		source, base, head string
		want               comparisonCacheKey
	}{
		{"", "", "", comparisonCacheKey{source: "main"}},
		{" pr-1 ", "", "", comparisonCacheKey{source: "pr-1"}},
		{"pr-1", "", "pr-1", comparisonCacheKey{baseSource: "main", headSource: "pr-1"}},
		{"ignored", "main", "pr-2", comparisonCacheKey{baseSource: "main", headSource: "pr-2"}},
		{"other", "main", "pr-2", comparisonCacheKey{baseSource: "main", headSource: "pr-2"}},
		{"pr-3", "pr-3", "", comparisonCacheKey{source: "pr-3"}},
	}
	for _, tt := range tests {
		if got := canonicalComparisonKey(tt.source, tt.base, tt.head); got != tt.want {
			t.Errorf("canonicalComparisonKey(%q, %q, %q) = %+v, want %+v", tt.source, tt.base, tt.head, got, tt.want)
		}
	}
}

func TestComparisonCacheEvictsExpiredEntries(t *testing.T) {
	c := &comparisonCache{entries: map[comparisonCacheKey]*comparisonCacheEntry{
		{source: "old"}:   {cached: &DeploymentComparisonResponse{}, cachedAt: time.Now().Add(-2 * comparisonCacheMaxStale)},
		{source: "fresh"}: {cached: &DeploymentComparisonResponse{}, cachedAt: time.Now()},
		{source: "busy"}:  {flight: &comparisonFlight{done: make(chan struct{})}},
	}}
	c.evictExpiredLocked()
	if _, ok := c.entries[comparisonCacheKey{source: "old"}]; ok {
		t.Fatal("expired entry was not evicted")
	}
	if len(c.entries) != 2 {
		t.Fatalf("entries = %d, want fresh and in-flight retained", len(c.entries))
	}
}
