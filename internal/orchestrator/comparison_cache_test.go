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
}
