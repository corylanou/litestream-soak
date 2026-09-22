package orchestrator

import (
	"context"
	"strings"
	"sync"
	"time"
)

const (
	comparisonCacheTTL      = 2 * time.Minute
	comparisonCacheMaxStale = 15 * time.Minute
	comparisonCacheMinCost  = time.Second
	comparisonBuildTimeout  = 5 * time.Minute
)

type comparisonCache struct {
	mu         sync.Mutex
	entries    map[comparisonCacheKey]*comparisonCacheEntry
	generation uint64
}

type comparisonCacheKey struct {
	source, baseSource, headSource string
}

type comparisonCacheEntry struct {
	cached   *DeploymentComparisonResponse
	cachedAt time.Time
	flight   *comparisonFlight
}

type comparisonFlight struct {
	done       chan struct{}
	generation uint64
	value      *DeploymentComparisonResponse
	err        error
}

func (a *API) deploymentComparison(ctx context.Context, source, baseSource, headSource string) (*DeploymentComparisonResponse, error) {
	key := canonicalComparisonKey(source, baseSource, headSource)
	c := &a.comparisons

	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[comparisonCacheKey]*comparisonCacheEntry)
	}
	c.evictExpiredLocked()
	e := c.entries[key]
	if e == nil {
		e = &comparisonCacheEntry{}
		c.entries[key] = e
	}
	if e.cached != nil && time.Since(e.cachedAt) < comparisonCacheTTL {
		cached := e.cached
		c.mu.Unlock()
		return cached, nil
	}
	f := e.flight
	if f == nil {
		f = &comparisonFlight{done: make(chan struct{}), generation: c.generation}
		e.flight = f
		go a.runComparisonFlight(e, f, key)
	}
	if e.cached != nil {
		stale := e.cached
		c.mu.Unlock()
		return stale, nil
	}
	c.mu.Unlock()

	select {
	case <-f.done:
		return f.value, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *API) runComparisonFlight(e *comparisonCacheEntry, f *comparisonFlight, key comparisonCacheKey) {
	parent := a.backgroundContext
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, comparisonBuildTimeout)
	defer cancel()
	start := time.Now()
	value, err := buildRequestedDeploymentComparison(a.db.WithReadContext(ctx), key.source, key.baseSource, key.headSource)

	a.comparisons.mu.Lock()
	e.flight = nil
	switch {
	case err != nil:
	case f.generation != a.comparisons.generation:
		e.cached = nil
	case value != nil && time.Since(start) >= comparisonCacheMinCost:
		e.cached, e.cachedAt = value, time.Now()
	default:
		e.cached = nil
	}
	if e.cached == nil && a.comparisons.entries[key] == e {
		delete(a.comparisons.entries, key)
	}
	a.comparisons.mu.Unlock()

	f.value, f.err = value, err
	close(f.done)
}

func (c *comparisonCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	for key, e := range c.entries {
		e.cached = nil
		if e.flight == nil {
			delete(c.entries, key)
		}
	}
}

func (c *comparisonCache) evictExpiredLocked() {
	for key, e := range c.entries {
		if e.cached != nil && time.Since(e.cachedAt) >= comparisonCacheMaxStale {
			e.cached = nil
		}
		if e.cached == nil && e.flight == nil {
			delete(c.entries, key)
		}
	}
}

func canonicalComparisonKey(source, baseSource, headSource string) comparisonCacheKey {
	source = firstNonEmpty(strings.TrimSpace(source), "main")
	baseSource = strings.TrimSpace(baseSource)
	headSource = strings.TrimSpace(headSource)
	if baseSource == "" && headSource == "" {
		return comparisonCacheKey{source: source}
	}
	headSource = firstNonEmpty(headSource, source)
	baseSource = firstNonEmpty(baseSource, "main")
	if headSource == baseSource {
		return comparisonCacheKey{source: headSource}
	}
	return comparisonCacheKey{baseSource: baseSource, headSource: headSource}
}
