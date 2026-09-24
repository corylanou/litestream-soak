package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	comparisonCacheTTL      = 10 * time.Minute
	comparisonCacheMaxStale = 6 * time.Hour
	comparisonPeekWait      = 2 * time.Second
	comparisonCacheMinCost  = time.Second
	comparisonBuildTimeout  = 20 * time.Minute
	comparisonQueueTimeout  = 25 * time.Minute
	homeComparisonWait      = 250 * time.Millisecond
)

var comparisonBuildSlot = make(chan struct{}, 1)

type comparisonCache struct {
	mu      sync.Mutex
	entries map[comparisonCacheKey]*comparisonCacheEntry
}

type comparisonCacheKey struct {
	source, baseSource, headSource string
	deployments                    string
}

type comparisonCacheEntry struct {
	cached   *DeploymentComparisonResponse
	cachedAt time.Time
	flight   *comparisonFlight
}

type comparisonFlight struct {
	done  chan struct{}
	value *DeploymentComparisonResponse
	err   error
}

func (a *API) deploymentComparison(ctx context.Context, source, baseSource, headSource string) (*DeploymentComparisonResponse, error) {
	key := canonicalComparisonKey(source, baseSource, headSource)
	fingerprint, err := a.comparisonDeploymentFingerprint(ctx, key)
	if err != nil {
		return nil, err
	}
	key.deployments = fingerprint
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
		if cached, at, ok := a.loadComparisonSnapshot(key); ok {
			e.cached, e.cachedAt = cached, at
		}
	}
	if e.cached != nil && time.Since(e.cachedAt) < comparisonCacheTTL {
		cached := e.cached
		c.mu.Unlock()
		return cached, nil
	}
	f := e.flight
	if f == nil {
		f = &comparisonFlight{done: make(chan struct{})}
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
	var value *DeploymentComparisonResponse
	var err error
	start := time.Now()
	queued, cancelQueue := context.WithTimeout(parent, comparisonQueueTimeout)
	defer cancelQueue()
	select {
	case comparisonBuildSlot <- struct{}{}:
		ctx, cancel := context.WithTimeout(parent, comparisonBuildTimeout)
		start = time.Now()
		value, err = buildRequestedDeploymentComparison(a.db.WithReadContext(ctx), key.source, key.baseSource, key.headSource)
		cancel()
		<-comparisonBuildSlot
	case <-queued.Done():
		err = queued.Err()
	}

	var persistAt time.Time
	a.comparisons.mu.Lock()
	e.flight = nil
	switch {
	case err != nil:
	case value != nil && time.Since(start) >= comparisonCacheMinCost:
		e.cached, e.cachedAt = value, time.Now()
		persistAt = e.cachedAt
	default:
		e.cached = nil
	}
	if e.cached == nil && a.comparisons.entries[key] == e {
		delete(a.comparisons.entries, key)
	}
	a.comparisons.mu.Unlock()

	f.value, f.err = value, err
	close(f.done)
	if !persistAt.IsZero() {
		a.storeComparisonSnapshot(key, value, persistAt)
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

func (a *API) comparisonDeploymentFingerprint(ctx context.Context, key comparisonCacheKey) (string, error) {
	db := a.db.WithReadContext(ctx)
	var parts []string
	for _, source := range []string{key.source, key.baseSource, key.headSource} {
		if source == "" {
			continue
		}
		deployment, err := db.GetLatestDeployment(source)
		if err != nil {
			return "", err
		}
		if deployment == nil {
			parts = append(parts, source+"=none")
			continue
		}
		completed := ""
		if deployment.CompletedAt != nil {
			completed = deployment.CompletedAt.UTC().Format(time.RFC3339Nano)
		}
		parts = append(parts, fmt.Sprintf("%s=%d/%s/%s", source, deployment.ID, deployment.Status, completed))
	}
	return strings.Join(parts, ","), nil
}

func (k comparisonCacheKey) String() string {
	return strings.Join([]string{k.source, k.baseSource, k.headSource, k.deployments}, "|")
}

func (a *API) loadComparisonSnapshot(key comparisonCacheKey) (*DeploymentComparisonResponse, time.Time, bool) {
	body, at, ok, err := a.db.GetComparisonSnapshot(key.String())
	if err != nil {
		slog.Warn("Failed to load comparison snapshot", "key", key.String(), "error", err)
		return nil, time.Time{}, false
	}
	if !ok || time.Since(at) >= comparisonCacheMaxStale {
		return nil, time.Time{}, false
	}
	var comparison DeploymentComparisonResponse
	if err := json.Unmarshal([]byte(body), &comparison); err != nil {
		slog.Warn("Failed to decode comparison snapshot", "key", key.String(), "error", err)
		return nil, time.Time{}, false
	}
	return &comparison, at, true
}

func (a *API) storeComparisonSnapshot(key comparisonCacheKey, comparison *DeploymentComparisonResponse, at time.Time) {
	body, err := json.Marshal(comparison)
	if err == nil {
		err = a.db.PutComparisonSnapshot(key.String(), string(body), at, at.Add(-comparisonCacheMaxStale))
	}
	if err != nil {
		slog.Warn("Failed to store comparison snapshot", "key", key.String(), "error", err)
	}
}

func (a *API) peekDeploymentComparison(source, baseSource, headSource string) (*DeploymentComparisonResponse, bool, error) {
	ctx, cancel := context.WithTimeout(a.backgroundContext, comparisonPeekWait)
	defer cancel()
	comparison, err := a.deploymentComparison(ctx, source, baseSource, headSource)
	if errors.Is(err, context.DeadlineExceeded) && a.backgroundContext.Err() == nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return comparison, true, nil
}
