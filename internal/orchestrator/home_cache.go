package orchestrator

import (
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	homeCacheFresh   = 10 * time.Second
	homeCacheMaxAge  = 5 * time.Minute
	homeCacheMinCost = 250 * time.Millisecond
)

type homeCache struct {
	mu      sync.Mutex
	entries map[string]*homeCacheEntry
}

type homeCacheEntry struct {
	data       homePageData
	builtAt    time.Time
	refreshing bool
}

func homeCacheKey(r *http.Request) string {
	query := r.URL.Query()
	return url.Values{
		"source":      {strings.TrimSpace(query.Get("source"))},
		"base_source": {strings.TrimSpace(query.Get("base_source"))},
		"head_source": {strings.TrimSpace(query.Get("head_source"))},
	}.Encode()
}

func (a *API) homePageData(r *http.Request) (homePageData, error) {
	key := homeCacheKey(r)
	c := &a.home

	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]*homeCacheEntry)
	}
	e := c.entries[key]
	if e != nil && time.Since(e.builtAt) < homeCacheMaxAge {
		data := e.data
		if time.Since(e.builtAt) >= homeCacheFresh && !e.refreshing {
			e.refreshing = true
			go a.refreshHomePage(key)
		}
		c.mu.Unlock()
		return data, nil
	}
	c.mu.Unlock()

	return a.buildAndCacheHomePage(r, key)
}

func (a *API) buildAndCacheHomePage(r *http.Request, key string) (homePageData, error) {
	start := time.Now()
	data, err := a.buildHomePageData(r)
	cost := time.Since(start)

	a.home.mu.Lock()
	defer a.home.mu.Unlock()
	switch {
	case err != nil:
		if e := a.home.entries[key]; e != nil {
			e.refreshing = false
		}
	case cost >= homeCacheMinCost:
		a.home.entries[key] = &homeCacheEntry{data: data, builtAt: time.Now()}
	default:
		delete(a.home.entries, key)
	}
	return data, err
}

func (a *API) refreshHomePage(key string) {
	r, err := http.NewRequestWithContext(a.backgroundContext, http.MethodGet, "/ui?"+key, nil)
	if err == nil {
		_, err = a.buildAndCacheHomePage(r, key)
	}
	if err != nil && a.backgroundContext.Err() == nil {
		slog.Warn("Failed to refresh dashboard snapshot", "key", key, "error", err)
	}
}

func (a *API) WarmDashboard() {
	a.peekDeploymentComparison("main", "", "")
	a.refreshHomePage(homeCacheKey(&http.Request{URL: &url.URL{}}))
}
