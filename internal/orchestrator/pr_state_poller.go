package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultPRStatePollInterval            = 15 * time.Minute
	defaultPRStateRequestTimeout          = 10 * time.Second
	defaultPRRateLimitVisibilityThreshold = time.Hour
	maxPRStateResponseBytes               = 1 << 20
	defaultGitHubAPIBaseURL               = "https://api.github.com"
	upstreamPRPollRateLimitedEvent        = "upstream_pr_poll_rate_limited"
	upstreamPRPollRateLimitSustainedEvent = "upstream_pr_poll_rate_limit_sustained"
	upstreamPRPollRateLimitRecoveredEvent = "upstream_pr_poll_rate_limit_recovered"
	upstreamPRPollConfigurationErrorEvent = "upstream_pr_poll_configuration_error"
)

var (
	errGitHubRateLimited = errors.New("github API rate limited")

	controlPRPollRateLimited = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_control_upstream_pr_poll_rate_limited",
		Help: "Whether upstream PR polling is currently blocked by a GitHub API rate limit.",
	})

	controlPRPollRateLimitedSince = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "soak_control_upstream_pr_poll_rate_limited_since_unixtime",
		Help: "Unix timestamp when the current upstream PR polling rate-limit condition began.",
	})
)

type PRStatePollerConfig struct {
	APIBaseURL                   string
	Client                       *http.Client
	RepositoryAllowlist          []string
	KeepAliveLabel               string
	PollInterval                 time.Duration
	RequestTimeout               time.Duration
	RateLimitVisibilityThreshold time.Duration
	Now                          func() time.Time
}

type PRStatePoller struct {
	apiBaseURL                   string
	client                       *http.Client
	manager                      *Manager
	repositories                 []string
	keepAliveLabel               string
	pollInterval                 time.Duration
	requestTimeout               time.Duration
	rateLimitVisibilityThreshold time.Duration
	now                          func() time.Time
	rateLimitedSince             time.Time
	lastSustainedEventAt         time.Time
}

type githubPullRequestState struct {
	Number int                 `json:"number"`
	State  string              `json:"state"`
	Merged *bool               `json:"merged"`
	Labels *[]pullRequestLabel `json:"labels"`
	Base   struct {
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"base"`
}

func NewPRStatePoller(config PRStatePollerConfig, manager *Manager) *PRStatePoller {
	apiBaseURL := strings.TrimRight(strings.TrimSpace(config.APIBaseURL), "/")
	if apiBaseURL == "" {
		apiBaseURL = defaultGitHubAPIBaseURL
	}
	pollInterval := config.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPRStatePollInterval
	}
	requestTimeout := config.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = defaultPRStateRequestTimeout
	}
	rateLimitVisibilityThreshold := config.RateLimitVisibilityThreshold
	if rateLimitVisibilityThreshold <= 0 {
		rateLimitVisibilityThreshold = defaultPRRateLimitVisibilityThreshold
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	keepAliveLabel := strings.TrimSpace(config.KeepAliveLabel)
	if keepAliveLabel == "" {
		keepAliveLabel = defaultPRKeepAliveLabel
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &PRStatePoller{
		apiBaseURL:                   apiBaseURL,
		client:                       client,
		manager:                      manager,
		repositories:                 normalizedRepositoryAllowlist(config.RepositoryAllowlist),
		keepAliveLabel:               keepAliveLabel,
		pollInterval:                 pollInterval,
		requestTimeout:               requestTimeout,
		rateLimitVisibilityThreshold: rateLimitVisibilityThreshold,
		now:                          now,
	}
}

func (p *PRStatePoller) Run(ctx context.Context) {
	if err := p.configurationError(); err != nil {
		slog.Error("Upstream PR polling disabled", "error", err)
		p.recordEvent(upstreamPRPollConfigurationErrorEvent, "Upstream PR polling is disabled by an unsafe repository configuration", err.Error())
		return
	}

	p.pollOnce(ctx)
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

func (p *PRStatePoller) pollOnce(ctx context.Context) {
	if err := p.configurationError(); err != nil {
		return
	}

	sources, err := p.manager.db.ListActiveWorkerSources()
	if err != nil {
		slog.Error("Failed to list active worker sources for upstream PR polling", "error", err)
		return
	}

	repository := p.repositories[0]
	polled := false
	for _, source := range sources {
		prNumber, ok := pullRequestNumberFromSource(source)
		if !ok {
			continue
		}
		polled = true

		state, err := p.fetchPullRequestState(ctx, repository, prNumber)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errGitHubRateLimited) {
				p.markRateLimited(repository, source, err)
				return
			}
			slog.Warn("Upstream PR state remains unknown; leaving source unchanged", "repo", repository, "source", source, "error", err)
			continue
		}

		if state.State == "open" {
			continue
		}
		if pullRequestHasLabel(*state.Labels, p.keepAliveLabel) {
			slog.Info("Keeping terminal upstream PR source active because of keep-alive label", "repo", repository, "source", source, "label", p.keepAliveLabel)
			continue
		}

		retirementCtx, cancel := context.WithTimeout(ctx, sourceRetirementTimeout)
		result, err := p.manager.archiveAndDestroyTerminalPRSource(retirementCtx, repository, prNumber, *state.Merged)
		cancel()
		switch {
		case errors.Is(err, errSourceNotFound), errors.Is(err, errSourceDeploymentNotFound):
			slog.Info("No active soak source found for terminal upstream PR", "repo", repository, "source", source, "error", err)
		case err != nil:
			slog.Error("Failed to retire soak source for polled terminal upstream PR", "repo", repository, "source", source, "error", err)
		case !result.Complete:
			slog.Error("Polled terminal upstream PR source teardown incomplete", "repo", repository, "source", source, "failed_workers", result.FailedWorkers)
		}
	}

	if polled {
		p.clearRateLimit()
		return
	}
	p.resetRateLimit()
}

func (p *PRStatePoller) fetchPullRequestState(ctx context.Context, repository string, prNumber int) (githubPullRequestState, error) {
	owner, name, ok := splitGitHubRepository(repository)
	if !ok {
		return githubPullRequestState{}, fmt.Errorf("invalid github repository %q", repository)
	}

	requestCtx, cancel := context.WithTimeout(ctx, p.requestTimeout)
	defer cancel()
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d", p.apiBaseURL, url.PathEscape(owner), url.PathEscape(name), prNumber)
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return githubPullRequestState{}, fmt.Errorf("create github pull request lookup: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "litestream-soak/pr-state-poller")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := p.client.Do(req)
	if err != nil {
		return githubPullRequestState{}, fmt.Errorf("request github pull request state: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Debug("Failed to close GitHub pull request response body", "error", err)
		}
	}()

	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
		return githubPullRequestState{}, fmt.Errorf("%w: status %d", errGitHubRateLimited, resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return githubPullRequestState{}, fmt.Errorf("github API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPRStateResponseBytes+1))
	if err != nil {
		return githubPullRequestState{}, fmt.Errorf("read github pull request response: %w", err)
	}
	if len(body) > maxPRStateResponseBytes {
		return githubPullRequestState{}, errors.New("github pull request response exceeds size limit")
	}

	var state githubPullRequestState
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&state); err != nil {
		return githubPullRequestState{}, fmt.Errorf("decode github pull request response: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return githubPullRequestState{}, errors.New("github pull request response contains trailing JSON")
		}
		return githubPullRequestState{}, fmt.Errorf("decode trailing github pull request response: %w", err)
	}
	if err := validateGitHubPullRequestState(state, repository, prNumber); err != nil {
		return githubPullRequestState{}, err
	}
	return state, nil
}

func validateGitHubPullRequestState(state githubPullRequestState, repository string, prNumber int) error {
	if state.Number != prNumber {
		return fmt.Errorf("github pull request response number %d does not match requested PR %d", state.Number, prNumber)
	}
	if !strings.EqualFold(strings.TrimSpace(state.Base.Repo.FullName), repository) {
		return fmt.Errorf("github pull request response repository %q does not match requested repository %q", state.Base.Repo.FullName, repository)
	}
	if state.State != "open" && state.State != "closed" {
		return fmt.Errorf("github pull request response has unexpected state %q", state.State)
	}
	if state.Merged == nil {
		return errors.New("github pull request response is missing merged state")
	}
	if state.State == "open" && *state.Merged {
		return errors.New("github pull request response reports an open merged PR")
	}
	if state.Labels == nil {
		return errors.New("github pull request response is missing labels")
	}
	for _, label := range *state.Labels {
		if strings.TrimSpace(label.Name) == "" {
			return errors.New("github pull request response contains an invalid label")
		}
	}
	return nil
}

func pullRequestNumberFromSource(source string) (int, bool) {
	raw := strings.TrimPrefix(source, "pr-")
	if raw == source || raw == "" {
		return 0, false
	}
	prNumber, err := strconv.Atoi(raw)
	if err != nil || prNumber <= 0 || fmt.Sprintf("pr-%d", prNumber) != source {
		return 0, false
	}
	return prNumber, true
}

func splitGitHubRepository(repository string) (string, string, bool) {
	if strings.Count(repository, "/") != 1 {
		return "", "", false
	}
	owner, name, _ := strings.Cut(repository, "/")
	if strings.TrimSpace(owner) != owner || strings.TrimSpace(name) != name || owner == "" || name == "" {
		return "", "", false
	}
	return owner, name, true
}

func (p *PRStatePoller) configurationError() error {
	if p == nil || p.manager == nil || p.manager.db == nil {
		return errors.New("manager database is unavailable")
	}
	if len(p.repositories) != 1 {
		return fmt.Errorf("exactly one upstream PR repository is required, got %d", len(p.repositories))
	}
	if _, _, ok := splitGitHubRepository(p.repositories[0]); !ok {
		return fmt.Errorf("invalid upstream PR repository %q", p.repositories[0])
	}
	return nil
}

func (p *PRStatePoller) markRateLimited(repository, source string, cause error) {
	now := p.now().UTC()
	controlPRPollRateLimited.Set(1)
	if p.rateLimitedSince.IsZero() {
		p.rateLimitedSince = now
		controlPRPollRateLimitedSince.Set(float64(now.Unix()))
		message := "GitHub rate limiting paused upstream PR retirement polling; all affected fleets remain unchanged"
		details := fmt.Sprintf("repository=%s source=%s since=%s error=%q", repository, source, now.Format(time.RFC3339), cause)
		slog.Warn(message, "repo", repository, "source", source, "error", cause)
		p.recordEvent(upstreamPRPollRateLimitedEvent, message, details)
		return
	}

	elapsed := now.Sub(p.rateLimitedSince)
	if elapsed < p.rateLimitVisibilityThreshold {
		slog.Warn("GitHub rate limiting continues to pause upstream PR retirement polling", "repo", repository, "source", source, "since", p.rateLimitedSince, "error", cause)
		return
	}
	if !p.lastSustainedEventAt.IsZero() && now.Sub(p.lastSustainedEventAt) < p.rateLimitVisibilityThreshold {
		return
	}

	p.lastSustainedEventAt = now
	message := fmt.Sprintf("GitHub rate limiting has blocked upstream PR retirement polling for %s; all affected fleets remain unchanged", elapsed.Round(time.Minute))
	details := fmt.Sprintf("repository=%s source=%s since=%s error=%q", repository, source, p.rateLimitedSince.Format(time.RFC3339), cause)
	slog.Error(message, "repo", repository, "source", source, "since", p.rateLimitedSince, "error", cause)
	p.recordEvent(upstreamPRPollRateLimitSustainedEvent, message, details)
}

func (p *PRStatePoller) clearRateLimit() {
	controlPRPollRateLimited.Set(0)
	controlPRPollRateLimitedSince.Set(0)
	if p.rateLimitedSince.IsZero() {
		return
	}

	now := p.now().UTC()
	message := "GitHub rate limiting no longer blocks upstream PR retirement polling"
	details := fmt.Sprintf("since=%s recovered_at=%s", p.rateLimitedSince.Format(time.RFC3339), now.Format(time.RFC3339))
	slog.Info(message, "since", p.rateLimitedSince)
	p.recordEvent(upstreamPRPollRateLimitRecoveredEvent, message, details)
	p.rateLimitedSince = time.Time{}
	p.lastSustainedEventAt = time.Time{}
}

func (p *PRStatePoller) resetRateLimit() {
	controlPRPollRateLimited.Set(0)
	controlPRPollRateLimitedSince.Set(0)
	p.rateLimitedSince = time.Time{}
	p.lastSustainedEventAt = time.Time{}
}

func (p *PRStatePoller) recordEvent(eventType, message, details string) {
	if p == nil || p.manager == nil || p.manager.db == nil {
		return
	}
	if err := p.manager.db.RecordEvent("", eventType, message, details); err != nil {
		slog.Warn("Failed to record upstream PR poll event", "event_type", eventType, "error", err)
	}
}
