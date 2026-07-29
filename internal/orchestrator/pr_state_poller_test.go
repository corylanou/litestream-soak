package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestPRStatePollerRetiresDefinitiveTerminalPullRequests(t *testing.T) {
	for _, test := range []struct {
		name          string
		prNumber      int
		merged        bool
		archiveReason string
		archiveState  string
	}{
		{name: "closed", prNumber: 201, archiveReason: "upstream_pr_closed", archiveState: "closed"},
		{name: "merged", prNumber: 202, merged: true, archiveReason: "upstream_pr_merged", archiveState: "merged"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			source := fmt.Sprintf("pr-%d", test.prNumber)
			workerID := "worker-" + source
			machineID := "machine-" + source
			volumeID := "volume-" + source
			createTeardownTestDeployment(t, db, source)
			createTestWorker(t, db, model.Worker{
				ID:            workerID,
				AppName:       "litestream-soak",
				Name:          workerID,
				Status:        model.WorkerDegraded,
				Source:        source,
				GitSHA:        "soak-sha",
				LitestreamSHA: "litestream-sha",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
				FlyMachineID:  machineID,
				FlyVolumeID:   volumeID,
			})
			mustRecordVerification(t, db, &model.Verification{
				WorkerID:     workerID,
				StartedAt:    time.Now().UTC(),
				Status:       "failed",
				CheckType:    "restore",
				ErrorMessage: "integrity check failed",
			})

			fly := newTeardownTestServer(t, nil)
			manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{
				Bucket:    "bucket",
				Endpoint:  fly.server.URL,
				AccessKey: "access",
				SecretKey: "secret",
				Region:    "auto",
			}, "", "")
			var requests atomic.Int32
			responseBody := pullRequestStateBody(t, test.prNumber, "closed", test.merged, "benbjohnson/litestream", nil)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != http.MethodGet {
					t.Errorf("method = %q, want GET", r.Method)
				}
				if r.Header.Get("Authorization") != "" {
					t.Errorf("Authorization = %q, want empty", r.Header.Get("Authorization"))
				}
				if r.Header.Get("User-Agent") != "litestream-soak/pr-state-poller" {
					t.Errorf("User-Agent = %q, want poller user agent", r.Header.Get("User-Agent"))
				}
				_, _ = w.Write([]byte(responseBody))
			}))
			t.Cleanup(api.Close)

			poller := newTestPRStatePoller(api, manager, nil)
			poller.pollOnce(context.Background())

			if requests.Load() != 1 {
				t.Fatalf("requests = %d, want 1", requests.Load())
			}
			archives, err := db.ListRunArchives(source, runArchiveTypeTeardown, 10)
			if err != nil {
				t.Fatalf("ListRunArchives() error = %v", err)
			}
			if len(archives) != 1 {
				t.Fatalf("archive count = %d, want 1", len(archives))
			}
			var payload runArchivePayload
			if err := json.Unmarshal([]byte(archives[0].Payload), &payload); err != nil {
				t.Fatalf("json.Unmarshal() archive error = %v", err)
			}
			if payload.Reason != test.archiveReason {
				t.Fatalf("archive reason = %q, want %q", payload.Reason, test.archiveReason)
			}
			if !strings.Contains(archives[0].Summary, "was "+test.archiveState) {
				t.Fatalf("archive summary = %q, want state %q", archives[0].Summary, test.archiveState)
			}
			if len(payload.Workers) != 1 || payload.Workers[0].LatestFailure == nil {
				t.Fatalf("archived worker evidence = %+v, want failed verification", payload.Workers)
			}
			if !fly.machineDestroyed(machineID) {
				t.Fatalf("machine %s was not destroyed", machineID)
			}
			if !fly.volumeDestroyed(volumeID) {
				t.Fatalf("volume %s was not destroyed", volumeID)
			}
			if len(fly.prefixes()) == 0 {
				t.Fatal("replica prefix was not cleared")
			}
			worker, err := db.GetWorker(workerID)
			if err != nil {
				t.Fatalf("GetWorker(%q) error = %v", workerID, err)
			}
			if worker.Status != model.WorkerStopped {
				t.Fatalf("worker status = %q, want %q", worker.Status, model.WorkerStopped)
			}
		})
	}
}

func TestPRStatePollerFailClosedLeavesFleetUntouched(t *testing.T) {
	validOpen := pullRequestStateBody(t, 301, "open", false, "benbjohnson/litestream", nil)
	validClosed := pullRequestStateBody(t, 301, "closed", false, "benbjohnson/litestream", nil)
	keepAlive := pullRequestStateBody(t, 301, "closed", false, "benbjohnson/litestream", []string{"SOAK:KEEP-ALIVE"})

	for _, test := range []struct {
		name           string
		status         int
		body           string
		timeout        bool
		requestTimeout time.Duration
	}{
		{name: "open PR", status: http.StatusOK, body: validOpen},
		{name: "HTTP error", status: http.StatusInternalServerError, body: `{"message":"internal error"}`},
		{name: "rate limit", status: http.StatusForbidden, body: `{"message":"API rate limit exceeded"}`},
		{name: "not found", status: http.StatusNotFound, body: `{"message":"Not Found"}`},
		{name: "timeout", timeout: true, requestTimeout: 10 * time.Millisecond},
		{name: "unparseable body", status: http.StatusOK, body: `{"number":`},
		{name: "malformed valid JSON shape", status: http.StatusOK, body: `{"number":301,"state":"closed","merged":false}`},
		{name: "missing merged field", status: http.StatusOK, body: `{"number":301,"state":"closed","labels":[],"base":{"repo":{"full_name":"benbjohnson/litestream"}}}`},
		{name: "missing labels field", status: http.StatusOK, body: `{"number":301,"state":"closed","merged":false,"base":{"repo":{"full_name":"benbjohnson/litestream"}}}`},
		{name: "unexpected repository identity", status: http.StatusOK, body: pullRequestStateBody(t, 301, "closed", false, "other/litestream", nil)},
		{name: "unexpected PR identity", status: http.StatusOK, body: pullRequestStateBody(t, 999, "closed", false, "benbjohnson/litestream", nil)},
		{name: "empty success response", status: http.StatusOK},
		{name: "trailing JSON", status: http.StatusOK, body: validClosed + `{}`},
		{name: "unexpected state", status: http.StatusOK, body: pullRequestStateBody(t, 301, "unknown", false, "benbjohnson/litestream", nil)},
		{name: "inconsistent merged state", status: http.StatusOK, body: pullRequestStateBody(t, 301, "open", true, "benbjohnson/litestream", nil)},
		{name: "keep-alive label", status: http.StatusOK, body: keepAlive},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			source := "pr-301"
			workerID := "worker-pr-301"
			machineID := "machine-pr-301"
			volumeID := "volume-pr-301"
			createTeardownTestDeployment(t, db, source)
			createTestWorker(t, db, model.Worker{
				ID:            workerID,
				AppName:       "litestream-soak",
				Name:          workerID,
				Status:        model.WorkerRunning,
				Source:        source,
				GitSHA:        "soak-sha",
				LitestreamSHA: "litestream-sha",
				ProfileName:   "low-volume",
				ProfileConfig: "{}",
				FlyMachineID:  machineID,
				FlyVolumeID:   volumeID,
			})

			fly := newTeardownTestServer(t, nil)
			manager := NewManager(fly.client, db, nil, nil, "litestream-soak", ReplicaConfig{
				Bucket:    "bucket",
				Endpoint:  fly.server.URL,
				AccessKey: "access",
				SecretKey: "secret",
				Region:    "auto",
			}, "", "")
			var requests atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if test.timeout {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			t.Cleanup(api.Close)

			poller := newTestPRStatePoller(api, manager, func(config *PRStatePollerConfig) {
				if test.requestTimeout > 0 {
					config.RequestTimeout = test.requestTimeout
				}
			})
			poller.pollOnce(context.Background())

			if requests.Load() != 1 {
				t.Fatalf("requests = %d, want 1", requests.Load())
			}
			assertPRSourceUntouched(t, db, fly, source, workerID, machineID, volumeID)
		})
	}
}

func TestPRStatePollerSkipsSourcesWithoutLiveWorkers(t *testing.T) {
	db := openTestDB(t)
	for _, worker := range []model.Worker{
		{ID: "worker-pr-401-stopped", Name: "worker-pr-401-stopped", Status: model.WorkerStopped, Source: "pr-401", ProfileName: "low-volume", ProfileConfig: "{}"},
		{ID: "worker-pr-402-failed", Name: "worker-pr-402-failed", Status: model.WorkerFailed, Source: "pr-402", ProfileName: "low-volume", ProfileConfig: "{}"},
		{ID: "worker-main-running", Name: "worker-main-running", Status: model.WorkerRunning, Source: "main", ProfileName: "low-volume", ProfileConfig: "{}"},
		{ID: "worker-malformed-source", Name: "worker-malformed-source", Status: model.WorkerRunning, Source: "pr-00403", ProfileName: "low-volume", ProfileConfig: "{}"},
	} {
		createTestWorker(t, db, worker)
	}
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	var requests atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	t.Cleanup(api.Close)

	poller := newTestPRStatePoller(api, manager, nil)
	poller.pollOnce(context.Background())

	if requests.Load() != 0 {
		t.Fatalf("requests = %d, want 0", requests.Load())
	}
}

func TestPRStatePollerUnsafeRepositoryConfigurationIssuesNoRequests(t *testing.T) {
	for _, repositories := range [][]string{
		nil,
		{"benbjohnson/litestream", "other/litestream"},
		{"not-a-repository"},
	} {
		db := openTestDB(t)
		createTestWorker(t, db, model.Worker{
			ID:            "worker-pr-501",
			Name:          "worker-pr-501",
			Status:        model.WorkerRunning,
			Source:        "pr-501",
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
		var requests atomic.Int32
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
		}))
		poller := newTestPRStatePoller(api, manager, func(config *PRStatePollerConfig) {
			config.RepositoryAllowlist = repositories
		})

		poller.pollOnce(context.Background())
		api.Close()

		if requests.Load() != 0 {
			t.Fatalf("repositories = %v, requests = %d, want 0", repositories, requests.Load())
		}
	}
}

func TestPRStatePollerRateLimitStopsCycleAndSurfacesSustainedCondition(t *testing.T) {
	db := openTestDB(t)
	for _, prNumber := range []int{601, 602} {
		source := fmt.Sprintf("pr-%d", prNumber)
		createTestWorker(t, db, model.Worker{
			ID:            "worker-" + source,
			Name:          "worker-" + source,
			Status:        model.WorkerRunning,
			Source:        source,
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
	}
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	var requests atomic.Int32
	var status atomic.Int32
	status.Store(http.StatusForbidden)
	openResponses := map[string]string{
		"/repos/benbjohnson/litestream/pulls/601": pullRequestStateBody(t, 601, "open", false, "benbjohnson/litestream", nil),
		"/repos/benbjohnson/litestream/pulls/602": pullRequestStateBody(t, 602, "open", false, "benbjohnson/litestream", nil),
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		currentStatus := int(status.Load())
		if currentStatus == http.StatusOK {
			_, _ = w.Write([]byte(openResponses[r.URL.Path]))
			return
		}
		http.Error(w, "rate limited", currentStatus)
	}))
	t.Cleanup(api.Close)

	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	poller := newTestPRStatePoller(api, manager, func(config *PRStatePollerConfig) {
		config.Now = func() time.Time { return now }
		config.RateLimitVisibilityThreshold = time.Hour
	})

	poller.pollOnce(context.Background())
	if requests.Load() != 1 {
		t.Fatalf("requests after first cycle = %d, want 1", requests.Load())
	}
	requireControlEventCount(t, db, upstreamPRPollRateLimitedEvent, 1)
	requireControlEventCount(t, db, upstreamPRPollRateLimitSustainedEvent, 0)

	now = now.Add(30 * time.Minute)
	poller.pollOnce(context.Background())
	requireControlEventCount(t, db, upstreamPRPollRateLimitSustainedEvent, 0)

	now = now.Add(30 * time.Minute)
	poller.pollOnce(context.Background())
	requireControlEventCount(t, db, upstreamPRPollRateLimitSustainedEvent, 1)

	now = now.Add(30 * time.Minute)
	poller.pollOnce(context.Background())
	requireControlEventCount(t, db, upstreamPRPollRateLimitSustainedEvent, 1)

	status.Store(http.StatusOK)
	now = now.Add(15 * time.Minute)
	poller.pollOnce(context.Background())
	requireControlEventCount(t, db, upstreamPRPollRateLimitRecoveredEvent, 1)

	if requests.Load() != 6 {
		t.Fatalf("requests = %d, want 6", requests.Load())
	}
}

func newTestPRStatePoller(api *httptest.Server, manager *Manager, configure func(*PRStatePollerConfig)) *PRStatePoller {
	config := PRStatePollerConfig{
		APIBaseURL:          api.URL,
		Client:              api.Client(),
		RepositoryAllowlist: []string{"benbjohnson/litestream"},
		KeepAliveLabel:      "soak:keep-alive",
		RequestTimeout:      time.Second,
	}
	if configure != nil {
		configure(&config)
	}
	return NewPRStatePoller(config, manager)
}

func pullRequestStateBody(t *testing.T, number int, state string, merged bool, repository string, labels []string) string {
	t.Helper()

	payloadLabels := make([]map[string]string, 0, len(labels))
	for _, label := range labels {
		payloadLabels = append(payloadLabels, map[string]string{"name": label})
	}
	body, err := json.Marshal(map[string]any{
		"number": number,
		"state":  state,
		"merged": merged,
		"labels": payloadLabels,
		"base": map[string]any{
			"repo": map[string]string{"full_name": repository},
		},
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return string(body)
}

func assertPRSourceUntouched(t *testing.T, db *model.DB, fly *teardownTestServer, source, workerID, machineID, volumeID string) {
	t.Helper()

	archives, err := db.ListRunArchives(source, runArchiveTypeTeardown, 10)
	if err != nil {
		t.Fatalf("ListRunArchives() error = %v", err)
	}
	if len(archives) != 0 {
		t.Fatalf("archive count = %d, want 0", len(archives))
	}
	worker, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker(%q) error = %v", workerID, err)
	}
	if worker.Status != model.WorkerRunning {
		t.Fatalf("worker status = %q, want %q", worker.Status, model.WorkerRunning)
	}
	if fly.machineDestroyed(machineID) {
		t.Fatalf("machine %s was destroyed", machineID)
	}
	if fly.volumeDestroyed(volumeID) {
		t.Fatalf("volume %s was destroyed", volumeID)
	}
	if len(fly.prefixes()) != 0 {
		t.Fatalf("replica prefixes were touched: %v", fly.prefixes())
	}
}

func requireControlEventCount(t *testing.T, db *model.DB, eventType string, want int) {
	t.Helper()

	events, err := db.ListEvents(100)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	got := 0
	for _, event := range events {
		if event.EventType == eventType {
			got++
		}
	}
	if got != want {
		t.Fatalf("%s event count = %d, want %d", eventType, got, want)
	}
}
