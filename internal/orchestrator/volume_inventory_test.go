package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestStaleUnattachedWorkerVolumes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	volumes := []flyapi.Volume{
		{ID: "old-worker", Name: "soak_worker_main_low_vol", State: "created", SizeGB: 10, CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "fresh-worker", Name: "soak_worker_main_high_vol", State: "created", SizeGB: 100, CreatedAt: now.Add(-30 * time.Minute)},
		{ID: "attached-worker", Name: "soak_worker_main_burst_vol", State: "created", SizeGB: 100, AttachedMachineID: "machine", CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "non-worker", Name: "soakctl_data", State: "created", SizeGB: 1, CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "unknown-created", Name: "soak_worker_main_read_heavy", State: "created", SizeGB: 10},
		{ID: "pending-destroy", Name: "soak_worker_main_taxi_replay", State: "pending_destroy", SizeGB: 10, CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "scheduling-destroy", Name: "soak_worker_main_taxi_mixed", State: "scheduling_destroy", SizeGB: 10, CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "unexpected-state", Name: "soak_worker_main_orders_replay", State: "hydrating", SizeGB: 10, CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "missing-state", Name: "soak_worker_main_gharchive", SizeGB: 50, CreatedAt: now.Add(-3 * time.Hour)},
	}

	stale := staleUnattachedWorkerVolumes(volumes, now, 2*time.Hour)
	if len(stale) != 1 {
		t.Fatalf("len(stale) = %d, want 1", len(stale))
	}
	if stale[0].ID != "old-worker" {
		t.Fatalf("stale[0].ID = %q, want old-worker", stale[0].ID)
	}
}

func TestStaleUnattachedWorkerVolumesDisabled(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	volumes := []flyapi.Volume{
		{ID: "old-worker", Name: "soak_worker_main_low_vol", State: "created", CreatedAt: now.Add(-3 * time.Hour)},
	}

	if stale := staleUnattachedWorkerVolumes(volumes, now, 0); len(stale) != 0 {
		t.Fatalf("len(stale) = %d, want 0", len(stale))
	}
}

func TestVolumeInventoryGCSchedulesDeletionOnNextFreshInventory(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	volume := flyapi.Volume{
		ID:        "stale-volume",
		Name:      "soak_worker_main_low_vol",
		State:     "created",
		SizeGB:    10,
		CreatedAt: now.Add(-3 * time.Hour),
	}
	fake := newVolumeGCInventoryTestServer(t, volume, "pending_destroy")
	db := openTestDB(t)
	manager := NewManager(fake.client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")

	manager.syncVolumeInventory(t.Context(), time.Hour)

	gets, _, deletes := fake.requestCounts()
	if gets != 1 {
		t.Fatalf("volume list requests after destroy = %d, want 1", gets)
	}
	if deletes != 1 {
		t.Fatalf("volume delete requests after destroy = %d, want 1", deletes)
	}
	if got := len(volumeGCEventsOfType(t, db, volumeGCEventDestroyRequested)); got != 1 {
		t.Fatalf("destroy requested events = %d, want 1", got)
	}
	if got := len(volumeGCEventsOfType(t, db, volumeGCEventDestroyScheduled)); got != 0 {
		t.Fatalf("destroy scheduled events before next inventory = %d, want 0", got)
	}

	manager.syncVolumeInventory(t.Context(), time.Hour)

	gets, _, deletes = fake.requestCounts()
	if gets != 2 {
		t.Fatalf("volume list requests after confirmation = %d, want 2", gets)
	}
	if deletes != 1 {
		t.Fatalf("volume delete requests after confirmation = %d, want 1", deletes)
	}
	if got := len(volumeGCEventsOfType(t, db, volumeGCEventDestroyScheduled)); got != 1 {
		t.Fatalf("destroy scheduled events after confirmation = %d, want 1", got)
	}
}

func TestVolumeInventoryGCRequiresTargetedNotFoundBeforeConfirmingAbsence(t *testing.T) {
	t.Parallel()

	volume := flyapi.Volume{
		ID:        "targeted-confirmation-volume",
		Name:      "soak_worker_main_low_vol",
		State:     "created",
		SizeGB:    10,
		CreatedAt: time.Now().UTC().Add(-3 * time.Hour),
	}
	fake := newVolumeGCInventoryTestServer(t, volume, "")
	db := openTestDB(t)
	manager := NewManager(fake.client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")

	manager.syncVolumeInventory(t.Context(), time.Hour)
	fake.hideVolumeFromList()
	manager.syncVolumeInventory(t.Context(), time.Hour)

	listGets, volumeGets, deletes := fake.requestCounts()
	if listGets != 2 {
		t.Fatalf("volume list requests = %d, want 2", listGets)
	}
	if volumeGets != 1 {
		t.Fatalf("targeted volume verification requests = %d, want 1", volumeGets)
	}
	if deletes != 1 {
		t.Fatalf("volume delete requests during backoff = %d, want 1", deletes)
	}
	if got := len(volumeGCEventsOfType(t, db, volumeGCEventDestroyConfirmed)); got != 0 {
		t.Fatalf("destroy confirmed events for partial inventory = %d, want 0", got)
	}
	if got := len(volumeGCEventsOfType(t, db, volumeGCEventDestroyStalled)); got != 1 {
		t.Fatalf("destroy stalled events after positive created response = %d, want 1", got)
	}

	fake.removeVolume()
	manager.syncVolumeInventory(t.Context(), time.Hour)

	_, volumeGets, _ = fake.requestCounts()
	if volumeGets != 2 {
		t.Fatalf("targeted volume verification requests after removal = %d, want 2", volumeGets)
	}
	if got := len(volumeGCEventsOfType(t, db, volumeGCEventDestroyConfirmed)); got != 1 {
		t.Fatalf("destroy confirmed events after targeted not found = %d, want 1", got)
	}
	attempts, err := db.ListVolumeGCAttempts("litestream-soak")
	if err != nil {
		t.Fatalf("ListVolumeGCAttempts() error = %v", err)
	}
	if len(attempts) != 0 {
		t.Fatalf("persisted attempts after targeted not found = %+v, want none", attempts)
	}
}

func TestVolumeInventoryGCRejectsEmptyConfirmationResponses(t *testing.T) {
	t.Parallel()

	volume := flyapi.Volume{
		ID:        "empty-confirmation-volume",
		Name:      "soak_worker_main_low_vol",
		State:     "created",
		SizeGB:    10,
		CreatedAt: time.Now().UTC().Add(-3 * time.Hour),
	}
	fake := newVolumeGCInventoryTestServer(t, volume, "")
	db := openTestDB(t)
	manager := NewManager(fake.client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")

	manager.syncVolumeInventory(t.Context(), time.Hour)
	fake.returnEmptyInventoryResponses()
	manager.syncVolumeInventory(t.Context(), time.Hour)

	_, volumeGets, deletes := fake.requestCounts()
	if volumeGets != 1 {
		t.Fatalf("targeted volume verification requests = %d, want 1", volumeGets)
	}
	if deletes != 1 {
		t.Fatalf("volume delete requests during backoff = %d, want 1", deletes)
	}
	if got := len(volumeGCEventsOfType(t, db, volumeGCEventDestroyConfirmed)); got != 0 {
		t.Fatalf("destroy confirmed events for empty responses = %d, want 0", got)
	}
	if got := len(volumeGCEventsOfType(t, db, volumeGCEventConfirmFailed)); got != 1 {
		t.Fatalf("confirmation failed events for empty responses = %d, want 1", got)
	}
	attempts, err := db.ListVolumeGCAttempts("litestream-soak")
	if err != nil {
		t.Fatalf("ListVolumeGCAttempts() error = %v", err)
	}
	if len(attempts) != 1 || attempts[0].VolumeID != volume.ID {
		t.Fatalf("persisted attempts after empty responses = %+v, want %s retained", attempts, volume.ID)
	}
}

func TestVolumeInventoryGCSkipsPendingVolumeAfterManagerRestart(t *testing.T) {
	t.Parallel()

	volume := flyapi.Volume{
		ID:        "stale-volume",
		Name:      "soak_worker_main_low_vol",
		State:     "created",
		SizeGB:    10,
		CreatedAt: time.Now().UTC().Add(-3 * time.Hour),
	}
	fake := newVolumeGCInventoryTestServer(t, volume, "pending_destroy")
	firstDB := openTestDB(t)
	firstManager := NewManager(fake.client, firstDB, NewControlMetrics(firstDB), nil, "litestream-soak", ReplicaConfig{}, "", "")
	firstManager.syncVolumeInventory(t.Context(), time.Hour)

	secondDB := openTestDB(t)
	secondManager := NewManager(fake.client, secondDB, NewControlMetrics(secondDB), nil, "litestream-soak", ReplicaConfig{}, "", "")
	secondManager.syncVolumeInventory(t.Context(), time.Hour)

	gets, _, deletes := fake.requestCounts()
	if gets != 2 {
		t.Fatalf("volume list requests across manager restart = %d, want 2", gets)
	}
	if deletes != 1 {
		t.Fatalf("volume delete requests across manager restart = %d, want 1", deletes)
	}
	if got := len(volumeGCEventsOfType(t, secondDB, volumeGCEventDestroyRequested)); got != 0 {
		t.Fatalf("destroy requested events after manager restart = %d, want 0", got)
	}
}

func TestVolumeInventoryGCBacksOffWhenAcceptedDeleteDoesNotTransition(t *testing.T) {
	t.Parallel()

	volume := flyapi.Volume{
		ID:        "stale-volume",
		Name:      "soak_worker_main_low_vol",
		State:     "created",
		SizeGB:    10,
		CreatedAt: time.Now().UTC().Add(-3 * time.Hour),
	}
	fake := newVolumeGCInventoryTestServer(t, volume, "")
	db := openTestDB(t)
	manager := NewManager(fake.client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")

	manager.syncVolumeInventory(t.Context(), time.Hour)
	manager.syncVolumeInventory(t.Context(), time.Hour)
	manager.syncVolumeInventory(t.Context(), time.Hour)

	gets, _, deletes := fake.requestCounts()
	if gets != 3 {
		t.Fatalf("volume list requests = %d, want 3", gets)
	}
	if deletes != 1 {
		t.Fatalf("volume delete requests during backoff = %d, want 1", deletes)
	}
	stalled := volumeGCEventsOfType(t, db, volumeGCEventDestroyStalled)
	if len(stalled) != 1 {
		t.Fatalf("destroy stalled events = %d, want one refreshed operator event", len(stalled))
	}
	if !strings.Contains(stalled[0].Message, "remains created") {
		t.Fatalf("destroy stalled message = %q, want remains-created context", stalled[0].Message)
	}
}

func TestVolumeInventoryGCBackoffSurvivesManagerRestart(t *testing.T) {
	t.Parallel()

	volume := flyapi.Volume{
		ID:        "persistent-backoff-volume",
		Name:      "soak_worker_main_low_vol",
		State:     "created",
		SizeGB:    10,
		CreatedAt: time.Now().UTC().Add(-3 * time.Hour),
	}
	fake := newVolumeGCInventoryTestServer(t, volume, "")
	db := openTestDB(t)
	firstManager := NewManager(fake.client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")
	firstManager.syncVolumeInventory(t.Context(), time.Hour)

	secondManager := NewManager(fake.client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")
	secondManager.syncVolumeInventory(t.Context(), time.Hour)

	listGets, volumeGets, deletes := fake.requestCounts()
	if listGets != 2 {
		t.Fatalf("volume list requests across manager restart = %d, want 2", listGets)
	}
	if volumeGets != 0 {
		t.Fatalf("targeted volume verification requests across manager restart = %d, want 0", volumeGets)
	}
	if deletes != 1 {
		t.Fatalf("volume delete requests across manager restart = %d, want persisted backoff to keep 1", deletes)
	}
	attempts, err := db.ListVolumeGCAttempts("litestream-soak")
	if err != nil {
		t.Fatalf("ListVolumeGCAttempts() error = %v", err)
	}
	if len(attempts) != 1 || attempts[0].VolumeID != volume.ID || attempts[0].RequestCount != 1 {
		t.Fatalf("persisted attempts = %+v, want one request for %s", attempts, volume.ID)
	}
}

func TestVolumeInventoryGCSurfacesUnexpectedStaleState(t *testing.T) {
	t.Parallel()

	volume := flyapi.Volume{
		ID:        "stale-volume",
		Name:      "soak_worker_main_low_vol",
		State:     "hydrating",
		SizeGB:    10,
		CreatedAt: time.Now().UTC().Add(-3 * time.Hour),
	}
	fake := newVolumeGCInventoryTestServer(t, volume, "")
	db := openTestDB(t)
	manager := NewManager(fake.client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")
	counter := controlVolumeGCSkipped.WithLabelValues("litestream-soak", volume.Region, volume.ID, volume.Name, volume.State, "false")
	before := testutil.ToFloat64(counter)

	manager.syncVolumeInventory(t.Context(), time.Hour)
	manager.syncVolumeInventory(t.Context(), time.Hour)

	_, _, deletes := fake.requestCounts()
	if deletes != 0 {
		t.Fatalf("volume delete requests for unexpected state = %d, want 0", deletes)
	}
	events := volumeGCEventsOfType(t, db, volumeGCEventUnexpectedState)
	if len(events) != 1 {
		t.Fatalf("unexpected state events = %d, want one refreshed operator event", len(events))
	}
	if !strings.Contains(events[0].Message, "hydrating") {
		t.Fatalf("unexpected state message = %q, want observed state", events[0].Message)
	}
	if got := testutil.ToFloat64(counter) - before; got != 2 {
		t.Fatalf("volume GC skipped metric increase = %v, want 2", got)
	}
}

func TestVolumeGCRetryBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		requestCount int
		want         time.Duration
	}{
		{name: "first request", requestCount: 1, want: time.Hour},
		{name: "second request", requestCount: 2, want: 2 * time.Hour},
		{name: "fifth request", requestCount: 5, want: 16 * time.Hour},
		{name: "sixth request is capped", requestCount: 6, want: 24 * time.Hour},
		{name: "later requests remain capped", requestCount: 20, want: 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := volumeGCRetryBackoff(tt.requestCount); got != tt.want {
				t.Fatalf("volumeGCRetryBackoff(%d) = %s, want %s", tt.requestCount, got, tt.want)
			}
		})
	}
}

func TestVolumeInventoryCoalescesMonitorAndAlertRefresh(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{}, 1)
	releaseRequest := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseRequest) })
	})

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		select {
		case requestStarted <- struct{}{}:
		default:
		}
		<-releaseRequest
		_ = json.NewEncoder(w).Encode([]flyapi.Volume{{ID: "volume-main", State: "created"}})
	}))
	t.Cleanup(server.Close)

	db := openTestDB(t)
	client := flyapi.NewClientWithBaseURL("litestream-soak", "test-token", server.URL)
	manager := NewManager(client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")
	workers := []model.Worker{{
		ID:          "worker-main",
		AppName:     "litestream-soak",
		FlyVolumeID: "volume-main",
	}}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	monitorDone := make(chan struct{})
	go func() {
		manager.syncVolumeInventory(ctx, 0)
		close(monitorDone)
	}()

	select {
	case <-ctx.Done():
		t.Fatalf("volume request did not start: %v", ctx.Err())
	case <-requestStarted:
	}

	alertDone := make(chan error, 1)
	go func() {
		_, err := manager.resumableWorkers(ctx, workers, 10*time.Minute)
		alertDone <- err
	}()
	releaseOnce.Do(func() { close(releaseRequest) })

	select {
	case <-ctx.Done():
		t.Fatalf("volume monitor did not finish: %v", ctx.Err())
	case <-monitorDone:
	}
	select {
	case <-ctx.Done():
		t.Fatalf("dormant alert inventory did not finish: %v", ctx.Err())
	case err := <-alertDone:
		if err != nil {
			t.Fatalf("resumableWorkers() error = %v", err)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("volume list requests = %d, want 1", got)
	}
}

func TestVolumeInventoryGCFreshReadDoesNotJoinAlertRefresh(t *testing.T) {
	t.Parallel()

	alertRequestStarted := make(chan struct{}, 1)
	freshRequestStarted := make(chan struct{}, 1)
	releaseAlertRequest := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseAlertRequest) })
	})

	staleVolume := flyapi.Volume{
		ID:        "stale-volume",
		Name:      "soak_worker_main_low_vol",
		SizeGB:    10,
		State:     "created",
		CreatedAt: time.Now().Add(-3 * time.Hour),
	}
	var gets atomic.Int64
	var deletes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}

		switch gets.Add(1) {
		case 1:
			alertRequestStarted <- struct{}{}
			<-releaseAlertRequest
			_ = json.NewEncoder(w).Encode([]flyapi.Volume{staleVolume})
		case 2:
			freshRequestStarted <- struct{}{}
			_ = json.NewEncoder(w).Encode([]flyapi.Volume{})
		default:
			http.Error(w, "unexpected volume list request", http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)

	db := openTestDB(t)
	client := flyapi.NewClientWithBaseURL("litestream-soak", "test-token", server.URL)
	manager := NewManager(client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")
	workers := []model.Worker{{
		ID:          "worker-main",
		AppName:     "litestream-soak",
		FlyVolumeID: staleVolume.ID,
	}}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	alertDone := make(chan error, 1)
	go func() {
		_, err := manager.resumableWorkers(ctx, workers, 10*time.Minute)
		alertDone <- err
	}()

	select {
	case <-ctx.Done():
		t.Fatalf("alert volume request did not start: %v", ctx.Err())
	case <-alertRequestStarted:
	}

	monitorDone := make(chan struct{})
	go func() {
		manager.syncVolumeInventory(ctx, time.Hour)
		close(monitorDone)
	}()

	select {
	case <-ctx.Done():
		t.Fatalf("fresh GC volume request did not start: %v", ctx.Err())
	case <-freshRequestStarted:
	}
	select {
	case <-ctx.Done():
		t.Fatalf("volume monitor did not finish: %v", ctx.Err())
	case <-monitorDone:
	}

	if got := gets.Load(); got != 2 {
		t.Fatalf("volume list requests = %d, want separate alert and fresh GC requests", got)
	}
	if got := deletes.Load(); got != 0 {
		t.Fatalf("volume delete requests = %d, want 0 from in-flight alert inventory", got)
	}

	releaseOnce.Do(func() { close(releaseAlertRequest) })
	select {
	case <-ctx.Done():
		t.Fatalf("alert inventory did not finish: %v", ctx.Err())
	case err := <-alertDone:
		if err != nil {
			t.Fatalf("resumableWorkers() error = %v", err)
		}
	}

	resumable, err := manager.resumableWorkers(ctx, workers, 10*time.Minute)
	if err != nil {
		t.Fatalf("resumableWorkers() after refresh error = %v", err)
	}
	if len(resumable) != 0 {
		t.Fatalf("resumableWorkers() after refresh = %+v, want newer empty inventory", resumable)
	}
	if got := gets.Load(); got != 2 {
		t.Fatalf("volume list requests after cached read = %d, want 2", got)
	}
}

func TestVolumeInventoryGCRequiresFreshInventory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		refreshStatus int
	}{
		{name: "cached inventory is not reused", refreshStatus: http.StatusOK},
		{name: "failed refresh does not fall back to cache", refreshStatus: http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gets atomic.Int64
			var deletes atomic.Int64
			staleVolume := flyapi.Volume{
				ID:        "stale-volume",
				Name:      "soak_worker_main_low_vol",
				SizeGB:    10,
				State:     "created",
				CreatedAt: time.Now().Add(-3 * time.Hour),
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					request := gets.Add(1)
					if request == 1 {
						_ = json.NewEncoder(w).Encode([]flyapi.Volume{staleVolume})
						return
					}
					if tt.refreshStatus != http.StatusOK {
						http.Error(w, "Fly API unavailable", tt.refreshStatus)
						return
					}
					_ = json.NewEncoder(w).Encode([]flyapi.Volume{})
				case http.MethodDelete:
					deletes.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			db := openTestDB(t)
			client := flyapi.NewClientWithBaseURL("litestream-soak", "test-token", server.URL)
			manager := NewManager(client, db, NewControlMetrics(db), nil, "litestream-soak", ReplicaConfig{}, "", "")
			workers := []model.Worker{{
				ID:          "worker-main",
				AppName:     "litestream-soak",
				FlyVolumeID: staleVolume.ID,
			}}
			if _, err := manager.resumableWorkers(t.Context(), workers, 10*time.Minute); err != nil {
				t.Fatalf("resumableWorkers() error = %v", err)
			}

			manager.syncVolumeInventory(t.Context(), time.Hour)

			if got := gets.Load(); got != 2 {
				t.Fatalf("volume list requests = %d, want forced fresh request after cached alert inventory", got)
			}
			if got := deletes.Load(); got != 0 {
				t.Fatalf("volume delete requests = %d, want 0 without fresh stale inventory", got)
			}
		})
	}
}

type volumeGCInventoryTestServer struct {
	client *flyapi.Client
	server *httptest.Server

	mu               sync.Mutex
	volume           *flyapi.Volume
	listVolume       bool
	emptyList        bool
	emptyVolume      bool
	deleteTransition string
	listGets         int
	volumeGets       int
	deletes          int
}

func newVolumeGCInventoryTestServer(t *testing.T, volume flyapi.Volume, deleteTransition string) *volumeGCInventoryTestServer {
	t.Helper()

	fake := &volumeGCInventoryTestServer{
		volume:           &volume,
		listVolume:       true,
		deleteTransition: deleteTransition,
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)
	fake.client = flyapi.NewClientWithBaseURL("litestream-soak", "test-token", fake.server.URL)
	return fake
}

func (f *volumeGCInventoryTestServer) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/apps/litestream-soak/volumes":
		f.listGets++
		if f.emptyList {
			return
		}
		if f.volume == nil || !f.listVolume {
			_ = json.NewEncoder(w).Encode([]flyapi.Volume{})
			return
		}
		_ = json.NewEncoder(w).Encode([]flyapi.Volume{*f.volume})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apps/litestream-soak/volumes/"):
		f.volumeGets++
		if f.emptyVolume {
			return
		}
		if f.volume == nil {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(f.volume)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/apps/litestream-soak/volumes/"):
		f.deletes++
		if f.volume != nil && f.deleteTransition != "" {
			f.volume.State = f.deleteTransition
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *volumeGCInventoryTestServer) requestCounts() (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listGets, f.volumeGets, f.deletes
}

func (f *volumeGCInventoryTestServer) hideVolumeFromList() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listVolume = false
}

func (f *volumeGCInventoryTestServer) returnEmptyInventoryResponses() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.emptyList = true
	f.emptyVolume = true
}

func (f *volumeGCInventoryTestServer) removeVolume() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volume = nil
}

func volumeGCEventsOfType(t *testing.T, db *model.DB, eventType string) []model.Event {
	t.Helper()

	events, err := db.ListEvents(100)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	matches := make([]model.Event, 0)
	for _, event := range events {
		if event.EventType == eventType {
			matches = append(matches, event)
		}
	}
	return matches
}
