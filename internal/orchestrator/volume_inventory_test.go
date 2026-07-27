package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
)

func TestStaleUnattachedWorkerVolumes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	volumes := []flyapi.Volume{
		{ID: "old-worker", Name: "soak_worker_main_low_vol", SizeGB: 10, CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "fresh-worker", Name: "soak_worker_main_high_vol", SizeGB: 100, CreatedAt: now.Add(-30 * time.Minute)},
		{ID: "attached-worker", Name: "soak_worker_main_burst_vol", SizeGB: 100, AttachedMachineID: "machine", CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "non-worker", Name: "soakctl_data", SizeGB: 1, CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "unknown-created", Name: "soak_worker_main_read_heavy", SizeGB: 10},
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
		{ID: "old-worker", Name: "soak_worker_main_low_vol", CreatedAt: now.Add(-3 * time.Hour)},
	}

	if stale := staleUnattachedWorkerVolumes(volumes, now, 0); len(stale) != 0 {
		t.Fatalf("len(stale) = %d, want 0", len(stale))
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
	start := make(chan struct{})
	monitorDone := make(chan struct{})
	alertDone := make(chan error, 1)
	go func() {
		<-start
		manager.syncVolumeInventory(ctx, 0)
		close(monitorDone)
	}()
	go func() {
		<-start
		_, err := manager.resumableWorkers(ctx, workers, 10*time.Minute)
		alertDone <- err
	}()
	close(start)

	select {
	case <-ctx.Done():
		t.Fatalf("volume request did not start: %v", ctx.Err())
	case <-requestStarted:
	}
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
