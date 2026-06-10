package orchestrator

import (
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
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
