package orchestrator

import (
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestResolveWorkerWorkload_CorruptConfig(t *testing.T) {
	t.Parallel()

	worker := model.Worker{
		ID:            "worker-test-corrupt",
		Name:          "worker-test-corrupt",
		Source:        "pr-9999",
		ProfileName:   "low-volume",
		ProfileConfig: `{"write_rate":`,
	}

	got := resolveWorkerWorkload(worker)
	want := normalizeWorkload(workload.Config{})
	if got != want {
		t.Fatalf("resolveWorkerWorkload(corrupt config) = %+v, want %+v", got, want)
	}
}

func TestResolveWorkerWorkload_ValidConfig(t *testing.T) {
	t.Parallel()

	worker := model.Worker{
		ID:            "worker-test-valid",
		Name:          "worker-test-valid",
		Source:        "pr-9999",
		ProfileName:   "low-volume",
		ProfileConfig: `{"write_rate":50,"load_mode":"synthetic"}`,
	}

	got := resolveWorkerWorkload(worker)
	if got.WriteRate != 50 {
		t.Fatalf("resolveWorkerWorkload(valid config) WriteRate = %d, want 50", got.WriteRate)
	}
	if got.LoadMode != "synthetic" {
		t.Fatalf("resolveWorkerWorkload(valid config) LoadMode = %q, want synthetic", got.LoadMode)
	}
}
