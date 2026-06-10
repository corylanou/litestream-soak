package orchestrator

import (
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestSourcePRNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		want   int
	}{
		{source: "main", want: 0},
		{source: "pr-1221", want: 1221},
		{source: "pr-0", want: 0},
		{source: "pr-nope", want: 0},
	}

	for _, tc := range tests {
		if got := sourcePRNumber(tc.source); got != tc.want {
			t.Fatalf("sourcePRNumber(%q) = %d, want %d", tc.source, got, tc.want)
		}
	}
}

func TestWorkerNameForSource(t *testing.T) {
	t.Parallel()

	if got := workerNameForSource("main", "worker-main-low-vol"); got != "worker-main-low-vol" {
		t.Fatalf("workerNameForSource(main) = %q", got)
	}
	if got := workerNameForSource("pr-1221", "worker-main-low-vol"); got != "worker-pr-1221-low-vol" {
		t.Fatalf("workerNameForSource(pr-1221) = %q, want worker-pr-1221-low-vol", got)
	}
}

func TestDefaultFleetForSource(t *testing.T) {
	t.Parallel()

	spec := DefaultFleetForSource("pr-1221", "soak-sha", "litestream-sha")
	if len(spec.Workers) == 0 {
		t.Fatal("DefaultFleetForSource() returned no workers")
	}

	first := spec.Workers[0]
	if first.Source != "pr-1221" {
		t.Fatalf("first.Source = %q, want pr-1221", first.Source)
	}
	if first.GitSHA != "soak-sha" {
		t.Fatalf("first.GitSHA = %q, want soak-sha", first.GitSHA)
	}
	if first.LitestreamSHA != "litestream-sha" {
		t.Fatalf("first.LitestreamSHA = %q, want litestream-sha", first.LitestreamSHA)
	}
	if first.PRNumber != 1221 {
		t.Fatalf("first.PRNumber = %d, want 1221", first.PRNumber)
	}
	if first.Name != "worker-pr-1221-low-vol" {
		t.Fatalf("first.Name = %q, want worker-pr-1221-low-vol", first.Name)
	}

	volumeSizes := map[string]int{}
	for _, worker := range spec.Workers {
		if worker.VolumeSizeGB != 0 {
			volumeSizes[worker.ProfileName] = worker.VolumeSizeGB
		}
		if worker.VolumeSizeGB != 0 && worker.Workload.VolumeSizeGB != worker.VolumeSizeGB {
			t.Fatalf("%s Workload.VolumeSizeGB = %d, want %d", worker.ProfileName, worker.Workload.VolumeSizeGB, worker.VolumeSizeGB)
		}
	}
	if got := volumeSizes["high-volume"]; got != 100 {
		t.Fatalf("high-volume VolumeSizeGB = %d, want 100", got)
	}
	if got := volumeSizes["burst-volume"]; got != 100 {
		t.Fatalf("burst-volume VolumeSizeGB = %d, want 100", got)
	}
	if got := volumeSizes["gharchive-replay"]; got != 50 {
		t.Fatalf("gharchive-replay VolumeSizeGB = %d, want 50", got)
	}
	if got := volumeSizes["gharchive-mixed"]; got != 50 {
		t.Fatalf("gharchive-mixed VolumeSizeGB = %d, want 50", got)
	}
	if desired, ok := defaultFleetDesiredWorker("pr-1221", "worker-pr-1221-high-vol", "worker-pr-1221-high-vol"); !ok {
		t.Fatal("defaultFleetDesiredWorker() missing PR high-volume worker")
	} else if desired.Name != "worker-pr-1221-high-vol" {
		t.Fatalf("desired.Name = %q, want worker-pr-1221-high-vol", desired.Name)
	}
}

func TestResolveWorkerVolumeSizeUsesDefaultFleetForRollouts(t *testing.T) {
	t.Parallel()

	worker := model.Worker{
		ID:            "worker-pr-1228-high-vol",
		Name:          "worker-pr-1228-high-vol",
		Source:        "pr-1228",
		ProfileName:   "high-volume",
		ProfileConfig: workload.Config{LoadMode: "synthetic"}.JSON(),
	}

	parsedCfg, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		t.Fatalf("ParseConfig(%q) error = %v, want nil", worker.ProfileConfig, err)
	}
	if got := resolveWorkerVolumeSize(worker, normalizeWorkload(parsedCfg)); got != 100 {
		t.Fatalf("resolveWorkerVolumeSize() = %d, want 100", got)
	}
}
