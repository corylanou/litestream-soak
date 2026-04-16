package orchestrator

import "testing"

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
}
