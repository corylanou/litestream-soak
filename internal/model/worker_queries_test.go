package model

import (
	"path/filepath"
	"testing"
)

func makeTestWorker(id, source string, status WorkerStatus) *Worker {
	return &Worker{
		ID:            id,
		Name:          id,
		Status:        status,
		Source:        source,
		GitSHA:        "abc123",
		LitestreamSHA: "ls123",
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	}
}

func TestListWorkersFilteredQueryAssembly(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name   string
		status string
		source string
		want   []string
		notIn  []string
	}

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	workers := []*Worker{
		makeTestWorker("w-main-running", "main", WorkerRunning),
		makeTestWorker("w-main-stopped", "main", WorkerStopped),
		makeTestWorker("w-pr-running", "pr-42", WorkerRunning),
		makeTestWorker("w-pr-degraded", "pr-42", WorkerDegraded),
	}
	for _, w := range workers {
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker(%s) error = %v", w.ID, err)
		}
	}

	cases := []testCase{
		{
			name:   "no filters returns all",
			status: "",
			source: "",
			want:   []string{"w-main-running", "w-main-stopped", "w-pr-running", "w-pr-degraded"},
		},
		{
			name:   "status only",
			status: "running",
			source: "",
			want:   []string{"w-main-running", "w-pr-running"},
			notIn:  []string{"w-main-stopped", "w-pr-degraded"},
		},
		{
			name:   "source only",
			status: "",
			source: "main",
			want:   []string{"w-main-running", "w-main-stopped"},
			notIn:  []string{"w-pr-running", "w-pr-degraded"},
		},
		{
			name:   "status and source",
			status: "running",
			source: "pr-42",
			want:   []string{"w-pr-running"},
			notIn:  []string{"w-main-running", "w-main-stopped", "w-pr-degraded"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := db.ListWorkersFiltered(tc.status, tc.source)
			if err != nil {
				t.Fatalf("ListWorkersFiltered(%q, %q) error = %v", tc.status, tc.source, err)
			}

			ids := make(map[string]bool, len(got))
			for _, w := range got {
				ids[w.ID] = true
			}
			for _, id := range tc.want {
				if !ids[id] {
					t.Errorf("want worker %q in result, not found; got IDs: %v", id, ids)
				}
			}
			for _, id := range tc.notIn {
				if ids[id] {
					t.Errorf("worker %q should not be in result, but found; got IDs: %v", id, ids)
				}
			}
		})
	}
}

func TestListDormantWorkersQueryAssembly(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	workers := []*Worker{
		makeTestWorker("w-main-dormant", "main", WorkerRunning),
		makeTestWorker("w-pr-dormant", "pr-99", WorkerRunning),
		makeTestWorker("w-main-running", "main", WorkerRunning),
	}
	for _, w := range workers {
		if err := db.CreateWorker(w); err != nil {
			t.Fatalf("CreateWorker(%s) error = %v", w.ID, err)
		}
	}
	if err := db.MarkWorkerDormant("w-main-dormant", "idle", "sig1", "probe"); err != nil {
		t.Fatalf("MarkWorkerDormant(w-main-dormant) error = %v", err)
	}
	if err := db.MarkWorkerDormant("w-pr-dormant", "idle", "sig2", "probe"); err != nil {
		t.Fatalf("MarkWorkerDormant(w-pr-dormant) error = %v", err)
	}

	t.Run("no source filter returns all dormant", func(t *testing.T) {
		t.Parallel()

		got, err := db.ListDormantWorkers("")
		if err != nil {
			t.Fatalf("ListDormantWorkers(\"\") error = %v", err)
		}
		ids := make(map[string]bool, len(got))
		for _, w := range got {
			ids[w.ID] = true
		}
		if !ids["w-main-dormant"] {
			t.Errorf("want w-main-dormant in result, not found")
		}
		if !ids["w-pr-dormant"] {
			t.Errorf("want w-pr-dormant in result, not found")
		}
		if ids["w-main-running"] {
			t.Errorf("w-main-running should not be in dormant results")
		}
	})

	t.Run("source filter returns only matching dormant", func(t *testing.T) {
		t.Parallel()

		got, err := db.ListDormantWorkers("main")
		if err != nil {
			t.Fatalf("ListDormantWorkers(\"main\") error = %v", err)
		}
		ids := make(map[string]bool, len(got))
		for _, w := range got {
			ids[w.ID] = true
		}
		if !ids["w-main-dormant"] {
			t.Errorf("want w-main-dormant in result, not found")
		}
		if ids["w-pr-dormant"] {
			t.Errorf("w-pr-dormant should not be in result when filtering by main source")
		}
		if ids["w-main-running"] {
			t.Errorf("w-main-running should not be in dormant results")
		}
	})
}
