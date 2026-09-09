package upgrade

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAgeRequiresObservedMaintenanceAndChurn(t *testing.T) {
	good := Age{Snapshots: 2, Compactions: 2, RetentionDeleted: 2, Updates: 2, Deletes: 2}
	if !good.Ready() {
		t.Fatal("observed fixture not aged")
	}
	for _, field := range []string{"snapshot", "compaction", "retention", "update", "delete"} {
		t.Run(field, func(t *testing.T) {
			a := good
			switch field {
			case "snapshot":
				a.Snapshots = 0
			case "compaction":
				a.Compactions = 0
			case "retention":
				a.RetentionDeleted = 0
			case "update":
				a.Updates = 0
			case "delete":
				a.Deletes = 0
			}
			if a.Ready() {
				t.Fatal("unobserved age passed")
			}
		})
	}
}

func TestCopyStatePreservesHistoryWithoutSharingFiles(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	for _, name := range []string{"db", ".db-litestream/meta", "replica/ltx/old"} {
		path := filepath.Join(source, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("known-bad"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, arm := range []string{"baseline", "candidate"} {
		if err := copyState(source, filepath.Join(root, arm)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "candidate/replica/ltx/old"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, arm := range []string{"source", "baseline"} {
		b, err := os.ReadFile(filepath.Join(root, arm, "replica/ltx/old"))
		if err != nil || string(b) != "known-bad" {
			t.Fatalf("%s changed: %q %v", arm, b, err)
		}
	}
	if err := copyState(source, filepath.Join(root, "baseline")); err == nil {
		t.Fatal("overwrote existing destination")
	}
	if err := os.Symlink(source, filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	if err := copyState(source, filepath.Join(root, "unsafe")); err == nil {
		t.Fatal("copied symlink")
	}
}

func TestMaintenanceCountsOnlyPositiveDeletions(t *testing.T) {
	a := observedAge([]byte("msg=\"snapshot complete\"\nmsg=\"compaction complete\"\nmsg=\"l0 retention enforced\" deleted_count=0\nmsg=\"l0 retention enforced\" deleted_count=3\n"))
	if a.Snapshots != 1 || a.Compactions != 1 || a.RetentionDeleted != 3 {
		t.Fatalf("age = %+v", a)
	}
}

func TestResultNeverErasesEarlierFailure(t *testing.T) {
	r := Result{Checks: []Check{{Name: "pre", Status: "failed", Error: "corrupt"}, {Name: "post", Status: "passed"}}}
	if r.Verdict() != "failed" {
		t.Fatal(r.Verdict())
	}
	r.Checks[0].Status = "unsupported"
	if r.Verdict() != "inconclusive" {
		t.Fatal(r.Verdict())
	}
	r.Checks[0].Status = "unexecuted"
	if r.Verdict() != "inconclusive" {
		t.Fatal(r.Verdict())
	}
}

func TestRunRejectsUnpinnedBinaryBeforeCreatingOutput(t *testing.T) {
	out := filepath.Join(t.TempDir(), "run")
	_, err := Run(context.Background(), Config{Mode: "persistent-upgrade", Output: out, Baseline: Binary{Path: os.Args[0]}, Candidate: Binary{Path: os.Args[0]}})
	if err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("error = %v", err)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("created output for invalid inputs")
	}
}

func TestPinnedBinaryLifecycle(t *testing.T) {
	path := os.Getenv("UPGRADE_TEST_BINARY")
	if path == "" {
		t.Skip("set UPGRADE_TEST_BINARY for real pinned binary lifecycle")
	}
	hash, err := fileHash(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"fresh-start", "persistent-upgrade"} {
		t.Run(mode, func(t *testing.T) {
			r, err := Run(context.Background(), Config{Mode: mode, Output: filepath.Join(t.TempDir(), "run"), Baseline: Binary{Path: path, SHA256: hash}, Candidate: Binary{Path: path, SHA256: hash}, Transition: "ltx-v1", Rollback: true, AgeTimeout: 45 * time.Second, ContinueFor: 2 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if r.Verdict() != "passed" {
				t.Fatalf("result = %+v", r)
			}
			if mode == "persistent-upgrade" && !r.Age.Ready() {
				t.Fatal("fixture not aged")
			}
		})
	}
}

func TestSealedFixtureRejectsChangedHistory(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "fixture")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := seed(state); err != nil {
		t.Fatal(err)
	}
	a := Age{Snapshots: 2, Compactions: 2, RetentionDeleted: 1, Updates: 1, Deletes: 1}
	if err := sealFixture(state, a, "baseline", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := importFixture(state, filepath.Join(root, "copy"), "baseline"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "new-history"), []byte("mutation"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := importFixture(state, filepath.Join(root, "changed"), "baseline"); err == nil {
		t.Fatal("accepted modified fixture")
	}
}

func TestCopyStateRejectsNestedDestination(t *testing.T) {
	source := t.TempDir()
	if err := copyState(source, filepath.Join(source, "nested")); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("expected overlap rejection, got %v", err)
	}
}
