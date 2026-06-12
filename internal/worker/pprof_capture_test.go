package worker

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestPprofCapturerSkipsSingleDBProfiles(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join(t.TempDir(), "litestream.sock")

	done := make(chan struct{})
	go func() {
		newPprofCapturer(&cfg).Run(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pprof capturer did not return for single-DB config")
	}

	if _, err := os.Stat(filepath.Join(cfg.DataDir, "profiles")); !os.IsNotExist(err) {
		t.Fatalf("profiles dir stat error = %v, want not exists", err)
	}
}

func TestPprofCapturerPrunesOldLocalProfiles(t *testing.T) {
	cfg := DefaultConfig()
	capturer := newPprofCapturer(&cfg)
	dir := t.TempDir()
	names := []string{
		"20260101T000000Z_baseline_heap.pprof",
		"20260101T010000Z_hourly_heap.pprof",
		"20260101T020000Z_hourly_heap.pprof",
		"20260101T030000Z_hourly_heap.pprof",
		"20260101T040000Z_hourly_heap.pprof",
	}
	for i, name := range names {
		target := filepath.Join(dir, name)
		if err := os.WriteFile(target, []byte("profile"), 0o644); err != nil {
			t.Fatal(err)
		}
		modTime := time.Unix(int64(i), 0)
		if err := os.Chtimes(target, modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}

	capturer.pruneLocalProfiles(dir, 2)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	slices.Sort(got)

	want := []string{
		"20260101T000000Z_baseline_heap.pprof",
		"20260101T030000Z_hourly_heap.pprof",
		"20260101T040000Z_hourly_heap.pprof",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("remaining profiles = %v, want %v", got, want)
	}
}
