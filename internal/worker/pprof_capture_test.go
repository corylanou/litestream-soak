package worker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

func TestPprofCapturerCapturesMemStatsAsText(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join("/tmp", fmt.Sprintf("litestream-soak-memstats-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socketPath) })

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/heap", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("debug") == "1" {
			_, _ = io.WriteString(w, "heap profile: 0: 0 [0: 0]\n\n# runtime.MemStats\n# StackInuse = 5668864\n# StackSys = 5668864\n")
			return
		}
		_, _ = io.WriteString(w, "binary-heap-profile")
	})
	mux.HandleFunc("/debug/pprof/allocs", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "binary-allocs-profile")
	})
	mux.HandleFunc("/debug/pprof/goroutine", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "goroutine profile")
	})
	startTestUnixServer(t, socketPath, mux)

	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.SocketPath = socketPath

	newPprofCapturer(&cfg).captureSet(context.Background(), "baseline")

	entries, err := os.ReadDir(filepath.Join(dir, "profiles"))
	if err != nil {
		t.Fatalf("read profiles dir: %v", err)
	}
	var memstatsFile, heapFile string
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasSuffix(name, "_memstats.txt"):
			memstatsFile = name
		case strings.HasSuffix(name, "_heap.pprof"):
			heapFile = name
		}
	}
	if heapFile == "" {
		t.Fatalf("expected a binary heap.pprof capture, got entries: %v", entries)
	}
	if memstatsFile == "" {
		t.Fatalf("expected a memstats.txt capture, got entries: %v", entries)
	}
	body, err := os.ReadFile(filepath.Join(dir, "profiles", memstatsFile))
	if err != nil {
		t.Fatalf("read memstats capture: %v", err)
	}
	if !strings.Contains(string(body), "StackInuse") {
		t.Fatalf("memstats capture missing StackInuse, got: %q", body)
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
