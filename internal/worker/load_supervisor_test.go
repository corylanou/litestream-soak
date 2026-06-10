package worker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func shellCommandSupervisor(t *testing.T, starts *atomic.Int64, script string) *loadSupervisor {
	t.Helper()

	sup := newLoadSupervisor(func(ctx context.Context) (*exec.Cmd, error) {
		if starts != nil {
			starts.Add(1)
		}
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", script)
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return cmd, nil
	})
	sup.initialBackoff = time.Hour
	sup.maxBackoff = time.Hour
	sup.healthyReset = time.Hour
	return sup
}

func supervisorExited(sup *loadSupervisor) bool {
	sup.mu.Lock()
	defer sup.mu.Unlock()
	return sup.exited
}

func TestLoadSupervisorReapsExitedProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell subprocess requires Unix")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sup := shellCommandSupervisor(t, nil, "exit 0")
	if err := sup.start(ctx); err != nil {
		t.Fatalf("start() error = %v", err)
	}

	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool {
		return supervisorExited(sup)
	}) {
		t.Fatal("supervisor never observed process exit")
	}

	sup.mu.Lock()
	state := sup.cmd.ProcessState
	sup.mu.Unlock()
	if state == nil {
		t.Fatal("expected ProcessState after reap, got nil (process not waited)")
	}
}

func TestLoadSupervisorRestartsWithBackoff(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell subprocess requires Unix")
	}

	cfg := DefaultConfig()
	cfg.WorkerID = "test-load-restart"
	cfg.ProfileName = "test-profile"
	cfg.Source = "test"
	SetWorkerInfo(cfg)

	restartCounter := loadRestarts.WithLabelValues(cfg.WorkerID, cfg.ProfileName, cfg.Source, "synthetic")
	before := testutil.ToFloat64(restartCounter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var starts atomic.Int64
	sup := shellCommandSupervisor(t, &starts, "exit 0")
	sup.initialBackoff = 5 * time.Millisecond
	sup.maxBackoff = 20 * time.Millisecond

	if err := sup.start(ctx); err != nil {
		t.Fatalf("start() error = %v", err)
	}

	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool {
		return starts.Load() >= 2
	}) {
		t.Fatalf("starts = %d, want >= 2 (no restart happened)", starts.Load())
	}
	cancel()

	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool {
		return testutil.ToFloat64(restartCounter)-before >= 1
	}) {
		t.Fatalf("soak_load_restarts_total delta = %v, want >= 1", testutil.ToFloat64(restartCounter)-before)
	}
}

func TestLoadSupervisorPauseBlocksRestarts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell subprocess requires Unix")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var starts atomic.Int64
	sup := shellCommandSupervisor(t, &starts, "exit 0")
	sup.initialBackoff = 300 * time.Millisecond
	sup.maxBackoff = 600 * time.Millisecond

	if err := sup.start(ctx); err != nil {
		t.Fatalf("start() error = %v", err)
	}

	if !waitUntil(5*time.Second, 5*time.Millisecond, func() bool {
		return supervisorExited(sup)
	}) {
		t.Fatal("supervisor never observed process exit")
	}

	if err := sup.Pause(context.Background()); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	time.Sleep(700 * time.Millisecond)
	if got := starts.Load(); got != 1 {
		t.Fatalf("starts while paused = %d, want 1", got)
	}

	sup.Resume()
	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool {
		return starts.Load() >= 2
	}) {
		t.Fatalf("starts after resume = %d, want >= 2", starts.Load())
	}
}

func TestLoadSupervisorPauseToleratesExitedProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell subprocess requires Unix")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sup := shellCommandSupervisor(t, nil, "exit 0")
	if err := sup.start(ctx); err != nil {
		t.Fatalf("start() error = %v", err)
	}

	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool {
		return supervisorExited(sup)
	}) {
		t.Fatal("supervisor never observed process exit")
	}

	if err := sup.Pause(context.Background()); err != nil {
		t.Fatalf("Pause() on exited process error = %v, want nil", err)
	}
}

func TestLoadSupervisorPauseStopsProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell subprocess requires Unix")
	}

	outPath := filepath.Join(t.TempDir(), "out")
	script := `while true; do echo x >> ` + outPath + `; sleep 0.05; done`

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sup := shellCommandSupervisor(t, nil, script)
	if err := sup.start(ctx); err != nil {
		t.Fatalf("start() error = %v", err)
	}
	defer sup.Resume()

	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool {
		info, err := os.Stat(outPath)
		return err == nil && info.Size() > 0
	}) {
		t.Fatal("load process never wrote output")
	}

	if err := sup.Pause(context.Background()); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	sizeAfterPause := fileSize(outPath)
	time.Sleep(200 * time.Millisecond)
	if got := fileSize(outPath); got != sizeAfterPause {
		t.Fatalf("file grew while paused: %d -> %d", sizeAfterPause, got)
	}

	sup.Resume()
	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool {
		return fileSize(outPath) > sizeAfterPause
	}) {
		t.Fatal("file did not grow after resume")
	}
}

func TestLoadSupervisorStopWhilePausedReturns(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signal handling requires Unix")
	}

	sup := newLoadSupervisor(func(ctx context.Context) (*exec.Cmd, error) {
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "trap 'exit 0' INT; while true; do sleep 0.05; done")
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return cmd, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sup.start(ctx); err != nil {
		t.Fatalf("start() error = %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if err := sup.Pause(context.Background()); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}

	stopDone := make(chan struct{})
	go func() {
		sup.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return while process was paused")
	}
}
