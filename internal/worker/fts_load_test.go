package worker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestFTSLoadPauseAndProfiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "fts.db")
	cfg.WriteRate = 1000
	load := newFTSLoad(&cfg)
	seen := make(chan string, 64)
	load.capture = func(_ context.Context, phase string) { seen <- phase }
	failure := make(chan error, 1)
	if err := load.Start(ctx, func(err error) { failure <- err }); err != nil {
		t.Fatal(err)
	}
	defer load.Stop()
	for {
		select {
		case phase := <-seen:
			if phase == "fts-optimize-after" {
				goto paused
			}
		case err := <-failure:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
paused:
	if err := load.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	var before, after int64
	if err := load.db.QueryRow("SELECT step FROM fts_progress").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := load.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if err := load.db.QueryRow("SELECT step FROM fts_progress").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("paused progress %d -> %d", before, after)
	}
	load.Resume()
	load.Stop()
}

func TestFTSLoadFailureIsTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "fts.db")
	cfg.WriteRate = 1000
	load := newFTSLoad(&cfg)
	failure := make(chan error, 1)
	load.capture = func(_ context.Context, phase string) {
		if phase == "fts-insert-before" {
			_, _ = load.db.Exec("DROP TABLE fts_documents")
		}
	}
	if err := load.Start(ctx, func(err error) { failure <- err }); err != nil {
		t.Fatal(err)
	}
	defer load.Stop()
	select {
	case err := <-failure:
		if err == nil {
			t.Fatal("failure missing")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := load.Pause(ctx); err == nil {
		t.Fatal("failed load pause passed")
	}
}

func TestFTSLoadRejectsCanceledPause(t *testing.T) {
	cfg := DefaultConfig()
	load := newFTSLoad(&cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := load.Pause(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pause=%v", err)
	}
}

func TestRunnerFTSPopulateAndStart(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.DBPath = filepath.Join(cfg.DataDir, "fts.db")
	cfg.LoadMode = "fts"
	cfg.WriteRate = 1000
	cfg.PprofCaptureEnabled = false
	runner := NewRunner(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runner.populate(ctx); err != nil {
		t.Fatal(err)
	}
	load, err := runner.startFTS(ctx, func(err error) { t.Errorf("workload failure: %v", err) })
	if err != nil {
		t.Fatal(err)
	}
	defer load.Stop()
	if err := load.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := readLogicalSnapshot(ctx, cfg.DBPath, cfg.logicalLimits()); err != nil {
		t.Fatal(err)
	}
}

func TestFTSUnavailable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.DBPath = cfg.DataDir
	cfg.LoadMode = "fts"
	load := newFTSLoad(&cfg)
	if err := load.Start(context.Background(), func(error) {}); err == nil {
		t.Fatal("directory accepted as database")
	}
	if err := NewRunner(cfg).populate(context.Background()); err == nil {
		t.Fatal("populate failure passed")
	}
	cfg.WriteRate = 0
	if err := newFTSLoad(&cfg).Start(context.Background(), func(error) {}); err == nil {
		t.Fatal("zero rate accepted")
	}
}
