package worker

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheProbeSeedsAndBumpsStripes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	for i := 0; i < 2; i++ {
		if err := ensureCacheProbe(ctx, path); err != nil {
			t.Fatalf("ensureCacheProbe() attempt %d error = %v", i+1, err)
		}
	}
	queries, err := primaryReadQueries(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 || queries[0].statement != cacheProbeQuery || queries[0].primary != "2048|0|0|0" || queries[1].name != cacheProbeTable+" count" || queries[1].primary != "2048" {
		t.Fatalf("queries = %+v", queries)
	}
	if err := bumpCacheProbe(ctx, path, 3); err != nil {
		t.Fatal(err)
	}
	if err := bumpCacheProbe(ctx, path, 3+cacheProbeStripes); err != nil {
		t.Fatal(err)
	}
	if err := bumpCacheProbe(ctx, path, 4); err != nil {
		t.Fatal(err)
	}
	queries, err = primaryReadQueries(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := parseVFSProbe(queries[0].primary)
	if err != nil {
		t.Fatal(err)
	}
	if probe.count != cacheProbeRows || probe.min != 0 || probe.max != 2 || probe.sum != 3*cacheProbeRows/cacheProbeStripes {
		t.Fatalf("probe after bumps = %+v", probe)
	}
}

func TestCacheProbeWriterPausesWithTheLoad(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	if err := ensureCacheProbe(ctx, path); err != nil {
		t.Fatal(err)
	}
	writer := newCacheProbeWriter(path, 5*time.Millisecond)
	if err := writer.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	writer.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	paused, err := primaryReadQueries(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if paused[0].primary != "2048|0|0|0" {
		t.Fatalf("paused writer advanced the probe: %s", paused[0].primary)
	}
	writer.Resume()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resumed, err := primaryReadQueries(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if resumed[0].primary != "2048|0|0|0" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resumed writer never advanced the probe")
		}
		time.Sleep(5 * time.Millisecond)
	}
	writer.Stop()
}

func TestCacheProbeWriterPauseWaitsForInFlightWrite(t *testing.T) {
	ctx := context.Background()
	writer := newCacheProbeWriter(filepath.Join(t.TempDir(), "test.db"), time.Millisecond)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var writes atomic.Int32
	writer.bump = func(context.Context, string, int) error {
		if writes.Add(1) == 1 {
			started <- struct{}{}
			<-release
		}
		return nil
	}
	writer.Start(ctx)
	defer writer.Stop()
	<-started

	paused := make(chan struct{})
	go func() {
		if err := writer.Pause(ctx); err != nil {
			t.Error(err)
		}
		close(paused)
	}()
	select {
	case <-paused:
		t.Fatal("Pause returned while a probe write was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("Pause did not return once the in-flight write finished")
	}
	committed := writes.Load()
	time.Sleep(30 * time.Millisecond)
	if writes.Load() != committed {
		t.Fatalf("probe writes continued after Pause returned: %d -> %d", committed, writes.Load())
	}
}
