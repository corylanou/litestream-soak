package worker

import (
	"context"
	"testing"
	"time"
)

func TestNewRunnerWiresLifecycleComponents(t *testing.T) {
	cfg := DefaultConfig()

	runner := NewRunner(cfg)

	if runner.litestreamManager.cfg != &runner.cfg {
		t.Fatal("litestream manager is not wired to runner config")
	}
	if runner.statsPoller.cfg != &runner.cfg {
		t.Fatal("stats poller is not wired to runner config")
	}
	if runner.loadReplayManager.cfg != &runner.cfg {
		t.Fatal("load/replay manager is not wired to runner config")
	}
	if runner.litestreamLog == nil {
		t.Fatal("expected litestream log buffer")
	}
	if runner.loadLog == nil {
		t.Fatal("expected load log buffer")
	}
}

func TestNewStatsPollerInitialSnapshot(t *testing.T) {
	cfg := DefaultConfig()
	poller := newStatsPoller(&cfg)

	snapshot := poller.currentSnapshot()
	if snapshot.DBStatus != "unknown" {
		t.Fatalf("db status=%q want unknown", snapshot.DBStatus)
	}
	if snapshot.LitestreamSnapshotError != "litestream stats not collected yet" {
		t.Fatalf("snapshot error=%q want initial collection message", snapshot.LitestreamSnapshotError)
	}
}

func TestLitestreamManagerMonitorCancelsRunContextOnUnexpectedExit(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	cfg := DefaultConfig()
	manager := newLitestreamManager(&cfg)
	done := make(chan struct{})
	manager.litestreamDone = done

	manager.monitorLitestream(ctx, cancel)
	close(done)

	if !waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
		return ctx.Err() != nil
	}) {
		t.Fatal("context was not canceled after Litestream exit")
	}

	if got := context.Cause(ctx); got == nil || got.Error() != "litestream exited unexpectedly" {
		t.Fatalf("context cause=%v want litestream exited unexpectedly", got)
	}
}
