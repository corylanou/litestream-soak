package worker

import (
	"context"
	"testing"
	"time"
)

func TestRunPinnedReaderReturnsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { RunPinnedReader(ctx, t.TempDir()+"/source.db", time.Hour, time.Hour); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pinned reader did not stop")
	}
}
