package replay

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type countingAdapter struct {
	rowsPerPass int
	inserted    *atomic.Int64
}

func (a countingAdapter) Name() string { return "counting" }

func (a countingAdapter) CreateTables(db *sql.DB) error { return nil }

func (a countingAdapter) Rows() (RowIterator, error) {
	return &countingIterator{remaining: a.rowsPerPass, inserted: a.inserted}, nil
}

type countingIterator struct {
	remaining int
	inserted  *atomic.Int64
}

func (it *countingIterator) Next() bool {
	if it.remaining == 0 {
		return false
	}
	it.remaining--
	return true
}

func (it *countingIterator) Timestamp() time.Time { return time.Unix(0, 0) }

func (it *countingIterator) Insert(db *sql.DB) error {
	it.inserted.Add(1)
	return nil
}

func (it *countingIterator) Err() error   { return nil }
func (it *countingIterator) Close() error { return nil }

type blockingAdapter struct {
	unblock  chan struct{}
	entered  chan struct{}
	enterPub *atomic.Bool
}

func (a blockingAdapter) Name() string { return "blocking" }

func (a blockingAdapter) CreateTables(db *sql.DB) error { return nil }

func (a blockingAdapter) Rows() (RowIterator, error) {
	return &blockingIterator{remaining: 1000, unblock: a.unblock, entered: a.entered, enterPub: a.enterPub}, nil
}

type blockingIterator struct {
	remaining int
	unblock   chan struct{}
	entered   chan struct{}
	enterPub  *atomic.Bool
}

func (it *blockingIterator) Next() bool {
	if it.remaining == 0 {
		return false
	}
	it.remaining--
	return true
}

func (it *blockingIterator) Timestamp() time.Time { return time.Unix(0, 0) }

func (it *blockingIterator) Insert(db *sql.DB) error {
	if it.enterPub.CompareAndSwap(false, true) {
		close(it.entered)
	}
	<-it.unblock
	return nil
}

func (it *blockingIterator) Err() error   { return nil }
func (it *blockingIterator) Close() error { return nil }

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func TestEnginePauseBlocksInserts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	inserted := &atomic.Int64{}
	engine := NewEngine(Config{
		DBPath: filepath.Join(dir, "replay.db"),
		Loop:   true,
	}, countingAdapter{rowsPerPass: 1000, inserted: inserted})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = engine.Run(runCtx)
	}()

	waitForCondition(t, 5*time.Second, func() bool { return inserted.Load() > 0 })

	pauseCtx, cancelPause := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPause()
	if err := engine.Pause(pauseCtx); err != nil {
		t.Fatalf("pause: %v", err)
	}

	snapshot := inserted.Load()
	time.Sleep(200 * time.Millisecond)
	if got := inserted.Load(); got != snapshot {
		t.Fatalf("inserts continued while paused: got %d, want %d", got, snapshot)
	}

	engine.Resume()
	waitForCondition(t, 5*time.Second, func() bool { return inserted.Load() > snapshot })

	cancelRun()
	<-runDone
}

func TestEnginePauseAcksWhenEngineNotRunning(t *testing.T) {
	t.Parallel()

	inserted := &atomic.Int64{}
	engine := NewEngine(Config{}, countingAdapter{inserted: inserted})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Pause(ctx); err != nil {
		t.Fatalf("pause on never-started engine: %v", err)
	}
}

func TestEnginePauseAcksWhenEngineExits(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	inserted := &atomic.Int64{}
	engine := NewEngine(Config{
		DBPath: filepath.Join(dir, "replay.db"),
		Loop:   false,
	}, countingAdapter{rowsPerPass: 3, inserted: inserted})

	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engine.Run(runCtx); err != nil {
		t.Fatalf("engine run: %v", err)
	}
	if inserted.Load() != 3 {
		t.Fatalf("inserted=%d, want 3", inserted.Load())
	}

	pauseCtx, cancelPause := context.WithTimeout(context.Background(), time.Second)
	defer cancelPause()
	if err := engine.Pause(pauseCtx); err != nil {
		t.Fatalf("pause after engine exit: %v", err)
	}
}

func TestEnginePauseRespectsContext(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	unblock := make(chan struct{})
	entered := make(chan struct{})
	engine := NewEngine(Config{
		DBPath: filepath.Join(dir, "replay.db"),
		Loop:   true,
	}, blockingAdapter{unblock: unblock, entered: entered, enterPub: &atomic.Bool{}})

	runCtx, cancelRun := context.WithCancel(context.Background())

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = engine.Run(runCtx)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("engine never entered insert")
	}

	pauseCtx, cancelPause := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelPause()
	err := engine.Pause(pauseCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pause err=%v, want context.DeadlineExceeded", err)
	}

	close(unblock)
	cancelRun()
	<-runDone
}

func TestEnginePauseAcksDuringEmptyLoop(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	var inserted atomic.Int64
	engine := NewEngine(Config{
		DBPath: filepath.Join(dir, "replay.db"),
		Loop:   true,
	}, countingAdapter{rowsPerPass: 0, inserted: &inserted})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = engine.Run(runCtx)
	}()

	waitForCondition(t, 2*time.Second, func() bool {
		engine.mu.Lock()
		defer engine.mu.Unlock()
		return engine.running
	})

	pauseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := engine.Pause(pauseCtx); err != nil {
		t.Fatalf("Pause() error = %v, want ack while looping over empty dataset", err)
	}
	engine.Resume()
	cancelRun()
	<-runDone
}
