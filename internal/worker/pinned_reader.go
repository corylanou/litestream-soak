package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// pinnedReader repeatedly opens a read transaction against the workload
// database and holds it for a configured window before releasing. A held
// read transaction pins the WAL above the reader's snapshot, which is the
// trigger condition for Litestream's emergency truncate and passive
// checkpoint paths under sustained writes.
//
// It implements loadPauser so verification cycles can release the held
// transaction: a pinned reader would otherwise block the verifier's
// TRUNCATE checkpoint indefinitely.
type pinnedReader struct {
	dbPath string
	hold   time.Duration
	pause  time.Duration

	mu          sync.Mutex
	paused      bool
	resume      chan struct{}
	pauseSignal chan struct{}
	stop        context.CancelFunc
	done        chan struct{}
}

func newPinnedReader(dbPath string, hold, pause time.Duration) *pinnedReader {
	if pause <= 0 {
		pause = 30 * time.Second
	}
	return &pinnedReader{
		dbPath:      dbPath,
		hold:        hold,
		pause:       pause,
		pauseSignal: make(chan struct{}, 1),
	}
}

func (p *pinnedReader) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	p.stop = cancel
	p.done = make(chan struct{})
	go p.run(runCtx)
}

func (p *pinnedReader) Stop() {
	p.mu.Lock()
	if p.stop != nil {
		p.stop()
	}
	done := p.done
	p.mu.Unlock()
	if done != nil {
		<-done
	}
}

// Pause releases any held read transaction and keeps the reader idle until
// Resume. It satisfies the loadPauser interface used by the verifier.
func (p *pinnedReader) Pause(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused {
		return nil
	}
	p.paused = true
	p.resume = make(chan struct{})
	select {
	case p.pauseSignal <- struct{}{}:
	default:
	}
	return nil
}

func (p *pinnedReader) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused {
		return
	}
	p.paused = false
	close(p.resume)
	p.resume = nil
}

func (p *pinnedReader) waitIfPaused(ctx context.Context) error {
	for {
		p.mu.Lock()
		resume := p.resume
		p.mu.Unlock()
		if resume == nil {
			return nil
		}
		select {
		case <-resume:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *pinnedReader) run(ctx context.Context) {
	defer close(p.done)

	slog.Info("Starting pinned reader",
		"db", p.dbPath, "hold", p.hold.String(), "pause", p.pause.String())

	for {
		if err := p.waitIfPaused(ctx); err != nil {
			return
		}
		if err := p.holdOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("Pinned reader cycle failed", "error", err)
		}
		select {
		case <-time.After(p.pause):
		case <-ctx.Done():
			return
		}
	}
}

// holdOnce opens a read transaction, reads a row so the snapshot is
// established, and holds the transaction until the hold window elapses,
// a pause is requested, or the context ends.
func (p *pinnedReader) holdOnce(ctx context.Context) error {
	select {
	case <-p.pauseSignal:
	default:
	}
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()
	if paused {
		return nil
	}

	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", p.dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var n int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master").Scan(&n); err != nil {
		return fmt.Errorf("establish read snapshot: %w", err)
	}

	slog.Debug("Pinned reader holding read transaction", "hold", p.hold.String())

	deadline := time.NewTimer(p.hold)
	defer deadline.Stop()

	select {
	case <-deadline.C:
		return nil
	case <-p.pauseSignal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
