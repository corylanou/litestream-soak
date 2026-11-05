package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"modernc.org/sqlite"
)

type unreplicatedBoundaryError struct {
	requested, local, replicated uint64
}

func (e *unreplicatedBoundaryError) Error() string {
	return fmt.Sprintf("source boundary unavailable: requested=%016x reserved=%016x replicated=%016x", e.requested, e.local, e.replicated)
}

type verificationBoundary struct {
	logicalSnapshot
	txid uint64
}

func (v *Verifier) captureVerificationBoundary(ctx context.Context, path string, txid uint64) (verificationBoundary, error) {
	v.logicalEvidence = "source_boundary=unavailable restore_boundary=unavailable"
	if txid == 0 {
		return verificationBoundary{}, fmt.Errorf("source boundary unavailable: zero TXID")
	}
	reservation, err := openVerificationReservation(ctx, path)
	if err != nil {
		return verificationBoundary{}, err
	}
	defer reservation.close()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = reservation.release(cleanup)
	}()
	if err := v.acquireBoundaryReservation(ctx, reservation, true); err != nil {
		return verificationBoundary{}, err
	}
	if err := reservation.release(ctx); err != nil {
		return verificationBoundary{}, err
	}
	synced, err := v.syncOnceDB(ctx, v.cfg.verifySyncTimeout(), path)
	if err != nil {
		return verificationBoundary{}, fmt.Errorf("source boundary unavailable: paused sync: %w", err)
	}
	// The snapshot below is labeled with this sync's txid, so from here on
	// the writers must stay paused: a resume would let commits land that the
	// txid does not cover. The pre-sync reservation already drained a frozen
	// writer, so this attempt only ever waits on Litestream's own writes.
	if err := v.acquireBoundaryReservation(ctx, reservation, false); err != nil {
		return verificationBoundary{}, err
	}
	if synced.TXID < txid {
		return verificationBoundary{}, fmt.Errorf("source boundary unavailable: requested=%016x reserved=%016x replicated=%016x", txid, synced.TXID, synced.ReplicatedTXID)
	}
	if synced.ReplicatedTXID < synced.TXID {
		return verificationBoundary{}, &unreplicatedBoundaryError{requested: txid, local: synced.TXID, replicated: synced.ReplicatedTXID}
	}

	source, err := v.logicalValidation(ctx, path, synced.TXID)
	if err != nil {
		return verificationBoundary{}, err
	}
	v.logicalEvidence += " source_boundary=paused-sync-writer-reserved-snapshot"
	return verificationBoundary{logicalSnapshot: source, txid: synced.TXID}, nil
}

type verificationReservation struct {
	db      *sql.DB
	conn    *sql.Conn
	path    string
	endHeld func()
}

func openVerificationReservation(ctx context.Context, path string) (*verificationReservation, error) {
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &verificationReservation{db: db, conn: conn, path: path}, nil
}

func (r *verificationReservation) close() {
	_ = r.conn.Close()
	_ = r.db.Close()
	r.endHeldTracking()
}

func (r *verificationReservation) endHeldTracking() {
	if r.endHeld != nil {
		r.endHeld()
		r.endHeld = nil
	}
}

func (r *verificationReservation) acquire(ctx context.Context) error {
	endWait := harnessWriters.begin("verifier_reservation_wait", r.path)
	err := acquireVerificationReservation(ctx, r.conn)
	endWait()
	if err == nil {
		r.endHeld = harnessWriters.begin("verifier_reservation", r.path)
	}
	return err
}

func (r *verificationReservation) release(ctx context.Context) error {
	if _, err := r.conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return err
	}
	r.endHeldTracking()
	return nil
}

// verificationReservationTimeout bounds one attempt to take the write lock;
// a var so tests can shorten it.
var verificationReservationTimeout = 15 * time.Second

// acquireBoundaryReservation takes the writer reservation. With cyclePausers
// set, a paused load that holds the write lock is cycled: the pause is a
// SIGSTOP that can freeze the load process inside a write transaction, which
// would otherwise hold the lock for the whole attempt and leave the cycle
// inconclusive, so the load is resumed for checkpointRetryDelay to finish its
// transaction and paused again, up to reservationAttempts times. Without it
// the reservation is tried once, for the attempt after the boundary sync,
// where resuming the writers would let commits past the synced txid into
// the snapshot.
func (v *Verifier) acquireBoundaryReservation(ctx context.Context, reservation *verificationReservation, cyclePausers bool) error {
	for attempt := 1; ; attempt++ {
		err := reservation.acquire(ctx)
		if err == nil || !cyclePausers || len(v.pausers) == 0 || attempt >= v.reservationAttempts || ctx.Err() != nil || !reservationBlocked(err) {
			return err
		}
		slog.Warn("Writer reservation blocked by the paused load; letting it finish its transaction", "attempt", attempt, "error", err)
		v.resumeLoad()
		select {
		case <-ctx.Done():
			return err
		case <-time.After(v.checkpointRetryDelay):
		}
		if err := v.pauseLoad(ctx); err != nil {
			return fmt.Errorf("source boundary unavailable: re-pause load for writer reservation: %w", err)
		}
	}
}

func reservationBlocked(err error) bool {
	text := err.Error()
	return errors.Is(err, context.DeadlineExceeded) || strings.Contains(text, "database is locked") || strings.Contains(text, "context deadline exceeded")
}

func acquireVerificationReservation(ctx context.Context, conn *sql.Conn) error {
	ctx, cancel := context.WithTimeout(ctx, verificationReservationTimeout)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("source boundary unavailable: acquire writer reservation: %w", err)
		}
		deadline, _ := ctx.Deadline()
		timeout := min(3000, max(1, time.Until(deadline).Milliseconds()))
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", timeout)); err != nil {
			return fmt.Errorf("source boundary unavailable: acquire writer reservation: %w", err)
		}
		_, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE")
		if err == nil {
			return nil
		}
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code()&255 != 5 {
			return fmt.Errorf("source boundary unavailable: acquire writer reservation: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("source boundary unavailable: acquire writer reservation: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func checkRestoredIntegrity(ctx context.Context, path string) error {
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("restored integrity: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("restored integrity: %s", result)
	}
	return nil
}
