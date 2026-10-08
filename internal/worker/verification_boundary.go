package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"modernc.org/sqlite"
)

type verificationBoundary struct {
	logicalSnapshot
	txid uint64
}

func (v *Verifier) captureVerificationBoundary(ctx context.Context, path string, txid uint64) (verificationBoundary, error) {
	v.logicalEvidence = "source_boundary=unavailable restore_boundary=unavailable"
	if txid == 0 {
		return verificationBoundary{}, fmt.Errorf("source boundary unavailable: zero TXID")
	}
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return verificationBoundary{}, err
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return verificationBoundary{}, err
	}
	defer func() { _ = conn.Close() }()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = conn.ExecContext(cleanup, "ROLLBACK")
	}()
	if err := acquireVerificationReservation(ctx, conn); err != nil {
		return verificationBoundary{}, err
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return verificationBoundary{}, err
	}
	synced, err := v.syncOnceDB(ctx, v.cfg.verifySyncTimeout(), path)
	if err != nil {
		return verificationBoundary{}, fmt.Errorf("source boundary unavailable: paused sync: %w", err)
	}
	if err := acquireVerificationReservation(ctx, conn); err != nil {
		return verificationBoundary{}, err
	}
	if synced.TXID < txid || synced.ReplicatedTXID < synced.TXID {
		return verificationBoundary{}, fmt.Errorf("source boundary unavailable: requested=%016x reserved=%016x replicated=%016x", txid, synced.TXID, synced.ReplicatedTXID)
	}
	source, err := v.logicalValidation(ctx, path, synced.TXID)
	if err != nil {
		return verificationBoundary{}, err
	}
	v.logicalEvidence += " source_boundary=paused-sync-writer-reserved-snapshot"
	return verificationBoundary{logicalSnapshot: source, txid: synced.TXID}, nil
}

func acquireVerificationReservation(ctx context.Context, conn *sql.Conn) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
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
