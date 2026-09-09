package worker

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"
)

func (v *Verifier) captureVerificationBoundary(ctx context.Context, path string, txid uint64) (logicalSnapshot, error) {
	v.logicalEvidence = "source_boundary=unavailable restore_boundary=unavailable"
	if txid == 0 {
		return logicalSnapshot{}, fmt.Errorf("source boundary unavailable: zero TXID")
	}
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return logicalSnapshot{}, err
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return logicalSnapshot{}, err
	}
	defer func() { _ = conn.Close() }()
	if _, err = conn.ExecContext(ctx, "PRAGMA busy_timeout=100; BEGIN IMMEDIATE"); err != nil {
		return logicalSnapshot{}, fmt.Errorf("source boundary unavailable: acquire writer reservation: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = conn.ExecContext(cleanup, "ROLLBACK")
	}()
	synced, err := v.syncOnceDB(ctx, v.cfg.verifySyncTimeout(), path)
	if err != nil {
		return logicalSnapshot{}, fmt.Errorf("source boundary unavailable: reserved sync: %w", err)
	}
	if synced.TXID != txid || synced.ReplicatedTXID < txid {
		return logicalSnapshot{}, fmt.Errorf("source boundary unavailable: requested=%016x reserved=%016x replicated=%016x", txid, synced.TXID, synced.ReplicatedTXID)
	}
	source, err := v.logicalValidation(ctx, path, txid)
	if err != nil {
		return source, err
	}
	v.logicalEvidence += " source_boundary=writer-reserved-sync"
	return source, nil
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
