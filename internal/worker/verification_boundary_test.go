package worker

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestVerificationRequiresPinnedRestore(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("boundary-%d.sock", time.Now().UnixNano()))
	logicalTestDB(t, cfg.DBPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
	logicalTestDB(t, cfg.DBPath+".restored", "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
	writeFakeLitestreamTest(t, dir, "case \"$*\" in *-txid*) echo 'flag provided but not defined: -txid'; exit 2;; esac\nexit 0\n")
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte("#!/bin/sh\necho 'flag provided but not defined: -txid'\nexit 2\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","txid":42,"replicated_txid":42}`))
	}))
	v := NewVerifier(cfg)
	passed, err := v.validate(context.Background(), 42)
	if passed || err == nil {
		t.Fatalf("unsupported pinned restore awarded credit: passed=%v err=%v evidence=%s", passed, err, v.logicalEvidence)
	}
	if !strings.Contains(v.logicalEvidence, "unavailable") {
		t.Fatalf("missing capability evidence: %s", v.logicalEvidence)
	}
}

func TestVerificationBoundaryRejectsDriftThenReacquires(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("drift-%d.sock", time.Now().UnixNano()))
	logicalTestDB(t, cfg.DBPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id); INSERT INTO t VALUES(1)")
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","txid":43,"replicated_txid":43}`))
	}))
	v := NewVerifier(cfg)
	if _, err := v.captureVerificationBoundary(context.Background(), cfg.DBPath, 42); err == nil {
		t.Fatal("credited stale TXID")
	}
	if _, err := v.captureVerificationBoundary(context.Background(), cfg.DBPath, 43); err != nil {
		t.Fatalf("reacquisition: %v", err)
	}
}

func TestVerificationBoundaryExcludesInFlightWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("flight-%d.sock", time.Now().UnixNano()))
	db := logicalTestDB(t, cfg.DBPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id); INSERT INTO t VALUES(1)")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("INSERT INTO t VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	v := NewVerifier(cfg)
	if _, err := v.captureVerificationBoundary(context.Background(), cfg.DBPath, 42); err == nil {
		t.Fatal("accepted in-flight writer")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := db.Exec("INSERT INTO t VALUES(3)"); err == nil {
			t.Error("writer committed during reserved sync")
		}
		_, _ = w.Write([]byte(`{"status":"ok","txid":43,"replicated_txid":43}`))
	}))
	source, err := v.captureVerificationBoundary(context.Background(), cfg.DBPath, 43)
	if err != nil {
		t.Fatal(err)
	}
	if len(source.tables) != 1 || source.tables[0].rows != 2 {
		t.Fatalf("wrong committed boundary: %+v", source)
	}
	if _, err := db.Exec("INSERT INTO t VALUES(3)"); err != nil {
		t.Fatalf("reservation leaked: %v", err)
	}
}

func TestVerificationBoundaryCancellationReleasesReservation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("cancel-boundary-%d.sock", time.Now().UnixNano()))
	db := logicalTestDB(t, cfg.DBPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id)")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { cancel(); <-r.Context().Done() }))
	if _, err := NewVerifier(cfg).captureVerificationBoundary(ctx, cfg.DBPath, 42); err == nil {
		t.Fatal("canceled boundary succeeded")
	}
	if _, err := db.Exec("INSERT INTO t VALUES(1)"); err != nil {
		t.Fatalf("reservation leaked: %v", err)
	}
}

func writeFakePinnedRestore(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
}

func startBoundarySyncFixture(t *testing.T, cfg *Config, txid uint64) {
	t.Helper()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("boundary-fixture-%d.sock", time.Now().UnixNano()))
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"status":"ok","txid":%d,"replicated_txid":%d}`, txid, txid)
	}))
}

func TestVerificationBoundaryFailureSurvivesLaterCycle(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("cycles-boundary-%d.sock", time.Now().UnixNano()))
	logicalTestDB(t, cfg.DBPath, "CREATE TABLE t(id)")
	writeFakePinnedRestore(t, dir, `cp "$SOURCE_PATH" "$RESTORED_PATH"`)
	t.Setenv("SOURCE_PATH", cfg.DBPath)
	t.Setenv("RESTORED_PATH", cfg.DBPath+".restored")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	var syncs atomic.Int64
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sync" {
			_, _ = w.Write([]byte(`{"active":false}`))
			return
		}
		txid := 43
		if syncs.Add(1) == 1 {
			txid = 42
		}
		_, _ = fmt.Fprintf(w, `{"status":"ok","txid":%d,"replicated_txid":%d}`, txid, txid)
	}))
	pauser := &fakePauser{}
	v := NewVerifier(cfg, pauser)
	failed, err := v.RunCycle(context.Background())
	if err == nil || failed.Passed || !strings.Contains(failed.ErrorMessage, "source boundary unavailable") {
		t.Fatalf("drift not retained: %+v %v", failed, err)
	}
	passed, err := v.RunCycle(context.Background())
	if err != nil || !passed.Passed {
		t.Fatalf("fresh cycle: %+v %v", passed, err)
	}
	if failed.Status != "failed" || failed.ErrorMessage == "" {
		t.Fatal("later success erased failure")
	}
	history := readFile(t, filepath.Join(dir, "verification.log"))
	if !strings.Contains(history, "| FAIL |") || !strings.Contains(history, "| PASS |") || !strings.Contains(history, "requested=000000000000002a reserved=000000000000002b") {
		t.Fatalf("attempt history lost: %s", history)
	}
	if pauser.resumeCalls != 2 {
		t.Fatalf("resume calls=%d", pauser.resumeCalls)
	}
}

func TestVerificationUsesSourceTXID(t *testing.T) {
	if got := (VerificationResult{SyncTXID: 42, SyncReplicatedTXID: 43}).restoreTXID(); got != 42 {
		t.Fatalf("source boundary=%d", got)
	}
}

func TestVerificationBoundaryFollowsCheckpointThaw(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(dir, "source.db")
	db := logicalTestDB(t, cfg.DBPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id); INSERT INTO t VALUES(1)")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("INSERT INTO t VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	pauser := &releasingPauser{onResume: func() {
		if err := tx.Commit(); err != nil {
			t.Error(err)
		}
	}}
	startBoundarySyncFixture(t, &cfg, 43)
	v := NewVerifier(cfg, pauser)
	v.checkpointAttempts = 2
	v.checkpointRetryDelay = time.Millisecond
	v.checkpointBusyTimeout = 10 * time.Millisecond
	if _, err := v.checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pauser.pauseCalls.Load() == 0 || pauser.resumeCalls.Load() == 0 {
		t.Fatal("checkpoint did not thaw and re-pause")
	}
	source, err := v.captureVerificationBoundary(context.Background(), cfg.DBPath, 43)
	if err != nil {
		t.Fatal(err)
	}
	if len(source.tables) != 1 || source.tables[0].rows != 2 {
		t.Fatalf("snapshot predates thaw commit: %+v", source)
	}
}
