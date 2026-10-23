package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/churn"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestHarnessWriterTrackerConcurrentWriters(t *testing.T) {
	tracker := &harnessWriterTracker{}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			end := tracker.begin("load_retention", "/data/db")
			_ = tracker.snapshot()
			tracker.setActive("load", "/data/db", i%2 == 0)
			end()
		}()
	}
	wg.Wait()
	for _, writer := range tracker.snapshot() {
		if writer.Name != "load" {
			t.Fatalf("ended writer still active: %+v", writer)
		}
	}
	tracker.setActive("load", "/data/db", false)
	if writers := tracker.snapshot(); len(writers) != 0 {
		t.Fatalf("writers=%+v", writers)
	}
	end := tracker.begin("verifier_checkpoint", "/data/db")
	end()
	end()
	if writers := tracker.snapshot(); len(writers) != 0 {
		t.Fatalf("double end left writers=%+v", writers)
	}
}

func harnessWriterActive(name, dbPath string) bool {
	for _, writer := range harnessWriters.snapshot() {
		if writer.Name == name && writer.DBPath == dbPath {
			return true
		}
	}
	return false
}

func harnessWritersFor(dbPath string) []string {
	var names []string
	for _, writer := range harnessWriters.snapshot() {
		if writer.DBPath == dbPath {
			names = append(names, writer.Name)
		}
	}
	return names
}

func TestLoadSupervisorTracksGeneratorProcessLifetime(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell subprocess requires Unix")
	}
	dbPath := filepath.Join(t.TempDir(), "load.db")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := shellCommandSupervisor(t, nil, "exec sleep 30")
	sup.dbPath = dbPath
	if err := sup.start(ctx); err != nil {
		t.Fatal(err)
	}
	if !harnessWriterActive("load_generator", dbPath) {
		t.Fatalf("running generator not tracked: %v", harnessWritersFor(dbPath))
	}
	cancel()
	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool { return len(harnessWritersFor(dbPath)) == 0 }) {
		t.Fatalf("canceled generator still tracked: %v", harnessWritersFor(dbPath))
	}

	failedPath := filepath.Join(t.TempDir(), "failed.db")
	failed := shellCommandSupervisor(t, nil, "exit 1")
	failed.dbPath = failedPath
	if err := failed.start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer failed.Stop()
	if !waitUntil(5*time.Second, 10*time.Millisecond, func() bool { return supervisorExited(failed) && len(harnessWritersFor(failedPath)) == 0 }) {
		t.Fatalf("failed generator still tracked: %v", harnessWritersFor(failedPath))
	}
}

func TestSuperviseReplayTracksEngineRunLifetime(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	running := make(chan struct{}, 2)
	failRun := make(chan struct{})
	var runs atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		superviseReplay(ctx, "test", dbPath, true, 50*time.Millisecond, 50*time.Millisecond, time.Minute, func(c context.Context) error {
			running <- struct{}{}
			if runs.Add(1) == 1 {
				<-failRun
				return errors.New("replay exploded")
			}
			<-c.Done()
			return c.Err()
		})
	}()
	<-running
	if !harnessWriterActive("replay", dbPath) {
		t.Fatalf("running replay not tracked: %v", harnessWritersFor(dbPath))
	}
	close(failRun)
	if !waitUntil(time.Second, time.Millisecond, func() bool { return len(harnessWritersFor(dbPath)) == 0 }) {
		t.Fatalf("failed replay still tracked: %v", harnessWritersFor(dbPath))
	}
	<-running
	if !harnessWriterActive("replay", dbPath) {
		t.Fatalf("restarted replay not tracked: %v", harnessWritersFor(dbPath))
	}
	cancel()
	<-done
	if names := harnessWritersFor(dbPath); len(names) != 0 {
		t.Fatalf("canceled replay still tracked: %v", names)
	}
}

func TestChurnTracksEngineLifetime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultConfig()
	cfg.LoadMode = "cache"
	cfg.DBPath = filepath.Join(t.TempDir(), "db")
	cfg.Churn = churn.Config{Slots: 4, Workers: 1, Rate: 10, PayloadSize: 8}
	if err := NewRunner(cfg).populate(ctx); err != nil {
		t.Fatal(err)
	}
	load, err := startChurn(ctx, cfg, func(string, int64, time.Duration, error) {})
	if err != nil {
		t.Fatal(err)
	}
	if !harnessWriterActive("churn", cfg.DBPath) {
		t.Fatalf("running churn not tracked: %v", harnessWritersFor(cfg.DBPath))
	}
	load.Stop()
	if names := harnessWritersFor(cfg.DBPath); len(names) != 0 {
		t.Fatalf("stopped churn still tracked: %v", names)
	}
}

func TestManyDBWriteTracksTransaction(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "many.db")
	db := logicalTestDB(t, dbPath, "PRAGMA journal_mode=WAL; CREATE TABLE soak_writes (id INTEGER PRIMARY KEY, body BLOB NOT NULL, written_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- writeManyDBRow(context.Background(), dbPath, 8, 1) }()
	if !waitUntil(2*time.Second, time.Millisecond, func() bool { return harnessWriterActive("many_db_write", dbPath) }) {
		t.Fatalf("blocked many-db write not tracked: %v", harnessWritersFor(dbPath))
	}
	if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := writeManyDBRow(context.Background(), dbPath, 8, 0); err == nil {
		t.Fatal("invalid slot accepted")
	}
	if names := harnessWritersFor(dbPath); len(names) != 0 {
		t.Fatalf("finished many-db write still tracked: %v", names)
	}
}

func TestVerificationBoundaryReleasesReservationTrackingDuringSync(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("busy-boundary-%d.sock", time.Now().UnixNano()))
	logicalTestDB(t, cfg.DBPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id); INSERT INTO t VALUES(1)")
	runner := NewRunner(Config{DataDir: dir, WorkerID: "worker", RunID: "run"})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runner.observeLogIncidents(cancel)
	var syncWriters []string
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		syncWriters = harnessWritersFor(cfg.DBPath)
		if _, err := runner.litestreamLog.Write([]byte(`level=ERROR msg="sync error" error="checkpoint: bump litestream seq: database is locked (5) (SQLITE_BUSY)"` + "\n")); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"status":"ok","txid":43,"replicated_txid":43}`))
	}))
	if _, err := NewVerifier(cfg).captureVerificationBoundary(context.Background(), cfg.DBPath, 43); err != nil {
		t.Fatal(err)
	}
	if len(syncWriters) != 0 {
		t.Fatalf("released reservation tracked during sync: %v", syncWriters)
	}
	var received []reporting.WorkerEventPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event reporting.WorkerEventPayload
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		received = append(received, event)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	runner.reporter = &Reporter{baseURL: server.URL, client: server.Client(), identity: reporting.WorkerIdentity{WorkerID: "worker", RunID: "run"}}
	if err := runner.flushRunEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	if len(received) != 1 || !received[0].SQLiteBusy {
		t.Fatalf("received=%+v", received)
	}
	for _, writer := range received[0].ActiveHarnessWriters {
		if writer.DBPath == cfg.DBPath {
			t.Fatalf("busy during released sync blamed %+v", writer)
		}
	}
	if names := harnessWritersFor(cfg.DBPath); len(names) != 0 {
		t.Fatalf("reservation still tracked after boundary: %v", names)
	}
}

func TestVerificationReservationTracksWaitThenHold(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "source.db")
	db := logicalTestDB(t, dbPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id)")
	blocker, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Close() }()
	if _, err := blocker.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	reservation, err := openVerificationReservation(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.close()
	done := make(chan error, 1)
	go func() { done <- reservation.acquire(context.Background()) }()
	if !waitUntil(2*time.Second, time.Millisecond, func() bool { return harnessWriterActive("verifier_reservation_wait", dbPath) }) {
		t.Fatalf("waiting reservation not tracked: %v", harnessWritersFor(dbPath))
	}
	if harnessWriterActive("verifier_reservation", dbPath) {
		t.Fatal("waiting reservation reported as held")
	}
	if _, err := blocker.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if names := harnessWritersFor(dbPath); len(names) != 1 || names[0] != "verifier_reservation" {
		t.Fatalf("held reservation writers=%v", names)
	}
	if err := reservation.release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if names := harnessWritersFor(dbPath); len(names) != 0 {
		t.Fatalf("released reservation writers=%v", names)
	}
}

func TestVerificationReservationFailedReleaseStaysTrackedUntilClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "source.db")
	db := logicalTestDB(t, dbPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id)")
	reservation, err := openVerificationReservation(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.close()
	if err := reservation.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := reservation.release(canceled); err == nil {
		t.Fatal("canceled rollback reported success")
	}
	if !harnessWriterActive("verifier_reservation", dbPath) {
		t.Fatalf("still-held reservation not tracked: %v", harnessWritersFor(dbPath))
	}
	if _, err := db.Exec("PRAGMA busy_timeout=50; INSERT INTO t VALUES(1)"); err == nil {
		t.Fatal("reservation released despite failed rollback")
	}
	reservation.close()
	if names := harnessWritersFor(dbPath); len(names) != 0 {
		t.Fatalf("closed reservation writers=%v", names)
	}
	if _, err := db.Exec("INSERT INTO t VALUES(2)"); err != nil {
		t.Fatalf("closed reservation still holds lock: %v", err)
	}
}

func TestSQLiteBusyLogIncidentIncludesActiveHarnessWriters(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), WorkerID: "worker", RunID: "run"}
	runner := NewRunner(cfg)
	runner.harnessWriters = &harnessWriterTracker{}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	runner.observeLogIncidents(cancel)
	endRetention := runner.harnessWriters.begin("load_retention", "/data/db")
	runner.harnessWriters.setActive("load", "/data/db", true)
	lines := []string{
		`level=ERROR msg="sync error" db=/data/db error="checkpoint: bump litestream seq: database is locked (5) (SQLITE_BUSY)"`,
		`level=ERROR msg="upload failed" error="connection reset"`,
	}
	for _, line := range lines {
		if _, err := runner.litestreamLog.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	endRetention()
	runner.harnessWriters.setActive("load", "/data/db", false)
	if _, err := runner.litestreamLog.Write([]byte(`level=ERROR msg="sync error" error="database is locked"` + "\n")); err != nil {
		t.Fatal(err)
	}
	var received []reporting.WorkerEventPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event reporting.WorkerEventPayload
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		received = append(received, event)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	runner.reporter = &Reporter{baseURL: server.URL, client: server.Client(), identity: reporting.WorkerIdentity{WorkerID: "worker", RunID: "run"}}
	if err := runner.flushRunEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	if len(received) != 3 {
		t.Fatalf("received=%+v", received)
	}
	slices.SortFunc(received, func(a, b reporting.WorkerEventPayload) int {
		return cmp.Compare(a.Errors, b.Errors)
	})
	busy := received[0]
	if !busy.SQLiteBusy || len(busy.ActiveHarnessWriters) != 2 {
		t.Fatalf("busy event missing writers: %+v", busy)
	}
	names := map[string]bool{}
	for _, writer := range busy.ActiveHarnessWriters {
		if writer.DBPath != "/data/db" || writer.StartedAt.IsZero() || writer.StartedAt.After(time.Now()) {
			t.Fatalf("writer=%+v", writer)
		}
		names[writer.Name] = true
	}
	if !names["load_retention"] || !names["load"] {
		t.Fatalf("writers=%+v", busy.ActiveHarnessWriters)
	}
	if received[1].SQLiteBusy || len(received[1].ActiveHarnessWriters) != 0 {
		t.Fatalf("non-busy event attributed: %+v", received[1])
	}
	if !received[2].SQLiteBusy || len(received[2].ActiveHarnessWriters) != 0 {
		t.Fatalf("idle busy event: %+v", received[2])
	}
}
