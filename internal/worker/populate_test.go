package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// writePopulateRows mimics litestream-test populate: WAL mode, batched inserts
// of 1 KiB rows with a random-integer index that rewrites index pages on every
// batch. autocheckpoint false reproduces the load binary (built with
// SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0); true leaves SQLite's default, which is
// what the populate binary runs with.
func writePopulateRows(t *testing.T, dbPath string, rows, batch int, autocheckpoint bool) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	statements := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"CREATE TABLE test_table_0 (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB, int_field INTEGER)",
		"CREATE INDEX idx_test_table_0_int ON test_table_0(int_field)",
	}
	if !autocheckpoint {
		statements = append([]string{"PRAGMA wal_autocheckpoint = 0"}, statements...)
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	payload := make([]byte, 1024)
	for written := 0; written < rows; written += batch {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		for i := 0; i < batch && written+i < rows; i++ {
			for j := range payload {
				payload[j] = byte(rand.IntN(256))
			}
			if _, err := tx.Exec("INSERT INTO test_table_0 (data, int_field) VALUES (?, ?)", payload, rand.Int64()); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
}

func sampleWALPeak(ctx context.Context, dbPath string) func() int64 {
	var (
		mu   sync.Mutex
		peak int64
		done = make(chan struct{})
	)
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				size := fileSizeOrZero(walPath(dbPath))
				mu.Lock()
				if size > peak {
					peak = size
				}
				mu.Unlock()
			}
		}
	}()
	return func() int64 {
		<-done
		mu.Lock()
		defer mu.Unlock()
		return peak
	}
}

// TestSQLiteAutocheckpointBoundsPopulateWAL is the reason the populate binary
// is built without SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0: with SQLite's default
// autocheckpoint the writer's own connection keeps the WAL near 1000 pages,
// and the independent watcher never sees the ceiling; with autocheckpoint off
// the same writer grows the WAL without bound and the watcher reports the
// breach even though nothing else ever touches the database.
func TestSQLiteAutocheckpointBoundsPopulateWAL(t *testing.T) {
	const (
		rows    = 24_000
		batch   = 500
		ceiling = int64(16 << 20)
	)
	tests := []struct {
		name            string
		autocheckpoint  bool
		wantPeakBelow   int64
		wantPeakAtLeast int64
		wantBreach      bool
	}{
		{name: "sqlite autocheckpoint keeps the wal small", autocheckpoint: true, wantPeakBelow: ceiling},
		{name: "autocheckpoint off grows the wal to the ceiling", wantPeakAtLeast: ceiling, wantBreach: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			ctx, cancel := context.WithCancel(context.Background())
			peak := sampleWALPeak(ctx, dbPath)
			watcher := newWALWatcher(dbPath, ceiling)
			watcher.interval = 2 * time.Millisecond
			watcherDone := make(chan struct{})
			go func() {
				defer close(watcherDone)
				watcher.run(ctx)
			}()

			writePopulateRows(t, dbPath, rows, batch, tt.autocheckpoint)
			cancel()
			<-watcherDone
			got := peak()
			watcherPeak, breach := watcher.stats()
			t.Logf("peak wal = %d bytes (watcher %d, breach %d), final db = %d bytes", got, watcherPeak, breach, fileSizeOrZero(dbPath))

			if tt.wantPeakBelow > 0 && got >= tt.wantPeakBelow {
				t.Fatalf("peak wal = %d bytes, want below %d", got, tt.wantPeakBelow)
			}
			if tt.wantPeakAtLeast > 0 && got < tt.wantPeakAtLeast {
				t.Fatalf("peak wal = %d bytes, want at least %d", got, tt.wantPeakAtLeast)
			}
			if (breach > 0) != tt.wantBreach {
				t.Fatalf("watcher breach = %d, want breach %v", breach, tt.wantBreach)
			}
			select {
			case <-watcher.breached():
				if !tt.wantBreach {
					t.Fatal("watcher signalled a breach under SQLite's own checkpointing")
				}
			default:
				if tt.wantBreach {
					t.Fatal("watcher never signalled the breach")
				}
			}
			if err := finalizePopulateCheckpoint(context.Background(), dbPath); err != nil {
				t.Fatalf("finalizePopulateCheckpoint() error = %v", err)
			}
			if size := fileSizeOrZero(walPath(dbPath)); size != 0 {
				t.Fatalf("wal after final checkpoint = %d bytes, want 0", size)
			}
			db, err := sql.Open("sqlite", dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM test_table_0").Scan(&count); err != nil || count != rows {
				t.Fatalf("rows = %d, %v; want %d", count, err, rows)
			}
		})
	}
}

// openWALHolder writes rows with auto-checkpoint off and keeps the connection
// open so the WAL stays populated; with holdWriteLock it also keeps a write
// transaction open, which is what a populate mid-batch looks like.
func openWALHolder(t *testing.T, dbPath string, rows int, holdWriteLock bool) *sql.Conn {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(200)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, stmt := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA wal_autocheckpoint = 0",
		"CREATE TABLE IF NOT EXISTS test_table_0 (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB, int_field INTEGER)",
		"CREATE INDEX IF NOT EXISTS idx_test_table_0_int ON test_table_0(int_field)",
	} {
		if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	payload := make([]byte, 1024)
	if _, err := conn.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if _, err := conn.ExecContext(context.Background(), "INSERT INTO test_table_0 (data, int_field) VALUES (?, ?)", payload, rand.Int64()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if holdWriteLock {
		if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
			t.Fatal(err)
		}
	}
	return conn
}

// TestRunPopulateCommandKillsPopulateAtWALCeiling drives the watcher's only
// intervention: the WAL reaches the ceiling while the "populate" process is
// still running and holds the write lock, and the watcher, which never opens
// the database, still sees it and populate is killed.
func TestRunPopulateCommandKillsPopulateAtWALCeiling(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sleep binary requires Unix")
	}
	tests := []struct {
		name          string
		holdWriteLock bool
	}{
		{name: "wal past the ceiling"},
		{name: "wal past the ceiling while the writer holds the write lock", holdWriteLock: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			holder := openWALHolder(t, dbPath, 400, tt.holdWriteLock)
			walSize := fileSizeOrZero(walPath(dbPath))
			if walSize == 0 {
				t.Fatal("fixture wal is empty")
			}
			watcher := newWALWatcher(dbPath, walSize)
			watcher.interval = 5 * time.Millisecond

			cmd := exec.Command("sleep", "30")
			started := time.Now()
			stats, err := runPopulateCommand(context.Background(), cmd, dbPath, watcher)
			elapsed := time.Since(started)
			if err == nil || !strings.Contains(err.Error(), "aborted populate") || !strings.Contains(err.Error(), "ceiling") {
				t.Fatalf("runPopulateCommand() error = %v, want aborted populate at the ceiling", err)
			}
			if elapsed > 10*time.Second {
				t.Fatalf("populate was not killed promptly: %s", elapsed)
			}
			if cmd.ProcessState == nil || cmd.ProcessState.Success() {
				t.Fatalf("populate process state = %v, want killed", cmd.ProcessState)
			}
			if stats.PeakWALBytes < walSize {
				t.Fatalf("stats = %+v, want peak at least %d", stats, walSize)
			}
			if tt.holdWriteLock {
				if _, err := holder.ExecContext(context.Background(), "ROLLBACK"); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestRunPopulateCommandBoundsWALWithPinnedBinary runs the real pinned workload
// source. LITESTREAM_TEST_BINARY must point at the populate build (SQLite's
// default autocheckpoint, as Dockerfile.worker builds litestream-test-populate);
// the WAL must stay under the production ceiling and populate must exit 0. If
// LITESTREAM_TEST_UNBOUNDED_BINARY points at the load build (autocheckpoint
// compiled off), the watcher must kill it at the ceiling instead.
// POPULATE_TARGET_SIZE (default 512MB) and POPULATE_INDEX_RATIO (default 1)
// shape the seed.
func TestRunPopulateCommandBoundsWALWithPinnedBinary(t *testing.T) {
	binary := os.Getenv("LITESTREAM_TEST_BINARY")
	unbounded := os.Getenv("LITESTREAM_TEST_UNBOUNDED_BINARY")
	if binary == "" && unbounded == "" {
		t.Skip("set LITESTREAM_TEST_BINARY (populate build) or LITESTREAM_TEST_UNBOUNDED_BINARY (load build) to run populate under the watcher")
	}
	target := os.Getenv("POPULATE_TARGET_SIZE")
	if target == "" {
		target = "512MB"
	}
	ratio := os.Getenv("POPULATE_INDEX_RATIO")
	if ratio == "" {
		ratio = "1"
	}
	run := func(t *testing.T, binary string) (populateStats, int64, time.Duration, error) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		ctx, cancel := context.WithCancel(context.Background())
		peak := sampleWALPeak(ctx, dbPath)
		cmd := exec.CommandContext(ctx, binary, "populate", "-db", dbPath, "-target-size", target, "-index-ratio", ratio)
		cmd.Env = populateEnv(os.Environ())
		started := time.Now()
		stats, err := runPopulateCommand(ctx, cmd, dbPath, newWALWatcher(dbPath, populateWALCeiling))
		elapsed := time.Since(started)
		cancel()
		observed := peak()
		t.Logf("%s target %s index-ratio %s: db %d bytes, sampled peak wal %d bytes, watcher peak %d bytes, elapsed %s, err %v",
			filepath.Base(binary), target, ratio, fileSizeOrZero(dbPath), observed, stats.PeakWALBytes, elapsed.Round(time.Millisecond), err)
		if err == nil {
			if size := fileSizeOrZero(walPath(dbPath)); size != 0 {
				t.Fatalf("wal after populate = %d bytes, want 0", size)
			}
			indexes, indexErr := populateIndexes(context.Background(), dbPath)
			if indexErr != nil {
				t.Fatal(indexErr)
			}
			t.Logf("indexes: %v", indexes)
		}
		return stats, observed, elapsed, err
	}
	if binary != "" {
		t.Run("populate build stays under the ceiling", func(t *testing.T) {
			_, observed, _, err := run(t, binary)
			if err != nil {
				t.Fatalf("runPopulateCommand() error = %v", err)
			}
			if observed >= populateWALCeiling {
				t.Fatalf("sampled peak wal %d bytes reached the %d byte ceiling", observed, populateWALCeiling)
			}
		})
	}
	if unbounded != "" {
		t.Run("load build is killed at the ceiling", func(t *testing.T) {
			_, observed, _, err := run(t, unbounded)
			if err == nil || !strings.Contains(err.Error(), "aborted populate") {
				t.Fatalf("runPopulateCommand() error = %v, want aborted populate at the ceiling", err)
			}
			if observed < populateWALCeiling {
				t.Fatalf("sampled peak wal %d bytes never reached the %d byte ceiling", observed, populateWALCeiling)
			}
		})
	}
}

func TestWALCheckpointTruncateReportsBusy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	writePopulateRows(t, dbPath, 200, 50, false)

	reader, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(50)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	reader.SetMaxOpenConns(1)
	tx, err := reader.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var n int
	if err := tx.QueryRow("SELECT COUNT(*) FROM test_table_0").Scan(&n); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	writer.SetMaxOpenConns(1)
	if _, err := writer.Exec("PRAGMA wal_autocheckpoint = 0"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec("INSERT INTO test_table_0 (data, int_field) VALUES (x'00', 1)"); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(50)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if err := walCheckpoint(context.Background(), db, "TRUNCATE"); err == nil {
		t.Fatal("walCheckpoint(TRUNCATE) succeeded with a reader pinning the wal, want busy")
	}
}

// writeFakePopulateBinaries installs the same shell script as both the populate
// build and the load build, so populateCommand resolves the populate build and
// the fallback is also covered.
func writeFakePopulateBinaries(t *testing.T, dir, body string) {
	t.Helper()
	for _, name := range []string{populateBinary, populateFallbackBinary} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// populateFixtureDB builds the database the fake populate copies into place,
// with the index populate creates at -index-ratio 1.
func populateFixtureDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, stmt := range []string{
		"CREATE TABLE test_table_0 (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB, int_field INTEGER, timestamp INTEGER)",
		"CREATE INDEX idx_test_table_0_int ON test_table_0(int_field)",
		"INSERT INTO test_table_0 (data, int_field, timestamp) VALUES (x'01', 7, 1)",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

func TestPopulateCommandPrefersAutocheckpointBuild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake binary requires Unix")
	}
	tests := []struct {
		name      string
		installed []string
		want      string
	}{
		{name: "populate build on PATH", installed: []string{populateBinary, populateFallbackBinary}, want: populateBinary},
		{name: "only the load build falls back", installed: []string{populateFallbackBinary}, want: populateFallbackBinary},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, name := range tt.installed {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("PATH", dir)
			if got := populateCommand(); got != tt.want {
				t.Fatalf("populateCommand() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPopulateSingleDBCompletionMarker(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake binary requires Unix")
	}
	const (
		succeed = `
db=""
while [ $# -gt 0 ]; do
  if [ "$1" = "-db" ]; then db="$2"; fi
  shift
done
printf '%s\n' "$GODEBUG" > "$POPULATE_GODEBUG_OUT"
printf '%s\n' "$0" > "$POPULATE_BINARY_OUT"
cp "$POPULATE_FIXTURE" "$db"
`
		fail = `
db=""
while [ $# -gt 0 ]; do
  if [ "$1" = "-db" ]; then db="$2"; fi
  shift
done
printf 'partial' > "$db"
printf 'partial' > "$db-wal"
echo "populate: no space left on device" >&2
exit 1
`
	)
	tests := []struct {
		name          string
		script        string
		existingDB    string
		marker        bool
		litestreamDir bool
		wantErr       string
		wantPopulated bool
		wantAdopted   bool
		wantRemoved   bool
	}{
		{name: "fresh volume populates and writes the marker", script: succeed, wantPopulated: true},
		{name: "marker present reuses the database", script: fail, existingDB: "existing", marker: true},
		{name: "database with litestream state but no marker is adopted", script: fail, existingDB: "fixture", litestreamDir: true, wantAdopted: true},
		{name: "database without marker or litestream state is removed and repopulated", script: succeed, existingDB: "partial", wantPopulated: true},
		{name: "failed populate removes the partial database", script: fail, wantErr: "exit status 1", wantRemoved: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			binDir := filepath.Join(dir, "bin")
			dataDir := filepath.Join(dir, "data")
			if err := os.MkdirAll(binDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(dataDir, 0o755); err != nil {
				t.Fatal(err)
			}
			fixture := filepath.Join(dir, "fixture.db")
			populateFixtureDB(t, fixture)
			godebugOut := filepath.Join(dir, "godebug")
			binaryOut := filepath.Join(dir, "binary")
			writeFakePopulateBinaries(t, binDir, tt.script)
			t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("POPULATE_FIXTURE", fixture)
			t.Setenv("POPULATE_GODEBUG_OUT", godebugOut)
			t.Setenv("POPULATE_BINARY_OUT", binaryOut)
			t.Setenv("GODEBUG", "")

			cfg := DefaultConfig()
			cfg.DataDir = dataDir
			cfg.DBPath = filepath.Join(dataDir, "test.db")
			cfg.InitialSize = "1MB"
			cfg.PopulateIndexRatio = 1
			cfg.WorkloadSHA = "workload-sha"
			switch tt.existingDB {
			case "fixture":
				populateFixtureDB(t, cfg.DBPath)
			case "":
			default:
				if err := os.WriteFile(cfg.DBPath, []byte(tt.existingDB), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if tt.marker {
				if err := writePopulateMarker(cfg.DBPath, populateMarker{CompletedAt: time.Now().UTC()}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.litestreamDir {
				if err := os.MkdirAll(litestreamStateDir(cfg.DBPath), 0o755); err != nil {
					t.Fatal(err)
				}
			}

			err := populateSingleDB(context.Background(), cfg)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("populateSingleDB() error = %v, want %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("populateSingleDB() error = %v", err)
			}

			if tt.wantRemoved {
				for _, path := range []string{cfg.DBPath, walPath(cfg.DBPath), populateMarkerPath(cfg.DBPath)} {
					if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
						t.Fatalf("%s still exists after failed populate: %v", path, statErr)
					}
				}
				return
			}
			marker, err := readPopulateMarker(cfg.DBPath)
			if err != nil || marker == nil {
				t.Fatalf("marker = %+v, %v; want marker", marker, err)
			}
			if marker.Adopted != tt.wantAdopted {
				t.Fatalf("marker.Adopted = %v, want %v (%+v)", marker.Adopted, tt.wantAdopted, marker)
			}
			if tt.wantPopulated || tt.wantAdopted {
				if !slices.Equal(marker.Indexes, []string{"idx_test_table_0_int"}) || marker.IndexRatio != 1 || marker.WorkloadSHA != "workload-sha" || marker.TargetSize != "1MB" {
					t.Fatalf("marker = %+v", marker)
				}
			}
			if tt.wantPopulated {
				if got := readFile(t, godebugOut); !strings.Contains(got, "randautoseed=0") {
					t.Fatalf("populate ran with GODEBUG=%q, want randautoseed=0", got)
				}
				if got := strings.TrimSpace(readFile(t, binaryOut)); filepath.Base(got) != populateBinary || filepath.Base(marker.Binary) != populateBinary {
					t.Fatalf("populate ran %q and recorded %q, want %s", got, marker.Binary, populateBinary)
				}
				if size := fileSizeOrZero(walPath(cfg.DBPath)); size != 0 {
					t.Fatalf("wal after populate = %d bytes, want 0", size)
				}
			} else if tt.existingDB != "fixture" {
				if got := readFile(t, cfg.DBPath); got != tt.existingDB {
					t.Fatalf("existing database was replaced: %q", got)
				}
			}
		})
	}
}

func TestPopulateSingleDBWithRetryReportsOnceAfterTwoAttempts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake binary requires Unix")
	}
	// The fake populate fails until the attempt counter reaches failUntil, then
	// copies the fixture into place.
	const script = `
db=""
while [ $# -gt 0 ]; do
  if [ "$1" = "-db" ]; then db="$2"; fi
  shift
done
n=$(cat "$POPULATE_ATTEMPTS" 2>/dev/null || echo 0)
n=$((n + 1))
printf '%s' "$n" > "$POPULATE_ATTEMPTS"
if [ "$n" -le "$POPULATE_FAIL_UNTIL" ]; then
  printf 'partial' > "$db"
  if [ -n "$POPULATE_VARY_ERROR" ]; then
    echo "populate database: insert row: transient failure $n" >&2
  else
    echo "populate database: insert row: database is locked" >&2
  fi
  exit 1
fi
cp "$POPULATE_FIXTURE" "$db"
`
	tests := []struct {
		name          string
		failUntil     int
		stopOnFailure bool
		varyError     bool
		cancel        bool
		wantAttempts  int
		wantErr       bool
		wantFinal     bool
	}{
		{name: "first attempt succeeds", failUntil: 0, stopOnFailure: true, wantAttempts: 1},
		{name: "transient failure is retried once from a clean database", failUntil: 1, stopOnFailure: true, wantAttempts: 2},
		{name: "gate worker with the same failure twice stops as a final failure", failUntil: 99, stopOnFailure: true, wantAttempts: 2, wantErr: true, wantFinal: true},
		{name: "gate worker with two different failures keeps restart-on-failure", failUntil: 99, stopOnFailure: true, varyError: true, wantAttempts: 2, wantErr: true},
		{name: "main sentinel with the same failure twice keeps restart-on-failure", failUntil: 99, wantAttempts: 2, wantErr: true},
		{name: "a cancelled gate run is not a final failure", failUntil: 99, stopOnFailure: true, cancel: true, wantAttempts: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			binDir := filepath.Join(dir, "bin")
			dataDir := filepath.Join(dir, "data")
			for _, path := range []string{binDir, dataDir} {
				if err := os.MkdirAll(path, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			fixture := filepath.Join(dir, "fixture.db")
			populateFixtureDB(t, fixture)
			attempts := filepath.Join(dir, "attempts")
			if err := os.WriteFile(attempts, []byte("0"), 0o644); err != nil {
				t.Fatal(err)
			}
			writeFakePopulateBinaries(t, binDir, script)
			t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("POPULATE_FIXTURE", fixture)
			t.Setenv("POPULATE_ATTEMPTS", attempts)
			t.Setenv("POPULATE_FAIL_UNTIL", strconv.Itoa(tt.failUntil))
			if tt.varyError {
				t.Setenv("POPULATE_VARY_ERROR", "1")
			} else {
				t.Setenv("POPULATE_VARY_ERROR", "")
			}

			cfg := DefaultConfig()
			cfg.DataDir = dataDir
			cfg.DBPath = filepath.Join(dataDir, "test.db")
			cfg.InitialSize = "1MB"
			cfg.PopulateIndexRatio = 1
			cfg.PopulateStopOnFailure = tt.stopOnFailure

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancel {
				cancel()
			}
			err := populateSingleDBWithRetry(ctx, cfg, populateAttempts)
			if got := readFile(t, attempts); got != strconv.Itoa(tt.wantAttempts) {
				t.Fatalf("populate ran %s times, want %d", got, tt.wantAttempts)
			}
			if IsPopulateFailure(err) != tt.wantFinal {
				t.Fatalf("IsPopulateFailure(%v) = %v, want %v", err, IsPopulateFailure(err), tt.wantFinal)
			}
			if tt.wantErr {
				if err == nil {
					t.Fatal("populateSingleDBWithRetry() error = nil, want failure")
				}
				if tt.wantAttempts == 2 && (!strings.Contains(err.Error(), "attempt 1 of 2") || !strings.Contains(err.Error(), "attempt 2 of 2")) {
					t.Fatalf("error = %v, want both attempts reported", err)
				}
				if _, statErr := os.Stat(cfg.DBPath); !os.IsNotExist(statErr) {
					t.Fatalf("partial database left behind: %v", statErr)
				}
				wrapped := fmt.Errorf("populate: %w", err)
				if IsPopulateFailure(wrapped) != tt.wantFinal || !strings.HasPrefix(wrapped.Error(), "populate: ") {
					t.Fatalf("wrapped %q final = %v, want %v", wrapped.Error(), IsPopulateFailure(wrapped), tt.wantFinal)
				}
				return
			}
			if err != nil {
				t.Fatalf("populateSingleDBWithRetry() error = %v", err)
			}
			marker, err := readPopulateMarker(cfg.DBPath)
			if err != nil || marker == nil || !slices.Equal(marker.Indexes, []string{"idx_test_table_0_int"}) {
				t.Fatalf("marker = %+v, %v; want a completed marker", marker, err)
			}
		})
	}
}

func TestDeterministicPopulateFailure(t *testing.T) {
	tests := []struct {
		name string
		errs []error
		want bool
	}{
		{name: "no attempts", want: false},
		{name: "same error twice", errs: []error{errors.New("exit status 1: database is locked"), errors.New("exit status 1: database is locked")}, want: true},
		{name: "different errors", errs: []error{errors.New("insert row: transient failure 1"), errors.New("insert row: transient failure 2")}, want: false},
		{name: "ceiling kills with different sizes", errs: []error{errors.New("aborted populate: wal is 536900000 bytes, at or above the 536870912 byte ceiling"), errors.New("aborted populate: wal is 537000000 bytes, at or above the 536870912 byte ceiling")}, want: true},
		{name: "full disk with different paths", errs: []error{errors.New("write /data/test.db-wal: no space left on device"), errors.New("write /data/test.db: no space left on device")}, want: true},
		{name: "one known class and one unknown", errs: []error{errors.New("aborted populate: wal ceiling"), errors.New("signal: killed")}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deterministicPopulateFailure(tt.errs); got != tt.want {
				t.Fatalf("deterministicPopulateFailure(%v) = %v, want %v", tt.errs, got, tt.want)
			}
		})
	}
}

func TestPopulateFailureOnlyWrapsFinalSeedFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil},
		{name: "plain populate error", err: errors.New("populate: exit status 1")},
		{name: "final failure", err: &PopulateFailure{err: errors.New("attempt 2 of 2: exit status 1")}, want: true},
		{name: "final failure wrapped by Run", err: fmt.Errorf("populate: %w", &PopulateFailure{err: errors.New("attempt 2 of 2")}), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPopulateFailure(tt.err); got != tt.want {
				t.Fatalf("IsPopulateFailure(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestPopulateArgsPinIndexRatio(t *testing.T) {
	tests := []struct {
		name  string
		ratio float64
		want  string
	}{
		{name: "default pins no indexes", ratio: 0, want: "0"},
		{name: "one pins both indexes", ratio: 1, want: "1"},
		{name: "fractions pass through", ratio: 0.25, want: "0.25"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.DBPath = "/data/test.db"
			cfg.InitialSize = "6GB"
			cfg.PopulateIndexRatio = tt.ratio
			args := populateArgs(cfg)
			want := []string{"populate", "-db", "/data/test.db", "-target-size", "6GB", "-index-ratio", tt.want}
			if !slices.Equal(args, want) {
				t.Fatalf("populateArgs() = %q, want %q", args, want)
			}
		})
	}
}

func TestPopulateEnvSeedsMathRand(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want []string
	}{
		{name: "adds GODEBUG", env: []string{"PATH=/bin"}, want: []string{"PATH=/bin", "GODEBUG=randautoseed=0"}},
		{name: "extends existing GODEBUG", env: []string{"GODEBUG=http2debug=1"}, want: []string{"GODEBUG=http2debug=1,randautoseed=0"}},
		{name: "fills empty GODEBUG", env: []string{"GODEBUG="}, want: []string{"GODEBUG=randautoseed=0"}},
		{name: "keeps an existing setting once", env: []string{"GODEBUG=randautoseed=0,x=1"}, want: []string{"GODEBUG=randautoseed=0,x=1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := populateEnv(slices.Clone(tt.env)); !slices.Equal(got, tt.want) {
				t.Fatalf("populateEnv(%q) = %q, want %q", tt.env, got, tt.want)
			}
		})
	}
}
