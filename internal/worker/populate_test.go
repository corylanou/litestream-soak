package worker

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// writeUncheckpointedRows mimics litestream-test populate as the harness
// builds it: WAL mode, auto-checkpoint disabled, batched inserts of 1 KiB rows
// with a random-integer index that rewrites index pages on every batch.
// afterBatch, when set, runs between commits so a test can gate the writer on
// checkpointer progress.
func writeUncheckpointedRows(t *testing.T, dbPath string, rows, batch int, afterBatch func()) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	for _, stmt := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA wal_autocheckpoint = 0",
		"PRAGMA synchronous = NORMAL",
		"CREATE TABLE test_table_0 (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB, int_field INTEGER)",
		"CREATE INDEX idx_test_table_0_int ON test_table_0(int_field)",
	} {
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
		if afterBatch != nil {
			afterBatch()
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

// TestPopulateCheckpointerBoundsWAL asserts the hard bound the production
// checkpointer enforces, not a timing-dependent figure: with the checkpointer
// running, the WAL never reaches the ceiling (run returns no error and the
// sampled peak stays under it), the checkpointer truncates, and the data
// survives. The writer waits for a truncation whenever the WAL is past the
// threshold, so how much it writes between checkpoints does not depend on
// scheduling; the writer-only case shows the WAL would otherwise keep growing.
func TestPopulateCheckpointerBoundsWAL(t *testing.T) {
	const (
		rows      = 24_000
		batch     = 500
		threshold = int64(2 << 20)
	)
	tests := []struct {
		name            string
		checkpoint      bool
		wantPeakAtLeast int64
	}{
		{name: "auto-checkpoint off keeps every frame in the wal", wantPeakAtLeast: 16 << 20},
		{name: "checkpointer holds the wal under its ceiling", checkpoint: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			ctx, cancel := context.WithCancel(context.Background())
			peak := sampleWALPeak(ctx, dbPath)

			checkpointer := testCheckpointer(dbPath, threshold)
			checkpointerDone := make(chan error, 1)
			var afterBatch func()
			if tt.checkpoint {
				go func() { checkpointerDone <- checkpointer.run(ctx) }()
				// Wait until the checkpointer has brought the WAL back under the
				// threshold. The size is rechecked on every spin, so a truncation
				// that lands between any two reads still releases the writer.
				afterBatch = func() {
					deadline := time.Now().Add(30 * time.Second)
					for fileSizeOrZero(walPath(dbPath)) >= threshold {
						if time.Now().After(deadline) {
							t.Fatalf("checkpointer did not truncate a %d byte wal within 30s (%+v)", fileSizeOrZero(walPath(dbPath)), checkpointer.stats())
						}
						runtime.Gosched()
					}
				}
			} else {
				checkpointerDone <- nil
			}

			writeUncheckpointedRows(t, dbPath, rows, batch, afterBatch)
			cancel()
			if err := <-checkpointerDone; err != nil {
				t.Fatalf("checkpointer.run() error = %v", err)
			}
			got := peak()
			t.Logf("peak wal = %d bytes, final db = %d bytes", got, fileSizeOrZero(dbPath))

			if tt.wantPeakAtLeast > 0 && got < tt.wantPeakAtLeast {
				t.Fatalf("peak wal = %d bytes, want at least %d", got, tt.wantPeakAtLeast)
			}
			if !tt.checkpoint {
				return
			}
			if got >= checkpointer.ceiling {
				t.Fatalf("peak wal = %d bytes, want below the %d byte ceiling", got, checkpointer.ceiling)
			}
			stats := checkpointer.stats()
			if stats.Checkpoints == 0 || stats.Truncations == 0 {
				t.Fatalf("checkpointer never truncated: %+v", stats)
			}
			if stats.PeakWALBytes >= checkpointer.ceiling {
				t.Fatalf("checkpointer observed peak %d, want below the %d byte ceiling", stats.PeakWALBytes, checkpointer.ceiling)
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
				t.Fatalf("rows after checkpointing = %d, %v; want %d", count, err, rows)
			}
		})
	}
}

// TestRunPopulateCommandBoundsWALWithPinnedBinary measures the real pinned
// workload binary (built with SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0, as
// Dockerfile.worker builds it) under the harness checkpointer. It runs only
// when LITESTREAM_TEST_BINARY points at such a build; POPULATE_TARGET_SIZE
// (default 512MB) and POPULATE_INDEX_RATIO (default 1) shape the run.
func TestRunPopulateCommandBoundsWALWithPinnedBinary(t *testing.T) {
	binary := os.Getenv("LITESTREAM_TEST_BINARY")
	if binary == "" {
		t.Skip("set LITESTREAM_TEST_BINARY to a litestream-test build to measure populate under the checkpointer")
	}
	target := os.Getenv("POPULATE_TARGET_SIZE")
	if target == "" {
		target = "512MB"
	}
	ratio := os.Getenv("POPULATE_INDEX_RATIO")
	if ratio == "" {
		ratio = "1"
	}
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx, cancel := context.WithCancel(context.Background())
	peak := sampleWALPeak(ctx, dbPath)
	cmd := exec.CommandContext(ctx, binary, "populate", "-db", dbPath, "-target-size", target, "-index-ratio", ratio)
	started := time.Now()
	stats, err := runPopulateCommand(ctx, cmd, dbPath, newPopulateCheckpointer(dbPath))
	elapsed := time.Since(started)
	cancel()
	observed := peak()
	if err != nil {
		t.Fatalf("runPopulateCommand() error = %v", err)
	}
	dbSize := fileSizeOrZero(dbPath)
	t.Logf("target %s index-ratio %s: db %d bytes, sampled peak wal %d bytes, checkpointer peak wal %d bytes, checkpoints %d (%d truncations), elapsed %s",
		target, ratio, dbSize, observed, stats.PeakWALBytes, stats.Checkpoints, stats.Truncations, elapsed.Round(time.Millisecond))
	if observed >= 4*populateCheckpointWALThreshold {
		t.Fatalf("sampled peak wal %d bytes exceeds 4x the %d byte threshold", observed, populateCheckpointWALThreshold)
	}
	if size := fileSizeOrZero(walPath(dbPath)); size != 0 {
		t.Fatalf("wal after populate = %d bytes, want 0", size)
	}
}

// testCheckpointer mirrors newPopulateCheckpointer with a small threshold and
// the production ratios, so tests exercise the same bound logic quickly.
func testCheckpointer(dbPath string, threshold int64) *populateCheckpointer {
	return &populateCheckpointer{
		dbPath:      dbPath,
		threshold:   threshold,
		ceiling:     8 * threshold,
		interval:    populateCheckpointPollInterval,
		spinLimit:   populateCheckpointSpinLimit,
		lockHold:    populateCheckpointLockHold,
		maxFailures: populateCheckpointMaxFailures,
	}
}

// openWALHolder writes rows with auto-checkpoint off and keeps the connection
// open, so the WAL stays populated for the test; with holdWriteLock it also
// keeps a write transaction open, which is what a populate that never yields
// looks like to the checkpointer.
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

func TestPopulateCheckpointerAbortsWhenTheBoundIsLost(t *testing.T) {
	tests := []struct {
		name          string
		holdWriteLock bool
		tune          func(*populateCheckpointer)
		wantErr       string
	}{
		{
			name:          "write lock starvation after consecutive failures",
			holdWriteLock: true,
			tune: func(c *populateCheckpointer) {
				c.ceiling = 64 << 20
				c.spinLimit = 20 * time.Millisecond
				c.maxFailures = 2
			},
			wantErr: "2 consecutive wal checkpoints failed",
		},
		{
			name:    "wal at the ceiling aborts at once",
			tune:    func(c *populateCheckpointer) { c.ceiling = 1 },
			wantErr: "at or above the 1 byte ceiling",
		},
		{
			name:          "backfill lock hold deadline counts as a failure and releases the lock",
			holdWriteLock: false,
			tune:          func(c *populateCheckpointer) { c.ceiling = 64 << 20; c.lockHold = time.Nanosecond; c.maxFailures = 2 },
			wantErr:       "2 consecutive wal checkpoints failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			holder := openWALHolder(t, dbPath, 400, tt.holdWriteLock)
			if size := fileSizeOrZero(walPath(dbPath)); size < 256<<10 {
				t.Fatalf("fixture wal = %d bytes, want frames to checkpoint", size)
			}
			checkpointer := testCheckpointer(dbPath, 64<<10)
			checkpointer.interval = 5 * time.Millisecond
			tt.tune(checkpointer)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			started := time.Now()
			err := checkpointer.run(ctx)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run() error = %v, want %q", err, tt.wantErr)
			}
			if ctx.Err() != nil {
				t.Fatalf("run() only stopped because the test deadline passed after %s", time.Since(started))
			}
			if tt.holdWriteLock {
				if _, err := holder.ExecContext(context.Background(), "COMMIT"); err != nil {
					t.Fatalf("holder commit: %v", err)
				}
			}
			if _, err := holder.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
				t.Fatalf("write lock not released after run(): %v", err)
			}
			if _, err := holder.ExecContext(context.Background(), "ROLLBACK"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestPopulateCheckpointerEnforcesCeilingWhileWaitingForWriteLock grows the
// WAL past the ceiling while the checkpointer is still spinning for a write
// lock a populate-like holder never releases. The breach must end the run
// during the wait, long before the spin limit, not after a later checkpoint
// has hidden it.
func TestPopulateCheckpointerEnforcesCeilingWhileWaitingForWriteLock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	holder := openWALHolder(t, dbPath, 200, true)
	start := fileSizeOrZero(walPath(dbPath))
	checkpointer := testCheckpointer(dbPath, start/2)
	checkpointer.interval = 5 * time.Millisecond
	checkpointer.ceiling = start + 512<<10
	checkpointer.spinLimit = 30 * time.Second
	checkpointer.maxFailures = 1

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result := make(chan error, 1)
	started := time.Now()
	go func() { result <- checkpointer.run(ctx) }()

	payload := make([]byte, 1024)
	deadline := time.Now().Add(10 * time.Second)
	for fileSizeOrZero(walPath(dbPath)) < checkpointer.ceiling+64<<10 && time.Now().Before(deadline) {
		if _, err := holder.ExecContext(context.Background(), "INSERT INTO test_table_0 (data, int_field) VALUES (?, ?)", payload, rand.Int64()); err != nil {
			t.Fatalf("holder insert while checkpointer waits: %v", err)
		}
	}
	grown := fileSizeOrZero(walPath(dbPath))
	if grown < checkpointer.ceiling {
		t.Fatalf("wal only grew to %d bytes, ceiling %d", grown, checkpointer.ceiling)
	}

	select {
	case err := <-result:
		elapsed := time.Since(started)
		if err == nil || !errors.Is(err, errWALCeiling) || !strings.Contains(err.Error(), "while waiting for the write lock") {
			t.Fatalf("run() error = %v, want the ceiling breach detected during the write-lock wait", err)
		}
		if elapsed >= checkpointer.spinLimit {
			t.Fatalf("breach detected only after the %s spin limit (%s)", checkpointer.spinLimit, elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("checkpointer kept waiting for the write lock after the wal passed the ceiling")
	}
	if _, err := holder.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
}

func TestRunPopulateCommandKillsPopulateWhenTheBoundIsLost(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sleep binary requires Unix")
	}
	dbPath := filepath.Join(t.TempDir(), "test.db")
	openWALHolder(t, dbPath, 400, true)
	checkpointer := testCheckpointer(dbPath, 64<<10)
	checkpointer.interval = 5 * time.Millisecond
	checkpointer.ceiling = 64 << 20
	checkpointer.spinLimit = 20 * time.Millisecond
	checkpointer.maxFailures = 2

	cmd := exec.Command("sleep", "30")
	started := time.Now()
	_, err := runPopulateCommand(context.Background(), cmd, dbPath, checkpointer)
	elapsed := time.Since(started)
	if err == nil || !strings.Contains(err.Error(), "aborted populate") || !strings.Contains(err.Error(), "consecutive wal checkpoints failed") {
		t.Fatalf("runPopulateCommand() error = %v, want aborted populate after checkpoint failures", err)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("populate was not killed promptly: %s", elapsed)
	}
	if cmd.ProcessState == nil || cmd.ProcessState.Success() {
		t.Fatalf("populate process state = %v, want killed", cmd.ProcessState)
	}
}

func TestWALCheckpointTruncateReportsBusy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	writeUncheckpointedRows(t, dbPath, 200, 50, nil)

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
	err = walCheckpoint(context.Background(), db, "TRUNCATE")
	if err == nil {
		t.Fatal("walCheckpoint(TRUNCATE) succeeded with a reader pinning the wal, want busy")
	}
}

// populateFixtureDB builds the database the fake litestream-test copies into
// place, with the index populate creates at -index-ratio 1.
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
			writeFakeLitestreamTest(t, binDir, tt.script)
			t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("POPULATE_FIXTURE", fixture)
			t.Setenv("POPULATE_GODEBUG_OUT", godebugOut)
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
