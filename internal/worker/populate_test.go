package worker

import (
	"context"
	"database/sql"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

// writeUncheckpointedRows mimics litestream-test populate as the harness
// builds it: WAL mode, auto-checkpoint disabled, batched inserts of 1 KiB rows
// with a random-integer index that rewrites index pages on every batch.
func writeUncheckpointedRows(t *testing.T, dbPath string, rows, batch int) {
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
		wantPeakBelow   int64
	}{
		{name: "auto-checkpoint off keeps every frame in the wal", wantPeakAtLeast: 16 << 20},
		{name: "checkpointer truncates the wal past its threshold", checkpoint: true, wantPeakBelow: 4 * threshold},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			ctx, cancel := context.WithCancel(context.Background())
			peak := sampleWALPeak(ctx, dbPath)

			checkpointer := &populateCheckpointer{dbPath: dbPath, threshold: threshold, interval: 5 * time.Millisecond, spinLimit: populateCheckpointSpinLimit}
			checkpointerDone := make(chan struct{})
			if tt.checkpoint {
				go func() {
					defer close(checkpointerDone)
					checkpointer.run(ctx)
				}()
			} else {
				close(checkpointerDone)
			}

			writeUncheckpointedRows(t, dbPath, rows, batch)
			cancel()
			<-checkpointerDone
			got := peak()
			t.Logf("peak wal = %d bytes, final db = %d bytes", got, fileSizeOrZero(dbPath))

			if tt.wantPeakAtLeast > 0 && got < tt.wantPeakAtLeast {
				t.Fatalf("peak wal = %d bytes, want at least %d", got, tt.wantPeakAtLeast)
			}
			if tt.wantPeakBelow > 0 && got >= tt.wantPeakBelow {
				t.Fatalf("peak wal = %d bytes, want below %d", got, tt.wantPeakBelow)
			}
			if !tt.checkpoint {
				return
			}
			stats := checkpointer.stats()
			if stats.Checkpoints == 0 {
				t.Fatalf("checkpointer never ran: %+v", stats)
			}
			if stats.PeakWALBytes >= tt.wantPeakBelow {
				t.Fatalf("checkpointer observed peak %d, want below %d", stats.PeakWALBytes, tt.wantPeakBelow)
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
	stats, err := runPopulateCommand(ctx, cmd, dbPath)
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

func TestWALCheckpointTruncateReportsBusy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	writeUncheckpointedRows(t, dbPath, 200, 50)

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
