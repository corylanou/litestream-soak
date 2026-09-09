package worker

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func logicalTestDB(t *testing.T, path, statements string) *sql.DB {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(statements); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestValidateLogicalContent(t *testing.T) {
	const schema = `CREATE TABLE records (id INTEGER PRIMARY KEY, value); INSERT INTO records VALUES (1, 'first'), (2, 'second');`
	for _, tt := range []struct{ name, change, want string }{
		{"equal", "", ""},
		{"missing row", "DELETE FROM records WHERE id=2", "records"},
		{"altered value", "UPDATE records SET value='changed' WHERE id=2", "records"},
		{"partial transaction", "BEGIN; UPDATE records SET value='changed' WHERE id=1; DELETE FROM records WHERE id=2; COMMIT", "records"},
		{"schema", "ALTER TABLE records ADD COLUMN extra TEXT", "schema"},
		{"physical layout", "PRAGMA page_size=8192; VACUUM", ""},
		{"storage type", "UPDATE records SET value=CAST(value AS BLOB) WHERE id=1", "records"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := DefaultConfig()
			cfg.DBPath = filepath.Join(dir, "source.db")
			cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
			logicalTestDB(t, cfg.DBPath, schema)
			restored := cfg.DBPath + ".restored"
			logicalTestDB(t, restored, schema+tt.change)
			writeFakePinnedRestore(t, dir, "exit 0\n")
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			startBoundarySyncFixture(t, &cfg, 42)
			passed, err := NewVerifier(cfg).validate(context.Background(), 42)
			if tt.want == "" {
				if err != nil || !passed {
					t.Fatalf("validate = %v, %v", passed, err)
				}
			} else if err == nil || passed || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validate = %v, %v; want mismatch for %s", passed, err, tt.want)
			}
		})
	}
}

func TestLogicalSnapshotValuesAndWAL(t *testing.T) {
	const schema = `CREATE TABLE "odd""table" (value, stamp DATETIME);`
	cases := []struct {
		name, source, restored string
		equal                  bool
	}{
		{"null versus empty", "NULL", "''", false},
		{"text bytes", "CAST(x'610062' AS TEXT)", "CAST(x'610063' AS TEXT)", false},
		{"blob bytes", "x'0001ff'", "x'0001fe'", false},
		{"text versus blob", "'abc'", "x'616263'", false},
		{"integer precision", "9223372036854775807", "9223372036854775806", false},
		{"real precision", "1.0000000000000002", "1.0", false},
		{"integer versus real", "1", "1.0", false},
		{"null", "NULL", "NULL", true},
		{"blob", "x'00ff'", "x'00ff'", true},
		{"negative real", "-1.25", "-1.25", true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			sourcePath, restoredPath := filepath.Join(dir, "source.db"), filepath.Join(dir, "restored.db")
			source := logicalTestDB(t, sourcePath, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;"+schema)
			logicalTestDB(t, restoredPath, schema+`INSERT INTO "odd""table" VALUES (`+tt.restored+`, '2026-01-01 00:00:00');`)
			if _, err := source.Exec(`INSERT INTO "odd""table" VALUES (` + tt.source + `, '2026-01-01 00:00:00')`); err != nil {
				t.Fatal(err)
			}
			if info, err := os.Stat(sourcePath + "-wal"); err != nil || info.Size() == 0 {
				t.Fatalf("committed WAL absent: %v", err)
			}
			expected, err := readLogicalSnapshot(context.Background(), sourcePath, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			actual, err := readLogicalSnapshot(context.Background(), restoredPath, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			if err := compareLogicalSnapshots(expected, actual); (err == nil) != tt.equal {
				t.Fatalf("comparison = %v, equal=%v", err, tt.equal)
			}
		})
	}
}

func TestLogicalSnapshotOrderAndDuplicates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")
	schema := `CREATE TABLE t (value COLLATE NOCASE);`
	logicalTestDB(t, a, schema+`INSERT INTO t VALUES ('A'), ('a'), (NULL), (1), (1.0), ('A');`)
	db := logicalTestDB(t, b, schema+`INSERT INTO t(rowid,value) VALUES (6,'A'), (5,1.0), (2,'a'), (4,1), (1,'A'), (3,NULL); VACUUM;`)
	expected, err := readLogicalSnapshot(context.Background(), a, DefaultConfig().logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	actual, err := readLogicalSnapshot(context.Background(), b, DefaultConfig().logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := compareLogicalSnapshots(expected, actual); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM t WHERE rowid=1`); err != nil {
		t.Fatal(err)
	}
	actual, err = readLogicalSnapshot(context.Background(), b, DefaultConfig().logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := compareLogicalSnapshots(expected, actual); err == nil {
		t.Fatal("lost duplicate passed")
	}
}

func TestLogicalSnapshotLimits(t *testing.T) {
	for _, tt := range []struct {
		name, schema, want string
		limit              func(*logicalLimits)
	}{
		{"rows", `CREATE TABLE t(v); INSERT INTO t VALUES(1),(2);`, "row limit", func(l *logicalLimits) { l.rows = 1 }},
		{"bytes", `CREATE TABLE t(v); INSERT INTO t VALUES('abcdef');`, "byte limit", func(l *logicalLimits) { l.bytes = 2 }},
		{"objects", `CREATE TABLE t(v); CREATE TABLE u(v);`, "object limit", func(l *logicalLimits) { l.objects = 1 }},
		{"value", `CREATE TABLE t(v); INSERT INTO t VALUES(zeroblob(2000));`, "too big", func(l *logicalLimits) { l.valueBytes = 1024 }},
		{"virtual", `CREATE VIRTUAL TABLE t USING rtree(id,x1,x2);`, "unsupported", func(l *logicalLimits) {}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "test.db")
			logicalTestDB(t, path, tt.schema)
			limits := DefaultConfig().logicalLimits()
			tt.limit(&limits)
			_, err := readLogicalSnapshot(context.Background(), path, limits)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("snapshot error=%v, want %q", err, tt.want)
			}
		})
	}
}

func TestLogicalSnapshotCanceledAndMissing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "missing.db")
	if _, err := readLogicalSnapshot(ctx, path, DefaultConfig().logicalLimits()); err == nil {
		t.Fatal("cancellation passed")
	}
	if _, err := readLogicalSnapshot(context.Background(), path, DefaultConfig().logicalLimits()); err == nil {
		t.Fatal("missing database passed")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("read created database: %v", err)
	}
}

func TestLogicalConfig(t *testing.T) {
	for _, name := range []string{"VERIFY_LOGICAL_MAX_ROWS", "VERIFY_LOGICAL_MAX_BYTES", "VERIFY_LOGICAL_MAX_OBJECTS", "VERIFY_LOGICAL_MAX_VALUE_BYTES", "VERIFY_LOGICAL_TIMEOUT"} {
		t.Run(name, func(t *testing.T) {
			for _, value := range []string{"0", "-1", "invalid"} {
				t.Setenv(name, value)
				cfg := DefaultConfig()
				if err := loadLogicalConfig(&cfg); err == nil {
					t.Fatalf("%s=%s accepted", name, value)
				}
			}
		})
	}
	t.Run("overrides", func(t *testing.T) {
		t.Setenv("VERIFY_LOGICAL_MAX_ROWS", "3000000000")
		t.Setenv("VERIFY_LOGICAL_MAX_BYTES", "2000000000000")
		t.Setenv("VERIFY_LOGICAL_MAX_OBJECTS", "6000")
		t.Setenv("VERIFY_LOGICAL_MAX_VALUE_BYTES", "100000000")
		t.Setenv("VERIFY_LOGICAL_TIMEOUT", "1h")
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.LogicalMaxRows != 3000000000 || cfg.LogicalMaxBytes != 2000000000000 || cfg.LogicalMaxObjects != 6000 || cfg.LogicalMaxValueBytes != 100000000 || cfg.LogicalTimeout.String() != "1h0m0s" {
			t.Fatalf("overrides not applied: %+v", cfg.logicalLimits())
		}
	})
}

func TestLogicalSnapshotRowIDAndStatistics(t *testing.T) {
	for _, tt := range []struct {
		name, schema, change string
		equal                bool
	}{
		{"implicit rowid", `CREATE TABLE t(v); INSERT INTO t(rowid,v) VALUES(1,'same');`, `UPDATE t SET rowid=9`, false},
		{"shadowed rowid", `CREATE TABLE t(rowid,v); INSERT INTO t(_rowid_,rowid,v) VALUES(1,'visible','same');`, `UPDATE t SET _rowid_=9`, false},
		{"without rowid", `CREATE TABLE t(k TEXT PRIMARY KEY,v) WITHOUT ROWID; INSERT INTO t VALUES('key','same');`, `VACUUM`, true},
		{"statistics", `CREATE TABLE t(v); CREATE INDEX idx ON t(v); INSERT INTO t VALUES(1),(2); ANALYZE;`, ``, true},
		{"statistics mismatch", `CREATE TABLE t(v); CREATE INDEX idx ON t(v); INSERT INTO t VALUES(1),(2); ANALYZE;`, `UPDATE sqlite_stat1 SET stat='100 50'`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			a, b := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")
			logicalTestDB(t, a, tt.schema)
			logicalTestDB(t, b, tt.schema+tt.change)
			x, err := readLogicalSnapshot(context.Background(), a, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			y, err := readLogicalSnapshot(context.Background(), b, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			if err := compareLogicalSnapshots(x, y); (err == nil) != tt.equal {
				t.Fatalf("comparison=%v, equal=%v", err, tt.equal)
			}
		})
	}
}

func TestManyDBLogicalVerification(t *testing.T) {
	for _, tt := range []struct {
		name    string
		corrupt bool
		limit   int
		want    string
	}{
		{"equal", false, 2, ""},
		{"missing committed row", true, 2, "source_rows=2 restored_rows=1"},
		{"truncated targets", false, 1, "pending"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := DefaultConfig()
			cfg.DataDir = dir
			cfg.NumDatabases = 2
			cfg.ActivePercent = 100
			cfg.VerifyChangedLimit = tt.limit
			cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
			cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("logical-%d.sock", time.Now().UnixNano()))
			cfg.GitSHA = "soak-fixed"
			t.Setenv("WORKLOAD_SHA", "workload-fixed")
			fixtures := filepath.Join(dir, "fixtures")
			for i, path := range cfg.ManyDBPaths() {
				schema := `CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,'a'),(2,'b');`
				logicalTestDB(t, path, schema)
				if tt.corrupt && i == 1 {
					schema += `DELETE FROM t WHERE id=2;`
				}
				logicalTestDB(t, filepath.Join(fixtures, filepath.Base(path)), schema)
			}
			writeFakePinnedRestore(t, dir, `
while [ "$#" -gt 0 ]; do
 case "$1" in
 -config|-txid) shift ;;
 -o) restored="$2"; shift ;;
 *) source="$1" ;;

 esac
 shift
done
cp "$LOGICAL_FIXTURES/$(basename "$source")" "$restored"
`)
			t.Setenv("LOGICAL_FIXTURES", fixtures)
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/sync" {
					_, _ = w.Write([]byte(`{"status":"ok","txid":42,"replicated_txid":42}`))
				} else {
					_, _ = w.Write([]byte(`{"active":false}`))
				}
			}))
			pauser := &fakePauser{}
			result, err := NewVerifier(cfg, pauser).RunCycle(context.Background())
			if tt.want == "pending" {
				if err != nil || result.Passed || result.Status != "pending" {
					t.Fatalf("cycle=%+v, %v", result, err)
				}
			} else if tt.want == "" {
				if err != nil || !result.Passed {
					t.Fatalf("cycle=%+v, %v", result, err)
				}
			} else if err == nil || result.Passed || !strings.Contains(result.ErrorMessage, tt.want) {
				t.Fatalf("cycle=%+v, %v; want %q", result, err, tt.want)
			}
			if pauser.resumeCalls == 0 {
				t.Fatal("load not resumed")
			}
			found := false
			for _, step := range result.Steps {
				if strings.Contains(step.OutputTail, "validator=soak-logical-v1") && strings.Contains(step.OutputTail, "workload-fixed") {
					found = true
				}
			}
			if !found {
				t.Fatal("missing independent validator/workload evidence")
			}
			if tt.want != "" && tt.want != "pending" && !strings.Contains(readFile(t, filepath.Join(dir, "verification.log")), "FAIL") {
				t.Fatal("failure not retained")
			}
		})
	}
}

func TestLogicalLitestreamBookkeeping(t *testing.T) {
	for _, tt := range []struct {
		name, schema, change string
		equal                bool
	}{
		{"sequence advance", `CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER); INSERT INTO _litestream_seq VALUES(1,1);`, `UPDATE _litestream_seq SET seq=10;`, true},
		{"first sequence", `CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);`, `INSERT INTO _litestream_seq VALUES(1,1);`, true},
		{"similar user name", `CREATE TABLE _litestream_seq_user (id INTEGER PRIMARY KEY, seq INTEGER); INSERT INTO _litestream_seq_user VALUES(1,1);`, `UPDATE _litestream_seq_user SET seq=10;`, false},
		{"user schema at reserved name", `CREATE TABLE _litestream_seq (value); INSERT INTO _litestream_seq VALUES('a');`, `UPDATE _litestream_seq SET value='b';`, false},
		{"lock table content", `CREATE TABLE _litestream_lock (id INTEGER);`, `INSERT INTO _litestream_lock VALUES(1);`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			a, b := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")
			logicalTestDB(t, a, tt.schema)
			logicalTestDB(t, b, tt.schema+tt.change)
			x, err := readLogicalSnapshot(context.Background(), a, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			y, err := readLogicalSnapshot(context.Background(), b, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			if err := compareLogicalSnapshots(x, y); (err == nil) != tt.equal {
				t.Fatalf("comparison=%v, equal=%v", err, tt.equal)
			}
		})
	}
}

func TestLogicalCommittedBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")
	schema := `PRAGMA journal_mode=WAL;CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,'old'),(2,'old');`
	source := logicalTestDB(t, a, schema)
	restored := logicalTestDB(t, b, schema)
	if _, err := source.Exec(`BEGIN;UPDATE t SET v='new';INSERT INTO t VALUES(3,'new');COMMIT;`); err != nil {
		t.Fatal(err)
	}
	expected, err := readLogicalSnapshot(context.Background(), a, DefaultConfig().logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Exec(`UPDATE t SET v='new' WHERE id=1;INSERT INTO t VALUES(3,'new');`); err != nil {
		t.Fatal(err)
	}
	partial, err := readLogicalSnapshot(context.Background(), b, DefaultConfig().logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := compareLogicalSnapshots(expected, partial); err == nil {
		t.Fatal("partially applied transaction passed with equal row count")
	}
	if _, err := restored.Exec(`UPDATE t SET v='new' WHERE id=2;`); err != nil {
		t.Fatal(err)
	}
	tx, err := source.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`INSERT INTO t VALUES(4,'uncommitted')`); err != nil {
		t.Fatal(err)
	}
	committed, err := readLogicalSnapshot(context.Background(), a, DefaultConfig().logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := compareLogicalSnapshots(expected, committed); err != nil {
		t.Fatalf("uncommitted data leaked: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	actual, err := readLogicalSnapshot(context.Background(), b, DefaultConfig().logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := compareLogicalSnapshots(expected, actual); err != nil {
		t.Fatalf("captured boundary changed after later commit: %v", err)
	}
}

func TestValidationOutputBounded(t *testing.T) {
	cmd := exec.Command("sh", "-c", `yes evidence | head -c 200000`)
	output, err := boundedValidationOutput(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) > syncDiagnosticOutputLimit+100 || !strings.Contains(string(output), "truncated") {
		t.Fatalf("unbounded or unlabeled output: %d bytes", len(output))
	}
}

func TestLogicalSchemaMetadata(t *testing.T) {
	for _, change := range []string{"PRAGMA user_version=2", "PRAGMA application_id=42", "CREATE INDEX idx ON t(v)", "CREATE VIEW view_t AS SELECT v FROM t", "CREATE TRIGGER trigger_t AFTER INSERT ON t BEGIN SELECT 1; END"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			a, b := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")
			logicalTestDB(t, a, "CREATE TABLE t(v)")
			logicalTestDB(t, b, "CREATE TABLE t(v);"+change)
			x, err := readLogicalSnapshot(context.Background(), a, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			y, err := readLogicalSnapshot(context.Background(), b, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			if err := compareLogicalSnapshots(x, y); err == nil {
				t.Fatal("schema/metadata discrepancy passed")
			}
		})
	}
}

func TestManyDBPendingCyclesRetainFailuresAndNewWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.NumDatabases = 3
	cfg.ActivePercent = 100
	cfg.VerifyChangedLimit = 2
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pending-%d.sock", time.Now().UnixNano()))
	paths := cfg.ManyDBPaths()
	fixtures := filepath.Join(dir, "fixtures")
	const schema = "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1),(2);"
	var damaged *sql.DB
	for i, path := range paths {
		logicalTestDB(t, path, schema)
		restored := logicalTestDB(t, filepath.Join(fixtures, filepath.Base(path)), schema)
		if i == 0 {
			damaged = restored
			if _, err := damaged.Exec("DELETE FROM t WHERE id=2"); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeFakePinnedRestore(t, dir, `
while [ "$#" -gt 0 ]; do
 case "$1" in
 -config|-txid) shift ;;
 -o) restored="$2"; shift ;;
 *) source="$1" ;;

 esac
 shift
done
cp "$LOGICAL_FIXTURES/$(basename "$source")" "$restored"
`)
	t.Setenv("LOGICAL_FIXTURES", fixtures)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	load := newManyDBLoad(&cfg)
	var mode atomic.Int32
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startTrackedUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sync" {
			switch mode.Load() {
			case 1:
				cancel()
			case 2:
				load.markChanged(paths[0])
			}
			_, _ = w.Write([]byte(`{"status":"ok","txid":42,"replicated_txid":42}`))
		} else {
			_, _ = w.Write([]byte(`{"active":false}`))
		}
	}))
	verifier := NewVerifier(cfg, load)
	first, err := verifier.RunCycle(context.Background())
	if err == nil || first.Status != "failed" {
		t.Fatalf("first failure=%+v, %v", first, err)
	}
	if count, _ := load.manyDBPendingCoverage(); count != 3 {
		t.Fatalf("first failure lost work: %d", count)
	}
	second, err := verifier.RunCycle(context.Background())
	if err != nil || second.Status != "pending" || second.Passed {
		t.Fatalf("later paths=%+v, %v", second, err)
	}
	if changes := load.pendingManyDBChanges(); len(changes) != 1 || changes[0].path != paths[0] {
		t.Fatalf("pending=%+v", changes)
	}
	load.markChanged(paths[1])
	load.markChanged(paths[2])
	mode.Store(1)
	canceled, err := verifier.RunCycle(cancelCtx)
	if err == nil || canceled.Status != "aborted" {
		t.Fatalf("cancel=%+v, %v", canceled, err)
	}
	if count, _ := load.manyDBPendingCoverage(); count != 3 {
		t.Fatalf("cancellation lost work: %d", count)
	}
	if _, err := damaged.Exec("INSERT INTO t VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	mode.Store(0)
	later, err := verifier.RunCycle(context.Background())
	if err != nil || later.Status != "pending" {
		t.Fatalf("progress after cancellation=%+v, %v", later, err)
	}
	if changes := load.pendingManyDBChanges(); len(changes) != 1 || changes[0].path != paths[0] {
		t.Fatalf("canceled path lost: %+v", changes)
	}
	mode.Store(2)
	newer, err := verifier.RunCycle(context.Background())
	if err != nil || newer.Status != "pending" || newer.Passed {
		t.Fatalf("new generation=%+v, %v", newer, err)
	}
	mode.Store(0)
	recovered, err := verifier.RunCycle(context.Background())
	if err != nil || !recovered.Passed {
		t.Fatalf("recovered=%+v, %v", recovered, err)
	}
	if count, age := load.manyDBPendingCoverage(); count != 0 || age != 0 {
		t.Fatalf("coverage=%d, %f", count, age)
	}
	if !strings.Contains(readFile(t, filepath.Join(dir, "verification.log")), "FAIL") {
		t.Fatal("failure evidence lost after recovery")
	}
}
