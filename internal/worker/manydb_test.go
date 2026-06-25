package worker

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestManyDBPathsAndActiveSubset(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 20

	paths := cfg.ManyDBPaths()
	if len(paths) != 10 {
		t.Fatalf("ManyDBPaths() len = %d, want 10", len(paths))
	}
	if paths[0] != "/data/dbs/db-00001.db" {
		t.Fatalf("first path = %q, want /data/dbs/db-00001.db", paths[0])
	}
	if paths[9] != "/data/dbs/db-00010.db" {
		t.Fatalf("last path = %q, want /data/dbs/db-00010.db", paths[9])
	}

	active := cfg.ManyDBActivePathsAt(time.Unix(0, 0))
	if len(active) != 2 {
		t.Fatalf("ManyDBActivePaths() len = %d, want 2", len(active))
	}
	for _, activePath := range active {
		if !slices.Contains(paths, activePath) {
			t.Fatalf("active path %q is not in all paths %v", activePath, paths)
		}
	}
}

func TestManyDBActivePathsRotateDeterministically(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 30
	cfg.ActiveRotateInterval = time.Minute
	cfg.ActiveSetSeed = 42

	base := time.Unix(1700000000, 0)
	first := cfg.ManyDBActivePathsAt(base)
	sameInterval := cfg.ManyDBActivePathsAt(base.Add(30 * time.Second))
	nextInterval := cfg.ManyDBActivePathsAt(base.Add(time.Minute))
	repeated := cfg.ManyDBActivePathsAt(base.Add(time.Minute))

	if len(first) != 3 {
		t.Fatalf("first active len = %d, want 3: %v", len(first), first)
	}
	if !slices.Equal(first, sameInterval) {
		t.Fatalf("active paths changed inside one interval: %v vs %v", first, sameInterval)
	}
	if slices.Equal(first, nextInterval) {
		t.Fatalf("active paths did not rotate across intervals: %v", first)
	}
	if !slices.Equal(nextInterval, repeated) {
		t.Fatalf("active paths are not deterministic for same seed and interval: %v vs %v", nextInterval, repeated)
	}
	assertUniqueSubset(t, cfg.ManyDBPaths(), first)
	assertUniqueSubset(t, cfg.ManyDBPaths(), nextInterval)
}

func TestManyDBLoadCurrentActivePathsRotates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 20
	cfg.ActiveRotateInterval = time.Minute
	cfg.ActiveSetSeed = 7

	now := time.Unix(1700000000, 0)
	load := newManyDBLoad(&cfg)
	load.now = func() time.Time { return now }

	first := load.currentActivePaths()
	now = now.Add(time.Minute)
	next := load.currentActivePaths()

	if !slices.Equal(first, cfg.ManyDBActivePathsAt(time.Unix(1700000000, 0))) {
		t.Fatalf("currentActivePaths() = %v, want config snapshot", first)
	}
	if !slices.Equal(next, cfg.ManyDBActivePathsAt(time.Unix(1700000060, 0))) {
		t.Fatalf("currentActivePaths() after rotation = %v, want config snapshot", next)
	}
	if slices.Equal(first, next) {
		t.Fatalf("currentActivePaths() did not rotate: %v", first)
	}
}

func TestManyDBLoadChangedPathsReset(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 5

	paths := cfg.ManyDBPaths()
	load := newManyDBLoad(&cfg)
	load.markChanged(paths[3])
	load.markChanged(paths[1])
	load.markChanged(paths[3])

	got := load.manyDBChangedPathsAndReset()
	want := []string{paths[1], paths[3]}
	if !slices.Equal(got, want) {
		t.Fatalf("changed paths = %v, want %v", got, want)
	}

	if got := load.manyDBChangedPathsAndReset(); len(got) != 0 {
		t.Fatalf("changed paths after reset = %v, want empty", got)
	}
}

func TestPopulateManyDBCreatesWALDatabases(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.NumDatabases = 3
	cfg.ActivePercent = 0

	runner := NewRunner(cfg)
	if err := runner.populate(context.Background()); err != nil {
		t.Fatalf("populate() error = %v", err)
	}

	for _, dbPath := range cfg.ManyDBPaths() {
		if _, err := os.Stat(dbPath); err != nil {
			t.Fatalf("stat seeded database %s: %v", dbPath, err)
		}
		db, err := sql.Open("sqlite", walDSN(dbPath))
		if err != nil {
			t.Fatalf("open seeded database %s: %v", dbPath, err)
		}
		var journalMode string
		if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
			_ = db.Close()
			t.Fatalf("read journal mode for %s: %v", dbPath, err)
		}
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM soak_payloads").Scan(&count); err != nil {
			_ = db.Close()
			t.Fatalf("read seeded row count for %s: %v", dbPath, err)
		}
		_ = db.Close()
		if journalMode != "wal" {
			t.Fatalf("journal_mode for %s = %q, want wal", dbPath, journalMode)
		}
		if count == 0 {
			t.Fatalf("seeded row count for %s = 0, want > 0", dbPath)
		}
	}
}

func TestWriteLitestreamConfigManyDBListUsesIsolatedReplicaTargets(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.NumDatabases = 3
	cfg.ConfigMode = "list"
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-many"

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}

	body, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	config := string(body)
	for _, dbName := range []string{"db-00001.db", "db-00002.db", "db-00003.db"} {
		if !strings.Contains(config, "path: "+filepath.Join(dir, "dbs", dbName)) {
			t.Fatalf("config missing db path %s:\n%s", dbName, config)
		}
		if !strings.Contains(config, "url: s3://bucket/soak/worker-many/"+dbName) {
			t.Fatalf("config missing isolated replica for %s:\n%s", dbName, config)
		}
	}
}

func TestWriteLitestreamConfigManyDBDirUsesWatcher(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.NumDatabases = 3
	cfg.ConfigMode = "dir"
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-many"

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}

	body, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	config := string(body)
	for _, want := range []string{
		"dir: " + filepath.Join(dir, "dbs"),
		`pattern: "*.db"`,
		"watch: true",
		"url: s3://bucket/soak/worker-many",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("config missing %q:\n%s", want, config)
		}
	}
}

func TestPollDBStatsAggregatesManyDBList(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-many-poll"
	cfg.ProfileName = "many-dbs-100-dir"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.NumDatabases = 3
	cfg.ActivePercent = 33
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-many-%d.sock", time.Now().UnixNano()))
	cfg.ReplicationLagThreshold = 1

	for i, dbPath := range cfg.ManyDBPaths() {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dbPath, []byte(strings.Repeat("d", 10+i)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dbPath+"-wal", []byte(strings.Repeat("w", 2+i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	SetWorkerInfo(cfg)
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info":
			_, _ = w.Write([]byte(`{"uptime_seconds":99}`))
		case "/list":
			now := time.Now().UTC()
			body := fmt.Sprintf(`{"databases":[
{"path":%q,"status":"replicating","txid":10,"replicated_txid":10,"last_sync_at":%q},
{"path":%q,"status":"replicating","txid":10,"replicated_txid":8,"last_sync_at":%q},
{"path":%q,"status":"replicating","txid":20,"replicated_txid":15,"last_sync_at":%q}
]}`,
				cfg.ManyDBPaths()[0], now.Add(-1*time.Second).Format(time.RFC3339Nano),
				cfg.ManyDBPaths()[1], now.Add(-10*time.Second).Format(time.RFC3339Nano),
				cfg.ManyDBPaths()[2], now.Add(-30*time.Second).Format(time.RFC3339Nano),
			)
			_, _ = w.Write([]byte(body))
		default:
			http.NotFound(w, r)
		}
	}))

	runner := NewRunner(cfg)
	runner.pollDBStats()
	snapshot := runner.currentSnapshot()

	if snapshot.DBCount != 3 {
		t.Fatalf("DBCount = %d, want 3", snapshot.DBCount)
	}
	if snapshot.DBTotalSizeBytes != 33 {
		t.Fatalf("DBTotalSizeBytes = %d, want 33", snapshot.DBTotalSizeBytes)
	}
	if snapshot.WALTotalSizeBytes != 9 {
		t.Fatalf("WALTotalSizeBytes = %d, want 9", snapshot.WALTotalSizeBytes)
	}
	if snapshot.ReplicationLagMax != 5 {
		t.Fatalf("ReplicationLagMax = %d, want 5", snapshot.ReplicationLagMax)
	}
	if snapshot.ReplicationLagP95 != 5 {
		t.Fatalf("ReplicationLagP95 = %d, want 5", snapshot.ReplicationLagP95)
	}
	if snapshot.ReplicationLagOverThreshold != 2 {
		t.Fatalf("ReplicationLagOverThreshold = %d, want 2", snapshot.ReplicationLagOverThreshold)
	}
	if snapshot.LastSyncAgeMaxSeconds < 20 {
		t.Fatalf("LastSyncAgeMaxSeconds = %v, want >= 20", snapshot.LastSyncAgeMaxSeconds)
	}
	if !snapshot.LitestreamSnapshotHealthy {
		t.Fatalf("expected healthy snapshot, got error %q", snapshot.LitestreamSnapshotError)
	}
}

func TestManyDBVerificationTargetsUseChangedPaths(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 20
	cfg.VerifySampleSize = 5

	paths := cfg.ManyDBPaths()
	changed := []string{paths[7], paths[2], paths[7], paths[4]}
	targets, totalChanged := selectManyDBVerificationTargets(cfg, changed)
	want := []string{paths[2], paths[4], paths[7]}

	if totalChanged != len(want) {
		t.Fatalf("totalChanged = %d, want %d", totalChanged, len(want))
	}
	if !slices.Equal(targets, want) {
		t.Fatalf("targets = %v, want changed paths %v", targets, want)
	}
}

func TestManyDBVerificationTargetsTruncateChangedPaths(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 60
	cfg.VerifySampleSize = 2
	cfg.VerifyChangedLimit = 3

	paths := cfg.ManyDBPaths()
	changed := []string{paths[6], paths[1], paths[4], paths[8], paths[2]}
	targets, totalChanged := selectManyDBVerificationTargets(cfg, changed)
	want := []string{paths[1], paths[2], paths[4]}

	if totalChanged != 5 {
		t.Fatalf("totalChanged = %d, want 5", totalChanged)
	}
	if !slices.Equal(targets, want) {
		t.Fatalf("targets = %v, want capped changed paths %v", targets, want)
	}
}

func assertUniqueSubset(t *testing.T, all []string, subset []string) {
	t.Helper()

	seen := map[string]bool{}
	for _, path := range subset {
		if seen[path] {
			t.Fatalf("duplicate path %q in %v", path, subset)
		}
		if !slices.Contains(all, path) {
			t.Fatalf("path %q is not in all paths %v", path, all)
		}
		seen[path] = true
	}
}
