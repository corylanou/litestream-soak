package worker

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestPreservedChainPinnedLitestream has a real Litestream build replicate a
// database and then lists, copies and restores the replica through the lane's
// own preservation code, so the key layout it depends on is the one Litestream
// writes and not one a test made up. The file replica runs whenever
// SOAK_LOGICAL_LITESTREAM_BINARY is set. The S3 replica also needs
// SOAK_CHAIN_S3_ENDPOINT, SOAK_CHAIN_S3_BUCKET, SOAK_CHAIN_S3_ACCESS_KEY and
// SOAK_CHAIN_S3_SECRET_KEY (a local MinIO is enough); it writes under a random
// soak/chain-e2e-* prefix and deletes what it wrote.
func TestPreservedChainPinnedLitestream(t *testing.T) {
	binary := os.Getenv("SOAK_LOGICAL_LITESTREAM_BINARY")
	if binary == "" {
		t.Skip("opt-in: set SOAK_LOGICAL_LITESTREAM_BINARY to a Litestream executable")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	settle := preservedChainSettle
	preservedChainSettle = 300 * time.Millisecond
	t.Cleanup(func() { preservedChainSettle = settle })

	t.Run("file", func(t *testing.T) {
		cfg := chainIntegrationConfig(t, binary)
		cfg.ReplicaType = "file"
		cfg.ReplicaPath = filepath.Join(cfg.DataDir, "replica")
		writeLaneFile(t, filepath.Join(cfg.ReplicaPath, "profiles", "cpu.pprof.json"), "{}")
		runChainIntegration(t, cfg, []string{"ltx/0/", "ltx/9/"})
	})
	t.Run("s3", func(t *testing.T) {
		endpoint, bucket := os.Getenv("SOAK_CHAIN_S3_ENDPOINT"), os.Getenv("SOAK_CHAIN_S3_BUCKET")
		access, secret := os.Getenv("SOAK_CHAIN_S3_ACCESS_KEY"), os.Getenv("SOAK_CHAIN_S3_SECRET_KEY")
		if endpoint == "" || bucket == "" || access == "" || secret == "" {
			t.Skip("opt-in: set SOAK_CHAIN_S3_ENDPOINT, _BUCKET, _ACCESS_KEY and _SECRET_KEY")
		}
		var id [6]byte
		if _, err := rand.Read(id[:]); err != nil {
			t.Fatal(err)
		}
		cfg := chainIntegrationConfig(t, binary)
		cfg.ReplicaType = "s3"
		cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey = endpoint, bucket, access, secret
		cfg.S3Region = firstNonEmpty(os.Getenv("SOAK_CHAIN_S3_REGION"), "us-east-1")
		cfg.S3Path = "soak/chain-e2e-" + hex.EncodeToString(id[:]) + "/vol_1"
		client, err := replicaS3Client(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			for _, prefix := range []string{cfg.S3Path + "/", cfg.S3Path + "-pre-upgrade/"} {
				objects, err := client.ListObjects(context.Background(), prefix)
				if err != nil {
					t.Errorf("list %s for cleanup: %v", prefix, err)
					continue
				}
				keys := make([]string, 0, len(objects))
				for _, object := range objects {
					keys = append(keys, object.Key)
				}
				if err := client.DeleteObjects(context.Background(), keys); err != nil {
					t.Errorf("delete %d object(s) under %s: %v", len(keys), prefix, err)
				}
			}
		})
		runChainIntegration(t, cfg, []string{"0000/", "0009/"})
	})
}

// chainKeyRange returns the txid range in an LTX key's file name.
func chainKeyRange(t *testing.T, key string) (uint64, uint64) {
	t.Helper()
	name := strings.TrimSuffix(filepath.Base(key), ".ltx")
	low, high, ok := strings.Cut(name, "-")
	minTXID, errMin := strconv.ParseUint(low, 16, 64)
	maxTXID, errMax := strconv.ParseUint(high, 16, 64)
	if !ok || errMin != nil || errMax != nil {
		t.Fatalf("unexpected LTX key %q", key)
	}
	return minTXID, maxTXID
}

func chainIntegrationConfig(t *testing.T, binary string) Config {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("chain-e2e-%d.sock", time.Now().UnixNano()))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.LitestreamMetricsAddr = listener.Addr().String()
	_ = listener.Close()
	cfg.SyncInterval = 200 * time.Millisecond
	cfg.SnapshotInterval = 5 * time.Second
	cfg.L1CompactionInterval, cfg.L2CompactionInterval, cfg.L3CompactionInterval = time.Second, 2*time.Second, 3*time.Second
	cfg.litestreamBinary = binary
	return cfg
}

func runChainIntegration(t *testing.T, cfg Config, wantLevels []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db := logicalTestDB(t, cfg.DBPath, `PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v); INSERT INTO t(v) VALUES('initial');`)
	manager := newLitestreamManager(&cfg)
	if err := manager.writeLitestreamConfig(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(cfg.DataDir, "replicate.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(cfg.litestreamCommand(), "replicate", "-config", cfg.ConfigPath)
	cmd.Stdout, cmd.Stderr = log, log
	if cfg.ReplicaType == "s3" {
		cmd.Env = cfg.s3CommandEnv("")
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = cmd.Process.Signal(syscall.SIGINT)
		_ = cmd.Wait()
		_ = log.Close()
	}
	t.Cleanup(func() {
		stop()
		if t.Failed() {
			t.Log(readFile(t, logPath))
		}
	})

	v := NewVerifier(cfg)
	var synced syncResponse
	for i := range 20 {
		if _, err := db.ExecContext(ctx, `INSERT INTO t(v) VALUES(?)`, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatal(err)
		}
		if !waitUntil(30*time.Second, 50*time.Millisecond, func() bool {
			synced, err = v.syncOnceDB(ctx, time.Second, cfg.DBPath)
			return err == nil && synced.TXID > 0 && synced.ReplicatedTXID >= synced.TXID
		}) {
			t.Fatalf("replica never caught up: %+v, %v", synced, err)
		}
	}
	snapshotLevel := wantLevels[len(wantLevels)-1]
	if !waitUntil(60*time.Second, 250*time.Millisecond, func() bool {
		if _, err := db.ExecContext(ctx, `INSERT INTO t(v) VALUES('more')`); err != nil {
			return false
		}
		objects, err := replicaObjectStats(ctx, cfg, replicaLTXKeys(cfg))
		if err != nil {
			return false
		}
		for key := range objects {
			if strings.HasPrefix(key, snapshotLevel) {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("Litestream never wrote a snapshot under %s", snapshotLevel)
	}
	// The preserved target has to lie past the newest snapshot, so restoring
	// it replays later LTX files on top of that snapshot. Snapshots keep
	// coming every few seconds: write a burst, let it replicate, and stop
	// Litestream while the newest snapshot is still behind the burst.
	const lateRows, lateTXIDs = 12, 6
	snapshotMax := func() uint64 {
		objects, err := replicaObjectStats(ctx, cfg, replicaLTXKeys(cfg))
		if err != nil {
			t.Fatal(err)
		}
		var highest uint64
		for key := range objects {
			if _, maxTXID := chainKeyRange(t, key); strings.HasPrefix(key, snapshotLevel) && maxTXID > highest {
				highest = maxTXID
			}
		}
		return highest
	}
	ahead := false
	for round := 0; round < 12 && !ahead; round++ {
		if round > 0 {
			time.Sleep(300 * time.Millisecond)
		}
		for i := range lateRows {
			if _, err := db.ExecContext(ctx, `INSERT INTO t(v) VALUES(?)`, fmt.Sprintf("late-%d-%d", round, i)); err != nil {
				t.Fatal(err)
			}
		}
		if !waitUntil(30*time.Second, 20*time.Millisecond, func() bool {
			synced, err = v.syncOnceDB(ctx, time.Second, cfg.DBPath)
			return err == nil && synced.TXID > 0 && synced.ReplicatedTXID >= synced.TXID
		}) {
			t.Fatalf("replica never caught up: %+v, %v", synced, err)
		}
		ahead = snapshotMax()+lateTXIDs <= uint64(synced.ReplicatedTXID)
	}
	stop()
	newestSnapshot := snapshotMax()
	if !ahead || newestSnapshot == 0 || newestSnapshot+3 > uint64(synced.ReplicatedTXID) {
		t.Fatalf("newest snapshot ends at %x and the replica at %x; want the replica well past the snapshot", newestSnapshot, synced.ReplicatedTXID)
	}

	everything, err := replicaObjectStats(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ltxObjects, err := replicaObjectStats(ctx, cfg, replicaLTXKeys(cfg))
	if err != nil {
		t.Fatal(err)
	}
	keys := slices.Sorted(func(yield func(string) bool) {
		for key := range everything {
			if !yield(key) {
				return
			}
		}
	})
	t.Logf("replica keys written by %s (%d): %s", filepath.Base(cfg.litestreamCommand()), len(keys), strings.Join(keys[:min(len(keys), 8)], ", "))
	if len(ltxObjects) == 0 {
		t.Fatalf("no key Litestream wrote matches %s: %v", replicaLTXKeyPattern(cfg), keys)
	}
	for _, level := range wantLevels {
		if !slices.ContainsFunc(keys, func(key string) bool { return strings.HasPrefix(key, level) && replicaLTXKeys(cfg)(key) }) {
			t.Fatalf("no LTX object under %s in %v", level, keys)
		}
	}
	for _, key := range keys {
		if strings.HasSuffix(key, ".ltx") && !replicaLTXKeys(cfg)(key) {
			t.Fatalf("Litestream wrote %s, which the lane would not preserve", key)
		}
	}

	record, err := newUpgradeChainPreserver().preserve(ctx, cfg, cfg, time.Now)
	if err != nil || !record.succeeded() {
		t.Fatalf("preserve() = %+v, %v; want the replica copied and restored by the same build", record, err)
	}
	if record.Objects != len(ltxObjects) || record.txid() != uint64(synced.ReplicatedTXID) || uint64(synced.TXID) != record.txid() {
		t.Fatalf("record = %+v, want %d object(s) and the source's final txid %x", record, len(ltxObjects), synced.TXID)
	}
	target := record.txid()
	if target <= newestSnapshot {
		t.Fatalf("target %x is not past the newest snapshot's max txid %x: restoring it would replay nothing", target, newestSnapshot)
	}
	replayed := map[string]int{}
	for key := range ltxObjects {
		minTXID, maxTXID := chainKeyRange(t, key)
		if !strings.HasPrefix(key, snapshotLevel) && minTXID > newestSnapshot && maxTXID <= target {
			replayed[strings.TrimSuffix(key, "/"+filepath.Base(key))]++
		}
	}
	later := 0
	for _, n := range replayed {
		later += n
	}
	if later < 3 {
		t.Fatalf("%d LTX file(s) lie between the snapshot (%x) and the target (%x), want several to replay: %v", later, newestSnapshot, target, replayed)
	}
	t.Logf("snapshot level %s ends at txid %x; target txid %x; %d later LTX file(s) by level %v", snapshotLevel, newestSnapshot, target, later, replayed)

	configPath, cleanupConfig, err := writePreservedChainConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupConfig()
	restoredPath := filepath.Join(cfg.DataDir, "restored-from-chain.db")
	restore := exec.CommandContext(ctx, cfg.litestreamCommand(), restoreArgs(configPath, restoreModeTXID, target, time.Time{}, restoredPath, cfg.DBPath)...)
	if cfg.ReplicaType == "s3" {
		restore.Env = cfg.s3CommandEnv("")
	}
	output, err := restore.CombinedOutput()
	if err != nil {
		t.Fatalf("restore txid %x from the preserved copy: %v\n%s", target, err, output)
	}
	rows := func(path string) []string {
		handle, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = handle.Close() }()
		result, err := handle.QueryContext(ctx, `SELECT id, v FROM t ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = result.Close() }()
		var out []string
		for result.Next() {
			var id int64
			var v string
			if err := result.Scan(&id, &v); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprintf("%d=%s", id, v))
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	want, got := rows(cfg.DBPath), rows(restoredPath)
	lateInRestore := 0
	for _, row := range got {
		if strings.Contains(row, "=late-") {
			lateInRestore++
		}
	}
	if !slices.Equal(got, want) || lateInRestore < lateRows || uint64(len(want)) < target-newestSnapshot {
		t.Fatalf("restored database has %d row(s), %d written after the snapshot; the source at txid %x has %d: the chain past the snapshot was not replayed exactly", len(got), lateInRestore, target, len(want))
	}
	t.Logf("restored %d row(s) at txid %x, identical to the source, %d of them written after the snapshot", len(got), target, lateInRestore)
	copied, err := replicaObjectStats(ctx, preservedChainConfig(cfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := matchReplicaObjects(ltxObjects, copied, cfg.ReplicaType != "file"); err != nil {
		t.Fatalf("preserved copy differs from the LTX objects Litestream wrote: %v", err)
	}
}
