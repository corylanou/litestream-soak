package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
	cfg.SnapshotInterval = 2 * time.Second
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
	if !waitUntil(30*time.Second, 50*time.Millisecond, func() bool {
		synced, err = v.syncOnceDB(ctx, time.Second, cfg.DBPath)
		return err == nil && synced.TXID > 0 && synced.ReplicatedTXID >= synced.TXID
	}) {
		t.Fatalf("replica never caught up: %+v, %v", synced, err)
	}
	stop()

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
	if record.Objects != len(ltxObjects) || record.txid() < uint64(synced.ReplicatedTXID) {
		t.Fatalf("record = %+v, want %d object(s) and a txid at or past the replicated %d", record, len(ltxObjects), synced.ReplicatedTXID)
	}
	copied, err := replicaObjectStats(ctx, preservedChainConfig(cfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := matchReplicaObjects(ltxObjects, copied, cfg.ReplicaType != "file"); err != nil {
		t.Fatalf("preserved copy differs from the LTX objects Litestream wrote: %v", err)
	}
}
