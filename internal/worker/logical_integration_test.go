package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLogicalOraclePinnedLitestream(t *testing.T) {
	binary := os.Getenv("SOAK_LOGICAL_LITESTREAM_BINARY")
	if binary == "" {
		t.Skip("opt-in: set SOAK_LOGICAL_LITESTREAM_BINARY to the pinned Litestream executable")
	}
	version, err := exec.Command(binary, "version").CombinedOutput()
	if err != nil || strings.TrimSpace(string(version)) != "4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3" {
		t.Fatalf("unexpected pinned binary: %s, %v", version, err)
	}
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.ReplicaPath = filepath.Join(dir, "replica")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("logical-real-%d.sock", time.Now().UnixNano()))
	cfg.LitestreamMetricsAddr = "127.0.0.1:0"
	db := logicalTestDB(t, cfg.DBPath, `PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY,v); INSERT INTO t VALUES(1,'initial');`)
	config := fmt.Sprintf("socket:\n  enabled: true\n  path: %q\ndbs:\n  - path: %q\n    min-checkpoint-page-count: 1\n    checkpoint-interval: 100ms\n    replicas:\n      - path: %q\n        sync-interval: 100ms\n", cfg.SocketPath, cfg.DBPath, cfg.ReplicaPath)
	if err := os.WriteFile(cfg.ConfigPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	logPath := filepath.Join(dir, "replicate.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "replicate", "-config", cfg.ConfigPath)
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		_ = log.Close()
		if t.Failed() {
			t.Log(readFile(t, logPath))
		}
	})
	v := NewVerifier(cfg)
	var synced syncResponse
	if !waitUntil(30*time.Second, 50*time.Millisecond, func() bool {
		synced, err = v.syncOnceDB(ctx, time.Second, cfg.DBPath)
		return err == nil && synced.TXID > 0 && synced.ReplicatedTXID >= synced.TXID
	}) {
		t.Fatalf("initial sync failed: %v", err)
	}
	if _, err := db.Exec(`BEGIN; INSERT INTO t VALUES(2,CAST(x'610062' AS TEXT)); INSERT INTO t VALUES(3,x'00ff'); COMMIT;`); err != nil {
		t.Fatal(err)
	}
	if err := v.waitForSync(ctx, nil); err != nil {
		t.Fatal(err)
	}
	synced, err = v.syncOnceDB(ctx, time.Second, cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := readLogicalSnapshot(ctx, cfg.DBPath, cfg.logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	restoredPath := filepath.Join(dir, "restored.db")
	output, err := exec.CommandContext(ctx, binary, "restore", "-config", cfg.ConfigPath, "-txid", formatTXID(synced.TXID), "-o", restoredPath, cfg.DBPath).CombinedOutput()
	if err != nil {
		t.Fatalf("restore: %s: %v", output, err)
	}
	actual, err := readLogicalSnapshot(ctx, restoredPath, cfg.logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := compareLogicalSnapshots(expected, actual); err != nil {
		t.Fatal(err)
	}
	restored := logicalTestDB(t, restoredPath, "")
	var sourceSeq, restoredSeq int64
	if err := db.QueryRow("SELECT coalesce(max(seq),0) FROM _litestream_seq").Scan(&sourceSeq); err != nil {
		t.Fatal(err)
	}
	if err := restored.QueryRow("SELECT coalesce(max(seq),0) FROM _litestream_seq").Scan(&restoredSeq); err != nil {
		t.Fatal(err)
	}
	t.Logf("pinned restore txid=%016x application rows=3 source_seq=%d restored_seq=%d", synced.TXID, sourceSeq, restoredSeq)
	if _, err := db.Exec("INSERT INTO _litestream_seq(id,seq) VALUES(1,1) ON CONFLICT(id) DO UPDATE SET seq=seq+1"); err != nil {
		t.Fatal(err)
	}
	var advancedSeq int64
	if err := db.QueryRow("SELECT seq FROM _litestream_seq WHERE id=1").Scan(&advancedSeq); err != nil {
		t.Fatal(err)
	}
	if advancedSeq <= sourceSeq {
		t.Fatalf("bookkeeping fixture did not advance: before=%d after=%d", sourceSeq, advancedSeq)
	}
	advanced, err := readLogicalSnapshot(ctx, cfg.DBPath, cfg.logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := compareLogicalSnapshots(advanced, actual); err != nil {
		t.Fatalf("bookkeeping-only change: %v", err)
	}
	if _, err := restored.Exec("DELETE FROM t WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	altered, err := readLogicalSnapshot(ctx, restoredPath, cfg.logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := compareLogicalSnapshots(expected, altered); err == nil {
		t.Fatal("missing committed row in real restore passed")
	}
}
