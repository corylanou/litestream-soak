package worker

import (
	"context"
	"debug/buildinfo"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

func TestVerificationBoundaryPinnedBinary(t *testing.T) {
	binary := os.Getenv("SOAK_LOGICAL_LITESTREAM_BINARY")
	if binary == "" {
		t.Skip("opt-in: set SOAK_LOGICAL_LITESTREAM_BINARY to the pinned Litestream executable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	candidate := os.Getenv("SOAK_COMPATIBILITY_SHA")
	if candidate == "" {
		candidate = "4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3"
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(candidate) {
		t.Fatal("unsupported: candidate must be immutable")
	}
	build, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{}
	for _, setting := range build.Settings {
		settings[setting.Key] = setting.Value
	}
	if build.GoVersion != "go1.25.13" || settings["vcs.revision"] != candidate || settings["vcs.modified"] != "false" {
		t.Fatalf("binary identity mismatch: %s", build.String())
	}
	t.Logf("immutable build: %s", build.String())
	dir := t.TempDir()
	if evidence := os.Getenv("SOAK_COMPATIBILITY_EVIDENCE"); evidence != "" {
		dir, err = os.MkdirTemp(evidence, "replication-")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("retained evidence: %s", dir)
	}

	toolDir := filepath.Join(dir, "tools")
	if err := os.Mkdir(toolDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{"litestream": binary} {
		absolute, err := filepath.Abs(target)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(absolute, filepath.Join(toolDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", toolDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("WORKLOAD_SHA", "ae88b164dd6304bcbb654a681df767ee59042eed")
	cfg := DefaultConfig()
	cfg.LitestreamSHA = candidate
	cfg.WorkloadSHA = "ae88b164dd6304bcbb654a681df767ee59042eed"
	cfg.ReplicaType = "file"
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

	if passed, err := v.validateDB(ctx, cfg.DBPath, filepath.Join(dir, "pipeline.db"), synced.TXID); err != nil || !passed {
		t.Fatalf("reserved boundary pipeline passed=%v: %v", passed, err)
	}
	t.Log(v.logicalEvidence)
	var source logicalSnapshot
	for attempt := 0; attempt < 4; attempt++ {
		synced, err = v.syncOnceDB(ctx, time.Second, cfg.DBPath)
		if err != nil {
			t.Fatal(err)
		}
		source, err = v.captureVerificationBoundary(ctx, cfg.DBPath, synced.TXID)
		if err == nil {
			break
		}
		t.Logf("retained failed acquisition %d: %v", attempt+1, err)
	}
	if err != nil {
		t.Fatalf("reacquisition exhausted: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES(4,'newer replica commit')"); err != nil {
		t.Fatal(err)
	}
	if err := v.waitForSync(ctx, nil); err != nil {
		t.Fatal(err)
	}
	pinnedPath := filepath.Join(dir, "older-pinned.db")
	output, err := exec.CommandContext(ctx, binary, "restore", "-config", cfg.ConfigPath, "-txid", formatTXID(synced.TXID), "-o", pinnedPath, cfg.DBPath).CombinedOutput()
	if err != nil {
		t.Fatalf("older pinned restore: %s: %v", output, err)
	}
	if err := v.compareRestoredLogical(ctx, source, pinnedPath); err != nil {
		t.Fatalf("newer replica changed pinned boundary: %v", err)
	}
	latestPath := filepath.Join(dir, "latest-negative.db")
	output, err = exec.CommandContext(ctx, binary, "restore", "-config", cfg.ConfigPath, "-o", latestPath, cfg.DBPath).CombinedOutput()
	if err != nil {
		t.Fatalf("latest negative restore: %s: %v", output, err)
	}
	if err := v.compareRestoredLogical(ctx, source, latestPath); err == nil {
		t.Fatal("newer unpinned restore unexpectedly matched source boundary")
	} else {
		t.Logf("retained unpinned mismatch: %v", err)
	}
	t.Logf("pinned boundary=%016x survives newer committed replica; latest remains a mismatch", synced.TXID)

}
