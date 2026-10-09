package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHeavyLoadRetention(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProfileName = "gharchive-mixed"
	cfg.DBPath = filepath.Join(t.TempDir(), "source.db")
	db := logicalTestDB(t, cfg.DBPath, `CREATE TABLE load_test(id INTEGER PRIMARY KEY, data BLOB); CREATE TABLE gh_events(id TEXT PRIMARY KEY); CREATE TABLE unrelated(id INTEGER); INSERT INTO unrelated VALUES(1); INSERT INTO load_test VALUES(1, zeroblob(100)),(2,zeroblob(100)),(3,zeroblob(100)),(4,zeroblob(100));`)
	if _, err := installLoadRetention(context.Background(), cfg, 2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db.Exec("INSERT INTO load_test(data) VALUES(zeroblob(100)); INSERT INTO gh_events VALUES(lower(hex(randomblob(16))))"); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"load_test", "gh_events"} {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("%s rows=%d", table, count)
		}
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM unrelated").Scan(&count); err != nil || count != 1 {
		t.Fatalf("unrelated rows=%d err=%v", count, err)
	}
	if _, err := installLoadRetention(context.Background(), cfg, 2); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRetentionRetriesBusyInstall(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProfileName = "high-volume"
	cfg.DBPath = filepath.Join(t.TempDir(), "source.db")
	db := logicalTestDB(t, cfg.DBPath, "CREATE TABLE load_test(id INTEGER PRIMARY KEY, data BLOB)")
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	releaseDelay := 5200 * time.Millisecond
	go func() {
		timer := time.NewTimer(releaseDelay)
		defer timer.Stop()
		<-timer.C
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		close(release)
	}()
	stats, err := installLoadRetention(context.Background(), cfg, 2)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Retries == 0 || !stats.Completed {
		t.Fatalf("stats=%+v", stats)
	}
	<-release
}

func TestLoadRetentionBusyUntilDeadlineIsInconclusive(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProfileName = "high-volume"
	cfg.DBPath = filepath.Join(t.TempDir(), "source.db")
	db := logicalTestDB(t, cfg.DBPath, "CREATE TABLE load_test(id INTEGER PRIMARY KEY, data BLOB)")
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
	}()
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	v := &Verifier{}
	result := VerificationResult{}
	phaseErr := v.runValidationPhase(context.Background(), "load_retention", 80*time.Millisecond, func(phaseCtx context.Context) error {
		_, err := installLoadRetention(phaseCtx, cfg, 2)
		return err
	})
	v.failValidationResult(context.Background(), &result, phaseErr)
	if !errors.Is(phaseErr, context.DeadlineExceeded) || result.Status != "aborted" || !strings.Contains(result.Summary, "inconclusive") || !strings.Contains(phaseErr.Error(), "load_retention") {
		t.Fatalf("result=%+v phase error=%v", result, phaseErr)
	}
	if len(v.validationSteps) != 1 || v.validationSteps[0].Name != "load_retention" {
		t.Fatalf("steps=%+v", v.validationSteps)
	}
}

func TestLoadRetentionPartialTrimResumesNextCycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProfileName = "high-volume"
	cfg.DBPath = filepath.Join(t.TempDir(), "source.db")
	db := logicalTestDB(t, cfg.DBPath, "CREATE TABLE load_test(id INTEGER PRIMARY KEY, data BLOB); WITH RECURSIVE ids(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM ids WHERE id < 10002) INSERT INTO load_test SELECT id, x'01' FROM ids")
	if _, err := installLoadRetention(context.Background(), cfg, 1); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM load_test").Scan(&count); err != nil || count != 2 {
		t.Fatalf("first cycle rows=%d err=%v", count, err)
	}
	if _, err := installLoadRetention(context.Background(), cfg, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT count(*) FROM load_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows remaining=%d", count)
	}
}

func TestLoadRetentionProfiles(t *testing.T) {
	for _, profile := range []string{"high-volume", "high-vol-ams", "burst-volume", "gharchive-replay", "gharchive-mixed", "pinned-reader", "overload-truncate0"} {
		cfg := DefaultConfig()
		cfg.ProfileName = profile
		if !cfg.boundedLoadProfile() {
			t.Fatalf("unbounded heavy profile %s", profile)
		}
	}
	for _, profile := range []string{"constrained-disk", "low-volume", "many-dbs-100-list", "fts-maintenance"} {
		cfg := DefaultConfig()
		cfg.ProfileName = profile
		if cfg.boundedLoadProfile() {
			t.Fatalf("changed unrelated profile %s", profile)
		}
	}
}

func TestLoadRetentionFollowsCheckpointThaw(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProfileName = "high-volume"
	cfg.DataDir = t.TempDir()
	cfg.DBPath = filepath.Join(cfg.DataDir, "source.db")
	db := logicalTestDB(t, cfg.DBPath, "PRAGMA journal_mode=WAL; CREATE TABLE load_test(id INTEGER PRIMARY KEY, data BLOB)")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("INSERT INTO load_test(data) VALUES(zeroblob(100))"); err != nil {
		t.Fatal(err)
	}
	pauser := &releasingPauser{onResume: func() {
		if err := tx.Commit(); err != nil {
			t.Error(err)
		}
	}}
	startBoundarySyncFixture(t, &cfg, 42)
	if err := os.WriteFile(filepath.Join(cfg.DataDir, "litestream"), []byte("#!/bin/sh\necho replica unavailable; exit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", cfg.DataDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	v := NewVerifier(cfg, pauser)
	v.checkpointAttempts = 2
	v.checkpointRetryDelay = time.Millisecond
	v.checkpointBusyTimeout = 10 * time.Millisecond
	result, err := v.RunCycle(context.Background())
	if err == nil || !strings.Contains(err.Error(), "validation failed (exit 7)") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	retentionStep := findStep(t, result.Steps, "load_retention")
	if !strings.Contains(retentionStep.OutputTail, "retries=") || !strings.Contains(retentionStep.OutputTail, "rows_trimmed=") || !strings.Contains(retentionStep.OutputTail, "completed=") {
		t.Fatalf("load retention step=%+v", retentionStep)
	}
	var installed int
	if err := db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name='soak_retain_load_test'").Scan(&installed); err != nil || installed != 1 {
		t.Fatalf("retention=%d err=%v", installed, err)
	}
}
