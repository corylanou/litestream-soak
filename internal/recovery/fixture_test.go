package recovery

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFixtureOracleDetectsCorruption(t *testing.T) {
	ctx := context.Background()
	source, err := openDB(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	if _, err := source.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := appendRow(ctx, source, 1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "restored.db")
	restored, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	if _, err := restored.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := appendRow(ctx, restored, 1); err != nil {
		t.Fatal(err)
	}
	if n, err := validate(ctx, source, path, 1); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	if _, err := restored.Exec("UPDATE t SET value='corrupted'"); err != nil {
		t.Fatal(err)
	}
	if _, err := validate(ctx, source, path, 1); err == nil {
		t.Fatal("accepted changed value")
	}
	if _, err := validate(ctx, source, filepath.Join(t.TempDir(), "missing"), 1); err == nil {
		t.Fatal("accepted missing restore")
	}
}

func TestMaintenancePreservesSelfHealingIncidents(t *testing.T) {
	c, d, incidents := maintenance("msg=\"compaction complete\"\nmsg=\"l0 retention enforced\" deleted_count=0\nmsg=\"l0 retention enforced\" deleted_count=3\nlevel=ERROR failed\nlevel=WARN recovered\nretry succeeded\nself-heal complete\n")
	if c != 1 || d != 3 || len(incidents) != 4 {
		t.Fatalf("%d %d %v", c, d, incidents)
	}
}

func TestRunRefusesExistingOutput(t *testing.T) {
	hash, err := hashFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	output := t.TempDir()
	marker := filepath.Join(output, "preserve")
	if err := os.WriteFile(marker, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Config{Binary: os.Args[0], SHA256: hash, Output: output}); err == nil {
		t.Fatal("accepted existing directory")
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "original" {
		t.Fatalf("changed fixture: %q %v", data, err)
	}
}

func TestProcessKillAndWaitCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := start(ctx, "/bin/sh", filepath.Join(t.TempDir(), "process.log"), "-c", "exec sleep 30")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.stop(true); err == nil {
		t.Fatal("missing killed exit")
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if err := wait(cancelled, func() (bool, error) { return false, nil }); err == nil {
		t.Fatal("ignored cancellation")
	}
	if err := wait(ctx, func() (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := start(ctx, "/missing-binary", filepath.Join(t.TempDir(), "failed.log")); err == nil {
		t.Fatal("missing start error")
	}
}

func TestRestoreExposureExcludesValidationAndNoopRetention(t *testing.T) {
	start := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Second)
	log := "time=2026-09-09T00:00:00.100Z msg=\"compaction complete\"\ntime=2026-09-09T00:00:00.200Z msg=\"l0 retention enforced\" deleted_count=2\n"
	if !restoreExposed(log, "opening ltx file for restore level=0", start, end) {
		t.Fatal("missed exposure")
	}
	if restoreExposed(log, "opening ltx file for restore level=9", start, end) {
		t.Fatal("snapshot only")
	}
	if restoreExposed(log, "opening ltx file for restore level=0", end, end.Add(time.Second)) {
		t.Fatal("counted maintenance outside restore")
	}
	if restoreExposed("time=2026-09-09T00:00:00.100Z msg=\"compaction complete\"\ntime=2026-09-09T00:00:00.200Z msg=\"l0 retention enforced\" deleted_count=0", "opening ltx file for restore level=0", start, end) {
		t.Fatal("no-op deletion")
	}
}

func TestFixtureOracleRejectsSchemaAndMetadataChanges(t *testing.T) {
	for _, change := range []string{
		"DROP INDEX value_idx",
		"DROP INDEX value_idx; CREATE INDEX value_idx ON t(id)",
		"ALTER TABLE t ADD COLUMN extra TEXT",
		"PRAGMA user_version=8",
		"PRAGMA application_id=43",
	} {
		t.Run(change, func(t *testing.T) {
			dir := t.TempDir()
			source, err := openDB(filepath.Join(dir, "source.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = source.Close() }()
			path := filepath.Join(dir, "restored.db")
			restored, err := openDB(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = restored.Close() }()
			for _, db := range []*sql.DB{source, restored} {
				if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT); CREATE INDEX value_idx ON t(value); PRAGMA user_version=7; PRAGMA application_id=42"); err != nil {
					t.Fatal(err)
				}
				if err := appendRow(context.Background(), db, 1); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := restored.Exec(change); err != nil {
				t.Fatal(err)
			}
			if _, err := validate(context.Background(), source, path, 1); err == nil {
				t.Fatal("accepted unchanged rows with altered schema/application metadata")
			}
		})
	}
}

func TestFixtureOracleAcceptsHistoricalPrefixWithMatchingSchema(t *testing.T) {
	dir := t.TempDir()
	source, err := openDB(filepath.Join(dir, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	path := filepath.Join(dir, "restored.db")
	restored, err := openDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	for _, db := range []*sql.DB{source, restored} {
		if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT); CREATE INDEX value_idx ON t(value); PRAGMA user_version=7; PRAGMA application_id=42"); err != nil {
			t.Fatal(err)
		}
		if err := appendRow(context.Background(), db, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := appendRow(context.Background(), source, 2); err != nil {
		t.Fatal(err)
	}
	if count, err := validate(context.Background(), source, path, 1); err != nil || count != 1 {
		t.Fatalf("historical prefix rejected: count=%d err=%v", count, err)
	}
}
