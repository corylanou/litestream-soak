package worker

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCompareLogicalSchemasIgnoresRowDifferences(t *testing.T) {
	dir := t.TempDir()
	source, restored := filepath.Join(dir, "source.db"), filepath.Join(dir, "restored.db")
	logicalTestDB(t, source, "CREATE TABLE t(v); INSERT INTO t VALUES(1),(2)")
	logicalTestDB(t, restored, "CREATE TABLE t(v); INSERT INTO t VALUES(1)")
	if err := CompareLogicalSchemas(context.Background(), source, restored); err != nil {
		t.Fatal(err)
	}
	if err := CompareLogicalDatabases(context.Background(), source, restored); err == nil {
		t.Fatal("whole-database comparison accepted a historical prefix")
	}
}

func TestCompareLogicalSchemasRejectsMetadataAndReadFailures(t *testing.T) {
	dir := t.TempDir()
	source, restored := filepath.Join(dir, "source.db"), filepath.Join(dir, "restored.db")
	logicalTestDB(t, source, "CREATE TABLE t(v); PRAGMA application_id=42")
	logicalTestDB(t, restored, "CREATE TABLE t(v); PRAGMA application_id=43")
	if err := CompareLogicalSchemas(context.Background(), source, restored); err == nil {
		t.Fatal("accepted metadata mismatch")
	}
	for _, paths := range [][2]string{{filepath.Join(dir, "missing"), restored}, {source, filepath.Join(dir, "missing")}} {
		if err := CompareLogicalSchemas(context.Background(), paths[0], paths[1]); err == nil {
			t.Fatal("accepted unreadable database")
		}
	}
}
