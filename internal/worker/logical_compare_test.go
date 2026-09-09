package worker

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCompareLogicalDatabases(t *testing.T) {
	for _, change := range []string{"", "CREATE INDEX extra ON t(value)", "PRAGMA user_version=2", "PRAGMA application_id=3", "UPDATE t SET value='changed'"} {
		t.Run(change, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			restored := filepath.Join(root, "restored")
			logicalTestDB(t, source, "CREATE TABLE t(value); INSERT INTO t VALUES('original')")
			db := logicalTestDB(t, restored, "CREATE TABLE t(value); INSERT INTO t VALUES('original')")
			if change != "" {
				if _, err := db.Exec(change); err != nil {
					t.Fatal(err)
				}
			}
			err := CompareLogicalDatabases(context.Background(), source, restored)
			if (err != nil) != (change != "") {
				t.Fatalf("error = %v", err)
			}
		})
	}
	root := t.TempDir()
	if err := CompareLogicalDatabases(context.Background(), filepath.Join(root, "missing"), filepath.Join(root, "other")); err == nil {
		t.Fatal("accepted missing source")
	}
	source := filepath.Join(root, "source")
	logicalTestDB(t, source, "CREATE TABLE t(value)")
	if err := CompareLogicalDatabases(context.Background(), source, filepath.Join(root, "missing")); err == nil {
		t.Fatal("accepted missing restore")
	}
}
