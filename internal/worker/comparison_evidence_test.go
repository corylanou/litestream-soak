package worker

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCompareLogicalDatabasesIncludesApplicationMetadata(t *testing.T) {
	for _, change := range []string{"PRAGMA application_id=99", "PRAGMA user_version=99", "CREATE INDEX extra ON t(value)"} {
		t.Run(change, func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(dir, "source.db")
			target := filepath.Join(dir, "target.db")
			a := logicalTestDB(t, source, "CREATE TABLE t(id INTEGER PRIMARY KEY,value); INSERT INTO t VALUES(1,'a')")
			b := logicalTestDB(t, target, "CREATE TABLE t(id INTEGER PRIMARY KEY,value); INSERT INTO t VALUES(1,'a')")
			if err := CompareLogicalDatabases(context.Background(), source, target); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Exec(change); err != nil {
				t.Fatal(err)
			}
			if err := CompareLogicalDatabases(context.Background(), source, target); err == nil {
				t.Fatal("accepted schema/application metadata mismatch")
			}
			_ = a
		})
	}
}

func TestReadComparisonProcessResources(t *testing.T) {
	_, err := ReadComparisonProcessResources(os.Getpid())
	if runtime.GOOS == "linux" && err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "linux" && err == nil {
		t.Fatal("unsupported host returned fabricated resources")
	}
}
