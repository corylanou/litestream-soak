package worker

import (
	"context"
	"path/filepath"
	"testing"
)

func TestLogicalFTS5Snapshot(t *testing.T) {
	t.Parallel()
	const schema = `CREATE VIRTUAL TABLE search USING fts5(title, body); INSERT INTO search(rowid,title,body) VALUES(7,'first','alpha quick brown fox'),(19,'second','beta');`
	for _, tt := range []struct {
		name, change string
		equal        bool
	}{
		{"equal", "", true},
		{"document", `UPDATE search SET body='gamma' WHERE rowid=7`, false},
		{"rowid", `UPDATE search SET rowid=8 WHERE rowid=7`, false},
		{"shadow", `UPDATE search_docsize SET sz=x'0102' WHERE id=7`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			a, b := filepath.Join(dir, "source.db"), filepath.Join(dir, "restore.db")
			logicalTestDB(t, a, schema)
			logicalTestDB(t, b, schema+tt.change)
			source, err := readLogicalSnapshot(context.Background(), a, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			if len(source.tables) != 6 {
				t.Fatalf("tables=%d; want virtual table and five shadow tables", len(source.tables))
			}
			restored, err := readLogicalSnapshot(context.Background(), b, DefaultConfig().logicalLimits())
			if err != nil {
				if !tt.equal {
					return
				}
				t.Fatal(err)
			}
			if err := compareLogicalSnapshots(source, restored); (err == nil) != tt.equal {
				t.Fatalf("compare=%v equal=%v", err, tt.equal)
			}
		})
	}
}

func TestLogicalFTS5Variants(t *testing.T) {
	t.Parallel()
	for _, schema := range []string{
		`CREATE VIRTUAL TABLE "odd""name" USING fts5(body); INSERT INTO "odd""name" VALUES('alpha');`,
		"CREATE VIRTUAL TABLE `odd name` USING fts5(body); INSERT INTO `odd name` VALUES('alpha');",
		`CREATE VIRTUAL TABLE search USING fts5(body,content=''); INSERT INTO search(rowid,body) VALUES(42,'alpha');`,
		`CREATE TABLE docs(id INTEGER PRIMARY KEY,body); INSERT INTO docs VALUES(42,'alpha'); CREATE VIRTUAL TABLE search USING fts5(body,content='docs',content_rowid='id'); INSERT INTO search(search) VALUES('rebuild');`,
	} {
		path := filepath.Join(t.TempDir(), "fts.db")
		logicalTestDB(t, path, schema)
		if _, err := readLogicalSnapshot(context.Background(), path, DefaultConfig().logicalLimits()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLogicalFTSNameDoesNotRequireWorkload(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "fts.db")
	logicalTestDB(t, path, `CREATE VIRTUAL TABLE fts_search USING fts5(body); INSERT INTO fts_search VALUES('alpha');`)
	if _, err := readLogicalSnapshot(context.Background(), path, DefaultConfig().logicalLimits()); err != nil {
		t.Fatal(err)
	}
}
