package worker

import (
	"context"
	"path/filepath"
	"testing"
)

func TestFTSDeterministicMaintenance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var snapshots []logicalSnapshot
	for range 2 {
		path := filepath.Join(t.TempDir(), "fts.db")
		db, err := openFTS(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		totals := map[string]int64{}
		for i := 0; i < 24; i++ {
			work, err := stepFTS(ctx, db)
			if err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
			totals[work.Phase] += work.Changes
			if _, err := checkFTSSearch(ctx, db); err != nil {
				t.Fatalf("step %d search: %v", i, err)
			}
		}
		for _, phase := range []string{"insert", "update", "delete", "query", "merge", "optimize"} {
			if totals[phase] <= 0 {
				t.Errorf("no actual %s work: %v", phase, totals)
			}
		}
		var count, step int
		if err := db.QueryRow("SELECT count(*) FROM fts_documents").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 48 {
			t.Fatalf("documents=%d want=48", count)
		}
		if err := db.QueryRow("SELECT step FROM fts_progress WHERE id=1").Scan(&step); err != nil {
			t.Fatal(err)
		}
		if step != 24 {
			t.Fatalf("step=%d", step)
		}
		snapshot, err := readLogicalSnapshot(ctx, path, DefaultConfig().logicalLimits())
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := compareLogicalSnapshots(snapshots[0], snapshots[1]); err != nil {
		t.Fatalf("not deterministic: %v", err)
	}
}

func TestFTSSearchRejectsStaleIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fts.db")
	db, err := openFTS(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := stepFTS(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE fts_search SET body='missing' WHERE rowid=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := checkFTSSearch(ctx, db); err == nil {
		t.Fatal("stale index passed search validation")
	}
	if _, err := readLogicalSnapshot(ctx, path, DefaultConfig().logicalLimits()); err == nil {
		t.Fatal("stale index passed oracle")
	}
}

func TestFTSStepRollbackAndResume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fts.db")
	db, err := openFTS(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := stepFTS(ctx, db); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := stepFTS(canceled, db); err == nil {
		t.Fatal("cancellation passed")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openFTS(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	work, err := stepFTS(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if work.Phase != "update" || work.Changes != 32 {
		t.Fatalf("resume work=%+v", work)
	}
}

func TestFTSConfigOptIn(t *testing.T) {
	cfg, err := configFromLookup(func(key string) string {
		if key == "PROFILE" {
			return "fts-maintenance"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LoadMode != "fts" || cfg.WriteRate != 1 {
		t.Fatalf("config=%+v", cfg.WorkloadConfig())
	}
	if DefaultConfig().LoadMode == "fts" {
		t.Fatal("FTS enabled by default")
	}
}

func TestFTSConfigurationRejectsInvalidRates(t *testing.T) {
	for _, values := range []map[string]string{
		{"LOAD_MODE": "fts", "WRITE_RATE": "0"},
		{"LOAD_MODE": "fts", "WRITE_RATE": "1001"},
		{"LOAD_MODE": "fts", "NUM_DATABASES": "2"},
	} {
		if _, err := configFromLookup(func(key string) string { return values[key] }); err == nil {
			t.Fatalf("accepted %v", values)
		}
	}
}

func TestFTSSearchRejectsIndexOnlyCorruption(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fts.db")
	db, err := openFTS(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := stepFTS(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DELETE FROM fts_search WHERE rowid=1; INSERT INTO fts_search_content(id,c0,c1) SELECT id,title,body FROM fts_documents WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := checkFTSSearch(ctx, db); err == nil {
		t.Fatal("missing inverted index posting passed")
	}
}

func TestFTSFailedMutationRollsBack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fts.db")
	db, err := openFTS(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TRIGGER reject_document BEFORE INSERT ON fts_documents WHEN new.id=2 BEGIN SELECT RAISE(ABORT,'injected write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := stepFTS(ctx, db); err == nil {
		t.Fatal("write failure passed")
	}
	var documents, step int
	if err := db.QueryRow("SELECT count(*) FROM fts_documents").Scan(&documents); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT step FROM fts_progress").Scan(&step); err != nil {
		t.Fatal(err)
	}
	if documents != 0 || step != 0 {
		t.Fatalf("partial commit documents=%d step=%d", documents, step)
	}
	if _, err := checkFTSSearch(ctx, db); err != nil {
		t.Fatal(err)
	}
}

func TestFTSMaintenanceNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := openFTS(ctx, filepath.Join(t.TempDir(), "fts.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, step := range []int{4, 7} {
		if _, err := db.Exec("UPDATE fts_progress SET step=?", step); err != nil {
			t.Fatal(err)
		}
		work, err := stepFTS(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if work.Changes != 0 {
			t.Fatalf("empty maintenance reported work: %+v", work)
		}
	}
}

func TestFTSPhaseSearchResults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := openFTS(ctx, filepath.Join(t.TempDir(), "fts.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	expected := [][3]int{{64, 0, 0}, {32, 32, 0}, {32, 16, 0}, {32, 16, 0}, {32, 16, 0}, {0, 16, 32}, {0, 16, 32}, {0, 16, 32}}
	for step, counts := range expected {
		if _, err := stepFTS(ctx, db); err != nil {
			t.Fatal(err)
		}
		for i, term := range []string{"alpha", "beta", "gamma"} {
			var count int
			if err := db.QueryRow("SELECT count(*) FROM fts_search WHERE fts_search MATCH ?", term).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != counts[i] {
				t.Fatalf("step=%d term=%s count=%d want=%d", step, term, count, counts[i])
			}
		}
	}
}
