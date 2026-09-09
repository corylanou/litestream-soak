package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSchemaFixtureTransitions(t *testing.T) {
	for _, mode := range []string{"vacuum", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := logicalTestDB(t, filepath.Join(t.TempDir(), "fixture.db"), "")
			steps, err := schemaFixtureSteps(128, 1024, mode)
			if err != nil {
				t.Fatal(err)
			}
			var deletedFree, reusedFree int
			for _, step := range steps {
				if err := step.run(ctx, db); err != nil {
					t.Fatalf("%s: %v", step.name, err)
				}
				if err := step.validate(ctx, db); err != nil {
					t.Fatalf("%s invariant: %v", step.name, err)
				}
				var free int
				if err := db.QueryRow("PRAGMA freelist_count").Scan(&free); err != nil {
					t.Fatal(err)
				}
				switch step.name {
				case "delete":
					deletedFree = free
				case "reuse":
					reusedFree = free
				case "reclaim":
					if free != 0 {
						t.Fatalf("reclamation left %d free pages", free)
					}
				}
			}
			if deletedFree == 0 || reusedFree >= deletedFree {
				t.Fatalf("no delete/reuse exposure: %d -> %d", deletedFree, reusedFree)
			}
		})
	}
}

func TestSchemaFixtureRejectsInvalidOptions(t *testing.T) {
	for _, mode := range []string{"", "full", "VACUUM; DROP TABLE t"} {
		if _, err := schemaFixtureSteps(128, 1024, mode); err == nil {
			t.Fatalf("accepted %q", mode)
		}
	}
	for _, rows := range []int{0, -1, 3} {
		if _, err := schemaFixtureSteps(rows, 1024, "vacuum"); err == nil {
			t.Fatalf("accepted %d rows", rows)
		}
	}
}

func TestSchemaFixtureRefusesExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	if _, err := RunSchemaFixture(context.Background(), SchemaFixtureOptions{Directory: dir, Binary: "unused", SHA: strings.Repeat("a", 40), Rows: 128, PayloadBytes: 1024, Vacuum: "vacuum"}); err == nil {
		t.Fatal("accepted existing directory")
	}
}

func TestSchemaFixtureRollbackPreservesSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fixture.db")
	db := logicalTestDB(t, path, "")
	steps, err := schemaFixtureSteps(128, 1024, "vacuum")
	if err != nil {
		t.Fatal(err)
	}
	var before logicalSnapshot
	for _, step := range steps {
		if step.name == "rollback" {
			before, err = readLogicalSnapshot(ctx, path, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := step.run(ctx, db); err != nil {
			t.Fatal(err)
		}
		if step.name == "rollback" {
			after, err := readLogicalSnapshot(ctx, path, DefaultConfig().logicalLimits())
			if err != nil {
				t.Fatal(err)
			}
			if err := compareLogicalSnapshots(before, after); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSchemaFixturePinnedRestore(t *testing.T) {
	binary := os.Getenv("SOAK_SCHEMA_LITESTREAM_BINARY")
	if binary == "" {
		t.Skip("opt-in: set SOAK_SCHEMA_LITESTREAM_BINARY for pinned real restores")
	}
	for _, mode := range []string{"vacuum", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			dir := filepath.Join(t.TempDir(), "run")
			result, err := RunSchemaFixture(ctx, SchemaFixtureOptions{Directory: dir, Binary: binary, SHA: "4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3", Rows: 128, PayloadBytes: 1024, Vacuum: mode})
			if err != nil {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			if result.Verdict != "scenario_success" || len(result.Boundaries) != 10 {
				t.Fatalf("result=%+v", result)
			}
			for _, b := range result.Boundaries {
				if !b.LogicalMatch || b.TXID == 0 {
					t.Fatalf("invalid boundary %+v", b)
				}
			}
		})
	}
}

func TestSchemaFixturePreservesSetupFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	result, err := RunSchemaFixture(context.Background(), SchemaFixtureOptions{Directory: dir, Binary: "/does-not-exist", SHA: strings.Repeat("a", 40), Rows: 128, PayloadBytes: 1024, Vacuum: "vacuum"})
	if err == nil || result.Verdict == "scenario_success" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	data, readErr := os.ReadFile(filepath.Join(dir, "result.json"))
	if readErr != nil || !strings.Contains(string(data), "pinned binary version mismatch") {
		t.Fatalf("evidence=%s err=%v", data, readErr)
	}
}

func TestSchemaFixtureDetectsInvalidApplicationState(t *testing.T) {
	ctx := context.Background()
	db := logicalTestDB(t, filepath.Join(t.TempDir(), "db"), "")
	steps, err := schemaFixtureSteps(128, 1024, "vacuum")
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps[:2] {
		if err := step.run(ctx, db); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("UPDATE items SET payload=x'00' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err := steps[1].validate(ctx, db); err == nil {
		t.Fatal("corrupt application state passed")
	}
}

func TestSchemaFixturePinnedLowHeadroomNotEngaged(t *testing.T) {
	binary := os.Getenv("SOAK_SCHEMA_LITESTREAM_BINARY")
	if binary == "" {
		t.Skip("opt-in: requires pinned executable")
	}
	dir := filepath.Join(t.TempDir(), "run")
	result, err := RunSchemaFixture(context.Background(), SchemaFixtureOptions{Directory: dir, Binary: binary, SHA: "4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3", Rows: 128, PayloadBytes: 1024, Vacuum: "vacuum", MaxHeadroomBytes: 1})
	if err == nil || !strings.Contains(err.Error(), "not engaged") || result.Verdict != "inconclusive" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestSchemaFixturePinnedRetainsRestoreFailure(t *testing.T) {
	binary := os.Getenv("SOAK_SCHEMA_LITESTREAM_BINARY")
	if binary == "" {
		t.Skip("opt-in: requires pinned executable")
	}
	wrapper := filepath.Join(t.TempDir(), "litestream")
	script := "#!/bin/sh\nif [ \"$1\" = restore ]; then echo injected-restore-failure >&2; exit 9; fi\nexec \"$SOAK_SCHEMA_LITESTREAM_BINARY\" \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "run")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	result, err := RunSchemaFixture(ctx, SchemaFixtureOptions{Directory: dir, Binary: wrapper, SHA: "4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3", Rows: 128, PayloadBytes: 1024, Vacuum: "vacuum"})
	if err == nil || result.Verdict != "unrelated_failure" || len(result.Boundaries) != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "boundaries.jsonl"))
	if err != nil || !strings.Contains(string(data), "injected-restore-failure") {
		t.Fatalf("evidence=%s err=%v", data, err)
	}
	log, err := os.ReadFile(filepath.Join(dir, "initialize-restore.log"))
	if err != nil || !strings.Contains(string(log), "injected-restore-failure") {
		t.Fatalf("log=%s err=%v", log, err)
	}
}

func TestSchemaFixturePinnedS3Restore(t *testing.T) {
	binary, endpoint := os.Getenv("SOAK_SCHEMA_LITESTREAM_BINARY"), os.Getenv("SOAK_SCHEMA_S3_ENDPOINT")
	if binary == "" || endpoint == "" {
		t.Skip("opt-in: requires pinned binary and isolated S3 fixture endpoint")
	}
	for _, mode := range []string{"vacuum", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			result, err := RunSchemaFixture(ctx, SchemaFixtureOptions{Directory: filepath.Join(t.TempDir(), "run"), Binary: binary, SHA: "4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3", Rows: 128, PayloadBytes: 1024, Vacuum: mode, S3Endpoint: endpoint, S3Bucket: "schema220", S3Environment: "emulator"})
			if err != nil || result.Verdict != "scenario_success" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			for _, b := range result.Boundaries {
				if b.RemoteBytes == nil || *b.RemoteBytes <= 0 {
					t.Fatalf("missing remote measurement: %+v", b)
				}
			}
		})
	}
}

func TestSchemaFixtureRejectsMissingBackfill(t *testing.T) {
	ctx := context.Background()
	db := logicalTestDB(t, filepath.Join(t.TempDir(), "db"), "")
	steps, err := schemaFixtureSteps(128, 1024, "vacuum")
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps[:6] {
		if err := step.run(ctx, db); err != nil {
			t.Fatal(err)
		}
	}
	if err := steps[6].validate(ctx, db); err == nil {
		t.Fatal("missing backfill passed")
	}
}

func TestSchemaFixturePinnedRecoveryRetainsProcessIncident(t *testing.T) {
	binary := os.Getenv("SOAK_SCHEMA_LITESTREAM_BINARY")
	if binary == "" {
		t.Skip("opt-in: requires pinned executable")
	}
	wrapper := filepath.Join(t.TempDir(), "litestream")
	script := "#!/bin/sh\nif [ \"$1\" = replicate ]; then echo 'time=2026-09-09T12:00:00Z level=ERROR msg=\"replication failed before recovery\"'; fi\nexec \"$SOAK_SCHEMA_LITESTREAM_BINARY\" \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "run")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	result, err := RunSchemaFixture(ctx, SchemaFixtureOptions{Directory: dir, Binary: wrapper, SHA: schemaProcessPatternSHA, Rows: 128, PayloadBytes: 1024, Vacuum: "vacuum"})
	if err != nil || result.Verdict != "recovered_with_incidents" || result.ProcessEvidence.IncidentCount != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(result.Boundaries) != 10 {
		t.Fatalf("missing actual boundaries: %+v", result.Boundaries)
	}
	for _, b := range result.Boundaries {
		if !b.LogicalMatch {
			t.Fatalf("invalid restore: %+v", b)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil || !strings.Contains(string(data), "recovered_with_incidents") {
		t.Fatalf("result evidence=%s err=%v", data, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "replicate.log"))
	if err != nil || !strings.Contains(string(raw), "replication failed before recovery") {
		t.Fatalf("raw log=%s err=%v", raw, err)
	}
}
