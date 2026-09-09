package worker

import (
	"context"
	"database/sql"
	"fmt"
)

type schemaFixtureStep struct {
	name     string
	run      func(context.Context, *sql.DB) error
	validate func(context.Context, *sql.DB) error
}

func schemaFixtureSteps(count, payload int, mode string) ([]schemaFixtureStep, error) {
	if count < 4 || count%4 != 0 || payload < 512 || payload > 1<<20 || count > 1000000 {
		return nil, fmt.Errorf("rows must be a multiple of 4 in [4,1000000]; payload bytes must be in [512,1048576]")
	}
	if mode != "vacuum" && mode != "incremental" {
		return nil, fmt.Errorf("vacuum mode must be vacuum or incremental")
	}
	execSQL := func(statement string) func(context.Context, *sql.DB) error {
		return func(ctx context.Context, db *sql.DB) error { _, err := db.ExecContext(ctx, statement); return err }
	}
	invariant := func(rows int, migrated bool) func(context.Context, *sql.DB) error {
		return func(ctx context.Context, db *sql.DB) error {
			var actual, invalid int
			query := "SELECT count(*), coalesce(sum(typeof(id) != 'integer' OR typeof(payload) != 'blob' OR length(payload) != ?),0) FROM items"
			if migrated {
				query = "SELECT count(*), coalesce(sum(typeof(id) != 'integer' OR typeof(payload) != 'blob' OR length(payload) != ? OR typeof(generation) != 'integer' OR generation != id*2),0) FROM items"
			}
			if err := db.QueryRowContext(ctx, query, payload).Scan(&actual, &invalid); err != nil {
				return err
			}
			if actual != rows || invalid != 0 {
				return fmt.Errorf("application invariant: rows=%d want=%d invalid=%d", actual, rows, invalid)
			}
			return nil
		}
	}
	insert := func(first, last int) func(context.Context, *sql.DB) error {
		return func(ctx context.Context, db *sql.DB) error {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback() }()
			stmt, err := tx.PrepareContext(ctx, "INSERT INTO items(id,payload) VALUES(?,?)")
			if err != nil {
				return err
			}
			defer func() { _ = stmt.Close() }()
			value := make([]byte, payload)
			for id := first; id <= last; id++ {
				for i := range value {
					value[i] = byte((id + i) % 251)
				}
				if _, err := stmt.ExecContext(ctx, id, value); err != nil {
					return err
				}
			}
			return tx.Commit()
		}
	}
	autoVacuum := "NONE"
	if mode == "incremental" {
		autoVacuum = "INCREMENTAL"
	}
	reclaim := execSQL("VACUUM")
	if mode == "incremental" {
		reclaim = func(ctx context.Context, db *sql.DB) error {
			for {
				var free int
				if err := db.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&free); err != nil {
					return err
				}
				if free == 0 {
					return nil
				}
				if _, err := db.ExecContext(ctx, "PRAGMA incremental_vacuum(128)"); err != nil {
					return err
				}
			}
		}
	}
	return []schemaFixtureStep{
		{"initialize", execSQL("PRAGMA auto_vacuum=" + autoVacuum + "; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE items(id INTEGER PRIMARY KEY,payload BLOB NOT NULL)"), invariant(0, false)},
		{"growth", insert(1, count), invariant(count, false)},
		{"delete", execSQL(fmt.Sprintf("DELETE FROM items WHERE id <= %d", count*3/4)), invariant(count/4, false)},
		{"reuse", insert(count+1, count+count/2), invariant(count*3/4, false)},
		{"index", execSQL("CREATE INDEX items_payload ON items(payload)"), invariant(count*3/4, false)},
		{"add-column", execSQL("ALTER TABLE items ADD COLUMN generation INTEGER"), invariant(count*3/4, false)},
		{"backfill", execSQL("UPDATE items SET generation=id*2"), invariant(count*3/4, true)},
		{"migration", execSQL("BEGIN; CREATE TABLE items_new(id INTEGER PRIMARY KEY,payload BLOB NOT NULL,generation INTEGER NOT NULL CHECK(generation=id*2)); INSERT INTO items_new SELECT * FROM items; DROP TABLE items; ALTER TABLE items_new RENAME TO items; CREATE INDEX items_generation ON items(generation); PRAGMA user_version=2; COMMIT"), invariant(count*3/4, true)},
		{"rollback", execSQL("BEGIN; ALTER TABLE items ADD COLUMN discarded TEXT; DELETE FROM items; PRAGMA user_version=99; ROLLBACK"), invariant(count*3/4, true)},
		{"reclaim", reclaim, invariant(count*3/4, true)},
	}, nil
}
