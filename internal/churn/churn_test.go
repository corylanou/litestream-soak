package churn

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "churn.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestQueueLifecycle(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	for _, step := range []struct {
		op    string
		rows  int64
		state string
	}{
		{"enqueue", 1, "ready"}, {"claim", 1, "claimed"}, {"retry", 1, "ready"},
		{"claim", 1, "claimed"}, {"complete", 2, "done"}, {"delete", 2, ""},
		{"enqueue", 1, "ready"}, {"expire", 1, "expired"}, {"delete", 1, ""},
	} {
		t.Run(step.op+step.state, func(t *testing.T) {
			n, err := Apply(ctx, db, "queue", step.op, 0, 10, 32)
			if err != nil || n != step.rows {
				t.Fatalf("rows=%d err=%v", n, err)
			}
			if err := Validate(ctx, db, "queue", 4); err != nil {
				t.Fatal(err)
			}
			var state string
			err = db.QueryRow("SELECT state FROM churn_jobs WHERE id=0").Scan(&state)
			if step.state == "" {
				if err != sql.ErrNoRows {
					t.Fatalf("expected deleted job: %v", err)
				}
			} else if err != nil || state != step.state {
				t.Fatalf("state=%s err=%v", state, err)
			}
		})
	}
}

func TestCacheBoundedAndExpires(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if _, err := Apply(ctx, db, "cache", "upsert", i%4, int64(i), 32); err != nil {
			t.Fatal(err)
		}
		if err := Validate(ctx, db, "cache", 4); err != nil {
			t.Fatal(err)
		}
	}
	n, err := Apply(ctx, db, "cache", "evict", 0, 200, 32)
	if err != nil || n != 4 {
		t.Fatalf("eviction rows=%d err=%v", n, err)
	}
}

func TestCompletionRollsBackOnReceiptFailure(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	for _, op := range []string{"enqueue", "claim"} {
		if _, err := Apply(ctx, db, "queue", op, 1, 0, 8); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("CREATE TRIGGER fail_receipt BEFORE INSERT ON churn_receipts BEGIN SELECT RAISE(ABORT,'receipt failure'); END"); err != nil {
		t.Fatal(err)
	}
	if n, err := Apply(ctx, db, "queue", "complete", 1, 0, 8); err == nil || n != 0 {
		t.Fatalf("rows=%d err=%v", n, err)
	}
	var state string
	if err := db.QueryRow("SELECT state FROM churn_jobs WHERE id=1").Scan(&state); err != nil || state != "claimed" {
		t.Fatalf("state=%s err=%v", state, err)
	}
}

func TestInvariantsRejectCorruption(t *testing.T) {
	for _, stmt := range []string{"INSERT INTO churn_receipts VALUES(1)", "INSERT INTO churn_jobs VALUES(1,'done',0,'x')", "INSERT INTO churn_cache VALUES(9,'x',2)"} {
		t.Run(stmt, func(t *testing.T) {
			db := testDB(t)
			if _, err := db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
			mode := "queue"
			if stmt == "INSERT INTO churn_cache VALUES(9,'x',2)" {
				mode = "cache"
			}
			if err := Validate(context.Background(), db, mode, 4); err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
}

func TestBusyAttemptHasNoMutations(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("INSERT INTO churn_jobs VALUES(1,'ready',0,'x')"); err != nil {
		t.Fatal(err)
	}
	n, err := Apply(ctx, db, "queue", "enqueue", 2, 0, 8)
	if err == nil || n != 0 {
		t.Fatalf("locked attempt rows=%d err=%v", n, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	n, err = Apply(ctx, db, "queue", "enqueue", 2, 0, 8)
	if err != nil || n != 1 {
		t.Fatalf("recovered attempt rows=%d err=%v", n, err)
	}
}

func TestNoOpDoesNotCountMutation(t *testing.T) {
	db := testDB(t)
	for _, op := range []string{"claim", "retry", "complete", "expire", "delete"} {
		n, err := Apply(context.Background(), db, "queue", op, 0, 0, 8)
		if err != nil || n != 0 {
			t.Fatalf("%s rows=%d err=%v", op, n, err)
		}
	}
}
