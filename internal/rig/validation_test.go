package rig

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestValidatePrefix(t *testing.T) {
	for _, tt := range []struct {
		name, values string
		minimum      int
		valid        bool
	}{
		{"equal", "(1, 'a'), (2, 'b')", 2, true},
		{"valid concurrent prefix", "(1, 'a')", 1, true},
		{"missing committed tail", "(1, 'a')", 2, false},
		{"missing first row", "(2, 'b')", 1, false},
		{"changed value", "(1, 'x'), (2, 'b')", 2, false},
		{"wrong storage type", "(1, CAST('a' AS BLOB)), (2, 'b')", 2, false},
		{"extra row", "(1, 'a'), (2, 'b'), (3, 'c')", 2, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := testDB(t, "(1, 'a'), (2, 'b')")
			restored := testDB(t, tt.values)
			err := ValidatePrefix(context.Background(), source, restored, tt.minimum, 1)
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%v error=%v", tt.valid, err)
			}
		})
	}
}

func testDB(t *testing.T, values string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, value BLOB); INSERT INTO t VALUES " + values); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestValidatePrefixRejectsPartialTransaction(t *testing.T) {
	source := testDB(t, "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
	restored := testDB(t, "(1, 'a'), (2, 'b'), (3, 'c')")
	if err := ValidatePrefix(context.Background(), source, restored, 2, 2); err == nil {
		t.Fatal("accepted a partial transaction after the minimum boundary")
	}
}
