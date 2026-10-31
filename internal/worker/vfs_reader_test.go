package worker

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestVFSSessionDrivesTheSQLiteShell(t *testing.T) {
	if _, err := exec.LookPath(vfsSQLiteCommand); err != nil {
		t.Skip("sqlite3 shell not installed")
	}
	dir := t.TempDir()
	spec := vfsSessionSpec{shell: vfsSQLiteCommand, dir: dir, readerURI: "file:" + filepath.Join(dir, "plain.db"), pragmas: []string{"PRAGMA cache_size=-1024", "PRAGMA mmap_size=0"}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session, err := startVFSSession(ctx, "test", spec)
	if err != nil {
		t.Fatalf("startVFSSession() error = %v", err)
	}
	defer session.close()

	if _, err := session.exec(ctx, "CREATE TABLE "+cacheProbeTable+"(id INTEGER PRIMARY KEY, generation INTEGER NOT NULL, payload BLOB NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.exec(ctx, "INSERT INTO "+cacheProbeTable+" VALUES (1, 2, zeroblob(10)), (2, 3, zeroblob(10));"); err != nil {
		t.Fatal(err)
	}
	row, err := session.queryRow(ctx, cacheProbeQuery)
	if err != nil || row != "2|2|3|5" {
		t.Fatalf("probe = %q, err %v", row, err)
	}
	rows, err := session.exec(ctx, "SELECT id FROM "+cacheProbeTable+" ORDER BY id")
	if err != nil || strings.Join(rows, ",") != "1,2" {
		t.Fatalf("rows = %v, err %v", rows, err)
	}
	if rows, err := session.exec(ctx, "BEGIN"); err != nil || len(rows) != 0 {
		t.Fatalf("BEGIN = %v, err %v", rows, err)
	}
	if _, err := session.exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if !session.alive() {
		t.Fatal("session exited after valid statements")
	}

	_, err = session.exec(ctx, "SELECT * FROM missing_table")
	if !errors.Is(err, errVFSSessionClosed) || !strings.Contains(err.Error(), "missing_table") {
		t.Fatalf("error after a bad statement = %v, want the shell to bail", err)
	}
	if session.alive() {
		t.Fatal("session still alive after -bail")
	}
	if _, err := session.exec(ctx, "SELECT 1"); !errors.Is(err, errVFSSessionClosed) {
		t.Fatalf("statement on a closed session = %v", err)
	}
}

func TestVFSSessionRequiresTheExtension(t *testing.T) {
	spec := vfsSessionSpec{shell: vfsSQLiteCommand, extension: filepath.Join(t.TempDir(), "missing.so"), dir: t.TempDir()}
	if _, err := startVFSSession(context.Background(), "test", spec); err == nil || !strings.Contains(err.Error(), "vfs extension unavailable") {
		t.Fatalf("startVFSSession() error = %v", err)
	}
}
