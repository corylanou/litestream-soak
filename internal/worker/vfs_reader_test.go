package worker

import (
	"context"
	"errors"
	"os"
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

func TestVFSSessionStartsFromAFreshHydration(t *testing.T) {
	if _, err := exec.LookPath(vfsSQLiteCommand); err != nil {
		t.Skip("sqlite3 shell not installed")
	}
	dir := t.TempDir()
	stale := []string{vfsHydrationFile, vfsHydrationFile + ".meta", ".litestream-snapshot-2965982544", vfsLogFile}
	for _, name := range stale {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session, err := startVFSSession(ctx, "test", vfsSessionSpec{shell: vfsSQLiteCommand, dir: dir, readerURI: "file:" + filepath.Join(dir, "plain.db")})
	if err != nil {
		t.Fatal(err)
	}
	session.close()
	for _, name := range stale {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s survived the session reset: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "keep.txt")); err != nil {
		t.Fatalf("unrelated file removed: %v", err)
	}
}

func TestVFSSessionArgsLoadTheExtensionBeforeOpeningTheReader(t *testing.T) {
	spec := vfsSessionSpec{shell: vfsSQLiteCommand, extension: "/usr/local/lib/litestream-vfs.so", readerURI: "file:/data/vfs/snapshot/replica.db?vfs=litestream&mode=ro"}
	want := []string{"-batch", "-bail", "-list", "-noheader", "-cmd", ".load /usr/local/lib/litestream-vfs.so", "-cmd", `.open "file:/data/vfs/snapshot/replica.db?vfs=litestream&mode=ro"`}
	if got := spec.args(); strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %q, want %q", got, want)
	}
	for _, arg := range spec.args() {
		if arg == spec.readerURI {
			t.Fatal("reader URI passed as the shell's database argument, which the shell opens before .load registers the vfs")
		}
	}
	spec.extension = ""
	if got := spec.args(); len(got) != 6 || got[4] != "-cmd" || !strings.HasPrefix(got[5], `.open "file:`) {
		t.Fatalf("args without extension = %q", got)
	}
}

func TestVFSSessionRefusesAnUnregisteredVFS(t *testing.T) {
	if _, err := exec.LookPath(vfsSQLiteCommand); err != nil {
		t.Skip("sqlite3 shell not installed")
	}
	dir := t.TempDir()
	spec := vfsSessionSpec{shell: vfsSQLiteCommand, dir: dir, readerURI: "file:" + filepath.Join(dir, vfsReaderFile) + "?vfs=" + vfsName + "&mode=ro"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session, err := startVFSSession(ctx, "test", spec)
	if err == nil {
		session.close()
		t.Fatal("startVFSSession() accepted a shell that fell back to an in-memory database")
	}
	if !strings.Contains(err.Error(), "no such vfs: litestream") || !strings.Contains(err.Error(), "open reader file:") {
		t.Fatalf("startVFSSession() error = %v", err)
	}
}

func TestVFSSessionOpenHonorsURIParameters(t *testing.T) {
	if _, err := exec.LookPath(vfsSQLiteCommand); err != nil {
		t.Skip("sqlite3 shell not installed")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "plain.db")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	writer, err := startVFSSession(ctx, "writer", vfsSessionSpec{shell: vfsSQLiteCommand, dir: dir, readerURI: "file:" + path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.exec(ctx, "CREATE TABLE t(x); INSERT INTO t VALUES (7)"); err != nil {
		t.Fatal(err)
	}
	writer.close()

	reader, err := startVFSSession(ctx, "reader", vfsSessionSpec{shell: vfsSQLiteCommand, dir: dir, readerURI: "file:" + path + "?vfs=unix&mode=ro", pragmas: []string{"PRAGMA cache_size=-1024", "PRAGMA mmap_size=0"}})
	if err != nil {
		t.Fatalf("startVFSSession() with vfs=unix&mode=ro error = %v", err)
	}
	defer reader.close()
	if row, err := reader.queryRow(ctx, "SELECT x FROM t"); err != nil || row != "7" {
		t.Fatalf("read through .open URI = %q, err %v", row, err)
	}
	_, err = reader.exec(ctx, "INSERT INTO t VALUES (8)")
	if !errors.Is(err, errVFSSessionClosed) || !strings.Contains(err.Error(), "readonly") {
		t.Fatalf("write on a mode=ro .open URI = %v, want a readonly error", err)
	}
}

func TestVFSSessionReadsHydrationOutcomeFromTheLog(t *testing.T) {
	complete := `time=2026-10-31T17:50:46.432Z level=DEBUG msg="opening ltx file for hydration" vfs=true level=9
time=2026-10-31T17:50:46.604Z level=DEBUG msg="hydration complete" vfs=true name=/data/vfs/snapshot/replica.db path=/data/vfs/snapshot/hydrated.db txid=0000000000000001
time=2026-10-31T17:50:46.605Z level=DEBUG msg="file control" vfs=true pragma=litestream_txid value=<nil>`
	if state := parseVFSHydration(complete); !state.complete || state.failure != "" {
		t.Fatalf("complete log parsed as %+v", state)
	}
	pending := `time=2026-10-31T17:50:46.432Z level=DEBUG msg="opening ltx file for hydration" vfs=true level=9
time=2026-10-31T17:50:46.433Z level=DEBUG msg="file control" vfs=true pragma=litestream_hydration_progress value=<nil>`
	if state := parseVFSHydration(pending); state.complete || state.failure != "" {
		t.Fatalf("pending log parsed as %+v", state)
	}
	failed := `time=2026-10-31T17:50:46.432Z level=DEBUG msg="opening ltx file for hydration" vfs=true level=9
time=2026-10-31T17:50:47.000Z level=ERROR msg="hydration catch-up failed" vfs=true error="fetch page 12: context canceled"`
	if state := parseVFSHydration(failed); state.complete || !strings.HasSuffix(state.failure, `msg="hydration catch-up failed" vfs=true error="fetch page 12: context canceled"`) {
		t.Fatalf("failed log parsed as %+v", state)
	}
	disabled := complete + "\n" + `time=2026-10-31T18:15:44.327Z level=ERROR msg="cannot fetch new ltx files" vfs=true name=/data/vfs/snapshot/replica.db error="apply hydration updates: fetch updated page 8904: open ltx file: file does not exist"`
	if state := parseVFSHydration(disabled); !state.complete || !strings.Contains(state.failure, "apply hydration updates") {
		t.Fatalf("disabled log parsed as %+v", state)
	}
	pollOnly := complete + "\n" + `time=2026-10-31T18:15:44.327Z level=ERROR msg="cannot fetch new ltx files" vfs=true error="poll L0: ltx files: context deadline exceeded"`
	if state := parseVFSHydration(pollOnly); !state.complete || state.failure != "" {
		t.Fatalf("a poll error that leaves hydration on parsed as %+v", state)
	}
	if state := parseVFSHydration(""); state.complete || state.failure != "" {
		t.Fatalf("empty log parsed as %+v", state)
	}
}

func TestVFSSessionKeepsWatchingTheLogAfterHydrationCompletes(t *testing.T) {
	dir := t.TempDir()
	session := &vfsSession{name: "test", dir: dir}
	if state, err := session.hydration(); err != nil || state.complete {
		t.Fatalf("missing log = %+v, %v", state, err)
	}
	appendLog := func(text string) {
		f, err := os.OpenFile(session.logPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(text); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	appendLog(`time=2026-10-31T17:50:46.604Z level=DEBUG msg="hydration complete" vfs=true txid=0000000000000001` + "\n")
	if state, err := session.hydration(); err != nil || !state.complete || state.failure != "" {
		t.Fatalf("complete log = %+v, %v", state, err)
	}
	appendLog(`time=2026-10-31T18:15:44.327Z level=ERROR msg="cannot fetch new ltx files" vfs=true error="apply hydration`)
	if state, err := session.hydration(); err != nil || state.failure != "" {
		t.Fatalf("partial line judged early: %+v, %v", state, err)
	}
	appendLog(` updates: fetch updated page 8904: open ltx file: file does not exist"` + "\n")
	state, err := session.hydration()
	if err != nil || !state.complete || !strings.Contains(state.failure, `msg="cannot fetch new ltx files"`) || !strings.Contains(state.failure, "page 8904") {
		t.Fatalf("disable after completion missed: %+v, %v", state, err)
	}

	fresh := &vfsSession{name: "fresh", dir: t.TempDir()}
	if fresh.logOversized() {
		t.Fatal("a missing log counted as oversized")
	}
	if err := os.WriteFile(fresh.logPath(), make([]byte, vfsLogCapBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if !fresh.logOversized() {
		t.Fatal("a log past the cap was not reported")
	}
	if state, err := fresh.hydration(); err != nil || state.complete || state.failure != "" {
		t.Fatalf("filler judged as a marker: %+v, %v", state, err)
	}
}
