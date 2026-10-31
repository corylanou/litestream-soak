package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	vfsExtensionDefaultPath = "/usr/local/lib/litestream-vfs.so"
	vfsSQLiteCommand        = "sqlite3"
	vfsName                 = "litestream"
	vfsHydrationFile        = "hydrated.db"
	vfsLogFile              = "vfs.log"
	vfsReaderFile           = "replica.db"
	vfsSentinelPrefix       = "soak-vfs-sentinel-"
	vfsLogTail              = 4096
	// vfsLogCapBytes bounds the DEBUG log the VFS writes. The hydration markers
	// the harness depends on ("hydration complete", the time-travel disable)
	// are DEBUG lines, and at DEBUG the VFS also logs every page read, roughly
	// 100 MiB an hour for the snapshot reader. The log is never truncated in
	// place, since the VFS could append a marker between a scan and the
	// truncate; once it passes the cap the session is dropped at its next cycle
	// and a fresh one starts with a new hydration directory and log.
	vfsLogCapBytes  = 256 << 20
	vfsMaxLineBytes = 1 << 20
)

var errVFSSessionClosed = errors.New("vfs reader session closed")

// vfsSession is one long-lived sqlite3 shell with the Litestream VFS extension
// loaded, reading the worker's replica through a hydrated read-only
// connection. Statements go in on stdin and results come back on stdout one
// row per line; a sentinel row after each statement marks where its output
// ends. The shell runs with -bail, so the first SQL error ends the process
// and the session reports every later statement as closed.
type vfsSession struct {
	name           string
	dir            string
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	lines          chan string
	done           chan struct{}
	mu             sync.Mutex
	next           uint64
	exit           error
	hydrationState vfsHydration
	logOffset      int64
	logRest        []byte
}

// vfsSessionSpec is everything needed to start a session: where the shell and
// extension live, which replica URL to read, and the environment the
// extension needs for that backend.
type vfsSessionSpec struct {
	shell     string
	extension string
	dir       string
	readerURI string
	env       []string
	pragmas   []string
}

func (c Config) vfsDir(name string) string {
	return filepath.Join(c.DataDir, "vfs", name)
}

func (c Config) vfsExtension() string {
	if strings.TrimSpace(c.VFSExtensionPath) != "" {
		return c.VFSExtensionPath
	}
	return vfsExtensionDefaultPath
}

// vfsSessionSpec builds the reader spec for one named session. Each session
// has its own hydration file and log under DATA_DIR/vfs/<name>, so two
// readers never share a hydrated copy. The reader never routes through the
// S3 fault proxy: it reads the replica the way a user's VFS would.
func (c Config) vfsSessionSpec(name string) vfsSessionSpec {
	dir := c.vfsDir(name)
	env := os.Environ()
	if c.ReplicaType == "s3" {
		env = c.s3CommandEnv("")
		if strings.TrimSpace(c.S3Endpoint) != "" {
			env = setCommandEnv(env, "LITESTREAM_S3_ENDPOINT", c.S3Endpoint)
		}
	}
	env = setCommandEnv(env, "LITESTREAM_REPLICA_URL", c.ReplicaURL())
	env = setCommandEnv(env, "LITESTREAM_HYDRATION_ENABLED", "true")
	env = setCommandEnv(env, "LITESTREAM_HYDRATION_PATH", filepath.Join(dir, vfsHydrationFile))
	env = setCommandEnv(env, "LITESTREAM_WRITE_ENABLED", "false")
	env = setCommandEnv(env, "LITESTREAM_LOG_LEVEL", "DEBUG")
	env = setCommandEnv(env, "LITESTREAM_LOG_FILE", filepath.Join(dir, vfsLogFile))
	return vfsSessionSpec{
		shell:     vfsSQLiteCommand,
		extension: c.vfsExtension(),
		dir:       dir,
		readerURI: "file:" + filepath.Join(dir, vfsReaderFile) + "?vfs=" + vfsName + "&mode=ro",
		env:       env,
	}
}

// args starts the shell with no database and opens the reader URI only after
// the extension is loaded. The shell opens a database named on the command
// line as soon as .load runs, before the extension has registered its VFS, so
// a vfs=litestream URI there fails with "no such vfs".
func (s vfsSessionSpec) args() []string {
	args := []string{"-batch", "-bail", "-list", "-noheader"}
	if s.extension != "" {
		args = append(args, "-cmd", ".load "+s.extension)
	}
	return append(args, "-cmd", `.open "`+s.readerURI+`"`)
}

// vfsOpenFailure matches the shell's report of a .open that failed; the shell
// then continues on a substitute in-memory database, so the session must not
// be used.
var vfsOpenFailure = regexp.MustCompile(`^(Error: |Notice: using substitute in-memory database)`)

// resetVFSSessionDir gives a new session a fresh hydration and a log of its
// own, so the hydration markers the session waits for are its own. Leftover
// snapshot files from a killed shell go too.
func resetVFSSessionDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create vfs reader dir: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list vfs reader dir: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == vfsHydrationFile || strings.HasPrefix(name, vfsHydrationFile+".") || strings.HasPrefix(name, ".litestream-snapshot-") || name == vfsLogFile {
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("reset vfs reader dir: %w", err)
			}
		}
	}
	return nil
}

func startVFSSession(ctx context.Context, name string, spec vfsSessionSpec) (*vfsSession, error) {
	if spec.extension != "" {
		if _, err := os.Stat(spec.extension); err != nil {
			return nil, fmt.Errorf("vfs extension unavailable: %w", err)
		}
	}
	if err := resetVFSSessionDir(spec.dir); err != nil {
		return nil, err
	}
	cmd := exec.Command(spec.shell, spec.args()...)
	cmd.Env = spec.env
	cmd.Dir = spec.dir
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", spec.shell, err)
	}
	session := &vfsSession{name: name, dir: spec.dir, cmd: cmd, stdin: stdin, lines: make(chan string, 64), done: make(chan struct{})}
	go session.read(stdout)
	rows, err := session.exec(ctx, "SELECT 1")
	if err != nil {
		session.close()
		return nil, err
	}
	for _, row := range rows {
		if vfsOpenFailure.MatchString(row) {
			session.close()
			return nil, fmt.Errorf("open reader %s: %s", spec.readerURI, row)
		}
	}
	for _, pragma := range spec.pragmas {
		if _, err := session.exec(ctx, pragma); err != nil {
			session.close()
			return nil, err
		}
	}
	return session, nil
}

func (s *vfsSession) read(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), vfsMaxLineBytes)
	for scanner.Scan() {
		s.lines <- scanner.Text()
	}
	close(s.lines)
	err := s.cmd.Wait()
	s.mu.Lock()
	s.exit = err
	s.mu.Unlock()
	close(s.done)
}

func (s *vfsSession) alive() bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

// exec runs one statement and returns its output rows. A closed session, a
// shell error (which ends the process under -bail) or a context deadline all
// return an error; after a deadline the stream can no longer be resynced, so
// the caller must close the session.
func (s *vfsSession) exec(ctx context.Context, statement string) ([]string, error) {
	s.mu.Lock()
	s.next++
	sentinel := fmt.Sprintf("%s%d", vfsSentinelPrefix, s.next)
	s.mu.Unlock()
	if !s.alive() {
		return nil, fmt.Errorf("%w: %s", errVFSSessionClosed, s.exitText())
	}
	if _, err := io.WriteString(s.stdin, strings.TrimSuffix(strings.TrimSpace(statement), ";")+";\nSELECT '"+sentinel+"';\n"); err != nil {
		return nil, fmt.Errorf("%w: write %q: %v", errVFSSessionClosed, statement, err)
	}
	var rows []string
	for {
		select {
		case <-ctx.Done():
			return rows, fmt.Errorf("vfs reader statement %q: %w", statement, ctx.Err())
		case line, ok := <-s.lines:
			if !ok {
				<-s.done
				return rows, fmt.Errorf("%w after %q: %s", errVFSSessionClosed, statement, firstNonEmpty(strings.Join(rows, "; "), s.exitText()))
			}
			if line == sentinel {
				return rows, nil
			}
			rows = append(rows, line)
		}
	}
}

func (s *vfsSession) exitText() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exit != nil {
		return s.exit.Error()
	}
	return "exited"
}

// queryRow runs a statement expected to return exactly one row.
func (s *vfsSession) queryRow(ctx context.Context, statement string) (string, error) {
	rows, err := s.exec(ctx, statement)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", fmt.Errorf("vfs reader statement %q returned %d rows, want 1: %s", statement, len(rows), strings.Join(rows, "; "))
	}
	return rows[0], nil
}

func (s *vfsSession) txid(ctx context.Context) (uint64, error) {
	row, err := s.queryRow(ctx, "PRAGMA litestream_txid")
	if err != nil {
		return 0, err
	}
	txid, err := strconv.ParseUint(strings.TrimSpace(row), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("parse litestream_txid %q: %w", row, err)
	}
	return txid, nil
}

// vfsHydration is what the VFS has logged about this session's hydration.
// PRAGMA litestream_hydration_progress cannot stand in for it: it reports the
// restore compactor's progress, which reaches 100 before the catch-up that
// precedes completion and stays 0 on the resume path, so a reader judged
// ready by it still serves every page from the replica. Completion is not
// final either: when the VFS cannot apply a poll's pages to the hydrated file
// it disables hydrated reads and logs the apply error through "cannot fetch
// new ltx files", and from then on every page comes from the replica again.
type vfsHydration struct {
	complete bool
	failure  string
}

var vfsHydrationFailureMarkers = []string{`msg="hydration failed"`, `msg="hydration catch-up failed"`, `msg="hydration initialization failed`, `msg="hydration truncate failed"`, `msg="hydration disabled for time travel"`}

const (
	vfsHydrationCompleteMarker = `msg="hydration complete"`
	vfsPollFailureMarker       = `msg="cannot fetch new ltx files"`
	vfsHydrationApplyMarker    = `apply hydration updates`
)

func vfsHydrationFailureLine(line string) bool {
	for _, marker := range vfsHydrationFailureMarkers {
		if strings.Contains(line, marker) {
			return true
		}
	}
	return strings.Contains(line, vfsPollFailureMarker) && strings.Contains(line, vfsHydrationApplyMarker)
}

func (h *vfsHydration) scanLine(line string) {
	if strings.Contains(line, vfsHydrationCompleteMarker) {
		h.complete = true
	}
	if h.failure == "" && vfsHydrationFailureLine(line) {
		h.failure = strings.TrimSpace(line)
	}
}

func parseVFSHydration(log string) vfsHydration {
	var state vfsHydration
	for _, line := range strings.Split(log, "\n") {
		state.scanLine(line)
	}
	return state
}

func (s *vfsSession) logPath() string {
	return filepath.Join(s.dir, vfsLogFile)
}

// hydration folds the VFS log written since the last call into the session's
// hydration state, so a failure logged after completion is seen as well.
func (s *vfsSession) hydration() (vfsHydration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.logPath())
	if errors.Is(err, os.ErrNotExist) {
		return s.hydrationState, nil
	}
	if err != nil {
		return s.hydrationState, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return s.hydrationState, err
	}
	if info.Size() <= s.logOffset {
		return s.hydrationState, nil
	}
	chunk := make([]byte, info.Size()-s.logOffset)
	n, err := f.ReadAt(chunk, s.logOffset)
	if err != nil && !errors.Is(err, io.EOF) {
		return s.hydrationState, err
	}
	s.logOffset += int64(n)
	lines := strings.Split(string(append(s.logRest, chunk[:n]...)), "\n")
	s.logRest = []byte(lines[len(lines)-1])
	for _, line := range lines[:len(lines)-1] {
		s.hydrationState.scanLine(line)
	}
	return s.hydrationState, nil
}

// logOversized reports whether the VFS log has passed vfsLogCapBytes, which
// retires the session at its next cycle.
func (s *vfsSession) logOversized() bool {
	info, err := os.Stat(s.logPath())
	return err == nil && info.Size() > vfsLogCapBytes
}

func (s *vfsSession) logTail() string {
	body, err := os.ReadFile(filepath.Join(s.dir, vfsLogFile))
	if err != nil {
		return ""
	}
	return tailString(string(body), vfsLogTail)
}

func (s *vfsSession) close() {
	if s == nil {
		return
	}
	_ = s.stdin.Close()
	go func() {
		for range s.lines {
		}
	}()
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		if s.cmd.Process != nil {
			_ = s.cmd.Process.Kill()
		}
		<-s.done
	}
}
