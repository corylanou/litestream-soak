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
	vfsMaxLineBytes         = 1 << 20
)

var errVFSSessionClosed = errors.New("vfs reader session closed")

// vfsSession is one long-lived sqlite3 shell with the Litestream VFS extension
// loaded, reading the worker's replica through a hydrated read-only
// connection. Statements go in on stdin and results come back on stdout one
// row per line; a sentinel row after each statement marks where its output
// ends. The shell runs with -bail, so the first SQL error ends the process
// and the session reports every later statement as closed.
type vfsSession struct {
	name  string
	dir   string
	cmd   *exec.Cmd
	stdin io.WriteCloser
	lines chan string
	done  chan struct{}
	mu    sync.Mutex
	next  uint64
	exit  error
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

func (s vfsSessionSpec) args() []string {
	args := []string{"-batch", "-bail", "-list", "-noheader"}
	if s.extension != "" {
		args = append(args, "-cmd", ".load "+s.extension)
	}
	return append(args, s.readerURI)
}

func startVFSSession(ctx context.Context, name string, spec vfsSessionSpec) (*vfsSession, error) {
	if spec.extension != "" {
		if _, err := os.Stat(spec.extension); err != nil {
			return nil, fmt.Errorf("vfs extension unavailable: %w", err)
		}
	}
	if err := os.MkdirAll(spec.dir, 0o755); err != nil {
		return nil, fmt.Errorf("create vfs reader dir: %w", err)
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

func (s *vfsSession) hydrationProgress(ctx context.Context) (float64, error) {
	row, err := s.queryRow(ctx, "PRAGMA litestream_hydration_progress")
	if err != nil {
		return 0, err
	}
	pct, err := strconv.ParseFloat(strings.TrimSpace(row), 64)
	if err != nil {
		return 0, fmt.Errorf("parse litestream_hydration_progress %q: %w", row, err)
	}
	return pct, nil
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
