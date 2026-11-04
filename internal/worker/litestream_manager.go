package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

type litestreamManager struct {
	cfg *Config

	litestreamCmd  *exec.Cmd
	litestreamProc *litestreamProcess
	litestreamLog  *lineBuffer
	litestreamMu   sync.Mutex

	s3FaultProxyEndpoint string
}

// litestreamProcess is the exit bookkeeping of one Litestream process. It is
// per process so a monitor started for an earlier process never reads the
// state of the one started after it.
type litestreamProcess struct {
	done chan struct{}
	cmd  *exec.Cmd

	mu   sync.Mutex
	err  error
	exit *reporting.ProcessExitSnapshot
	// expectedExit marks a stop the harness asked for, so the monitor does
	// not treat the exit as a failure.
	expectedExit bool
	// stop is the request currently responsible for the exit. A later
	// request supersedes it, and a superseded request never sends its signal.
	stop *litestreamStop
}

// litestreamStop is one stop request for one process. The process is marked
// as stopping the moment the request is made; the signal goes out later and
// only while the request still owns the stop.
type litestreamStop struct {
	sig os.Signal
}

func (p *litestreamProcess) markExpectedExit() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expectedExit = true
}

func (p *litestreamProcess) exitExpected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expectedExit
}

func (p *litestreamProcess) finish(err error, exit *reporting.ProcessExitSnapshot) {
	p.mu.Lock()
	p.err = err
	p.exit = exit
	p.mu.Unlock()
	close(p.done)
}

func (p *litestreamProcess) exitError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func newLitestreamManager(cfg *Config) litestreamManager {
	return litestreamManager{
		cfg:           cfg,
		litestreamLog: newLineBuffer(120),
	}
}

var litestreamConfigTmpl = template.Must(template.New("config").Parse(`socket:
  enabled: true
  path: {{.SocketPath}}

addr: {{.MetricsAddr}}

{{- if .Levels}}

levels:
{{- range .Levels}}
  - interval: {{.}}
{{- end}}
{{- end}}
{{- if .L0Retention}}

l0-retention: {{.L0Retention}}
{{- end}}
{{- if .L0RetentionCheckInterval}}

l0-retention-check-interval: {{.L0RetentionCheckInterval}}
{{- end}}

dbs:
{{- range .Databases}}
  - {{if .Dir}}dir: {{.Dir}}{{else}}path: {{.Path}}{{end}}
{{- if .Pattern}}
    pattern: "{{.Pattern}}"
{{- end}}
{{- if .Watch}}
    watch: true
{{- end}}
{{- if .TruncatePageN}}
    truncate-page-n: {{.TruncatePageN}}
{{- end}}
    snapshot:
      interval: {{.SnapshotInterval}}
    replicas:
{{- if eq .Replica.Type "file"}}
      - path: {{.Replica.Path}}
        sync-interval: {{.Replica.SyncInterval}}
{{- else if .Replica.URL}}
      - url: {{.Replica.URL}}
        type: {{.Replica.Type}}
        sync-interval: {{.Replica.SyncInterval}}
{{- range .Replica.Extra}}
        {{.}}
{{- end}}
{{- else}}
      - url: s3://{{.Replica.S3Bucket}}/{{.Replica.S3Path}}
        sync-interval: {{.Replica.SyncInterval}}
        endpoint: {{.Replica.S3Endpoint}}
        force-path-style: true
{{- if .Replica.S3PartSize}}
        part-size: {{.Replica.S3PartSize}}
{{- end}}
{{- if gt .Replica.S3Concurrency 0}}
        concurrency: {{.Replica.S3Concurrency}}
{{- end}}
{{- end}}
{{- end}}
`))

func (m *litestreamManager) writeLitestreamConfig() error {
	if err := validateLitestreamMetricsAddr(m.cfg.LitestreamMetricsAddr); err != nil {
		return err
	}
	if err := m.cleanupStaleLitestreamState(); err != nil {
		return err
	}

	if m.cfg.ReplicaType == "file" {
		if err := os.MkdirAll(m.cfg.ReplicaPath, 0755); err != nil {
			return fmt.Errorf("create replica dir: %w", err)
		}
	}

	var rendered bytes.Buffer
	if err := litestreamConfigTmpl.Execute(&rendered, m.litestreamConfigData()); err != nil {
		return err
	}
	if err := writeFileAtomic(m.cfg.ConfigPath, rendered.Bytes()); err != nil {
		return fmt.Errorf("config file: %w", err)
	}
	if err := m.writeLitestreamReplicaTarget(); err != nil {
		return fmt.Errorf("write replica target marker: %w", err)
	}
	return nil
}

type litestreamConfigData struct {
	SocketPath               string
	MetricsAddr              string
	Levels                   []string
	L0Retention              string
	L0RetentionCheckInterval string
	Databases                []litestreamConfigDB
}

type litestreamConfigDB struct {
	Path             string
	Dir              string
	Pattern          string
	Watch            bool
	TruncatePageN    string
	SnapshotInterval string
	Replica          litestreamConfigReplica
}

type litestreamConfigReplica struct {
	Type          string
	Path          string
	URL           string
	Extra         []string
	S3Bucket      string
	S3Path        string
	SyncInterval  string
	S3Endpoint    string
	S3PartSize    string
	S3Concurrency int
}

func (m *litestreamManager) litestreamConfigData() litestreamConfigData {
	data := litestreamConfigData{
		SocketPath:  m.cfg.SocketPath,
		MetricsAddr: m.cfg.LitestreamMetricsAddr,
	}
	if m.cfg.L1CompactionInterval > 0 && m.cfg.L2CompactionInterval > 0 && m.cfg.L3CompactionInterval > 0 {
		data.Levels = []string{
			m.cfg.L1CompactionInterval.String(),
			m.cfg.L2CompactionInterval.String(),
			m.cfg.L3CompactionInterval.String(),
		}
	}
	if m.cfg.L0Retention > 0 {
		data.L0Retention = m.cfg.L0Retention.String()
	}
	if m.cfg.L0RetentionCheckInterval > 0 {
		data.L0RetentionCheckInterval = m.cfg.L0RetentionCheckInterval.String()
	}

	truncatePageN := ""
	if m.cfg.TruncatePageN != nil {
		truncatePageN = strconv.Itoa(*m.cfg.TruncatePageN)
	}

	if !m.cfg.ManyDBEnabled() {
		data.Databases = []litestreamConfigDB{{
			Path:             m.cfg.DBPath,
			TruncatePageN:    truncatePageN,
			SnapshotInterval: m.cfg.SnapshotInterval.String(),
			Replica:          m.litestreamConfigReplica(m.cfg.DBPath),
		}}
		return data
	}

	if m.cfg.manyDBConfigMode() == "dir" {
		data.Databases = []litestreamConfigDB{{
			Dir:              m.cfg.ManyDBDir(),
			Pattern:          "*.db",
			Watch:            true,
			TruncatePageN:    truncatePageN,
			SnapshotInterval: m.cfg.SnapshotInterval.String(),
			Replica:          m.litestreamConfigReplica(""),
		}}
		return data
	}

	paths := m.cfg.ManyDBPaths()
	dbs := make([]litestreamConfigDB, 0, len(paths))
	for _, dbPath := range paths {
		dbs = append(dbs, litestreamConfigDB{
			Path:             dbPath,
			TruncatePageN:    truncatePageN,
			SnapshotInterval: m.cfg.SnapshotInterval.String(),
			Replica:          m.litestreamConfigReplica(dbPath),
		})
	}
	data.Databases = dbs
	return data
}

func validateLitestreamMetricsAddr(addr string) error {
	host, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return fmt.Errorf("invalid Litestream metrics address %q: %w", addr, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("litestream metrics address %q must use a loopback IP", addr)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("litestream metrics address %q has an invalid port", addr)
	}
	return nil
}

func (m *litestreamManager) litestreamConfigReplica(dbPath string) litestreamConfigReplica {
	replica := litestreamConfigReplica{
		Type:          m.cfg.ReplicaType,
		SyncInterval:  m.cfg.SyncInterval.String(),
		S3Bucket:      m.cfg.S3Bucket,
		S3Endpoint:    m.cfg.S3Endpoint,
		S3PartSize:    m.cfg.S3PartSize,
		S3Concurrency: m.cfg.S3Concurrency,
	}
	if m.cfg.ReplicaType == "file" {
		replica.Path = strings.TrimPrefix(m.cfg.ReplicaURLForDB(dbPath), "file://")
		return replica
	}
	if m.cfg.remoteReplicaType() {
		replica.URL = m.cfg.ReplicaURLForDB(dbPath)
		replica.Extra = m.cfg.remoteReplicaExtra()
		return replica
	}
	replicaURL := strings.TrimPrefix(m.cfg.ReplicaURLForDB(dbPath), "s3://")
	parts := strings.SplitN(replicaURL, "/", 2)
	if len(parts) == 2 {
		replica.S3Bucket = parts[0]
		replica.S3Path = parts[1]
	} else {
		replica.S3Bucket = replicaURL
	}
	return replica
}

func (m *litestreamManager) cleanupStaleLitestreamState() error {
	for _, dbPath := range m.replicaTargetDBPaths() {
		current := strings.TrimSpace(m.replicaTargetMarker(dbPath))
		if current == "" {
			continue
		}

		targets, err := m.previousLitestreamReplicaTargets(dbPath)
		if err != nil {
			return err
		}
		for _, target := range targets {
			if target == "" || target == current || target == strings.TrimSpace(m.cfg.ReplicaURL()) {
				continue
			}
			stateDir := litestreamStateDir(dbPath)
			slog.Info("Removing stale Litestream local state", "state_dir", stateDir, "previous_replica", target, "current_replica", current)
			if err := os.RemoveAll(stateDir); err != nil {
				return fmt.Errorf("remove stale litestream state: %w", err)
			}
			break
		}
	}
	return nil
}

func (m *litestreamManager) previousLitestreamReplicaTargets(dbPath string) ([]string, error) {
	var targets []string

	marker, err := os.ReadFile(litestreamReplicaTargetPath(dbPath))
	if err == nil {
		if target := strings.TrimSpace(string(marker)); target != "" {
			targets = append(targets, target)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read replica target marker: %w", err)
	}

	target, ok, err := litestreamConfigReplicaTarget(m.cfg.ConfigPath)
	if err != nil {
		return nil, err
	}
	if ok {
		targets = append(targets, target)
	}
	return targets, nil
}

func (m *litestreamManager) writeLitestreamReplicaTarget() error {
	for _, dbPath := range m.replicaTargetDBPaths() {
		target := strings.TrimSpace(m.replicaTargetMarker(dbPath))
		if target == "" {
			continue
		}
		stateDir := litestreamStateDir(dbPath)
		if err := os.MkdirAll(stateDir, 0755); err != nil {
			return fmt.Errorf("create litestream state dir: %w", err)
		}
		if err := writeFileAtomic(litestreamReplicaTargetPath(dbPath), []byte(target+"\n")); err != nil {
			return err
		}
	}
	return nil
}

func (m *litestreamManager) replicaTargetDBPaths() []string {
	if m.cfg.ManyDBEnabled() {
		return m.cfg.ManyDBPaths()
	}
	return []string{m.cfg.DBPath}
}

func (m *litestreamManager) replicaTargetMarker(dbPath string) string {
	if m.cfg.ManyDBEnabled() && m.cfg.manyDBConfigMode() == "list" {
		return m.cfg.ReplicaURLForDB(dbPath)
	}
	return m.cfg.ReplicaURL()
}

func litestreamReplicaTargetPath(dbPath string) string {
	return filepath.Join(litestreamStateDir(dbPath), "soak-replica-url")
}

func litestreamConfigReplicaTarget(configPath string) (string, bool, error) {
	body, err := os.ReadFile(configPath)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read litestream config: %w", err)
	}

	inReplicas := false
	for _, raw := range strings.Split(string(body), "\n") {
		line := strings.TrimSpace(raw)
		if line == "replicas:" {
			inReplicas = true
			continue
		}
		if !inReplicas {
			continue
		}
		if target := strings.TrimSpace(strings.TrimPrefix(line, "- url:")); target != line {
			return target, target != "", nil
		}
		if target := strings.TrimSpace(strings.TrimPrefix(line, "- path:")); target != line {
			if target == "" {
				return "", false, nil
			}
			return "file://" + target, true, nil
		}
	}
	return "", false, nil
}

func (m *litestreamManager) startLitestream(ctx context.Context) error {
	slog.Info("Starting Litestream")

	m.litestreamCmd = exec.CommandContext(ctx, m.cfg.litestreamCommand(), "replicate", "-config", m.cfg.ConfigPath)
	m.litestreamCmd.Stdout = io.MultiWriter(os.Stdout, m.litestreamLog)
	m.litestreamCmd.Stderr = io.MultiWriter(os.Stderr, m.litestreamLog)

	if m.cfg.ReplicaType == "s3" {
		m.litestreamCmd.Env = m.litestreamEnv()
	}

	if err := m.litestreamCmd.Start(); err != nil {
		return err
	}

	proc := &litestreamProcess{done: make(chan struct{}), cmd: m.litestreamCmd}
	m.litestreamMu.Lock()
	m.litestreamProc = proc
	m.litestreamMu.Unlock()

	go func(cmd *exec.Cmd) {
		err := cmd.Wait()
		err = errors.Join(err, m.litestreamLog.FlushPending())
		proc.finish(err, processExitSnapshot("litestream", time.Now().UTC(), err))
	}(m.litestreamCmd)

	return nil
}

func (m *litestreamManager) currentProcess() *litestreamProcess {
	m.litestreamMu.Lock()
	defer m.litestreamMu.Unlock()
	return m.litestreamProc
}

// litestreamRunning reports whether the current Litestream process has not
// exited yet.
func (m *litestreamManager) litestreamRunning() bool {
	proc := m.currentProcess()
	if proc == nil {
		return false
	}
	select {
	case <-proc.done:
		return false
	default:
		return true
	}
}

// litestreamExitExpected reports whether the current process was asked to
// stop by the harness.
func (m *litestreamManager) litestreamExitExpected() bool {
	proc := m.currentProcess()
	return proc != nil && proc.exitExpected()
}

func (m *litestreamManager) litestreamEnv() []string {
	return m.cfg.s3CommandEnv(m.s3FaultProxyEndpoint)
}

func setCommandEnv(env []string, key, value string) []string {
	prefix := key + "="
	filtered := env[:0]
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return append(filtered, prefix+value)
}

func (m *litestreamManager) stopLitestream() {
	if err := m.stopLitestreamWithSignal(os.Interrupt); err != nil && !errors.Is(err, errLitestreamNotRunning) {
		slog.Warn("Litestream stop did not complete as signalled", "error", err)
	}
}

var (
	errLitestreamNotRunning        = errors.New("litestream is not running")
	errLitestreamExitedBeforeStop  = errors.New("litestream exited before the stop signal was delivered")
	errLitestreamExitNotAttributed = errors.New("litestream exit is not attributable to the stop signal")
	errLitestreamStopSuperseded    = errors.New("litestream stop was superseded by a later stop")
)

// stopLitestreamWithSignal stops the current Litestream process with sig and
// waits for it to exit. The process and its bookkeeping are captured once, so
// a stop that runs late can never signal a replacement started in between.
func (m *litestreamManager) stopLitestreamWithSignal(sig os.Signal) error {
	return stopLitestreamProcess(m.currentProcess(), sig)
}

// stopLitestreamProcess signals exactly proc and waits for its exit. The exit
// is expected, so the monitor for proc does not cancel the run. It fails when
// the process had already exited, when the signal could not be delivered, or
// when the exit came from a different signal, so a caller that needs the stop
// to be its own doing can tell.
func stopLitestreamProcess(proc *litestreamProcess, sig os.Signal) error {
	stop, err := proc.beginStop(sig)
	if err != nil {
		return err
	}
	return proc.finishStop(stop)
}

// beginStop marks p as stopping on behalf of sig before any signal is sent,
// so a recovery that runs between the request and the signal already sees a
// stopping process. A later beginStop takes the stop over: the earlier
// request's signal is then never sent.
func (p *litestreamProcess) beginStop(sig os.Signal) (*litestreamStop, error) {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil, errLitestreamNotRunning
	}
	select {
	case <-p.done:
		return nil, fmt.Errorf("%w: %s", errLitestreamExitedBeforeStop, describeProcessExit(p.exitSnapshot()))
	default:
	}
	slog.Info("Stopping Litestream", "signal", sig, "pid", p.cmd.Process.Pid)
	stop := &litestreamStop{sig: sig}
	p.mu.Lock()
	p.expectedExit = true
	p.stop = stop
	p.mu.Unlock()
	return stop, nil
}

// finishStop sends the request's signal and waits for the exit. It fails when
// the process exited first, when the signal could not be delivered, when a
// later request superseded this one, or when the exit came from a different
// signal.
func (p *litestreamProcess) finishStop(stop *litestreamStop) error {
	if err := p.signal(stop); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			<-p.done
			return fmt.Errorf("%w: %s", errLitestreamExitedBeforeStop, describeProcessExit(p.exitSnapshot()))
		}
		if errors.Is(err, errLitestreamStopSuperseded) {
			return err
		}
		return fmt.Errorf("signal litestream with %v: %w", stop.sig, err)
	}
	<-p.done
	if exit := p.exitSnapshot(); exit != nil && exit.Signal != "" && !signalMatches(exit.Signal, stop.sig) {
		return fmt.Errorf("%w: exited on %s after %v was sent", errLitestreamExitNotAttributed, exit.Signal, stop.sig)
	}
	return nil
}

// signal delivers the request's signal under the process lock, so a request
// superseded before it sends never sends.
func (p *litestreamProcess) signal(stop *litestreamStop) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop != stop {
		return errLitestreamStopSuperseded
	}
	return p.cmd.Process.Signal(stop.sig)
}

func signalMatches(reported string, sig os.Signal) bool {
	return strings.EqualFold(strings.TrimSpace(reported), sig.String())
}

func (p *litestreamProcess) exitSnapshot() *reporting.ProcessExitSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exit == nil {
		return nil
	}
	exit := *p.exit
	return &exit
}

func (m *litestreamManager) monitorLitestream(ctx context.Context, cancel context.CancelCauseFunc) {
	m.monitorProcess(ctx, cancel, m.currentProcess())
}

// monitorProcess cancels the run when proc exits on its own. The expected-exit
// flag and the exit error are read from proc itself, so a monitor for a
// process the harness stopped on purpose stays quiet even after a new process
// has been started in its place.
func (m *litestreamManager) monitorProcess(ctx context.Context, cancel context.CancelCauseFunc, proc *litestreamProcess) {
	if proc == nil {
		return
	}

	go func() {
		<-proc.done
		if ctx.Err() != nil || proc.exitExpected() {
			return
		}
		if err := proc.exitError(); err != nil {
			cancel(fmt.Errorf("litestream exited unexpectedly: %w", err))
			return
		}
		cancel(errors.New("litestream exited unexpectedly"))
	}()
}

func (m *litestreamManager) litestreamDoneChan() <-chan struct{} {
	proc := m.currentProcess()
	if proc == nil {
		return nil
	}
	return proc.done
}

func (m *litestreamManager) litestreamExitError() error {
	proc := m.currentProcess()
	if proc == nil {
		return nil
	}
	return proc.exitError()
}

func (m *litestreamManager) litestreamPID() int {
	m.litestreamMu.Lock()
	defer m.litestreamMu.Unlock()
	if m.litestreamCmd == nil || m.litestreamCmd.Process == nil {
		return 0
	}
	return m.litestreamCmd.Process.Pid
}

func (m *litestreamManager) waitForFirstSync(ctx context.Context) error {
	slog.Info("Waiting for first Litestream sync")

	deadline := time.After(2 * time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
				return cause
			}
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for first sync")
		case <-ticker.C:
			if _, err := os.Stat(m.cfg.SocketPath); err == nil {
				slog.Info("Litestream socket ready")
				time.Sleep(2 * time.Second)
				return nil
			}
		}
	}
}

func (m *litestreamManager) litestreamExitSnapshot() *reporting.ProcessExitSnapshot {
	proc := m.currentProcess()
	if proc == nil {
		return nil
	}
	return proc.exitSnapshot()
}

func processExitSnapshot(process string, exitedAt time.Time, err error) *reporting.ProcessExitSnapshot {
	snapshot := &reporting.ProcessExitSnapshot{
		Process:  process,
		ExitedAt: exitedAt,
	}
	if err == nil {
		code := 0
		snapshot.ExitCode = &code
		return snapshot
	}
	snapshot.Error = err.Error()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := exitErr.ExitCode()
		snapshot.ExitCode = &code
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			snapshot.Signal = status.Signal().String()
		}
	}
	return snapshot
}
