package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
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
	litestreamDone chan struct{}
	litestreamErr  error
	litestreamExit *reporting.ProcessExitSnapshot
	litestreamLog  *lineBuffer
	litestreamMu   sync.Mutex
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

dbs:
{{- range .Databases}}
  - {{if .Dir}}dir: {{.Dir}}{{else}}path: {{.Path}}{{end}}
{{- if .Pattern}}
    pattern: "{{.Pattern}}"
{{- end}}
{{- if .Watch}}
    watch: true
{{- end}}
    snapshot:
      interval: {{.SnapshotInterval}}
    replicas:
{{- if eq .Replica.Type "file"}}
      - path: {{.Replica.Path}}
        sync-interval: {{.Replica.SyncInterval}}
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
	if err := m.cleanupStaleLitestreamState(); err != nil {
		return err
	}

	if m.cfg.ReplicaType == "file" {
		if err := os.MkdirAll(m.cfg.ReplicaPath, 0755); err != nil {
			return fmt.Errorf("create replica dir: %w", err)
		}
	}

	f, err := os.Create(m.cfg.ConfigPath)
	if err != nil {
		return fmt.Errorf("create config file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := litestreamConfigTmpl.Execute(f, m.litestreamConfigData()); err != nil {
		return err
	}
	if err := m.writeLitestreamReplicaTarget(); err != nil {
		return fmt.Errorf("write replica target marker: %w", err)
	}
	return nil
}

type litestreamConfigData struct {
	SocketPath string
	Databases  []litestreamConfigDB
}

type litestreamConfigDB struct {
	Path             string
	Dir              string
	Pattern          string
	Watch            bool
	SnapshotInterval string
	Replica          litestreamConfigReplica
}

type litestreamConfigReplica struct {
	Type          string
	Path          string
	S3Bucket      string
	S3Path        string
	SyncInterval  string
	S3Endpoint    string
	S3PartSize    string
	S3Concurrency int
}

func (m *litestreamManager) litestreamConfigData() litestreamConfigData {
	if !m.cfg.ManyDBEnabled() {
		return litestreamConfigData{
			SocketPath: m.cfg.SocketPath,
			Databases: []litestreamConfigDB{{
				Path:             m.cfg.DBPath,
				SnapshotInterval: m.cfg.SnapshotInterval.String(),
				Replica:          m.litestreamConfigReplica(m.cfg.DBPath),
			}},
		}
	}

	if m.cfg.manyDBConfigMode() == "dir" {
		return litestreamConfigData{
			SocketPath: m.cfg.SocketPath,
			Databases: []litestreamConfigDB{{
				Dir:              m.cfg.ManyDBDir(),
				Pattern:          "*.db",
				Watch:            true,
				SnapshotInterval: m.cfg.SnapshotInterval.String(),
				Replica:          m.litestreamConfigReplica(""),
			}},
		}
	}

	paths := m.cfg.ManyDBPaths()
	dbs := make([]litestreamConfigDB, 0, len(paths))
	for _, dbPath := range paths {
		dbs = append(dbs, litestreamConfigDB{
			Path:             dbPath,
			SnapshotInterval: m.cfg.SnapshotInterval.String(),
			Replica:          m.litestreamConfigReplica(dbPath),
		})
	}
	return litestreamConfigData{
		SocketPath: m.cfg.SocketPath,
		Databases:  dbs,
	}
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
		if err := os.WriteFile(litestreamReplicaTargetPath(dbPath), []byte(target+"\n"), 0644); err != nil {
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

	m.litestreamCmd = exec.CommandContext(ctx, "litestream", "replicate", "-config", m.cfg.ConfigPath)
	m.litestreamCmd.Stdout = io.MultiWriter(os.Stdout, m.litestreamLog)
	m.litestreamCmd.Stderr = io.MultiWriter(os.Stderr, m.litestreamLog)

	if m.cfg.ReplicaType == "s3" {
		m.litestreamCmd.Env = append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+m.cfg.S3AccessKey,
			"AWS_SECRET_ACCESS_KEY="+m.cfg.S3SecretKey,
		)
	}

	if err := m.litestreamCmd.Start(); err != nil {
		return err
	}

	done := make(chan struct{})
	m.litestreamMu.Lock()
	m.litestreamDone = done
	m.litestreamErr = nil
	m.litestreamMu.Unlock()

	go func(cmd *exec.Cmd) {
		err := cmd.Wait()
		m.litestreamMu.Lock()
		m.litestreamErr = err
		m.litestreamExit = processExitSnapshot("litestream", time.Now().UTC(), err)
		m.litestreamMu.Unlock()
		close(done)
	}(m.litestreamCmd)

	return nil
}

func (m *litestreamManager) stopLitestream() {
	if m.litestreamCmd == nil || m.litestreamCmd.Process == nil {
		return
	}
	slog.Info("Stopping Litestream")
	if err := m.litestreamCmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		slog.Warn("Failed to interrupt Litestream", "error", err)
	}
	if done := m.litestreamDoneChan(); done != nil {
		<-done
	}
}

func (m *litestreamManager) monitorLitestream(ctx context.Context, cancel context.CancelCauseFunc) {
	done := m.litestreamDoneChan()
	if done == nil {
		return
	}

	go func() {
		<-done
		if ctx.Err() != nil {
			return
		}
		if err := m.litestreamExitError(); err != nil {
			cancel(fmt.Errorf("litestream exited unexpectedly: %w", err))
			return
		}
		cancel(errors.New("litestream exited unexpectedly"))
	}()
}

func (m *litestreamManager) litestreamDoneChan() <-chan struct{} {
	m.litestreamMu.Lock()
	defer m.litestreamMu.Unlock()
	return m.litestreamDone
}

func (m *litestreamManager) litestreamExitError() error {
	m.litestreamMu.Lock()
	defer m.litestreamMu.Unlock()
	return m.litestreamErr
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
	m.litestreamMu.Lock()
	defer m.litestreamMu.Unlock()
	if m.litestreamExit == nil {
		return nil
	}
	exit := *m.litestreamExit
	return &exit
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
