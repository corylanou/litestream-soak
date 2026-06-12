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
  - path: {{.DBPath}}
    snapshot:
      interval: {{.SnapshotInterval}}
    replicas:
{{- if eq .ReplicaType "file"}}
      - path: {{.ReplicaPath}}
        sync-interval: {{.SyncInterval}}
{{- else}}
      - url: s3://{{.S3Bucket}}/{{.S3Path}}
        sync-interval: {{.SyncInterval}}
        endpoint: {{.S3Endpoint}}
        force-path-style: true
{{- if .S3PartSize}}
        part-size: {{.S3PartSize}}
{{- end}}
{{- if gt .S3Concurrency 0}}
        concurrency: {{.S3Concurrency}}
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

	data := struct {
		SocketPath       string
		DBPath           string
		SnapshotInterval string
		ReplicaType      string
		ReplicaPath      string
		S3Bucket         string
		S3Path           string
		SyncInterval     string
		S3Endpoint       string
		S3PartSize       string
		S3Concurrency    int
	}{
		SocketPath:       m.cfg.SocketPath,
		DBPath:           m.cfg.DBPath,
		SnapshotInterval: m.cfg.SnapshotInterval.String(),
		ReplicaType:      m.cfg.ReplicaType,
		ReplicaPath:      m.cfg.ReplicaPath,
		S3Bucket:         m.cfg.S3Bucket,
		S3Path:           m.cfg.S3Path,
		SyncInterval:     m.cfg.SyncInterval.String(),
		S3Endpoint:       m.cfg.S3Endpoint,
		S3PartSize:       m.cfg.S3PartSize,
		S3Concurrency:    m.cfg.S3Concurrency,
	}

	if err := litestreamConfigTmpl.Execute(f, data); err != nil {
		return err
	}
	if err := m.writeLitestreamReplicaTarget(); err != nil {
		return fmt.Errorf("write replica target marker: %w", err)
	}
	return nil
}

func (m *litestreamManager) cleanupStaleLitestreamState() error {
	current := strings.TrimSpace(m.cfg.ReplicaURL())
	if current == "" {
		return nil
	}

	targets, err := m.previousLitestreamReplicaTargets()
	if err != nil {
		return err
	}
	for _, target := range targets {
		if target == "" || target == current {
			continue
		}
		stateDir := litestreamStateDir(m.cfg.DBPath)
		slog.Info("Removing stale Litestream local state", "state_dir", stateDir, "previous_replica", target, "current_replica", current)
		if err := os.RemoveAll(stateDir); err != nil {
			return fmt.Errorf("remove stale litestream state: %w", err)
		}
		return nil
	}
	return nil
}

func (m *litestreamManager) previousLitestreamReplicaTargets() ([]string, error) {
	var targets []string

	marker, err := os.ReadFile(litestreamReplicaTargetPath(m.cfg.DBPath))
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
	target := strings.TrimSpace(m.cfg.ReplicaURL())
	if target == "" {
		return nil
	}
	stateDir := litestreamStateDir(m.cfg.DBPath)
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return fmt.Errorf("create litestream state dir: %w", err)
	}
	return os.WriteFile(litestreamReplicaTargetPath(m.cfg.DBPath), []byte(target+"\n"), 0644)
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
