package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"text/template"
	"time"
)

type Runner struct {
	cfg Config

	litestreamCmd *exec.Cmd
	loadCmd       *exec.Cmd
	verifier      *Verifier
}

func NewRunner(cfg Config) *Runner {
	return &Runner{cfg: cfg}
}

func (r *Runner) Run(ctx context.Context) error {
	SetWorkerInfo(r.cfg)
	startTime := time.Now()

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				SetUptime(time.Since(startTime).Seconds())
			}
		}
	}()

	if err := r.populate(ctx); err != nil {
		return fmt.Errorf("populate: %w", err)
	}

	if err := r.writeLitestreamConfig(); err != nil {
		return fmt.Errorf("write litestream config: %w", err)
	}

	if err := r.startLitestream(ctx); err != nil {
		return fmt.Errorf("start litestream: %w", err)
	}
	defer r.stopLitestream()

	if err := r.waitForFirstSync(ctx); err != nil {
		return fmt.Errorf("wait for first sync: %w", err)
	}

	if err := r.startLoad(ctx); err != nil {
		return fmt.Errorf("start load: %w", err)
	}
	defer r.stopLoad()

	r.verifier = NewVerifier(r.cfg, r.loadCmd)

	return r.runVerifyLoop(ctx)
}

func (r *Runner) populate(ctx context.Context) error {
	if _, err := os.Stat(r.cfg.DBPath); err == nil {
		slog.Info("Database already exists, skipping populate", "path", r.cfg.DBPath)
		return nil
	}

	if err := os.MkdirAll(r.cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	slog.Info("Populating database", "target_size", r.cfg.InitialSize)
	cmd := exec.CommandContext(ctx, "litestream-test", "populate",
		"-db", r.cfg.DBPath,
		"-target-size", r.cfg.InitialSize,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
{{- end}}
`))

func (r *Runner) writeLitestreamConfig() error {
	if r.cfg.ReplicaType == "file" {
		if err := os.MkdirAll(r.cfg.ReplicaPath, 0755); err != nil {
			return fmt.Errorf("create replica dir: %w", err)
		}
	}

	f, err := os.Create(r.cfg.ConfigPath)
	if err != nil {
		return fmt.Errorf("create config file: %w", err)
	}
	defer f.Close()

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
	}{
		SocketPath:       r.cfg.SocketPath,
		DBPath:           r.cfg.DBPath,
		SnapshotInterval: r.cfg.SnapshotInterval.String(),
		ReplicaType:      r.cfg.ReplicaType,
		ReplicaPath:      r.cfg.ReplicaPath,
		S3Bucket:         r.cfg.S3Bucket,
		S3Path:           r.cfg.S3Path,
		SyncInterval:     r.cfg.SyncInterval.String(),
		S3Endpoint:       r.cfg.S3Endpoint,
	}

	return litestreamConfigTmpl.Execute(f, data)
}

func (r *Runner) startLitestream(ctx context.Context) error {
	slog.Info("Starting Litestream")

	r.litestreamCmd = exec.CommandContext(ctx, "litestream", "replicate", "-config", r.cfg.ConfigPath)
	r.litestreamCmd.Stdout = os.Stdout
	r.litestreamCmd.Stderr = os.Stderr

	if r.cfg.ReplicaType == "s3" {
		r.litestreamCmd.Env = append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+r.cfg.S3AccessKey,
			"AWS_SECRET_ACCESS_KEY="+r.cfg.S3SecretKey,
		)
	}

	return r.litestreamCmd.Start()
}

func (r *Runner) stopLitestream() {
	if r.litestreamCmd == nil || r.litestreamCmd.Process == nil {
		return
	}
	slog.Info("Stopping Litestream")
	r.litestreamCmd.Process.Signal(os.Interrupt)
	r.litestreamCmd.Wait()
}

func (r *Runner) waitForFirstSync(ctx context.Context) error {
	slog.Info("Waiting for first Litestream sync")

	deadline := time.After(2 * time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for first sync")
		case <-ticker.C:
			if _, err := os.Stat(r.cfg.SocketPath); err == nil {
				slog.Info("Litestream socket ready")
				time.Sleep(2 * time.Second)
				return nil
			}
		}
	}
}

func (r *Runner) startLoad(ctx context.Context) error {
	slog.Info("Starting load generator",
		"profile", r.cfg.ProfileName,
		"write_rate", r.cfg.WriteRate,
		"pattern", r.cfg.Pattern,
	)

	r.loadCmd = exec.CommandContext(ctx, "litestream-test", "load",
		"-db", r.cfg.DBPath,
		"-write-rate", strconv.Itoa(r.cfg.WriteRate),
		"-duration", r.cfg.LoadDuration.String(),
		"-pattern", r.cfg.Pattern,
		"-payload-size", strconv.Itoa(r.cfg.PayloadSize),
		"-read-ratio", fmt.Sprintf("%.2f", r.cfg.ReadRatio),
		"-workers", strconv.Itoa(r.cfg.Workers),
	)
	r.loadCmd.Stdout = os.Stdout
	r.loadCmd.Stderr = os.Stderr

	if err := r.loadCmd.Start(); err != nil {
		return fmt.Errorf("start load: %w", err)
	}

	SetLoadRunning(true)
	return nil
}

func (r *Runner) stopLoad() {
	if r.loadCmd == nil || r.loadCmd.Process == nil {
		return
	}
	slog.Info("Stopping load generator")
	r.loadCmd.Process.Signal(os.Interrupt)
	r.loadCmd.Wait()
	SetLoadRunning(false)
}

func (r *Runner) runVerifyLoop(ctx context.Context) error {
	slog.Info("Starting verification loop", "interval", r.cfg.VerifyInterval)

	ticker := time.NewTicker(r.cfg.VerifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Verification loop stopped")
			return nil
		case <-ticker.C:
			passed, err := r.verifier.RunCycle(ctx)
			if err != nil {
				slog.Error("Verification cycle error", "error", err)
			}
			if !passed {
				slog.Error("VERIFICATION FAILED — replication integrity compromised")
			}
		}
	}
}
