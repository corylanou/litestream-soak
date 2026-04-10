package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type Verifier struct {
	cfg        Config
	loadCmd    *exec.Cmd
	httpClient *http.Client
}

func NewVerifier(cfg Config, loadCmd *exec.Cmd) *Verifier {
	return &Verifier{
		cfg:     cfg,
		loadCmd: loadCmd,
		httpClient: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", cfg.SocketPath)
				},
			},
			Timeout: 90 * time.Second,
		},
	}
}

func (v *Verifier) RunCycle(ctx context.Context) (bool, error) {
	start := time.Now()
	slog.Info("Starting verification cycle")

	if err := v.pauseLoad(); err != nil {
		v.logResult(start, false, fmt.Sprintf("pause load: %v", err))
		return false, fmt.Errorf("pause load: %w", err)
	}
	defer v.resumeLoad()

	time.Sleep(2 * time.Second)

	if err := v.checkpoint(ctx); err != nil {
		slog.Warn("Checkpoint failed (non-fatal)", "error", err)
	}

	if err := v.waitForSync(ctx); err != nil {
		v.logResult(start, false, fmt.Sprintf("wait for sync: %v", err))
		return false, fmt.Errorf("wait for sync: %w", err)
	}

	passed, err := v.validate(ctx)
	duration := time.Since(start).Seconds()
	RecordVerification(passed, duration)

	if err != nil {
		slog.Error("Verification failed", "error", err, "duration", time.Since(start))
		v.logResult(start, false, err.Error())
		return false, err
	}

	if passed {
		slog.Info("Verification passed", "duration", time.Since(start))
		v.logResult(start, true, "")
	} else {
		slog.Error("Verification FAILED", "duration", time.Since(start))
		v.logResult(start, false, "validation returned false")
	}

	os.Remove(v.cfg.DBPath + ".restored")

	return passed, nil
}

func (v *Verifier) logResult(start time.Time, passed bool, errMsg string) {
	logPath := v.cfg.DataDir + "/verification.log"
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open verification log", "error", err)
		return
	}
	defer f.Close()

	result := "PASS"
	if !passed {
		result = "FAIL"
	}

	dbSize := int64(0)
	if info, err := os.Stat(v.cfg.DBPath); err == nil {
		dbSize = info.Size()
	}

	entry := fmt.Sprintf("%s | %s | duration=%s | db_size=%d | error=%s\n",
		start.UTC().Format(time.RFC3339), result, time.Since(start).Round(time.Millisecond), dbSize, errMsg)
	f.WriteString(entry)
}

func (v *Verifier) pauseLoad() error {
	if v.loadCmd == nil || v.loadCmd.Process == nil {
		return nil
	}
	SetLoadRunning(false)
	slog.Info("Pausing load generator")
	return v.loadCmd.Process.Signal(syscall.SIGSTOP)
}

func (v *Verifier) resumeLoad() {
	if v.loadCmd == nil || v.loadCmd.Process == nil {
		return
	}
	if err := v.loadCmd.Process.Signal(syscall.SIGCONT); err != nil {
		slog.Error("Failed to resume load generator", "error", err)
		return
	}
	SetLoadRunning(true)
	slog.Info("Resumed load generator")
}

func (v *Verifier) checkpoint(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "sqlite3", v.cfg.DBPath, "PRAGMA wal_checkpoint(PASSIVE);")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("checkpoint: %w: %s", err, output)
	}
	slog.Info("WAL checkpoint complete")
	return nil
}

func (v *Verifier) waitForSync(ctx context.Context) error {
	slog.Info("Waiting for Litestream sync")

	body, err := json.Marshal(map[string]interface{}{
		"path":    v.cfg.DBPath,
		"wait":    true,
		"timeout": 60,
	})
	if err != nil {
		return fmt.Errorf("marshal sync request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "http://localhost/sync", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create sync request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sync request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errBody map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("sync returned %d: %v", resp.StatusCode, errBody)
	}

	slog.Info("Litestream sync complete")
	return nil
}

func (v *Verifier) validate(ctx context.Context) (bool, error) {
	args := []string{
		"validate",
		"-source-db", v.cfg.DBPath,
		"-config", v.cfg.ConfigPath,
		"-restored-db", v.cfg.DBPath + ".restored",
		"-check-type", v.cfg.VerifyType,
	}

	cmd := exec.CommandContext(ctx, "litestream-test", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return false, fmt.Errorf("validation failed with exit code %d", exitErr.ExitCode())
		}
		return false, fmt.Errorf("run validate: %w", err)
	}

	return true, nil
}
