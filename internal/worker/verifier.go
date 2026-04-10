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
	"strings"
	"syscall"
	"time"
)

type VerificationResult struct {
	StartedAt    time.Time
	CompletedAt  time.Time
	CheckType    string
	Status       string
	Passed       bool
	Summary      string
	ErrorMessage string
	DurationMS   int
	DBSizeBytes  int64
	WALSizeBytes int64
}

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

func (v *Verifier) RunCycle(ctx context.Context) (VerificationResult, error) {
	start := time.Now()
	result := VerificationResult{
		StartedAt: start.UTC(),
		CheckType: v.cfg.VerifyType,
		Status:    "running",
	}
	slog.Info("Starting verification cycle")

	if err := v.pauseLoad(); err != nil {
		result.Status = "failed"
		result.ErrorMessage = fmt.Sprintf("pause load: %v", err)
		result.Summary = summarizeVerificationMessage(result.ErrorMessage)
		v.finalizeResult(&result)
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("pause load: %w", err)
	}
	defer v.resumeLoad()

	time.Sleep(2 * time.Second)

	if err := v.checkpoint(ctx); err != nil {
		slog.Warn("Checkpoint failed (non-fatal)", "error", err)
	}

	if err := v.waitForSync(ctx); err != nil {
		result.Status = "failed"
		result.ErrorMessage = fmt.Sprintf("wait for sync: %v", err)
		result.Summary = summarizeVerificationMessage(result.ErrorMessage)
		v.finalizeResult(&result)
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("wait for sync: %w", err)
	}

	passed, err := v.validate(ctx)
	duration := time.Since(start).Seconds()
	RecordVerification(passed, duration)

	if err != nil {
		result.Status = "failed"
		result.ErrorMessage = err.Error()
		result.Summary = summarizeVerificationMessage(result.ErrorMessage)
		v.finalizeResult(&result)
		slog.Error("Verification failed", "error", err, "duration", time.Since(start))
		v.logResult(start, false, result.ErrorMessage)
		return result, err
	}

	if passed {
		result.Status = "passed"
		result.Passed = true
		result.Summary = "verification passed"
		v.finalizeResult(&result)
		slog.Info("Verification passed", "duration", time.Since(start))
		v.logResult(start, true, "")
	} else {
		result.Status = "failed"
		result.ErrorMessage = "validation returned false"
		result.Summary = summarizeVerificationMessage(result.ErrorMessage)
		v.finalizeResult(&result)
		slog.Error("Verification FAILED", "duration", time.Since(start))
		v.logResult(start, false, "validation returned false")
	}

	os.Remove(v.cfg.DBPath + ".restored")

	return result, nil
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
	output, err := cmd.CombinedOutput()

	slog.Info("Validate output", "output", string(output))

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return false, fmt.Errorf("validation failed (exit %d): %s", exitErr.ExitCode(), string(output))
		}
		return false, fmt.Errorf("run validate: %w: %s", err, string(output))
	}

	return true, nil
}

func (v *Verifier) finalizeResult(result *VerificationResult) {
	result.CompletedAt = time.Now().UTC()
	result.DurationMS = int(result.CompletedAt.Sub(result.StartedAt).Milliseconds())
	result.DBSizeBytes = fileSize(v.cfg.DBPath)
	result.WALSizeBytes = fileSize(v.cfg.DBPath + "-wal")
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func summarizeVerificationMessage(msg string) string {
	for _, line := range strings.Split(msg, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if len(line) > 240 {
			return line[:240]
		}
		return line
	}
	return ""
}
