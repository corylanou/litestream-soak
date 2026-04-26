package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
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
	Steps        []reporting.VerificationStep
}

type verificationStepMetadataError struct {
	err             error
	command         []string
	deadlineAt      *time.Time
	exitCode        *int
	signal          string
	contextCanceled bool
	contextError    string
	outputTail      string
}

func (e *verificationStepMetadataError) Error() string {
	return e.err.Error()
}

func (e *verificationStepMetadataError) Unwrap() error {
	return e.err
}

type Verifier struct {
	cfg        Config
	loadCmd    *exec.Cmd
	httpClient *http.Client
}

func NewVerifier(cfg Config, loadCmd *exec.Cmd) *Verifier {
	return &Verifier{
		cfg:        cfg,
		loadCmd:    loadCmd,
		httpClient: newIPCClient(cfg.SocketPath, 90*time.Second),
	}
}

func (v *Verifier) RunCycle(ctx context.Context) (result VerificationResult, retErr error) {
	start := time.Now()
	result = VerificationResult{
		StartedAt: start.UTC(),
		CheckType: v.cfg.VerifyType,
		Status:    "running",
	}
	slog.Info("Starting verification cycle")

	if err := recordVerificationStep(&result, "pause_load", v.pauseLoad); err != nil {
		result.Status = "failed"
		result.ErrorMessage = fmt.Sprintf("pause load: %v", err)
		result.Summary = summarizeVerificationMessage(result.ErrorMessage)
		v.finalizeResult(&result)
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("pause load: %w", err)
	}
	defer func() {
		_ = recordVerificationStep(&result, "resume_load", func() error {
			v.resumeLoad()
			return nil
		})
	}()

	time.Sleep(2 * time.Second)

	if err := recordVerificationStep(&result, "checkpoint", func() error {
		return v.checkpoint(ctx)
	}); err != nil {
		slog.Warn("Checkpoint failed (non-fatal)", "error", err)
	}

	if err := recordVerificationStep(&result, "sync", func() error {
		return v.waitForSync(ctx)
	}); err != nil {
		result.Status = "failed"
		result.ErrorMessage = fmt.Sprintf("wait for sync: %v", err)
		result.Summary = summarizeVerificationMessage(result.ErrorMessage)
		v.finalizeResult(&result)
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("wait for sync: %w", err)
	}

	var passed bool
	var err error
	validateErr := recordVerificationStep(&result, "restore_validate", func() error {
		passed, err = v.validate(ctx)
		return err
	})
	duration := time.Since(start).Seconds()
	RecordVerification(passed, duration)

	if validateErr != nil {
		result.Status = "failed"
		result.ErrorMessage = validateErr.Error()
		result.Summary = summarizeVerificationMessage(result.ErrorMessage)
		v.finalizeResult(&result)
		slog.Error("Verification failed", "error", validateErr, "duration", time.Since(start))
		v.logResult(start, false, result.ErrorMessage)
		return result, validateErr
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
	defer v.httpClient.CloseIdleConnections()
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
		metadata := validationStepMetadata(ctx, cmd, output, err)
		if exitErr, ok := err.(*exec.ExitError); ok {
			metadata.err = fmt.Errorf("validation failed (exit %d): %s", exitErr.ExitCode(), string(output))
			return false, metadata
		}
		metadata.err = fmt.Errorf("run validate: %w: %s", err, string(output))
		return false, metadata
	}

	return true, nil
}

func validationStepMetadata(ctx context.Context, cmd *exec.Cmd, output []byte, err error) *verificationStepMetadataError {
	metadata := &verificationStepMetadataError{
		err:        err,
		command:    append([]string{cmd.Path}, cmd.Args[1:]...),
		outputTail: tailString(string(output), 8192),
	}
	if deadline, ok := ctx.Deadline(); ok {
		deadline = deadline.UTC()
		metadata.deadlineAt = &deadline
	}
	if ctx.Err() != nil {
		metadata.contextCanceled = true
		metadata.contextError = ctx.Err().Error()
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := exitErr.ExitCode()
		metadata.exitCode = &code
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			metadata.signal = status.Signal().String()
		}
	}
	return metadata
}

func (v *Verifier) finalizeResult(result *VerificationResult) {
	result.CompletedAt = time.Now().UTC()
	result.DurationMS = int(result.CompletedAt.Sub(result.StartedAt).Milliseconds())
	result.DBSizeBytes = fileSize(v.cfg.DBPath)
	result.WALSizeBytes = fileSize(v.cfg.DBPath + "-wal")
}

func recordVerificationStep(result *VerificationResult, name string, fn func() error) error {
	startedAt := time.Now().UTC()
	err := fn()
	completedAt := time.Now().UTC()
	step := reporting.VerificationStep{
		Name:        name,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		DurationMS:  int(completedAt.Sub(startedAt).Milliseconds()),
		Status:      "ok",
	}
	if err != nil {
		step.Status = "error"
		step.Error = err.Error()
		var metadata *verificationStepMetadataError
		if errors.As(err, &metadata) {
			step.Command = metadata.command
			step.DeadlineAt = metadata.deadlineAt
			step.ExitCode = metadata.exitCode
			step.Signal = metadata.signal
			step.ContextCanceled = metadata.contextCanceled
			step.ContextError = metadata.contextError
			step.OutputTail = metadata.outputTail
		}
	}
	result.Steps = append(result.Steps, step)
	return err
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

func tailString(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[len(value)-limit:]
}
