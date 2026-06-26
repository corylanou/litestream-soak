package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	_ "modernc.org/sqlite"
)

type VerificationResult struct {
	StartedAt                         time.Time
	CompletedAt                       time.Time
	CheckType                         string
	Status                            string
	Passed                            bool
	Summary                           string
	ErrorMessage                      string
	DurationMS                        int
	DBSizeBytes                       int64
	WALSizeBytes                      int64
	Steps                             []reporting.VerificationStep
	SyncStatusBeforeSync              *reporting.LitestreamSyncStatus
	SyncStatusAfterSyncFailure        *reporting.LitestreamSyncStatus
	LitestreamGoroutinesOnSyncFailure *reporting.LitestreamGoroutineSnapshot
	SyncTXID                          uint64
	SyncReplicatedTXID                uint64
	CheckpointResidualBusy            bool
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

type loadPauser interface {
	Pause(ctx context.Context) error
	Resume()
}

type manyDBChangeTracker interface {
	manyDBChangedPathsAndReset() []string
}

type Verifier struct {
	cfg           Config
	pausers       []loadPauser
	manyDBChanges manyDBChangeTracker
	httpClient    *http.Client
	onStart       func(context.Context, VerificationResult)

	checkpointAttempts    int
	checkpointRetryDelay  time.Duration
	checkpointBusyTimeout time.Duration
	syncRetryDelay        time.Duration
}

type syncResponse struct {
	Status         string `json:"status"`
	TXID           uint64 `json:"txid"`
	ReplicatedTXID uint64 `json:"replicated_txid"`
}

const (
	checkpointMode            = "TRUNCATE"
	syncDiagnosticTimeout     = 2 * time.Second
	syncDiagnosticOutputLimit = 64 * 1024
)

func NewVerifier(cfg Config, pausers ...loadPauser) *Verifier {
	verifier := &Verifier{
		cfg:                   cfg,
		pausers:               pausers,
		httpClient:            newIPCClient(cfg.SocketPath, cfg.verifySyncTimeout()+30*time.Second),
		checkpointAttempts:    3,
		checkpointRetryDelay:  2 * time.Second,
		checkpointBusyTimeout: 5 * time.Second,
		syncRetryDelay:        2 * time.Second,
	}
	for _, pauser := range pausers {
		if changes, ok := pauser.(manyDBChangeTracker); ok {
			verifier.manyDBChanges = changes
			break
		}
	}
	return verifier
}

func (v *Verifier) SetStartHook(fn func(context.Context, VerificationResult)) {
	v.onStart = fn
}

func (v *Verifier) RunCycle(ctx context.Context) (result VerificationResult, retErr error) {
	if v.cfg.ManyDBEnabled() {
		return v.runManyDBCycle(ctx)
	}

	start := time.Now()
	result = VerificationResult{
		StartedAt: start.UTC(),
		CheckType: v.cfg.VerifyType,
		Status:    "running",
	}
	defer func() {
		RecordVerificationOutcome(result.Status, time.Since(start).Seconds())
	}()
	slog.Info("Starting verification cycle")
	if v.onStart != nil {
		v.onStart(ctx, result)
	}

	restoredPath := v.cfg.DBPath + ".restored"
	if err := recordVerificationStep(&result, "clean_restored", func() error {
		return removeRestoredArtifacts(restoredPath)
	}); err != nil {
		v.failResult(ctx, &result, fmt.Sprintf("clean restored artifacts: %v", err))
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("clean restored artifacts: %w", err)
	}
	defer func() {
		if err := removeRestoredArtifacts(restoredPath); err != nil {
			slog.Warn("Failed to remove restored artifacts", "error", err)
		}
	}()

	if err := recordVerificationStep(&result, "pause_load", func() error {
		return v.pauseLoad(ctx)
	}); err != nil {
		v.failResult(ctx, &result, fmt.Sprintf("pause load: %v", err))
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
		residualBusy, cpErr := v.checkpoint(ctx)
		result.CheckpointResidualBusy = residualBusy
		return cpErr
	}); err != nil {
		v.failResult(ctx, &result, fmt.Sprintf("checkpoint: %v", err))
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("checkpoint: %w", err)
	}

	if err := recordVerificationStep(&result, "sync", func() error {
		return v.waitForSync(ctx, &result)
	}); err != nil {
		v.failResult(ctx, &result, fmt.Sprintf("wait for sync: %v", err))
		v.logResult(start, false, result.ErrorMessage)
		return result, fmt.Errorf("wait for sync: %w", err)
	}

	var passed bool
	var err error
	validateErr := recordVerificationStep(&result, "restore_validate", func() error {
		passed, err = v.validate(ctx, result.restoreTXID())
		return err
	})

	if validateErr != nil {
		v.failResult(ctx, &result, validateErr.Error())
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
		v.failResult(ctx, &result, "validation returned false")
		slog.Error("Verification FAILED", "duration", time.Since(start))
		v.logResult(start, false, "validation returned false")
	}

	return result, nil
}

func failureStatus(ctx context.Context) string {
	if ctx.Err() != nil {
		return "aborted"
	}
	return "failed"
}

func (v *Verifier) failResult(ctx context.Context, result *VerificationResult, message string) {
	result.Status = failureStatus(ctx)
	result.ErrorMessage = message
	summary := summarizeVerificationMessage(message)
	if result.Status == "aborted" {
		summary = "verification aborted: " + summary
	}
	result.Summary = summary
	v.finalizeResult(result)
}

func removeRestoredArtifacts(restoredPath string) error {
	var errs []error
	for _, path := range []string{restoredPath, restoredPath + "-wal", restoredPath + "-shm"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (v *Verifier) logResult(start time.Time, passed bool, errMsg string) {
	logPath := v.cfg.DataDir + "/verification.log"
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open verification log", "error", err)
		return
	}
	defer func() { _ = f.Close() }()

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
	if _, err := f.WriteString(entry); err != nil {
		slog.Error("Failed to write verification log", "error", err)
	}
}

func (v *Verifier) pauseLoad(ctx context.Context) error {
	if len(v.pausers) == 0 {
		return nil
	}
	slog.Info("Pausing load generators", "count", len(v.pausers))
	for i, pauser := range v.pausers {
		pauseCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := pauser.Pause(pauseCtx)
		cancel()
		if err != nil {
			// Resume the failing pauser too: Pause may mark internal paused
			// state (e.g. a quiesce wait that timed out) before returning.
			for j := i; j >= 0; j-- {
				v.pausers[j].Resume()
			}
			return fmt.Errorf("pause load generator %d of %d: %w", i+1, len(v.pausers), err)
		}
	}
	return nil
}

func (v *Verifier) resumeLoad() {
	if len(v.pausers) == 0 {
		return
	}
	for _, pauser := range v.pausers {
		pauser.Resume()
	}
	slog.Info("Resumed load generators", "count", len(v.pausers))
}

func (v *Verifier) checkpoint(ctx context.Context) (residualBusy bool, _ error) {
	return v.checkpointDB(ctx, v.cfg.DBPath)
}

func (v *Verifier) checkpointDB(ctx context.Context, dbPath string) (residualBusy bool, _ error) {
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)",
		dbPath, v.checkpointBusyTimeout.Milliseconds())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return false, fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	var busy, logFrames, checkpointed int
	var lastErr error
	for attempt := 1; attempt <= v.checkpointAttempts; attempt++ {
		if attempt > 1 {
			// SIGSTOP can freeze a load process mid-transaction, making the
			// busy state permanent for the cycle; briefly resuming the
			// writers lets the lock holder finish before the retry.
			if len(v.pausers) > 0 {
				v.resumeLoad()
			}
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(v.checkpointRetryDelay):
			}
			if len(v.pausers) > 0 {
				if err := v.pauseLoad(ctx); err != nil {
					return false, fmt.Errorf("re-pause load during checkpoint retry: %w", err)
				}
			}
		}
		lastErr = db.QueryRowContext(ctx, "PRAGMA wal_checkpoint("+checkpointMode+");").
			Scan(&busy, &logFrames, &checkpointed)
		if lastErr != nil {
			continue
		}
		if busy == 0 {
			slog.Info("WAL checkpoint complete",
				"mode", checkpointMode, "busy", busy, "log_frames", logFrames, "checkpointed", checkpointed)
			return false, nil
		}
	}
	if lastErr != nil {
		return false, fmt.Errorf("checkpoint after %d attempts: %w", v.checkpointAttempts, lastErr)
	}
	// Litestream holds a long-lived WAL read transaction by design, so a
	// second-process TRUNCATE checkpoint can stay busy indefinitely. The
	// un-checkpointed tail is still replicated from the WAL, and the sync
	// step's ReplicatedTXID >= TXID gate is the stale-restore guard, so a
	// residual-busy checkpoint must not fail the cycle.
	slog.Warn("WAL checkpoint left residual frames; relying on sync TXID gate",
		"mode", checkpointMode, "attempts", v.checkpointAttempts,
		"busy", busy, "log_frames", logFrames, "checkpointed", checkpointed)
	return true, nil
}

func (v *Verifier) waitForSync(ctx context.Context, result *VerificationResult) error {
	return v.waitForSyncDB(ctx, result, v.cfg.DBPath)
}

func (v *Verifier) waitForSyncDB(ctx context.Context, result *VerificationResult, dbPath string) error {
	slog.Info("Waiting for Litestream sync")
	if result != nil {
		result.SyncStatusBeforeSync = v.collectSyncStatus()
	}

	startedAt := time.Now()
	degradedAfter := v.cfg.verifySyncDegradedAfter()
	degradedLogged := false
	deadline := time.Now().Add(v.cfg.verifySyncTimeout())
	for {
		syncResp, err := v.syncOnceDB(ctx, time.Until(deadline), dbPath)
		if err != nil {
			v.captureSyncFailureDiagnostics(result, err)
			return err
		}
		if result != nil {
			result.SyncTXID = syncResp.TXID
			result.SyncReplicatedTXID = syncResp.ReplicatedTXID
		}
		lag := uint64(0)
		if syncResp.TXID > syncResp.ReplicatedTXID {
			lag = syncResp.TXID - syncResp.ReplicatedTXID
		}
		SetReplicatedTXID(float64(syncResp.ReplicatedTXID))
		SetReplicationLag(float64(lag))

		if syncResp.ReplicatedTXID >= syncResp.TXID {
			if degradedAfter > 0 && time.Since(startedAt) > degradedAfter {
				slog.Warn("Litestream sync completed after degraded threshold",
					"elapsed", time.Since(startedAt).Round(time.Second),
					"degraded_after", degradedAfter,
					"hard_timeout", v.cfg.verifySyncTimeout())
			}
			slog.Info("Litestream sync complete",
				"status", syncResp.Status,
				"txid", formatTXID(syncResp.TXID),
				"replicated_txid", formatTXID(syncResp.ReplicatedTXID))
			return nil
		}

		if time.Now().Add(v.syncRetryDelay).After(deadline) {
			syncErr := syncLagNotResolvedError(v.cfg, syncResp.TXID, syncResp.ReplicatedTXID)
			v.captureSyncFailureDiagnostics(result, syncErr)
			return syncErr
		}

		slog.Warn("Litestream sync lagging; retrying",
			"txid", formatTXID(syncResp.TXID),
			"replicated_txid", formatTXID(syncResp.ReplicatedTXID),
			"lag", lag)
		if !degradedLogged && degradedAfter > 0 && time.Since(startedAt) > degradedAfter {
			degradedLogged = true
			slog.Warn("Litestream sync exceeded degraded threshold; continuing until hard timeout",
				"elapsed", time.Since(startedAt).Round(time.Second),
				"degraded_after", degradedAfter,
				"hard_timeout", v.cfg.verifySyncTimeout())
		}
		select {
		case <-ctx.Done():
			syncErr := fmt.Errorf("sync retry wait: %w", ctx.Err())
			v.captureSyncFailureDiagnostics(result, syncErr)
			return syncErr
		case <-time.After(v.syncRetryDelay):
		}
	}
}

func syncLagNotResolvedError(cfg Config, txid, replicatedTXID uint64) error {
	err := fmt.Errorf("sync lag not resolved: txid=%016x replicated=%016x", txid, replicatedTXID)
	if snapshotUnderDiskPressure(collectDiskPressureRuntime(cfg)) {
		return fmt.Errorf("disk is full: no replication progress while data disk cannot stage current database snapshot: %w", err)
	}
	return err
}

func (v *Verifier) syncOnceDB(ctx context.Context, budget time.Duration, dbPath string) (syncResponse, error) {
	requestBody, err := json.Marshal(map[string]any{
		"path":    dbPath,
		"wait":    true,
		"timeout": timeoutSeconds(budget),
	})
	if err != nil {
		return syncResponse{}, fmt.Errorf("marshal sync request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "http://localhost/sync", bytes.NewReader(requestBody))
	if err != nil {
		return syncResponse{}, fmt.Errorf("create sync request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return syncResponse{}, fmt.Errorf("sync request: %w", err)
	}
	defer v.httpClient.CloseIdleConnections()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, truncated, readErr := readLimited(resp.Body, syncDiagnosticOutputLimit)
		if readErr != nil {
			return syncResponse{}, fmt.Errorf("sync returned %d: read response: %w", resp.StatusCode, readErr)
		}
		message := strings.TrimSpace(body)
		if truncated {
			message += " [truncated]"
		}
		if message == "" {
			message = resp.Status
		}
		return syncResponse{}, fmt.Errorf("sync returned %d: %s", resp.StatusCode, message)
	}

	body, truncated, readErr := readLimited(resp.Body, syncDiagnosticOutputLimit)
	if readErr != nil {
		return syncResponse{}, fmt.Errorf("read sync response: %w", readErr)
	}
	if truncated {
		return syncResponse{}, fmt.Errorf("sync response exceeded %d bytes", syncDiagnosticOutputLimit)
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return syncResponse{}, errors.New("sync response was empty")
	}
	var raw struct {
		Status         string  `json:"status"`
		TXID           *uint64 `json:"txid"`
		ReplicatedTXID *uint64 `json:"replicated_txid"`
	}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		return syncResponse{}, fmt.Errorf("decode sync response: %w", err)
	}
	if raw.TXID == nil || raw.ReplicatedTXID == nil {
		return syncResponse{}, errors.New("sync response missing txid fields")
	}
	return syncResponse{
		Status:         raw.Status,
		TXID:           *raw.TXID,
		ReplicatedTXID: *raw.ReplicatedTXID,
	}, nil
}

func (v *Verifier) captureSyncFailureDiagnostics(result *VerificationResult, syncErr error) {
	if result == nil {
		return
	}
	result.SyncStatusAfterSyncFailure = v.collectSyncStatus()
	if isSyncTimeout(syncErr) {
		result.LitestreamGoroutinesOnSyncFailure = v.collectLitestreamGoroutines()
	}
}

func (v *Verifier) collectSyncStatus() *reporting.LitestreamSyncStatus {
	capturedAt := time.Now().UTC()
	start := time.Now()
	diagnostic := &reporting.LitestreamSyncStatus{
		CapturedAt: capturedAt,
	}
	body, statusCode, truncated, err := v.getLitestreamDebug("http://localhost/debug/sync-status")
	diagnostic.DurationMS = int(time.Since(start).Milliseconds())
	diagnostic.StatusCode = statusCode
	diagnostic.Truncated = truncated
	if err != nil {
		diagnostic.Error = err.Error()
		diagnostic.Output = body
		return diagnostic
	}

	body = strings.TrimSpace(body)
	if body == "" {
		return diagnostic
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		diagnostic.Error = "decode sync status: " + err.Error()
		diagnostic.Output = body
		return diagnostic
	}
	diagnostic.Raw = raw
	diagnostic.Active = valueBool(raw["active"])
	diagnostic.Operation = valueString(raw["operation"])
	diagnostic.Phase = valueString(raw["phase"])
	diagnostic.ElapsedSeconds = valueFloat(raw["elapsed_seconds"])
	diagnostic.ExecutorWaiterCount = valueInt(raw["executor_waiter_count"])
	diagnostic.ExecutorWaitStartedAt = valueString(raw["executor_wait_started_at"])
	diagnostic.ExecutorWaitSeconds = valueFloat(raw["executor_wait_seconds"])
	return diagnostic
}

func (v *Verifier) collectLitestreamGoroutines() *reporting.LitestreamGoroutineSnapshot {
	capturedAt := time.Now().UTC()
	start := time.Now()
	body, statusCode, truncated, err := v.getLitestreamDebug("http://localhost/debug/pprof/goroutine?debug=2")
	diagnostic := &reporting.LitestreamGoroutineSnapshot{
		CapturedAt: capturedAt,
		DurationMS: int(time.Since(start).Milliseconds()),
		StatusCode: statusCode,
		Output:     body,
		Truncated:  truncated,
	}
	if err != nil {
		diagnostic.Error = err.Error()
	}
	return diagnostic
}

func (v *Verifier) getLitestreamDebug(url string) (string, int, bool, error) {
	client := newIPCClient(v.cfg.SocketPath, syncDiagnosticTimeout)
	defer client.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), syncDiagnosticTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", 0, false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, truncated, err := readLimited(resp.Body, syncDiagnosticOutputLimit)
	if err != nil {
		return body, resp.StatusCode, truncated, err
	}
	if resp.StatusCode != http.StatusOK {
		return body, resp.StatusCode, truncated, fmt.Errorf("debug endpoint returned %d", resp.StatusCode)
	}
	return body, resp.StatusCode, truncated, nil
}

func isSyncTimeout(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "context deadline exceeded") ||
		strings.Contains(text, "client.timeout exceeded") ||
		strings.Contains(text, "timeout")
}

func (v *Verifier) validate(ctx context.Context, txid uint64) (bool, error) {
	return v.validateDB(ctx, v.cfg.DBPath, v.cfg.DBPath+".restored", txid)
}

func (v *Verifier) validateDB(ctx context.Context, sourcePath, restoredPath string, txid uint64) (bool, error) {
	configPath, cleanupConfig, err := v.validateConfigPath(sourcePath)
	if err != nil {
		return false, err
	}
	defer cleanupConfig()

	args := []string{
		"validate",
		"-source-db", sourcePath,
		"-config", configPath,
		"-restored-db", restoredPath,
		"-check-type", v.cfg.VerifyType,
	}
	if txid > 0 {
		args = append(args, "-txid", formatTXID(txid))
	}

	cmd := exec.CommandContext(ctx, "litestream-test", args...)
	if v.cfg.ReplicaType == "s3" {
		cmd.Env = v.cfg.s3CommandEnv(v.cfg.S3FaultProxyEndpoint)
	}
	output, err := cmd.CombinedOutput()
	if err != nil && txid > 0 && validateUnsupportedTXID(output) {
		slog.Warn("litestream-test validate does not support -txid; retrying validation without pinned restore txid")
		return v.validateDB(ctx, sourcePath, restoredPath, 0)
	}

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

func (v *Verifier) validateConfigPath(sourcePath string) (string, func(), error) {
	if !v.needsPerDBValidateConfig(sourcePath) {
		return v.cfg.ConfigPath, func() {}, nil
	}

	path, err := v.writePerDBValidateConfig(sourcePath)
	if err != nil {
		return "", func() {}, err
	}
	return path, func() {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			slog.Warn("Failed to remove temporary validate config", "path", path, "error", err)
		}
	}, nil
}

func (v *Verifier) needsPerDBValidateConfig(sourcePath string) bool {
	return v.cfg.ManyDBEnabled() &&
		v.cfg.manyDBConfigMode() == "dir" &&
		strings.TrimSpace(sourcePath) != "" &&
		sourcePath != v.cfg.DBPath
}

func (v *Verifier) writePerDBValidateConfig(sourcePath string) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(v.cfg.ConfigPath), "litestream-validate-*.yml")
	if err != nil {
		return "", fmt.Errorf("create validate config: %w", err)
	}
	path := f.Name()

	manager := newLitestreamManager(&v.cfg)
	data := litestreamConfigData{
		SocketPath: v.cfg.SocketPath,
		Databases: []litestreamConfigDB{{
			Path:             sourcePath,
			SnapshotInterval: v.cfg.SnapshotInterval.String(),
			Replica:          manager.litestreamConfigReplica(sourcePath),
		}},
	}
	if err := litestreamConfigTmpl.Execute(f, data); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return "", fmt.Errorf("write validate config: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close validate config: %w", err)
	}
	return path, nil
}

func (r VerificationResult) restoreTXID() uint64 {
	if r.SyncReplicatedTXID > 0 {
		return r.SyncReplicatedTXID
	}
	return r.SyncTXID
}

func (c Config) verifySyncTimeout() time.Duration {
	if c.VerifySyncTimeout > 0 {
		return c.VerifySyncTimeout
	}
	return DefaultConfig().VerifySyncTimeout
}

func (c Config) verifySyncDegradedAfter() time.Duration {
	if c.VerifySyncDegradedAfter > 0 {
		return c.VerifySyncDegradedAfter
	}
	return DefaultConfig().VerifySyncDegradedAfter
}

func timeoutSeconds(budget time.Duration) int {
	seconds := int(budget / time.Second)
	if budget%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		return 1
	}
	return seconds
}

func formatTXID(txid uint64) string {
	if txid == 0 {
		return ""
	}
	return fmt.Sprintf("%016x", txid)
}

func validateUnsupportedTXID(output []byte) bool {
	return strings.Contains(string(output), "flag provided but not defined: -txid")
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
	if v.cfg.ManyDBEnabled() {
		for _, dbPath := range v.cfg.ManyDBPaths() {
			result.DBSizeBytes += fileSize(dbPath)
			result.WALSizeBytes += fileSize(dbPath + "-wal")
		}
		return
	}
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

func readLimited(reader io.Reader, limit int64) (string, bool, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return string(body), false, err
	}
	truncated := int64(len(body)) > limit
	if truncated {
		body = body[:limit]
	}
	return string(body), truncated, nil
}

func valueBool(value any) *bool {
	switch typed := value.(type) {
	case bool:
		return &typed
	case string:
		parsed, err := strconv.ParseBool(typed)
		if err == nil {
			return &parsed
		}
	}
	return nil
}

func valueString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case nil:
		return ""
	default:
		return fmt.Sprint(typed)
	}
}

func valueFloat(value any) *float64 {
	switch typed := value.(type) {
	case float64:
		return &typed
	case float32:
		value := float64(typed)
		return &value
	case int:
		value := float64(typed)
		return &value
	case int64:
		value := float64(typed)
		return &value
	case json.Number:
		value, err := typed.Float64()
		if err == nil {
			return &value
		}
	case string:
		value, err := strconv.ParseFloat(typed, 64)
		if err == nil {
			return &value
		}
	}
	return nil
}

func valueInt(value any) *int {
	switch typed := value.(type) {
	case int:
		return &typed
	case int64:
		value := int(typed)
		return &value
	case float64:
		value := int(typed)
		return &value
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			value := int(parsed)
			return &value
		}
	case string:
		parsed, err := strconv.Atoi(typed)
		if err == nil {
			return &parsed
		}
	}
	return nil
}
