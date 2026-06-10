package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/corylanou/litestream-soak/internal/replay"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

type runtimeSnapshot struct {
	reporting.RuntimePayload
}

type Runner struct {
	cfg Config

	litestreamCmd   *exec.Cmd
	litestreamDone  chan struct{}
	litestreamErr   error
	litestreamExit  *reporting.ProcessExitSnapshot
	litestreamLog   *lineBuffer
	litestreamMu    sync.Mutex
	failureDebugMu  sync.Mutex
	failureDebugKey string
	failureDebugAt  time.Time
	loadSup         *loadSupervisor
	replayEngine    *replay.Engine
	loadLog         *lineBuffer
	verifier        *Verifier
	reporter        *Reporter
	snapshotMu      sync.Mutex
	snapshot        runtimeSnapshot
	lastLocalPoll   time.Time
}

func NewRunner(cfg Config) *Runner {
	return &Runner{
		cfg:           cfg,
		litestreamLog: newLineBuffer(120),
		loadLog:       newLineBuffer(120),
		snapshot: runtimeSnapshot{
			RuntimePayload: reporting.RuntimePayload{
				DBStatus:                "unknown",
				LitestreamSnapshotError: "litestream stats not collected yet",
			},
		},
	}
}

func (r *Runner) Run(ctx context.Context) error {
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)

	SetWorkerInfo(r.cfg)
	startTime := time.Now()
	r.reporter = NewReporter(r.cfg)

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				uptime := time.Since(startTime).Seconds()
				SetUptime(uptime)
				r.setUptime(uptime)
				r.pollDBStats()
				r.sendHeartbeat(runCtx)
			}
		}
	}()

	if err := r.populate(runCtx); err != nil {
		return fmt.Errorf("populate: %w", err)
	}

	if err := r.writeLitestreamConfig(); err != nil {
		return fmt.Errorf("write litestream config: %w", err)
	}

	if err := r.startLitestream(runCtx); err != nil {
		return fmt.Errorf("start litestream: %w", err)
	}
	defer r.stopLitestream()
	r.monitorLitestream(runCtx, cancelRun)

	if err := r.waitForFirstSync(runCtx); err != nil {
		return fmt.Errorf("wait for first sync: %w", err)
	}

	if r.cfg.LoadMode == "synthetic" || r.cfg.LoadMode == "both" {
		if err := r.startLoad(runCtx); err != nil {
			return fmt.Errorf("start load: %w", err)
		}
		defer r.stopLoad()
	}

	if r.cfg.LoadMode == "replay" || r.cfg.LoadMode == "both" {
		if err := r.startReplay(runCtx); err != nil {
			return fmt.Errorf("start replay: %w", err)
		}
	}

	var pausers []loadPauser
	if r.loadSup != nil {
		pausers = append(pausers, r.loadSup)
	}
	if r.replayEngine != nil {
		pausers = append(pausers, r.replayEngine)
	}
	r.verifier = NewVerifier(r.cfg, pausers...)
	r.verifier.SetStartHook(r.sendVerificationStarted)

	if err := r.runVerifyLoop(runCtx); err != nil {
		r.sendWorkerFailureEvent(err)
		return err
	}
	return nil
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
	r.litestreamCmd.Stdout = io.MultiWriter(os.Stdout, r.litestreamLog)
	r.litestreamCmd.Stderr = io.MultiWriter(os.Stderr, r.litestreamLog)

	if r.cfg.ReplicaType == "s3" {
		r.litestreamCmd.Env = append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+r.cfg.S3AccessKey,
			"AWS_SECRET_ACCESS_KEY="+r.cfg.S3SecretKey,
		)
	}

	if err := r.litestreamCmd.Start(); err != nil {
		return err
	}

	done := make(chan struct{})
	r.litestreamMu.Lock()
	r.litestreamDone = done
	r.litestreamErr = nil
	r.litestreamMu.Unlock()

	go func(cmd *exec.Cmd) {
		err := cmd.Wait()
		r.litestreamMu.Lock()
		r.litestreamErr = err
		r.litestreamExit = processExitSnapshot("litestream", time.Now().UTC(), err)
		r.litestreamMu.Unlock()
		close(done)
	}(r.litestreamCmd)

	return nil
}

func (r *Runner) stopLitestream() {
	if r.litestreamCmd == nil || r.litestreamCmd.Process == nil {
		return
	}
	slog.Info("Stopping Litestream")
	if err := r.litestreamCmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		slog.Warn("Failed to interrupt Litestream", "error", err)
	}
	if done := r.litestreamDoneChan(); done != nil {
		<-done
	}
}

func (r *Runner) monitorLitestream(ctx context.Context, cancel context.CancelCauseFunc) {
	done := r.litestreamDoneChan()
	if done == nil {
		return
	}

	go func() {
		<-done
		if ctx.Err() != nil {
			return
		}
		if err := r.litestreamExitError(); err != nil {
			cancel(fmt.Errorf("litestream exited unexpectedly: %w", err))
			return
		}
		cancel(errors.New("litestream exited unexpectedly"))
	}()
}

func (r *Runner) litestreamDoneChan() <-chan struct{} {
	r.litestreamMu.Lock()
	defer r.litestreamMu.Unlock()
	return r.litestreamDone
}

func (r *Runner) litestreamExitError() error {
	r.litestreamMu.Lock()
	defer r.litestreamMu.Unlock()
	return r.litestreamErr
}

func (r *Runner) waitForFirstSync(ctx context.Context) error {
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

	r.loadSup = newLoadSupervisor(func(ctx context.Context) (*exec.Cmd, error) {
		cmd := exec.CommandContext(ctx, "litestream-test", "load",
			"-db", r.cfg.DBPath,
			"-write-rate", strconv.Itoa(r.cfg.WriteRate),
			"-duration", r.cfg.LoadDuration.String(),
			"-pattern", r.cfg.Pattern,
			"-payload-size", strconv.Itoa(r.cfg.PayloadSize),
			"-read-ratio", fmt.Sprintf("%.2f", r.cfg.ReadRatio),
			"-workers", strconv.Itoa(r.cfg.Workers),
		)
		cmd.Stdout = io.MultiWriter(os.Stdout, r.loadLog)
		cmd.Stderr = io.MultiWriter(os.Stderr, r.loadLog)

		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("start load: %w", err)
		}
		return cmd, nil
	})

	if err := r.loadSup.start(ctx); err != nil {
		return err
	}

	r.sendHeartbeat(ctx)
	return nil
}

func (r *Runner) startReplay(ctx context.Context) error {
	if r.cfg.ReplayDataset == "" {
		return fmt.Errorf("REPLAY_DATASET is required for replay mode")
	}
	if err := r.prepareReplayData(ctx); err != nil {
		return fmt.Errorf("prepare replay data: %w", err)
	}
	if r.cfg.ReplayDataPath == "" {
		return fmt.Errorf("REPLAY_DATA_PATH or REPLAY_DATA_URL is required for replay mode")
	}

	var adapter replay.Adapter
	switch r.cfg.ReplayDataset {
	case "taxi":
		adapter = replay.NewTaxiAdapter(r.cfg.ReplayDataPath)
	case "gharchive":
		adapter = replay.NewGHArchiveAdapter(r.cfg.ReplayDataPath)
	case "orders":
		adapter = replay.NewOrdersAdapter(r.cfg.ReplayDataPath)
	default:
		return fmt.Errorf("unknown replay dataset: %s", r.cfg.ReplayDataset)
	}

	engine := replay.NewEngine(replay.Config{
		Dataset:         r.cfg.ReplayDataset,
		DataPath:        r.cfg.ReplayDataPath,
		DBPath:          r.cfg.DBPath,
		SpeedMultiplier: r.cfg.ReplaySpeed,
		Loop:            r.cfg.ReplayLoop,
		WorkerID:        r.cfg.WorkerID,
		ProfileName:     r.cfg.ProfileName,
		Source:          r.cfg.Source,
	}, adapter)

	r.replayEngine = engine

	go superviseReplay(ctx, r.cfg.ReplayDataset, r.cfg.ReplayLoop,
		5*time.Second, 2*time.Minute, 5*time.Minute, engine.Run)

	slog.Info("Replay engine started", "dataset", r.cfg.ReplayDataset, "speed", r.cfg.ReplaySpeed)
	return nil
}

func superviseReplay(ctx context.Context, dataset string, loop bool, initialBackoff, maxBackoff, healthyReset time.Duration, run func(context.Context) error) {
	backoff := initialBackoff
	for {
		started := time.Now()
		err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil && !loop {
			slog.Info("Replay complete", "dataset", dataset)
			return
		}
		if err != nil {
			slog.Error("Replay engine failed", "dataset", dataset, "error", err)
		} else {
			slog.Warn("Replay engine exited unexpectedly", "dataset", dataset)
		}
		if time.Since(started) >= healthyReset {
			backoff = initialBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		IncLoadRestart("replay")
		slog.Info("Restarting replay engine", "dataset", dataset)
	}
}

func (r *Runner) prepareReplayData(ctx context.Context) error {
	if strings.TrimSpace(r.cfg.ReplayDataPath) != "" {
		if _, err := os.Stat(r.cfg.ReplayDataPath); err == nil {
			return nil
		}
		if strings.TrimSpace(r.cfg.ReplayDataURL) == "" {
			return fmt.Errorf("replay data path %s not found", r.cfg.ReplayDataPath)
		}
	}
	if strings.TrimSpace(r.cfg.ReplayDataURL) == "" {
		return nil
	}

	dataURL, err := url.Parse(r.cfg.ReplayDataURL)
	if err != nil {
		return fmt.Errorf("parse replay data url: %w", err)
	}

	name := filepath.Base(dataURL.Path)
	if name == "." || name == "/" || name == "" {
		return fmt.Errorf("replay data url must include a file name")
	}

	targetDir := filepath.Join(r.cfg.DataDir, "datasets")
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create replay dataset dir: %w", err)
	}

	targetPath := filepath.Join(targetDir, name)
	if _, err := os.Stat(targetPath); err == nil {
		r.cfg.ReplayDataPath = targetPath
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.cfg.ReplayDataURL, nil)
	if err != nil {
		return fmt.Errorf("create replay data request: %w", err)
	}

	slog.Info("Downloading replay dataset", "dataset", r.cfg.ReplayDataset, "url", r.cfg.ReplayDataURL, "target", targetPath)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download replay data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download replay data returned %d", resp.StatusCode)
	}

	tmpPath := targetPath + ".download"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create replay dataset file: %w", err)
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write replay dataset: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close replay dataset: %w", err)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("activate replay dataset: %w", err)
	}

	r.cfg.ReplayDataPath = targetPath
	return nil
}

func (r *Runner) stopLoad() {
	if r.loadSup == nil {
		return
	}
	slog.Info("Stopping load generator")
	r.loadSup.Stop()
}

func (r *Runner) pollDBStats() {
	if info, err := os.Stat(r.cfg.DBPath); err == nil {
		SetDBSize(info.Size())
		r.setDBSize(info.Size())
	}
	if info, err := os.Stat(r.cfg.DBPath + "-wal"); err == nil {
		SetWALSize(info.Size())
		r.setWALSize(info.Size())
	}
	r.pollDataDiskStats()
	if time.Since(r.lastLocalPoll) >= time.Minute {
		r.lastLocalPoll = time.Now()
		r.pollLitestreamLocalState()
	}

	client := r.ipcClient()
	defer client.CloseIdleConnections()
	snapshot, err := r.collectLitestreamRuntime(client, time.Now().UTC())
	if err != nil {
		r.setLitestreamSnapshotFailure(time.Now().UTC(), err)
		return
	}
	r.setLitestreamSnapshot(snapshot)
}

func (r *Runner) ipcClient() *http.Client {
	return newIPCClient(r.cfg.SocketPath, 5*time.Second)
}

func newIPCClient(socketPath string, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socketPath)
			},
		},
		Timeout: timeout,
	}
}

func (r *Runner) collectLitestreamRuntime(client *http.Client, collectedAt time.Time) (reporting.RuntimePayload, error) {
	txid, err := r.pollTXID(client)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}
	uptimeSeconds, err := r.pollInfo(client)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}
	dbStatus, lastSyncAgeSeconds, err := r.pollList(client)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}

	return reporting.RuntimePayload{
		DBTXID:                    txid,
		DBStatus:                  dbStatus,
		LastSyncAgeSeconds:        lastSyncAgeSeconds,
		LitestreamUptimeSeconds:   uptimeSeconds,
		SnapshotCollectedAt:       collectedAt,
		LitestreamSnapshotHealthy: true,
	}, nil
}

func (r *Runner) pollTXID(client *http.Client) (uint64, error) {
	resp, err := client.Get("http://localhost/txid?path=" + r.cfg.DBPath)
	if err != nil {
		return 0, fmt.Errorf("read txid: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		TXID uint64 `json:"txid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode txid: %w", err)
	}
	return result.TXID, nil
}

func (r *Runner) pollInfo(client *http.Client) (float64, error) {
	resp, err := client.Get("http://localhost/info")
	if err != nil {
		return 0, fmt.Errorf("read info: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		UptimeSeconds int64 `json:"uptime_seconds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode info: %w", err)
	}
	return float64(result.UptimeSeconds), nil
}

func (r *Runner) pollList(client *http.Client) (string, float64, error) {
	resp, err := client.Get("http://localhost/list")
	if err != nil {
		return "", 0, fmt.Errorf("read database list: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Databases []struct {
			Status         string     `json:"status"`
			TXID           *uint64    `json:"txid"`
			ReplicatedTXID *uint64    `json:"replicated_txid"`
			LastSyncAt     *time.Time `json:"last_sync_at,omitempty"`
		} `json:"databases"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, fmt.Errorf("decode database list: %w", err)
	}
	if len(result.Databases) == 0 {
		return "", 0, errors.New("database list was empty")
	}

	db := result.Databases[0]
	if db.TXID != nil && db.ReplicatedTXID != nil {
		lag := uint64(0)
		if *db.TXID > *db.ReplicatedTXID {
			lag = *db.TXID - *db.ReplicatedTXID
		}
		SetReplicatedTXID(float64(*db.ReplicatedTXID))
		SetReplicationLag(float64(lag))
	}
	age := 0.0
	if db.LastSyncAt != nil {
		age = time.Since(*db.LastSyncAt).Seconds()
	}
	return db.Status, age, nil
}

func (r *Runner) runVerifyLoop(ctx context.Context) error {
	slog.Info("Starting verification loop", "interval", r.cfg.VerifyInterval)

	ticker := time.NewTicker(r.cfg.VerifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Verification loop stopped")
			if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
				return cause
			}
			return nil
		case <-ticker.C:
			result, err := r.verifier.RunCycle(ctx)
			r.sendVerification(context.WithoutCancel(ctx), result)
			if err != nil {
				slog.Error("Verification cycle error", "error", err)
			}
			switch result.Status {
			case "failed":
				slog.Error("VERIFICATION FAILED — replication integrity compromised")
			case "aborted":
				slog.Warn("Verification aborted", "cause", context.Cause(ctx))
			}
		}
	}
}

func (r *Runner) sendHeartbeat(ctx context.Context) {
	if r.reporter == nil || !r.reporter.Enabled() {
		return
	}

	snapshot := r.currentSnapshot()
	reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.reporter.SendHeartbeat(reportCtx, reporting.HeartbeatPayload{
		SentAt:         time.Now().UTC(),
		RuntimePayload: snapshot.RuntimePayload,
	}); err != nil {
		slog.Warn("Failed to send heartbeat", "error", err)
	}
}

func (r *Runner) sendVerificationStarted(ctx context.Context, result VerificationResult) {
	if r.reporter == nil || !r.reporter.Enabled() {
		return
	}

	startedAt := result.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	snapshot := r.currentSnapshot()
	reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.reporter.SendEvent(reportCtx, reporting.WorkerEventPayload{
		EventType: "verification_started",
		Message:   "verification started",
		SentAt:    startedAt,
		ActiveVerification: &reporting.ActiveVerification{
			StartedAt: startedAt,
			CheckType: result.CheckType,
			Status:    result.Status,
		},
		RuntimePayload: snapshot.RuntimePayload,
	}); err != nil {
		slog.Warn("Failed to send verification start event", "error", err)
	}
}

func (r *Runner) sendVerification(ctx context.Context, result VerificationResult) {
	if r.reporter == nil || !r.reporter.Enabled() {
		return
	}

	snapshot := r.currentSnapshot()
	var failureDebug *reporting.FailureDebugSnapshot
	switch {
	case result.Status == "aborted":
	case !result.Passed:
		failureDebug = r.captureFailureDebugSnapshotIfDue(result)
	default:
		r.resetFailureDebugState()
	}
	reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.reporter.SendVerification(reportCtx, reporting.VerificationPayload{
		StartedAt:             result.StartedAt,
		CompletedAt:           result.CompletedAt,
		CheckType:             result.CheckType,
		Status:                result.Status,
		Passed:                result.Passed,
		Summary:               result.Summary,
		ErrorMessage:          result.ErrorMessage,
		DurationMS:            result.DurationMS,
		Steps:                 result.Steps,
		FailureClassification: r.failureClassification(result),
		FailureDebug:          failureDebug,
		RuntimePayload:        snapshot.RuntimePayload,
	}); err != nil {
		slog.Warn("Failed to send verification report", "error", err)
	}
}

func (r *Runner) failureClassification(result VerificationResult) *reporting.FailureClassification {
	if result.Passed || result.Status == "aborted" {
		return nil
	}
	classification := reporting.ClassifyVerificationFailure(result.CheckType, result.ErrorMessage)
	if classification.ObjectStore != nil {
		classification.ObjectStore.Bucket = firstNonEmpty(classification.ObjectStore.Bucket, r.cfg.S3Bucket)
		classification.ObjectStore.Prefix = firstNonEmpty(classification.ObjectStore.Prefix, strings.Trim(strings.TrimPrefix(r.cfg.S3Path, "/"), "/"))
		classification.ObjectStore.RedactedPrefix = reporting.RedactObjectPrefix(classification.ObjectStore.Prefix)
	}
	return &classification
}

func (r *Runner) captureFailureDebugSnapshotIfDue(result VerificationResult) *reporting.FailureDebugSnapshot {
	reason := firstNonEmpty(result.Summary, summarizeVerificationMessage(result.ErrorMessage), result.Status, "verification_failed")
	key := failureDebugKey(result, reason)
	now := time.Now().UTC()

	r.failureDebugMu.Lock()
	defer r.failureDebugMu.Unlock()
	if r.failureDebugKey == key && now.Sub(r.failureDebugAt) < 6*time.Hour {
		return nil
	}
	r.failureDebugKey = key
	r.failureDebugAt = now
	snapshot := r.captureFailureDebugSnapshot(reason, result.Steps, r.failureClassification(result), result.restoreTXID())
	if snapshot != nil {
		snapshot.SyncStatusBeforeSync = result.SyncStatusBeforeSync
		snapshot.SyncStatusAfterSyncFailure = result.SyncStatusAfterSyncFailure
		snapshot.LitestreamGoroutinesOnSyncFailure = result.LitestreamGoroutinesOnSyncFailure
	}
	return snapshot
}

func failureDebugKey(result VerificationResult, reason string) string {
	classification := reporting.ClassifyVerificationFailure(result.CheckType, result.ErrorMessage)
	if classification.Signature != "" {
		return classification.Signature
	}
	text := strings.ToLower(result.Status + " " + result.Summary + " " + result.ErrorMessage + " " + reason)
	switch {
	case strings.Contains(text, "too many open files"):
		return "sync_fd_exhausted"
	case strings.Contains(text, "litestream.sock") && strings.Contains(text, "connection refused"):
		return "sync_socket_refused"
	case strings.Contains(text, "no space left on device") || strings.Contains(text, "database or disk is full") || strings.Contains(text, "disk is full"):
		return "disk_full"
	case strings.Contains(text, "wait for sync") || strings.Contains(text, "sync request"):
		return "sync_failure"
	case strings.Contains(text, "accessdenied") || strings.Contains(text, "403"):
		return "object_storage_access_denied"
	case strings.Contains(text, "408") || strings.Contains(text, "timeout"):
		return "timeout"
	case strings.Contains(text, "sqlite_index_mismatch"):
		return "sqlite_index_mismatch"
	case strings.Contains(text, "validation failed"):
		return "validation_failed"
	default:
		return firstNonEmpty(result.CheckType, result.Status, "verification_failed")
	}
}

func (r *Runner) resetFailureDebugState() {
	r.failureDebugMu.Lock()
	defer r.failureDebugMu.Unlock()
	r.failureDebugKey = ""
	r.failureDebugAt = time.Time{}
}

func (r *Runner) sendWorkerFailureEvent(err error) {
	if r.reporter == nil || !r.reporter.Enabled() {
		return
	}

	snapshot := r.currentSnapshot()
	message := err.Error()
	failureDebug := r.captureFailureDebugSnapshot(message, nil, nil, 0)
	reportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if sendErr := r.reporter.SendEvent(reportCtx, reporting.WorkerEventPayload{
		EventType:      "worker_failed",
		Message:        message,
		SentAt:         time.Now().UTC(),
		FailureDebug:   failureDebug,
		RuntimePayload: snapshot.RuntimePayload,
	}); sendErr != nil {
		slog.Warn("Failed to send worker failure event", "error", sendErr)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (r *Runner) currentSnapshot() runtimeSnapshot {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	return r.snapshot
}

func (r *Runner) setUptime(seconds float64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.RuntimePayload.UptimeSeconds = seconds
}

func (r *Runner) setDBSize(bytes int64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.RuntimePayload.DBSizeBytes = bytes
}

func (r *Runner) setWALSize(bytes int64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.RuntimePayload.WALSizeBytes = bytes
}

func (r *Runner) pollDataDiskStats() {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(r.cfg.DataDir, &stat); err != nil {
		return
	}

	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	used := total - free
	usedPercent := 0.0
	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}

	SetDataDiskStats(total, used, free, usedPercent)
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.RuntimePayload.DataDiskTotalBytes = total
	r.snapshot.RuntimePayload.DataDiskUsedBytes = used
	r.snapshot.RuntimePayload.DataDiskFreeBytes = free
	r.snapshot.RuntimePayload.DataDiskAvailableBytes = available
	r.snapshot.RuntimePayload.DataDiskUsedPercent = usedPercent
}

func (r *Runner) pollLitestreamLocalState() {
	stateDir := litestreamStateDir(r.cfg.DBPath)
	dirBytes := directorySize(stateDir)
	ltxBytes := directorySize(filepath.Join(stateDir, "ltx"))

	SetLitestreamLocalStateSize(dirBytes, ltxBytes)
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.RuntimePayload.LitestreamDirSizeBytes = dirBytes
	r.snapshot.RuntimePayload.LitestreamLTXSizeBytes = ltxBytes
}

func litestreamStateDir(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "."+filepath.Base(dbPath)+"-litestream")
}

func directorySize(path string) int64 {
	var total int64
	_ = filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

func (r *Runner) setLitestreamSnapshot(snapshot reporting.RuntimePayload) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.RuntimePayload.DBTXID = snapshot.DBTXID
	r.snapshot.RuntimePayload.DBStatus = snapshot.DBStatus
	r.snapshot.RuntimePayload.LastSyncAgeSeconds = snapshot.LastSyncAgeSeconds
	r.snapshot.RuntimePayload.LitestreamUptimeSeconds = snapshot.LitestreamUptimeSeconds
	r.snapshot.RuntimePayload.SnapshotCollectedAt = snapshot.SnapshotCollectedAt
	r.snapshot.RuntimePayload.LitestreamSnapshotHealthy = snapshot.LitestreamSnapshotHealthy
	r.snapshot.RuntimePayload.LitestreamSnapshotError = snapshot.LitestreamSnapshotError
	SetDBTXID(float64(snapshot.DBTXID))
	SetDBStatus(snapshot.DBStatus)
	SetLastSyncAge(snapshot.LastSyncAgeSeconds)
	SetLitestreamUptime(snapshot.LitestreamUptimeSeconds)
	SetLitestreamSnapshotHealthy(snapshot.LitestreamSnapshotHealthy)
}

func (r *Runner) setLitestreamSnapshotFailure(collectedAt time.Time, err error) {
	r.setLitestreamSnapshot(reporting.RuntimePayload{
		DBTXID:                    0,
		DBStatus:                  "unknown",
		LastSyncAgeSeconds:        0,
		LitestreamUptimeSeconds:   0,
		SnapshotCollectedAt:       collectedAt,
		LitestreamSnapshotHealthy: false,
		LitestreamSnapshotError:   err.Error(),
	})
}
