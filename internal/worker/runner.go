package worker

import (
	"context"
	"encoding/json"
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
	"text/template"
	"time"

	"github.com/corylanou/litestream-soak/internal/replay"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

type runtimeSnapshot struct {
	UptimeSeconds           float64
	DBSizeBytes             int64
	WALSizeBytes            int64
	DBTXID                  uint64
	DBStatus                string
	LastSyncAgeSeconds      float64
	LitestreamUptimeSeconds float64
}

type Runner struct {
	cfg Config

	litestreamCmd *exec.Cmd
	loadCmd       *exec.Cmd
	verifier      *Verifier
	reporter      *Reporter
	snapshotMu    sync.Mutex
	snapshot      runtimeSnapshot
}

func NewRunner(cfg Config) *Runner {
	return &Runner{cfg: cfg}
}

func (r *Runner) Run(ctx context.Context) error {
	SetWorkerInfo(r.cfg)
	startTime := time.Now()
	r.reporter = NewReporter(r.cfg)

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				uptime := time.Since(startTime).Seconds()
				SetUptime(uptime)
				r.setUptime(uptime)
				r.pollDBStats()
				r.sendHeartbeat(ctx)
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

	if r.cfg.LoadMode == "synthetic" || r.cfg.LoadMode == "both" {
		if err := r.startLoad(ctx); err != nil {
			return fmt.Errorf("start load: %w", err)
		}
		defer r.stopLoad()
	}

	if r.cfg.LoadMode == "replay" || r.cfg.LoadMode == "both" {
		if err := r.startReplay(ctx); err != nil {
			return fmt.Errorf("start replay: %w", err)
		}
	}

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

	go func() {
		if err := engine.Run(ctx); err != nil {
			slog.Error("Replay engine failed", "dataset", r.cfg.ReplayDataset, "error", err)
		}
	}()

	slog.Info("Replay engine started", "dataset", r.cfg.ReplayDataset, "speed", r.cfg.ReplaySpeed)
	return nil
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
	if r.loadCmd == nil || r.loadCmd.Process == nil {
		return
	}
	slog.Info("Stopping load generator")
	r.loadCmd.Process.Signal(os.Interrupt)
	r.loadCmd.Wait()
	SetLoadRunning(false)
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

	client := r.ipcClient()

	r.pollTXID(client)
	r.pollInfo(client)
	r.pollList(client)
}

func (r *Runner) ipcClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", r.cfg.SocketPath)
			},
		},
		Timeout: 5 * time.Second,
	}
}

func (r *Runner) pollTXID(client *http.Client) {
	resp, err := client.Get("http://localhost/txid?path=" + r.cfg.DBPath)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var result struct {
		TXID uint64 `json:"txid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return
	}
	SetDBTXID(float64(result.TXID))
	r.setDBTXID(result.TXID)
}

func (r *Runner) pollInfo(client *http.Client) {
	resp, err := client.Get("http://localhost/info")
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var result struct {
		UptimeSeconds int64 `json:"uptime_seconds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return
	}
	SetLitestreamUptime(float64(result.UptimeSeconds))
	r.setLitestreamUptime(float64(result.UptimeSeconds))
}

func (r *Runner) pollList(client *http.Client) {
	resp, err := client.Get("http://localhost/list")
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var result struct {
		Databases []struct {
			Status     string     `json:"status"`
			LastSyncAt *time.Time `json:"last_sync_at,omitempty"`
		} `json:"databases"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return
	}
	for _, db := range result.Databases {
		SetDBStatus(db.Status)
		r.setDBStatus(db.Status)
		if db.LastSyncAt != nil {
			age := time.Since(*db.LastSyncAt).Seconds()
			SetLastSyncAge(age)
			r.setLastSyncAge(age)
		}
	}
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
			result, err := r.verifier.RunCycle(ctx)
			r.sendVerification(ctx, result)
			if err != nil {
				slog.Error("Verification cycle error", "error", err)
			}
			if !result.Passed {
				slog.Error("VERIFICATION FAILED — replication integrity compromised")
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
		SentAt:                  time.Now().UTC(),
		UptimeSeconds:           snapshot.UptimeSeconds,
		DBSizeBytes:             snapshot.DBSizeBytes,
		WALSizeBytes:            snapshot.WALSizeBytes,
		DBTXID:                  snapshot.DBTXID,
		DBStatus:                snapshot.DBStatus,
		LastSyncAgeSeconds:      snapshot.LastSyncAgeSeconds,
		LitestreamUptimeSeconds: snapshot.LitestreamUptimeSeconds,
	}); err != nil {
		slog.Warn("Failed to send heartbeat", "error", err)
	}
}

func (r *Runner) sendVerification(ctx context.Context, result VerificationResult) {
	if r.reporter == nil || !r.reporter.Enabled() {
		return
	}

	snapshot := r.currentSnapshot()
	reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.reporter.SendVerification(reportCtx, reporting.VerificationPayload{
		StartedAt:               result.StartedAt,
		CompletedAt:             result.CompletedAt,
		CheckType:               result.CheckType,
		Status:                  result.Status,
		Passed:                  result.Passed,
		Summary:                 result.Summary,
		ErrorMessage:            result.ErrorMessage,
		DurationMS:              result.DurationMS,
		DBSizeBytes:             result.DBSizeBytes,
		WALSizeBytes:            result.WALSizeBytes,
		DBTXID:                  snapshot.DBTXID,
		DBStatus:                snapshot.DBStatus,
		LastSyncAgeSeconds:      snapshot.LastSyncAgeSeconds,
		LitestreamUptimeSeconds: snapshot.LitestreamUptimeSeconds,
	}); err != nil {
		slog.Warn("Failed to send verification report", "error", err)
	}
}

func (r *Runner) currentSnapshot() runtimeSnapshot {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	return r.snapshot
}

func (r *Runner) setUptime(seconds float64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.UptimeSeconds = seconds
}

func (r *Runner) setDBSize(bytes int64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.DBSizeBytes = bytes
}

func (r *Runner) setWALSize(bytes int64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.WALSizeBytes = bytes
}

func (r *Runner) setDBTXID(txid uint64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.DBTXID = txid
}

func (r *Runner) setDBStatus(status string) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.DBStatus = status
}

func (r *Runner) setLastSyncAge(seconds float64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.LastSyncAgeSeconds = seconds
}

func (r *Runner) setLitestreamUptime(seconds float64) {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()
	r.snapshot.LitestreamUptimeSeconds = seconds
}
