package worker

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const pprofMaxCaptureBytes = 8 << 20
const pprofMaxEvidenceFiles = 256

type profileBinary struct {
	Revision  string `json:"revision,omitempty"`
	GoVersion string `json:"go_version,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
	Build     string `json:"build,omitempty"`
	Error     string `json:"error,omitempty"`
}

type profileUploadFailure struct {
	At      time.Time `json:"at"`
	Attempt uint64    `json:"attempt"`
	Stage   string    `json:"stage"`
	Error   string    `json:"error"`
}

type profileRecord struct {
	UploadFailureCount      uint64 `json:"upload_failure_count"`
	UploadFailuresDropped   uint64 `json:"upload_failures_dropped"`
	UploadHistoryIncomplete bool   `json:"upload_history_incomplete"`

	UploadAttempts uint64                 `json:"upload_attempts"`
	UploadFailures []profileUploadFailure `json:"upload_failures,omitempty"`

	DeploymentID      int                      `json:"deployment_id"`
	MachineID         string                   `json:"machine_id"`
	WorkerID          string                   `json:"worker_id"`
	EndpointAvailable bool                     `json:"endpoint_available"`
	ValidatorID       string                   `json:"validator_id"`
	ValidatorVersion  string                   `json:"validator_version"`
	WorkloadID        string                   `json:"workload_id"`
	WorkloadHash      string                   `json:"workload_hash"`
	WorkloadConfig    json.RawMessage          `json:"workload_config"`
	Sampling          string                   `json:"sampling,omitempty"`
	Phase             string                   `json:"phase"`
	Kind              string                   `json:"kind"`
	Artifact          string                   `json:"artifact"`
	Status            string                   `json:"status"`
	Error             string                   `json:"error,omitempty"`
	UploadError       string                   `json:"upload_error,omitempty"`
	Upload            string                   `json:"upload"`
	CapturedAt        time.Time                `json:"captured_at"`
	RunID             string                   `json:"run_id"`
	CandidateSHA      string                   `json:"candidate_sha"`
	WorkloadSHA       string                   `json:"workload_sha"`
	WorkerSHA         string                   `json:"worker_sha"`
	Workload          string                   `json:"workload"`
	LoadMode          string                   `json:"load_mode"`
	Image             string                   `json:"image"`
	Binaries          map[string]profileBinary `json:"binaries"`
}

func profileLabel(label string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		return '-'
	}, label)
}

func readProfileBinary(name string) profileBinary {
	filename, err := exec.LookPath(name)
	if err != nil {
		return profileBinary{Error: "binary unavailable"}
	}
	info, err := buildinfo.ReadFile(filename)
	if err != nil {
		return profileBinary{Error: "build information unavailable"}
	}
	result := profileBinary{Build: info.String(), GoVersion: info.GoVersion}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			result.Revision = setting.Value
		}
	}
	f, err := os.Open(filename)
	if err != nil {
		result.Error = "binary unreadable"
		return result
	}
	defer func() { _ = f.Close() }()
	digest := sha256.New()
	if _, err := io.Copy(digest, f); err != nil {
		result.Error = "binary digest unavailable"
		return result
	}
	result.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return result
}

func (c *pprofCapturer) newRecord(label, kind, artifact string) *profileRecord {
	c.identityOnce.Do(func() {
		c.binaries = make(map[string]profileBinary)
		for _, name := range []string{"litestream", "litestream-test", "soakworker"} {
			c.binaries[name] = readProfileBinary(name)
		}
	})
	upload := "local"
	if c.cfg.ReplicaType == "s3" {
		upload = "pending"
	}
	workloadConfig := c.cfg.WorkloadConfig()
	workloadHash := profileHash(workloadConfig.JSON())
	if workloadConfig.ReplayDataURL != "" {
		workloadConfig.ReplayDataURL = "redacted"
	}
	return &profileRecord{
		ValidatorID: "soak-verifier:" + c.cfg.GitSHA, ValidatorVersion: logicalValidatorVersion,
		DeploymentID: c.cfg.DeploymentID, MachineID: c.cfg.MachineID, WorkerID: c.cfg.WorkerID,
		WorkloadID: c.cfg.WorkloadID, WorkloadHash: workloadHash, WorkloadConfig: json.RawMessage(workloadConfig.JSON()),
		Phase: label, Kind: kind, Artifact: artifact, Status: "unavailable", Upload: upload,
		CapturedAt: time.Now().UTC(), RunID: c.cfg.RunID, CandidateSHA: c.cfg.LitestreamSHA,
		WorkloadSHA: c.cfg.WorkloadSHA, WorkerSHA: c.cfg.GitSHA,
		Workload: c.cfg.ProfileName, LoadMode: c.cfg.LoadMode, Image: c.cfg.ImageRef, Binaries: c.binaries,
	}
}

func (c *pprofCapturer) saveRecord(ctx context.Context, filename string, record *profileRecord) {
	body, err := json.Marshal(record)
	if err == nil {
		err = writeProfileJSON(filename, body)
	}
	if err != nil {
		slog.Warn("Persist pprof metadata failed", "error", err)
		return
	}
	slog.Info("Pprof capture result", "phase", record.Phase, "kind", record.Kind, "status", record.Status, "error", record.Error, "upload", record.Upload)
}

func (c *pprofCapturer) pending(filename string) bool {
	body, err := os.ReadFile(filename + ".json")
	if os.IsNotExist(err) {
		return c.cfg.ReplicaType == "s3"
	}
	var record profileRecord
	return err != nil || json.Unmarshal(body, &record) != nil || record.Upload == "pending"
}

func (c *pprofCapturer) retryPending(ctx context.Context, dir string) {
	c.retryPendingPhase(ctx, dir, "")
}

func (c *pprofCapturer) retryPendingPhase(ctx context.Context, dir, phase string) {
	if c.cfg.ReplicaType != "s3" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	if phase != "" {
		slices.Reverse(entries)
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		filename := filepath.Join(dir, entry.Name())
		body, err := os.ReadFile(filename)
		var record profileRecord
		if err != nil || json.Unmarshal(body, &record) != nil || record.Upload != "pending" || (phase != "" && record.Phase != phase) {
			continue
		}
		record.UploadAttempts++
		if record.Status == "available" {
			artifact := strings.TrimSuffix(filename, ".json")
			if err := c.upload(ctx, artifact, filepath.Base(artifact)); err != nil {
				record.addUploadFailure("artifact", err)
				c.saveRecord(ctx, filename, &record)
				c.recordStatus(record.Phase, "upload-failed")
				continue
			}
		}
		if err := c.uploadRecord(ctx, filename, &record); err != nil {
			record.addUploadFailure("manifest", err)
			c.saveRecord(ctx, filename, &record)
			c.recordStatus(record.Phase, "upload-failed")
			continue
		}
		record.Upload = "uploaded"
		record.UploadError = ""
		c.saveRecord(ctx, filename, &record)
	}
}

func (c *pprofCapturer) captureSpaceAvailable(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	if len(entries)+2 > pprofMaxEvidenceFiles {
		slog.Warn("Pprof capture unavailable: evidence storage full; retained uploads require retrieval", "limit", pprofMaxEvidenceFiles)
		return false
	}
	return true
}

func (r *Runner) startProfileCapture(ctx context.Context) func() {
	captureCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); r.profiles.Run(captureCtx) }()
	return func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		cancel()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			r.profiles.recordStatus("final", "cancelled-waiting")
			return
		}
		if !r.cfg.PprofCaptureEnabled {
			return
		}
		finalCtx, finalCancel := context.WithTimeout(shutdownCtx, 5*time.Second)
		r.profiles.captureSet(finalCtx, "final")
		finalCancel()
		r.profiles.retryPendingPhase(shutdownCtx, filepath.Join(r.cfg.DataDir, "profiles"), "final")
	}
}

func (r *Runner) triggerProfile(label string) {
	if r.profiles == nil {
		return
	}
	r.profiles.Trigger(label)
}

func writeProfileJSON(filename string, body []byte) error {
	if err := os.WriteFile(filename+".tmp", body, 0600); err != nil {
		return err
	}
	return os.Rename(filename+".tmp", filename)
}

type profileStatusEvent struct {
	At     time.Time `json:"at"`
	Phase  string    `json:"phase"`
	Reason string    `json:"reason"`
}

type profileStatus struct {
	Counts map[string]uint64    `json:"counts"`
	Recent []profileStatusEvent `json:"recent"`
}

func (c *pprofCapturer) recordStatus(phase, reason string) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	dir := filepath.Join(c.cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		slog.Warn("Persist profile status", "error", err)
		return
	}
	filename := filepath.Join(dir, "status.json")
	state := profileStatus{Counts: make(map[string]uint64)}
	if body, err := os.ReadFile(filename); err == nil {
		if err := json.Unmarshal(body, &state); err != nil {
			slog.Warn("Read profile status", "error", err)
		}
	}
	if state.Counts == nil {
		state.Counts = make(map[string]uint64)
	}
	state.Counts[reason]++
	state.Recent = append(state.Recent, profileStatusEvent{At: time.Now().UTC(), Phase: profileLabel(phase), Reason: reason})
	if len(state.Recent) > 64 {
		state.Recent = state.Recent[len(state.Recent)-64:]
	}
	body, err := json.Marshal(state)
	if err == nil {
		err = writeProfileJSON(filename, body)
	}
	if err != nil {
		slog.Warn("Persist profile status", "error", err)
	}
	slog.Info("Profile capture status", "phase", phase, "reason", reason)
}

func (c *pprofCapturer) runUploads(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.uploadWake:
		}
		c.retryPending(ctx, filepath.Join(c.cfg.DataDir, "profiles"))
	}
}

func (c *pprofCapturer) uploadRecord(ctx context.Context, filename string, record *profileRecord) error {
	delivery := *record
	delivery.Upload = "uploaded"
	delivery.UploadError = ""
	body, err := json.Marshal(delivery)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(filename), ".upload-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			slog.Warn("Remove upload staging metadata", "error", err)
		}
	}()
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return c.upload(ctx, f.Name(), filepath.Base(filename))
}

func (r *profileRecord) addUploadFailure(stage string, err error) {
	r.UploadError = err.Error()
	r.UploadFailureCount++
	r.UploadFailures = append(r.UploadFailures, profileUploadFailure{At: time.Now().UTC(), Attempt: r.UploadAttempts, Stage: stage, Error: r.UploadError})
	if len(r.UploadFailures) > 16 {
		r.UploadFailures = append(r.UploadFailures[:1], r.UploadFailures[len(r.UploadFailures)-15:]...)
		r.UploadFailuresDropped++
		r.UploadHistoryIncomplete = true
	}
}
