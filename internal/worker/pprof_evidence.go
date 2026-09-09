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

type profileRecord struct {
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
	if c.cfg.ReplicaType != "s3" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
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
		if err != nil || json.Unmarshal(body, &record) != nil || record.Upload != "pending" {
			continue
		}
		if record.Status == "available" {
			artifact := strings.TrimSuffix(filename, ".json")
			if err := c.upload(ctx, artifact, filepath.Base(artifact)); err != nil {
				c.recordStatus(record.Phase, "upload-failed")
				continue
			}
		}
		if err := c.upload(ctx, filename, filepath.Base(filename)); err != nil {
			c.recordStatus(record.Phase, "upload-failed")
			continue
		}
		record.Upload = "uploaded"
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
		cancel()
		<-done
		if !r.cfg.PprofCaptureEnabled {
			return
		}
		finalCtx, finalCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer finalCancel()
		r.profiles.captureSet(finalCtx, "final")
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
