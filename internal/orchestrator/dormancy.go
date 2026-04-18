package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

type DormancyPolicy struct {
	Threshold     time.Duration
	CheckInterval time.Duration
	MinFailures   int
}

type dormancyCandidate struct {
	Signature string
	Since     time.Time
	Count     int
}

func normalizeWorkloadConfig(cfg workload.Config) workload.Config {
	if cfg.InitialSize == "" {
		cfg.InitialSize = "5MB"
	}
	if cfg.VerifyInterval == "" {
		cfg.VerifyInterval = "30m"
	}
	if cfg.SnapshotInterval == "" {
		cfg.SnapshotInterval = "10m"
	}
	if cfg.SyncInterval == "" {
		cfg.SyncInterval = "1s"
	}
	if cfg.LoadMode == "" {
		cfg.LoadMode = "synthetic"
	}
	if cfg.CPUs == 0 {
		cfg.CPUs = 1
	}
	if cfg.MemoryMB == 0 {
		cfg.MemoryMB = 1024
	}
	return cfg
}

func (m *Manager) workerEnv(worker model.Worker, workloadCfg workload.Config) map[string]string {
	s3Path := fmt.Sprintf("soak/%s", worker.Name)
	env := map[string]string{
		"WORKER_ID":           worker.ID,
		"WORKER_NAME":         worker.Name,
		"GIT_SHA":             worker.GitSHA,
		"SOURCE":              worker.Source,
		"PROFILE":             worker.ProfileName,
		"INITIAL_SIZE":        workloadCfg.InitialSize,
		"VERIFY_INTERVAL":     workloadCfg.VerifyInterval,
		"VERIFY_TYPE":         workloadCfg.VerifyType,
		"SNAPSHOT_INTERVAL":   workloadCfg.SnapshotInterval,
		"SYNC_INTERVAL":       workloadCfg.SyncInterval,
		"LOAD_MODE":           workloadCfg.LoadMode,
		"REPLICA_TYPE":        "s3",
		"S3_BUCKET":           m.replica.Bucket,
		"BUCKET_NAME":         m.replica.Bucket,
		"S3_PATH":             s3Path,
		"S3_ENDPOINT":         m.replica.Endpoint,
		"AWS_ENDPOINT_URL_S3": m.replica.Endpoint,
		"CONTROL_BASE_URL":    m.controlBaseURL,
	}
	if m.replica.AccessKey != "" {
		env["AWS_ACCESS_KEY_ID"] = m.replica.AccessKey
	}
	if m.replica.SecretKey != "" {
		env["AWS_SECRET_ACCESS_KEY"] = m.replica.SecretKey
	}
	if m.replica.Region != "" {
		env["AWS_REGION"] = m.replica.Region
	}
	if strings.TrimSpace(worker.LitestreamSHA) != "" {
		env["LITESTREAM_SHA"] = worker.LitestreamSHA
	}

	if workloadCfg.WriteRate > 0 {
		env["WRITE_RATE"] = fmt.Sprintf("%d", workloadCfg.WriteRate)
	}
	if workloadCfg.Pattern != "" {
		env["PATTERN"] = workloadCfg.Pattern
	}
	if workloadCfg.PayloadSize > 0 {
		env["PAYLOAD_SIZE"] = fmt.Sprintf("%d", workloadCfg.PayloadSize)
	}
	if workloadCfg.ReadRatio > 0 {
		env["READ_RATIO"] = fmt.Sprintf("%.2f", workloadCfg.ReadRatio)
	}
	if workloadCfg.Workers > 0 {
		env["LOAD_WORKERS"] = fmt.Sprintf("%d", workloadCfg.Workers)
	}
	if workloadCfg.ReplayDataset != "" {
		env["REPLAY_DATASET"] = workloadCfg.ReplayDataset
	}
	if workloadCfg.ReplayDataPath != "" {
		env["REPLAY_DATA_PATH"] = workloadCfg.ReplayDataPath
	}
	if workloadCfg.ReplayDataURL != "" {
		env["REPLAY_DATA_URL"] = workloadCfg.ReplayDataURL
	}
	if workloadCfg.ReplaySpeed > 0 {
		env["REPLAY_SPEED"] = fmt.Sprintf("%.2f", workloadCfg.ReplaySpeed)
	}
	if !workloadCfg.ReplayLoop {
		env["REPLAY_LOOP"] = "false"
	}

	return env
}

func (m *Manager) flyClientForWorker(worker model.Worker) *flyapi.Client {
	appName := strings.TrimSpace(worker.AppName)
	if appName == "" {
		appName = m.appName
	}
	return m.fly.ForApp(appName)
}

func (m *Manager) createWorkerMachine(ctx context.Context, worker model.Worker, imageRef string, volumeID string, workloadCfg workload.Config) (*flyapi.Machine, error) {
	workloadCfg = normalizeWorkloadConfig(workloadCfg)
	request := flyapi.CreateMachineRequest{
		Name:   worker.Name,
		Region: worker.Region,
		Config: flyapi.MachineConfig{
			Image: imageRef,
			Env:   m.workerEnv(worker, workloadCfg),
			Guest: flyapi.Guest{
				CPUKind:  "shared",
				CPUs:     workloadCfg.CPUs,
				MemoryMB: workloadCfg.MemoryMB,
			},
			Mounts: []flyapi.Mount{{
				Volume: volumeID,
				Path:   "/data",
			}},
			Metrics: &flyapi.MetricsConfig{
				Port: 9091,
				Path: "/metrics",
			},
		},
	}

	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		machine, err := m.flyClientForWorker(worker).CreateMachine(ctx, request)
		if err == nil {
			return machine, nil
		}
		if !retriableMachineCreateError(err) || attempt == 5 {
			return nil, err
		}

		lastErr = err
		delay := time.Duration(attempt*2) * time.Second
		slog.Warn("Retrying worker machine creation", "worker_id", worker.ID, "attempt", attempt, "delay", delay, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	return nil, lastErr
}

func retriableMachineCreateError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "failed to get manifest") ||
		strings.Contains(message, "manifest unknown") ||
		strings.Contains(message, "http 404")
}

func (m *Manager) RunDormancyLoop(ctx context.Context, policy DormancyPolicy) {
	if policy.CheckInterval <= 0 {
		policy.CheckInterval = 10 * time.Minute
	}
	if policy.Threshold <= 0 {
		policy.Threshold = 24 * time.Hour
	}
	if policy.MinFailures <= 0 {
		policy.MinFailures = 3
	}

	m.evaluateDormancy(ctx, policy)

	ticker := time.NewTicker(policy.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evaluateDormancy(ctx, policy)
		}
	}
}

func (m *Manager) evaluateDormancy(ctx context.Context, policy DormancyPolicy) {
	workers, err := m.db.ListMainWorkers()
	if err != nil {
		slog.Error("Failed to list main workers for dormancy", "error", err)
		return
	}

	now := time.Now().UTC()
	for _, worker := range workers {
		if worker.Status != model.WorkerRunning && worker.Status != model.WorkerDegraded {
			continue
		}

		verifications, err := m.db.ListVerifications(worker.ID, 256)
		if err != nil {
			slog.Error("Failed to list worker verifications for dormancy", "worker_id", worker.ID, "error", err)
			continue
		}

		candidate, ok := detectDormancyCandidate(verifications, now, policy.Threshold, policy.MinFailures)
		if !ok {
			continue
		}

		reason := fmt.Sprintf("worker paused after %d consecutive %s failures since %s", candidate.Count, candidate.Signature, candidate.Since.Format(time.RFC3339))
		if err := m.DormantWorker(ctx, worker.ID, reason, candidate.Signature, "sustained_failure"); err != nil {
			slog.Error("Failed to pause dormant worker", "worker_id", worker.ID, "error", err)
		}
	}
}

func detectDormancyCandidate(verifications []model.Verification, now time.Time, threshold time.Duration, minFailures int) (dormancyCandidate, bool) {
	if len(verifications) == 0 {
		return dormancyCandidate{}, false
	}
	if minFailures <= 0 {
		minFailures = 1
	}
	latest := verifications[0]
	if !activeFailure(&latest) {
		return dormancyCandidate{}, false
	}

	signature := inferFailureSignature(&latest)
	if signature == "" {
		signature = "unknown_failure"
	}

	count := 0
	var oldest time.Time
	for _, verification := range verifications {
		if !activeFailure(&verification) {
			break
		}
		if inferFailureSignature(&verification) != signature {
			break
		}

		count++
		oldest = verification.StartedAt
		if verification.CompletedAt != nil && !verification.CompletedAt.IsZero() {
			oldest = *verification.CompletedAt
		}
	}

	if count < minFailures || oldest.IsZero() {
		return dormancyCandidate{}, false
	}
	if now.Sub(oldest) < threshold {
		return dormancyCandidate{}, false
	}

	return dormancyCandidate{
		Signature: signature,
		Since:     oldest,
		Count:     count,
	}, true
}

func (m *Manager) DormantWorker(ctx context.Context, workerID, reason, signature, resumeTrigger string) error {
	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return fmt.Errorf("get worker: %w", err)
	}

	if worker.FlyMachineID != "" {
		if err := m.flyClientForWorker(*worker).StopMachine(ctx, worker.FlyMachineID); err != nil {
			slog.Warn("Failed to stop dormant worker machine", "worker_id", workerID, "machine_id", worker.FlyMachineID, "error", err)
		}
	}

	if err := m.db.MarkWorkerDormant(workerID, reason, signature, resumeTrigger); err != nil {
		return fmt.Errorf("mark worker dormant: %w", err)
	}
	m.observeWorkerByID(workerID)
	if err := m.db.RecordEvent(workerID, "worker_dormant", reason, signature); err != nil {
		return fmt.Errorf("record dormant event: %w", err)
	}
	return nil
}

func (m *Manager) ResumeDormantWorkers(ctx context.Context, source, imageRef, gitSHA, litestreamSHA, resumeTrigger string) error {
	workers, err := m.db.ListDormantWorkers(source)
	if err != nil {
		return fmt.Errorf("list dormant workers: %w", err)
	}

	for _, worker := range workers {
		if err := m.resumeDormantWorker(ctx, worker, imageRef, gitSHA, litestreamSHA, resumeTrigger); err != nil {
			slog.Error("Failed to resume dormant worker", "worker_id", worker.ID, "error", err)
			_ = m.db.RecordEvent(worker.ID, "worker_probe_start_failed", err.Error(), imageRef)
		}
	}
	return nil
}

func (m *Manager) resumeDormantWorker(ctx context.Context, worker model.Worker, imageRef, gitSHA, litestreamSHA, resumeTrigger string) error {
	volumeID, err := m.resolveWorkerVolumeID(ctx, worker)
	if err != nil {
		return err
	}
	resumeSHA := strings.TrimSpace(gitSHA)
	if resumeSHA == "" {
		resumeSHA = worker.GitSHA
	}
	resumeLitestreamSHA := strings.TrimSpace(litestreamSHA)
	if resumeLitestreamSHA == "" {
		resumeLitestreamSHA = worker.LitestreamSHA
	}

	workloadCfg := normalizeWorkloadConfig(resolveWorkerWorkload(worker))

	if worker.FlyMachineID != "" {
		if err := m.flyClientForWorker(worker).DestroyMachine(ctx, worker.FlyMachineID, true); err != nil {
			slog.Warn("Failed to destroy dormant worker machine before resume", "worker_id", worker.ID, "machine_id", worker.FlyMachineID, "error", err)
		}
	}

	resumeWorker := worker
	resumeWorker.GitSHA = resumeSHA
	resumeWorker.LitestreamSHA = resumeLitestreamSHA
	machine, err := m.createWorkerMachine(ctx, resumeWorker, imageRef, volumeID, workloadCfg)
	if err != nil {
		return fmt.Errorf("create probe machine: %w", err)
	}

	if err := m.db.UpdateWorkerMachine(worker.ID, machine.ID, volumeID); err != nil {
		return fmt.Errorf("update worker machine: %w", err)
	}
	if err := m.db.UpdateWorkerMachineVersion(worker.ID, machine.ID, resumeSHA, resumeLitestreamSHA); err != nil {
		return fmt.Errorf("update worker machine version: %w", err)
	}
	if err := m.db.MarkWorkerProbing(worker.ID, resumeTrigger); err != nil {
		return fmt.Errorf("mark worker probing: %w", err)
	}
	m.observeWorkerByID(worker.ID)

	message := fmt.Sprintf("Worker resumed for probe on soak %s / litestream %s (%s)", shortVersionValue(resumeSHA), shortVersionValue(resumeLitestreamSHA), resumeTrigger)
	if err := m.db.RecordEvent(worker.ID, "worker_probe_started", message, imageRef); err != nil {
		return fmt.Errorf("record probe event: %w", err)
	}
	return nil
}

func (m *Manager) resolveWorkerVolumeID(ctx context.Context, worker model.Worker) (string, error) {
	if worker.FlyVolumeID != "" {
		return worker.FlyVolumeID, nil
	}
	if worker.FlyMachineID == "" {
		return "", fmt.Errorf("worker %s has no machine or volume to resume", worker.ID)
	}

	machine, err := m.flyClientForWorker(worker).GetMachine(ctx, worker.FlyMachineID)
	if err != nil {
		return "", fmt.Errorf("worker %s has no volume to resume: %w", worker.ID, err)
	}
	for _, mount := range machine.Config.Mounts {
		if mount.Volume == "" {
			continue
		}
		if err := m.db.UpdateWorkerMachine(worker.ID, worker.FlyMachineID, mount.Volume); err != nil {
			return "", fmt.Errorf("backfill worker volume: %w", err)
		}
		return mount.Volume, nil
	}

	return "", fmt.Errorf("worker %s has no mounted volume in machine config", worker.ID)
}
