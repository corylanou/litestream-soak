package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestDetectDormancyCandidate(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-12*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-25*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	candidate, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 3)
	if !ok {
		t.Fatal("expected dormancy candidate")
	}
	if candidate.Signature != "litestream_sync_socket_refused" {
		t.Fatalf("signature=%q want %q", candidate.Signature, "litestream_sync_socket_refused")
	}
	if candidate.Count != 3 {
		t.Fatalf("count=%d want 3", candidate.Count)
	}
	if !candidate.Since.Equal(now.Add(-25 * time.Hour)) {
		t.Fatalf("since=%s want %s", candidate.Since, now.Add(-25*time.Hour))
	}
}

func TestDetectDormancyCandidateRequiresConsecutiveFailures(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	passCompletedAt := now.Add(-8 * time.Hour)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		{
			StartedAt:    passCompletedAt.Add(-10 * time.Second),
			CompletedAt:  &passCompletedAt,
			Status:       "completed",
			CheckType:    "integrity",
			Passed:       true,
			ErrorMessage: "",
		},
		failedVerificationAt(now.Add(-30*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	if _, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 2); ok {
		t.Fatal("expected no dormancy candidate when a pass interrupts the run")
	}
}

func TestDetectDormancyCandidateRequiresSameSignature(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-12*time.Hour), `wait for sync: context deadline exceeded`),
		failedVerificationAt(now.Add(-30*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	if _, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 2); ok {
		t.Fatal("expected no dormancy candidate when signatures differ")
	}
}

func TestVerificationsSinceIgnoresFailuresBeforeWorkerRun(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-10 * time.Minute)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-1*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-12*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-25*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	filtered := verificationsSince(verifications, cutoff)
	if len(filtered) != 1 {
		t.Fatalf("len(filtered)=%d want 1", len(filtered))
	}
	if _, ok := detectDormancyCandidate(filtered, now, 24*time.Hour, 3); ok {
		t.Fatal("expected no dormancy candidate from pre-run failures")
	}
}

func TestDormancyEvaluationStartUsesProbeTime(t *testing.T) {
	createdAt := time.Date(2026, 4, 13, 8, 0, 0, 0, time.UTC)
	lastProbeAt := createdAt.Add(4 * time.Hour)
	worker := model.Worker{
		CreatedAt:   createdAt,
		LastProbeAt: &lastProbeAt,
	}

	if got := dormancyEvaluationStart(worker); !got.Equal(lastProbeAt) {
		t.Fatalf("dormancyEvaluationStart()=%s want %s", got, lastProbeAt)
	}
}

func TestInferDeploymentRolloutStatus(t *testing.T) {
	tests := []struct {
		name    string
		rollout DeploymentRolloutResponse
		want    string
	}{
		{name: "no workers", rollout: DeploymentRolloutResponse{}, want: "no_workers"},
		{name: "outdated workers", rollout: DeploymentRolloutResponse{TotalWorkers: 9, OutdatedWorkers: 2}, want: "rolling_out"},
		{name: "probing workers", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, ProbingWorkers: 3}, want: "probing"},
		{name: "attention workers", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, DegradedWorkers: 1}, want: "needs_attention"},
		{name: "runtime unhealthy workers", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, RunningWorkers: 9, RuntimeUnhealthyWorkers: 1}, want: "needs_attention"},
		{name: "awaiting verification", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, RunningWorkers: 9, AwaitingVerification: 9}, want: "settling"},
		{name: "stable fleet", rollout: DeploymentRolloutResponse{TotalWorkers: 9, UpdatedWorkers: 9, RunningWorkers: 9}, want: "stable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := inferDeploymentRolloutStatus(test.rollout); got != test.want {
				t.Fatalf("inferDeploymentRolloutStatus()=%q want %q", got, test.want)
			}
		})
	}
}

func TestSummarizeDeploymentRollout(t *testing.T) {
	rollout := DeploymentRolloutResponse{
		Deployment:           model.Deployment{GitSHA: "0123456789abcdef", LitestreamSHA: "fedcba9876543210", Source: "main"},
		Status:               "probing",
		TotalWorkers:         9,
		UpdatedWorkers:       9,
		ProbingWorkers:       3,
		VerifiedSinceDeploy:  6,
		AwaitingVerification: 3,
	}

	summary := summarizeDeploymentRollout(rollout)
	if summary != "The main branch rollout is still settling. All 9 workers are on the new release, 6 have verified since rollout, and 3 still need a fresh verification." {
		t.Fatalf("summary=%q", summary)
	}
}

func TestSummarizeDeploymentRolloutUsesSingularAttentionGrammar(t *testing.T) {
	rollout := DeploymentRolloutResponse{
		Deployment:       model.Deployment{Source: "pr-1228", PRNumber: 1228},
		Status:           "needs_attention",
		TotalWorkers:     9,
		UpdatedWorkers:   9,
		AttentionWorkers: 1,
		DegradedWorkers:  1,
		DormantWorkers:   0,
	}

	summary := summarizeDeploymentRollout(rollout)
	if summary != "The PR #1228 rollout needs attention. All 9 workers are on the new release, but 1 worker still needs investigation: 1 degraded worker." {
		t.Fatalf("summary=%q", summary)
	}
}

func TestSummarizeDeploymentComparisonUsesPlainEnglish(t *testing.T) {
	comparison := DeploymentComparisonResponse{
		Base: &DeploymentScorecard{
			Deployment:    model.Deployment{Source: "main"},
			PassedWorkers: 4,
			FailedWorkers: 4,
		},
		Head: DeploymentScorecard{
			Deployment:    model.Deployment{Source: "pr-1228", PRNumber: 1228},
			PassedWorkers: 9,
			FailedWorkers: 0,
		},
		Verdict: "better",
	}

	summary := summarizeDeploymentComparison(comparison)
	if summary != "The PR #1228 rollout looks better than the main branch rollout so far: 9 workers passed versus 4, and 0 failed versus 4." {
		t.Fatalf("summary=%q", summary)
	}
}

func TestResumeDormantWorkersReturnsWorkerFailures(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	worker := &model.Worker{
		ID:            "worker-pr-1228-taxi-mixed",
		Name:          "worker-pr-1228-taxi-mixed",
		Status:        model.WorkerRunning,
		Source:        "pr-1228",
		GitSHA:        "old-soak",
		LitestreamSHA: "old-litestream",
		ProfileName:   "taxi-mixed",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	if err := db.MarkWorkerDormant(worker.ID, "stale failure", "sqlite_index_mismatch", "test"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}

	manager := &Manager{db: db, appName: "litestream-soak"}
	err = manager.ResumeDormantWorkers(context.Background(), "pr-1228", "image", "new-soak", "new-litestream", "test")
	if err == nil {
		t.Fatal("ResumeDormantWorkers() error = nil, want worker failure")
	}
	if !strings.Contains(err.Error(), worker.ID) {
		t.Fatalf("ResumeDormantWorkers() error = %q, want worker id", err)
	}

	events, err := db.ListWorkerEvents(worker.ID, 5)
	if err != nil {
		t.Fatalf("ListWorkerEvents() error = %v", err)
	}
	found := false
	for _, event := range events {
		if event.EventType == "worker_probe_start_failed" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("worker_probe_start_failed event not recorded: %+v", events)
	}
}

func TestWorkerEnvIncludesWorkerToken(t *testing.T) {
	t.Setenv("SOAK_WORKER_TOKEN", "test-worker-token")

	mgr := &Manager{
		replica: ReplicaConfig{
			Bucket:   "litestream-soak-replicas-shared",
			Endpoint: "https://fly.storage.tigris.dev",
		},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-low-vol",
		Name: "worker-main-low-vol",
	}, workload.Config{})

	if got, want := env["SOAK_WORKER_TOKEN"], "test-worker-token"; got != want {
		t.Fatalf("SOAK_WORKER_TOKEN=%q, want %q", got, want)
	}
}

func TestWorkerEnvOmitsWorkerTokenWhenUnset(t *testing.T) {
	t.Setenv("SOAK_WORKER_TOKEN", "")

	mgr := &Manager{
		replica: ReplicaConfig{
			Bucket:   "litestream-soak-replicas-shared",
			Endpoint: "https://fly.storage.tigris.dev",
		},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-low-vol",
		Name: "worker-main-low-vol",
	}, workload.Config{})

	if _, ok := env["SOAK_WORKER_TOKEN"]; ok {
		t.Fatal("SOAK_WORKER_TOKEN should be omitted when unset")
	}
}

func TestWorkerEnvTrimsWorkerToken(t *testing.T) {
	t.Setenv("SOAK_WORKER_TOKEN", "  padded-token \n")

	mgr := &Manager{
		replica:        ReplicaConfig{Bucket: "bucket", Endpoint: "endpoint"},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-low-vol",
		Name: "worker-main-low-vol",
	}, workload.Config{})

	if got, want := env["SOAK_WORKER_TOKEN"], "padded-token"; got != want {
		t.Fatalf("SOAK_WORKER_TOKEN=%q, want %q", got, want)
	}
}

func TestWorkerEnvIncludesOptionalWorkloadFields(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica:        ReplicaConfig{Bucket: "bucket", Endpoint: "endpoint"},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-replay",
		Name: "worker-main-replay",
	}, workload.Config{
		WriteRate:                250,
		Pattern:                  "burst",
		PayloadSize:              512,
		ReadRatio:                0.25,
		Workers:                  4,
		ReplayDataset:            "taxi",
		ReplayDataPath:           "/data/taxi.csv",
		ReplayDataURL:            "https://example.com/taxi.csv",
		ReplaySpeed:              1.5,
		ReplayLoop:               false,
		S3PartSize:               "16MB",
		S3Concurrency:            8,
		VerifySyncDegradedAfter:  "1m",
		VerifySyncTimeout:        "3m",
		DiskFullNoProgressWindow: "2m",
		DiskFullRecoveryReserve:  314572800,
		DiskFullRecoveryTimeout:  "5m",
	})

	want := map[string]string{
		"WRITE_RATE":                       "250",
		"PATTERN":                          "burst",
		"PAYLOAD_SIZE":                     "512",
		"READ_RATIO":                       "0.25",
		"LOAD_WORKERS":                     "4",
		"REPLAY_DATASET":                   "taxi",
		"REPLAY_DATA_PATH":                 "/data/taxi.csv",
		"REPLAY_DATA_URL":                  "https://example.com/taxi.csv",
		"REPLAY_SPEED":                     "1.50",
		"REPLAY_LOOP":                      "false",
		"LITESTREAM_S3_PART_SIZE":          "16MB",
		"LITESTREAM_S3_CONCURRENCY":        "8",
		"VERIFY_SYNC_DEGRADED_AFTER":       "1m",
		"VERIFY_SYNC_TIMEOUT":              "3m",
		"DISK_FULL_NO_PROGRESS_WINDOW":     "2m",
		"DISK_FULL_RECOVERY_RESERVE_BYTES": "314572800",
		"DISK_FULL_RECOVERY_TIMEOUT":       "5m",
	}
	for key, value := range want {
		if got := env[key]; got != value {
			t.Fatalf("%s=%q, want %q", key, got, value)
		}
	}
}

func TestWorkerEnvIncludesS3FaultProxyWorkloadFields(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica:        ReplicaConfig{Bucket: "bucket", Endpoint: "https://fly.storage.tigris.dev"},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-compaction-source-stream-drop",
		Name: "worker-main-compaction-source-stream-drop",
	}, workload.Config{
		S3FaultProxyEnabled:                  true,
		S3FaultProxyMode:                     "source-get-reset",
		S3FaultProxyListenAddr:               "127.0.0.1:19000",
		S3FaultProxyMinContentLength:         8 * 1024 * 1024,
		S3FaultProxyResetAfterBytes:          2 * 1024 * 1024,
		S3FaultProxyFailFirstAttempts:        1,
		S3FaultProxyMaxFailures:              6,
		S3FaultProxySourceLevel:              "0001",
		S3FaultProxyRequireObservedSourceGet: true,
		ReplicaLevelReporting:                true,
	})

	want := map[string]string{
		"S3_FAULT_PROXY_ENABLED":                     "true",
		"S3_FAULT_PROXY_TARGET_ENDPOINT":             "https://fly.storage.tigris.dev",
		"S3_FAULT_PROXY_MODE":                        "source-get-reset",
		"S3_FAULT_PROXY_LISTEN_ADDR":                 "127.0.0.1:19000",
		"S3_FAULT_PROXY_MIN_CONTENT_LENGTH":          "8388608",
		"S3_FAULT_PROXY_RESET_AFTER_BYTES":           "2097152",
		"S3_FAULT_PROXY_FAIL_FIRST_ATTEMPTS":         "1",
		"S3_FAULT_PROXY_MAX_FAILURES":                "6",
		"S3_FAULT_PROXY_SOURCE_LEVEL":                "0001",
		"S3_FAULT_PROXY_REQUIRE_OBSERVED_SOURCE_GET": "true",
		"REPLICA_LEVEL_REPORTING":                    "true",
	}
	for key, wantValue := range want {
		if got := env[key]; got != wantValue {
			t.Fatalf("%s=%q, want %q", key, got, wantValue)
		}
	}
}

func TestWorkerEnvIncludesManyDBWorkloadFields(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica:        ReplicaConfig{Bucket: "bucket", Endpoint: "endpoint"},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-many-dbs-100-dir",
		Name: "worker-main-many-dbs-100-dir",
	}, workload.Config{
		LoadMode:                "many-db",
		NumDatabases:            100,
		ActivePercent:           2,
		ConfigMode:              "dir",
		VerifySampleSize:        5,
		ReplicationLagThreshold: 3,
	})

	want := map[string]string{
		"LOAD_MODE":                 "many-db",
		"NUM_DATABASES":             "100",
		"ACTIVE_PERCENT":            "2.00",
		"CONFIG_MODE":               "dir",
		"VERIFY_SAMPLE_SIZE":        "5",
		"REPLICATION_LAG_THRESHOLD": "3",
	}
	for key, wantValue := range want {
		if got := env[key]; got != wantValue {
			t.Fatalf("%s=%q, want %q", key, got, wantValue)
		}
	}
}

func TestWorkerEnvIncludesZeroActivePercentForManyDBWorkload(t *testing.T) {
	t.Parallel()

	mgr := &Manager{
		replica:        ReplicaConfig{Bucket: "bucket", Endpoint: "endpoint"},
		controlBaseURL: "https://litestream-soak-ctl.fly.dev",
	}

	workloadCfg := normalizeWorkloadConfig(workload.Config{
		LoadMode:         "many-db",
		NumDatabases:     100,
		ActivePercent:    0,
		ActivePercentSet: true,
		ConfigMode:       "dir",
		VerifySampleSize: 5,
	})
	env := mgr.workerEnv(model.Worker{
		ID:   "worker-main-many-dbs-100-dir-idle",
		Name: "worker-main-many-dbs-100-dir-idle",
	}, workloadCfg)

	if got := env["ACTIVE_PERCENT"]; got != "0.00" {
		t.Fatalf("ACTIVE_PERCENT=%q, want 0.00", got)
	}
}

func failedVerificationAt(completedAt time.Time, errorMessage string) model.Verification {
	startedAt := completedAt.Add(-10 * time.Second)
	return model.Verification{
		StartedAt:    startedAt,
		CompletedAt:  &completedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: errorMessage,
	}
}

func TestNormalizeWorkloadConfigDefaults(t *testing.T) {
	t.Parallel()

	got := normalizeWorkloadConfig(workload.Config{})

	if got.InitialSize != "5MB" {
		t.Fatalf("InitialSize=%q want %q", got.InitialSize, "5MB")
	}
	if got.VerifyInterval != "30m" {
		t.Fatalf("VerifyInterval=%q want %q", got.VerifyInterval, "30m")
	}
	if got.SnapshotInterval != "10m" {
		t.Fatalf("SnapshotInterval=%q want %q", got.SnapshotInterval, "10m")
	}
	if got.SyncInterval != "1s" {
		t.Fatalf("SyncInterval=%q want %q", got.SyncInterval, "1s")
	}
	if got.LoadMode != "synthetic" {
		t.Fatalf("LoadMode=%q want %q", got.LoadMode, "synthetic")
	}
	if got.CPUs != 1 {
		t.Fatalf("CPUs=%d want 1", got.CPUs)
	}
	if got.MemoryMB != 1024 {
		t.Fatalf("MemoryMB=%d want 1024", got.MemoryMB)
	}
}

func TestNormalizeWorkloadConfigDefaultsManyDBActivePercent(t *testing.T) {
	t.Parallel()

	got := normalizeWorkloadConfig(workload.Config{
		LoadMode:     "many-db",
		NumDatabases: 100,
	})

	if got.ActivePercent != 2 {
		t.Fatalf("ActivePercent=%v want 2", got.ActivePercent)
	}
}

func TestNormalizeWorkloadConfigPreservesExplicitZeroActivePercent(t *testing.T) {
	t.Parallel()

	got := normalizeWorkloadConfig(workload.Config{
		LoadMode:         "many-db",
		NumDatabases:     100,
		ActivePercent:    0,
		ActivePercentSet: true,
	})

	if got.ActivePercent != 0 {
		t.Fatalf("ActivePercent=%v want 0", got.ActivePercent)
	}
}

func TestNormalizeWorkloadConfigPreservesExplicitValues(t *testing.T) {
	t.Parallel()

	input := workload.Config{
		InitialSize:      "100MB",
		VerifyInterval:   "5m",
		SnapshotInterval: "2m",
		SyncInterval:     "500ms",
		LoadMode:         "replay",
		CPUs:             4,
		MemoryMB:         2048,
	}

	got := normalizeWorkloadConfig(input)

	if got.InitialSize != "100MB" {
		t.Fatalf("InitialSize=%q want %q", got.InitialSize, "100MB")
	}
	if got.VerifyInterval != "5m" {
		t.Fatalf("VerifyInterval=%q want %q", got.VerifyInterval, "5m")
	}
	if got.SnapshotInterval != "2m" {
		t.Fatalf("SnapshotInterval=%q want %q", got.SnapshotInterval, "2m")
	}
	if got.SyncInterval != "500ms" {
		t.Fatalf("SyncInterval=%q want %q", got.SyncInterval, "500ms")
	}
	if got.LoadMode != "replay" {
		t.Fatalf("LoadMode=%q want %q", got.LoadMode, "replay")
	}
	if got.CPUs != 4 {
		t.Fatalf("CPUs=%d want 4", got.CPUs)
	}
	if got.MemoryMB != 2048 {
		t.Fatalf("MemoryMB=%d want 2048", got.MemoryMB)
	}
}

func TestFlyClientForWorkerUsesManagerAppName(t *testing.T) {
	t.Parallel()

	base := flyapi.NewClient("litestream-soak", "token")
	mgr := &Manager{fly: base, appName: "litestream-soak"}

	worker := model.Worker{ID: "w1", AppName: ""}
	client := mgr.flyClientForWorker(worker)

	if client.AppName() != "litestream-soak" {
		t.Fatalf("AppName()=%q want %q", client.AppName(), "litestream-soak")
	}
}

func TestFlyClientForWorkerUsesWorkerAppName(t *testing.T) {
	t.Parallel()

	base := flyapi.NewClient("litestream-soak", "token")
	mgr := &Manager{fly: base, appName: "litestream-soak"}

	worker := model.Worker{ID: "w1", AppName: "other-app"}
	client := mgr.flyClientForWorker(worker)

	if client.AppName() != "other-app" {
		t.Fatalf("AppName()=%q want %q", client.AppName(), "other-app")
	}
}

func TestFlyClientForWorkerTrimSpaceAppName(t *testing.T) {
	t.Parallel()

	base := flyapi.NewClient("litestream-soak", "token")
	mgr := &Manager{fly: base, appName: "litestream-soak"}

	worker := model.Worker{ID: "w1", AppName: "   "}
	client := mgr.flyClientForWorker(worker)

	if client.AppName() != "litestream-soak" {
		t.Fatalf("AppName()=%q want %q", client.AppName(), "litestream-soak")
	}
}

func TestRetriableMachineCreateErrorRetriableSubstrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
		want    bool
	}{
		{name: "failed to get manifest", message: "failed to get manifest for image", want: true},
		{name: "manifest unknown", message: "manifest unknown", want: true},
		{name: "http 404", message: "http 404 not found", want: true},
		{name: "mixed case manifest", message: "Failed To Get Manifest: blah", want: true},
		{name: "non retriable", message: "out of capacity", want: false},
		{name: "random error", message: "connection refused", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := retriableMachineCreateError(errors.New(tc.message))
			if got != tc.want {
				t.Fatalf("retriableMachineCreateError(%q)=%v want %v", tc.message, got, tc.want)
			}
		})
	}
}

func TestRunDormancyLoopExitsOnCanceledContext(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{db: db, appName: "litestream-soak"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cancel()

	done := make(chan struct{})
	go func() {
		mgr.RunDormancyLoop(ctx, DormancyPolicy{
			CheckInterval: 10 * time.Millisecond,
			Threshold:     24 * time.Hour,
			MinFailures:   3,
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunDormancyLoop did not exit after context was canceled")
	}
}

func TestEvaluateDormancyNoCandidatesNoWorkers(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{db: db, appName: "litestream-soak"}

	mgr.evaluateDormancy(context.Background(), DormancyPolicy{
		Threshold:   24 * time.Hour,
		MinFailures: 3,
	})
}

func TestEvaluateDormancySkipsNonRunningWorkers(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &model.Worker{
		ID:            "worker-stopped",
		Name:          "worker-stopped",
		Status:        model.WorkerStopped,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "def456",
		ProfileName:   "default",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	if err := db.UpdateWorkerStatus(worker.ID, model.WorkerStopped, ""); err != nil {
		t.Fatalf("UpdateWorkerStatus() error = %v", err)
	}

	mgr := &Manager{db: db, appName: "litestream-soak"}
	mgr.evaluateDormancy(context.Background(), DormancyPolicy{
		Threshold:   24 * time.Hour,
		MinFailures: 3,
	})

	got, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if got.Status != model.WorkerStopped {
		t.Fatalf("Status=%q want %q", got.Status, model.WorkerStopped)
	}
}

func TestEvaluateDormancyInsufficientFailuresNotDormant(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &model.Worker{
		ID:            "worker-few-fails",
		Name:          "worker-few-fails",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "abc123",
		LitestreamSHA: "def456",
		ProfileName:   "default",
		ProfileConfig: "{}",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	if err := db.UpdateWorkerStatus(worker.ID, model.WorkerRunning, ""); err != nil {
		t.Fatalf("UpdateWorkerStatus() error = %v", err)
	}

	now := time.Now().UTC()
	v := &model.Verification{
		WorkerID:     worker.ID,
		StartedAt:    now.Add(-31 * 24 * time.Hour),
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`,
	}
	completedAt := v.StartedAt.Add(10 * time.Second)
	v.CompletedAt = &completedAt
	if err := db.RecordVerification(v); err != nil {
		t.Fatalf("RecordVerification() error = %v", err)
	}

	mgr := &Manager{db: db, appName: "litestream-soak"}
	mgr.evaluateDormancy(context.Background(), DormancyPolicy{
		Threshold:   24 * time.Hour,
		MinFailures: 3,
	})

	got, err := db.GetWorker(worker.ID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if got.Status == model.WorkerDormant {
		t.Fatalf("Status=%q want not dormant (only 1 failure, need 3)", got.Status)
	}
}

func TestResolveWorkerVolumeIDFromWorker(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{db: db, appName: "litestream-soak"}

	worker := model.Worker{
		ID:          "w-vol",
		FlyVolumeID: "vol-abc123",
	}

	volumeID, err := mgr.resolveWorkerVolumeID(context.Background(), worker)
	if err != nil {
		t.Fatalf("resolveWorkerVolumeID() error = %v", err)
	}
	if volumeID != "vol-abc123" {
		t.Fatalf("volumeID=%q want %q", volumeID, "vol-abc123")
	}
}

func TestResolveWorkerVolumeIDNoMachineOrVolume(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{db: db, appName: "litestream-soak"}

	worker := model.Worker{
		ID:           "w-no-vol",
		FlyVolumeID:  "",
		FlyMachineID: "",
	}

	_, err = mgr.resolveWorkerVolumeID(context.Background(), worker)
	if err == nil {
		t.Fatal("resolveWorkerVolumeID() error = nil, want error for missing machine and volume")
	}
	if !strings.Contains(err.Error(), worker.ID) {
		t.Fatalf("resolveWorkerVolumeID() error = %q, want worker id in message", err)
	}
}

func TestResumeDormantWorkerNoVolumeNoMachine(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &model.Worker{
		ID:            "worker-no-vol",
		Name:          "worker-no-vol",
		Status:        model.WorkerDormant,
		Source:        "main",
		GitSHA:        "old-sha",
		LitestreamSHA: "old-litestream",
		ProfileName:   "default",
		ProfileConfig: "{}",
		FlyMachineID:  "",
		FlyVolumeID:   "",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}
	if err := db.MarkWorkerDormant(worker.ID, "test", "test_sig", "test"); err != nil {
		t.Fatalf("MarkWorkerDormant() error = %v", err)
	}

	mgr := &Manager{db: db, appName: "litestream-soak"}
	err = mgr.resumeDormantWorker(context.Background(), *worker, "image:latest", "new-sha", "new-litestream", "test")
	if err == nil {
		t.Fatal("resumeDormantWorker() error = nil, want error for missing volume and machine")
	}
	if !strings.Contains(err.Error(), worker.ID) {
		t.Fatalf("resumeDormantWorker() error = %q, want worker id in error", err)
	}
}

func TestResumeDormantWorkerUsesWorkerGitSHAWhenEmpty(t *testing.T) {
	t.Parallel()

	db, err := model.Open(filepath.Join(t.TempDir(), "soak.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	worker := &model.Worker{
		ID:            "worker-sha-fallback",
		Name:          "worker-sha-fallback",
		Status:        model.WorkerDormant,
		Source:        "main",
		GitSHA:        "original-sha",
		LitestreamSHA: "original-litestream",
		ProfileName:   "default",
		ProfileConfig: "{}",
		FlyMachineID:  "",
		FlyVolumeID:   "",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker() error = %v", err)
	}

	mgr := &Manager{db: db, appName: "litestream-soak"}
	err = mgr.resumeDormantWorker(context.Background(), *worker, "image:latest", "", "", "test")
	if err == nil {
		t.Fatal("resumeDormantWorker() error = nil, want error (no volume/machine)")
	}
	if !strings.Contains(err.Error(), worker.ID) {
		t.Fatalf("resumeDormantWorker() error = %q, want worker id", err)
	}
}

func TestRetriableMachineCreateErrorWrappedError(t *testing.T) {
	t.Parallel()

	inner := errors.New("failed to get manifest for registry.fly.io/app:latest")
	wrapped := fmt.Errorf("outer: %w", inner)

	if !retriableMachineCreateError(wrapped) {
		t.Fatalf("retriableMachineCreateError(wrapped) = false, want true")
	}
}
