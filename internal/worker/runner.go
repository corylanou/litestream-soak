package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Runner struct {
	cfg      Config
	profiles *pprofCapturer

	litestreamManager
	statsPoller
	loadReplayManager
	churnLoad     *churnLoad
	churnEvidence churnEvidence

	failureDebug            failureDebugState
	noProgress              diskPressureNoProgressState
	litestreamMetricsStatus string
	verifier                *Verifier
	reporter                *Reporter
	s3FaultProxy            *s3FaultProxy
}

func NewRunner(cfg Config) *Runner {
	runner := &Runner{
		cfg: cfg,
	}
	runner.profiles = newPprofCapturer(&runner.cfg)
	runner.litestreamManager = newLitestreamManager(&runner.cfg)
	runner.statsPoller = newStatsPoller(&runner.cfg)
	runner.statsPoller.litestreamPID = runner.litestreamManager.litestreamPID
	runner.s3ListRequests = func() int64 {
		if runner.s3FaultProxy == nil {
			return 0
		}
		return runner.s3FaultProxy.ListRequests()
	}
	runner.loadReplayManager = newLoadReplayManager(&runner.cfg)
	return runner
}

func (r *Runner) Run(ctx context.Context) error {
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)

	SetWorkerInfo(r.cfg)
	startTime := time.Now()
	r.reporter = NewReporter(r.cfg)
	stopChurnUploader := r.startChurnUploader(runCtx)
	defer stopChurnUploader()

	if err := r.startS3FaultProxy(runCtx); err != nil {
		return fmt.Errorf("start s3 fault proxy: %w", err)
	}
	defer r.stopS3FaultProxy()

	go func() {
		defer func() {
			if r.localStateScan != nil {
				r.localStateScan.close()
			}
		}()
		ticker := time.NewTicker(r.cfg.monitorInterval())
		defer ticker.Stop()
		lastHeartbeat := time.Time{}
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				uptime := time.Since(startTime).Seconds()
				SetUptime(uptime)
				r.setUptime(uptime)
				r.pollDBStats()
				snapshot := r.currentSnapshot()
				metricsCondition := r.observeLitestreamMetricsCondition(snapshot.RuntimePayload)
				if metricsCondition.ShouldReport {
					r.triggerProfile(metricsCondition.EventType)
					r.sendLitestreamMetricsEvent(runCtx, metricsCondition)
				}
				pressure := r.observeDiskPressureNoProgress(time.Now().UTC(), snapshot)
				if pressure.ShouldReport {
					r.triggerProfile("disk-pressure")
					r.sendDiskFullEvent(runCtx, pressure)
				}
				if lastHeartbeat.IsZero() || time.Since(lastHeartbeat) >= 15*time.Second {
					r.sendHeartbeat(runCtx)
					lastHeartbeat = time.Now()
				}
			}
		}
	}()

	if err := r.populate(runCtx); err != nil {
		return fmt.Errorf("populate: %w", err)
	}
	if err := r.prepareDiskFullRecoveryReserve(); err != nil {
		return fmt.Errorf("prepare disk-full recovery reserve: %w", err)
	}

	if err := r.writeLitestreamConfig(); err != nil {
		return fmt.Errorf("write litestream config: %w", err)
	}

	litestreamCtx, cancelLitestream := context.WithCancel(context.WithoutCancel(runCtx))
	if err := r.startLitestream(litestreamCtx); err != nil {
		cancelLitestream()
		return fmt.Errorf("start litestream: %w", err)
	}
	defer r.stopLitestream()
	defer cancelLitestream()
	r.monitorLitestream(runCtx, cancelRun)
	stopProfiles := r.startProfileCapture(runCtx)
	defer stopProfiles()

	if err := r.waitForFirstSync(runCtx); err != nil {
		return fmt.Errorf("wait for first sync: %w", err)
	}
	go newReplicaLevelPoller(&r.cfg).Run(runCtx)

	if r.cfg.churnEnabled() {
		load, err := startChurn(runCtx, r.cfg, func(op string, n int64, duration time.Duration, attemptErr error) {
			if err := r.recordChurnAttempt(runCtx, op, n, duration, attemptErr); err != nil {
				cancelRun(fmt.Errorf("persist churn evidence: %w", err))
			}
		})
		if err != nil {
			return err
		}
		r.churnLoad = load
		defer load.Stop()
	} else if r.cfg.ManyDBEnabled() {
		if err := r.startManyDBLoad(runCtx); err != nil {
			return fmt.Errorf("start many database load: %w", err)
		}
		defer r.stopLoad()
	} else if r.cfg.LoadMode == "synthetic" || r.cfg.LoadMode == "both" {
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

	var pinned *pinnedReader
	if r.cfg.PinnedReaderHold > 0 && !r.cfg.ManyDBEnabled() {
		pinned = newPinnedReader(r.cfg.DBPath, r.cfg.PinnedReaderHold, r.cfg.PinnedReaderPause)
		pinned.Start(runCtx)
		defer pinned.Stop()
	}

	var pausers []loadPauser
	if r.churnLoad != nil {
		pausers = append(pausers, r.churnLoad.engine)
	}
	if r.loadSup != nil {
		pausers = append(pausers, r.loadSup)
	}
	if r.replayEngine != nil {
		pausers = append(pausers, r.replayEngine)
	}
	if r.manyDBLoad != nil {
		pausers = append(pausers, r.manyDBLoad)
	}
	if pinned != nil {
		pausers = append(pausers, pinned)
	}
	r.verifier = NewVerifier(r.cfg, pausers...)
	r.verifier.SetStartHook(r.sendVerificationStarted)
	r.verifier.onProfileIncident = r.triggerProfile

	if err := r.runVerifyLoop(runCtx); err != nil {
		r.sendWorkerFailureEvent(err)
		return err
	}
	return nil
}

func (r *Runner) populate(ctx context.Context) error {
	if r.cfg.churnEnabled() {
		return populateChurn(ctx, r.cfg)
	}
	if r.cfg.ManyDBEnabled() {
		return populateManyDBs(ctx, r.cfg)
	}

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
			r.resetS3FaultProxyCycle()
			result, err := r.verifier.RunCycle(ctx)
			result = r.applyS3FaultProxyVerificationGuards(result)
			if !result.Passed {
				r.triggerProfile("verification-failed")
			}
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

func (r *Runner) applyS3FaultProxyVerificationGuards(result VerificationResult) VerificationResult {
	if !result.Passed {
		return result
	}
	if r.cfg.S3FaultProxyEnabled && r.cfg.s3FaultProxyObserveMode() {
		return result
	}

	if r.cfg.ReplicaType == "s3" && r.cfg.S3FaultProxyRequireObservedSourceGet {
		if r.s3FaultProxy == nil || r.s3FaultProxy.ObservedSourceGETs() == 0 {
			level := strings.Trim(strings.TrimSpace(r.cfg.S3FaultProxySourceLevel), "/")
			if level == "" {
				level = defaultS3FaultProxySourceLevel
			}
			return failedS3FaultGuardResult(result, "s3 source GET fault guard failed", fmt.Sprintf("s3 source GET fault guard: no remote %s source GET observed by fault proxy; local compactor cache may have bypassed the source-read path", level))
		}
	}
	if r.cfg.ReplicaType == "s3" && r.cfg.S3FaultProxyRequireObservedSourceRangeGet {
		if r.s3FaultProxy == nil || r.s3FaultProxy.ObservedSourceRangeGETs() == 0 {
			level := strings.Trim(strings.TrimSpace(r.cfg.S3FaultProxySourceLevel), "/")
			if level == "" {
				level = defaultS3FaultProxySourceLevel
			}
			return failedS3FaultGuardResult(result, "s3 source range GET fault guard failed", fmt.Sprintf("s3 source range GET fault guard: no resumed remote %s source range GET observed by fault proxy", level))
		}
	}

	if r.cfg.ReplicaType == "s3" && r.cfg.S3FaultProxyEnabled && r.cfg.S3FaultProxyFailFirstAttempts > 0 {
		if r.s3FaultProxy != nil && r.s3FaultProxy.TotalFailures() > 0 {
			return result
		}
		mode := firstNonEmpty(r.cfg.S3FaultProxyMode, s3FaultProxyModeUploadPartReset)
		return failedS3FaultGuardResult(result, "s3 fault guard failed", fmt.Sprintf("s3 fault guard: no induced %s fault observed by fault proxy", mode))
	}

	return result
}

func (r *Runner) resetS3FaultProxyCycle() {
	if r.s3FaultProxy != nil {
		r.s3FaultProxy.ResetCycle()
	}
}

func failedS3FaultGuardResult(result VerificationResult, summary string, message string) VerificationResult {
	result.Status = "failed"
	result.Passed = false
	result.ErrorMessage = message
	result.Summary = summary
	return result
}
