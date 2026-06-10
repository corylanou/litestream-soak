package worker

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

type failureDebugState struct {
	mu  sync.Mutex
	key string
	at  time.Time
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

	r.failureDebug.mu.Lock()
	defer r.failureDebug.mu.Unlock()
	if r.failureDebug.key == key && now.Sub(r.failureDebug.at) < 6*time.Hour {
		return nil
	}
	r.failureDebug.key = key
	r.failureDebug.at = now
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
	r.failureDebug.mu.Lock()
	defer r.failureDebug.mu.Unlock()
	r.failureDebug.key = ""
	r.failureDebug.at = time.Time{}
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
