package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/google/uuid"
	"modernc.org/sqlite"
)

type churnEvidence struct {
	mu        sync.Mutex
	persistMu sync.Mutex
	uploadMu  sync.Mutex
	notify    chan struct{}
	counters  reporting.WorkloadCounters
}

func (r *Runner) currentSnapshot() runtimeSnapshot {
	snapshot := r.statsPoller.currentSnapshot()
	snapshot.ProfilingEvidence = r.profilingSnapshot()
	if r.cfg.churnEnabled() {
		r.churnEvidence.mu.Lock()
		r.initializeChurnCounters()
		snapshot.WorkloadCounters = r.churnEvidence.counters
		r.churnEvidence.mu.Unlock()
	}
	return snapshot
}

func (r *Runner) initializeChurnCounters() {
	r.churnEvidence.counters.WorkloadCountersPresent = true
	if r.churnEvidence.counters.WorkloadCounterEpoch == "" {
		r.churnEvidence.counters.WorkloadCounterEpoch = uuid.NewString()
	}
}

func (r *Runner) recordChurnAttempt(_ context.Context, op string, n int64, d time.Duration, attemptErr error) error {
	recordChurn(r.cfg, op, n, d, attemptErr)
	r.churnEvidence.mu.Lock()
	r.initializeChurnCounters()
	counters := &r.churnEvidence.counters
	counters.WorkloadAttemptsTotal++
	if attemptErr == nil {
		counters.WorkloadMutationsTotal += uint64(n)
		r.churnEvidence.mu.Unlock()
		return nil
	}
	counters.WorkloadErrorsTotal++
	kind := "other"
	var sqliteErr *sqlite.Error
	if errors.As(attemptErr, &sqliteErr) && (sqliteErr.Code()&255 == 5 || sqliteErr.Code()&255 == 6) {
		counters.WorkloadBusyTotal++
		kind = "busy"
	}
	event := reporting.WorkerEventPayload{
		WorkerIdentity: workerIdentity(r.cfg), EventType: "workload_error", Message: fmt.Sprintf("%s %s failed: %v", r.cfg.LoadMode, op, attemptErr), SentAt: time.Now().UTC(),
		RuntimePayload: reporting.RuntimePayload{WorkloadCounters: *counters},
		WorkloadEvent:  reporting.WorkloadEvent{WorkloadEventID: fmt.Sprintf("%s:%d", counters.WorkloadCounterEpoch, counters.WorkloadAttemptsTotal), WorkloadMode: r.cfg.LoadMode, WorkloadOperation: op, WorkloadErrorKind: kind, WorkloadLatencySeconds: d.Seconds()},
	}
	r.churnEvidence.mu.Unlock()
	err := r.persistRunEvidence(event)
	if err != nil {
		return err
	}
	r.requestEvidenceFlush()
	return nil
}

const evidenceOutboxMaxEvents = 1024
const evidenceOutboxMaxBytes = 16 << 20

func (r *Runner) persistRunEvidence(event reporting.WorkerEventPayload) error {
	r.churnEvidence.persistMu.Lock()
	defer r.churnEvidence.persistMu.Unlock()
	dir := filepath.Join(r.cfg.DataDir, "churn-outbox")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%x.json", sha256.Sum256([]byte(event.WorkerID+"\x00"+event.RunID+"\x00"+event.WorkloadEventID)))
	path := filepath.Join(dir, name)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var used int64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		used += info.Size()
	}
	if len(entries) >= evidenceOutboxMaxEvents || used+int64(len(body)) > evidenceOutboxMaxBytes {
		return fmt.Errorf("run evidence unavailable: outbox capacity exceeded (%d events, %d bytes)", len(entries), used)
	}
	file, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(body)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	return syncEvidenceDirectory(dir)
}

func syncEvidenceDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (r *Runner) flushRunEvidence(ctx context.Context) error {
	if r.reporter == nil || !r.reporter.Enabled() {
		return nil
	}
	if !r.churnEvidence.uploadMu.TryLock() {
		return nil
	}
	defer r.churnEvidence.uploadMu.Unlock()
	dir := filepath.Join(r.cfg.DataDir, "churn-outbox")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var event reporting.WorkerEventPayload
		if err := json.Unmarshal(body, &event); err != nil {
			return err
		}
		if err := r.reporter.postJSON(ctx, "/api/workers/"+url.PathEscape(event.WorkerID)+"/events", event); err != nil {
			return err
		}
		r.churnEvidence.persistMu.Lock()
		removeErr := os.Remove(path)
		syncErr := syncEvidenceDirectory(dir)
		r.churnEvidence.persistMu.Unlock()
		if err := errors.Join(removeErr, syncErr); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) requestEvidenceFlush() {
	r.churnEvidence.mu.Lock()
	if r.churnEvidence.notify == nil {
		r.churnEvidence.notify = make(chan struct{}, 1)
	}
	notify := r.churnEvidence.notify
	r.churnEvidence.mu.Unlock()
	select {
	case notify <- struct{}{}:
	default:
	}
}

func (r *Runner) startEvidenceUploader(ctx context.Context) func() {
	if r.reporter == nil || !r.reporter.Enabled() {
		return func() {}
	}
	r.requestEvidenceFlush()
	r.churnEvidence.mu.Lock()
	notify := r.churnEvidence.notify
	r.churnEvidence.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-notify:
			case <-ticker.C:
			}
			if err := r.flushRunEvidence(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("Run evidence delivery pending", "error", err)
			}
		}
	}()
	return func() { cancel(); <-done }
}
