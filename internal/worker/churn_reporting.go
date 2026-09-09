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
	mu       sync.Mutex
	counters reporting.WorkloadCounters
}

func (r *Runner) currentSnapshot() runtimeSnapshot {
	snapshot := r.statsPoller.currentSnapshot()
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

func (r *Runner) recordChurnAttempt(ctx context.Context, op string, n int64, d time.Duration, attemptErr error) error {
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
	var sqliteErr *sqlite.Error
	if errors.As(attemptErr, &sqliteErr) && (sqliteErr.Code()&255 == 5 || sqliteErr.Code()&255 == 6) {
		counters.WorkloadBusyTotal++
	}
	event := reporting.WorkerEventPayload{
		WorkerIdentity: workerIdentity(r.cfg), EventType: "workload_error", Message: fmt.Sprintf("%s %s failed: %v", r.cfg.LoadMode, op, attemptErr), SentAt: time.Now().UTC(),
		RuntimePayload: reporting.RuntimePayload{WorkloadCounters: *counters},
	}
	err := r.persistChurnEvidence(event)
	r.churnEvidence.mu.Unlock()
	if err != nil {
		return err
	}
	if err := r.flushChurnEvidence(ctx); err != nil {
		slog.Warn("Churn error retained for delivery", "error", err)
	}
	return nil
}

func (r *Runner) persistChurnEvidence(event reporting.WorkerEventPayload) error {
	dir := filepath.Join(r.cfg.DataDir, "churn-outbox")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%x.json", sha256.Sum256([]byte(event.WorkerID+"\x00"+event.RunID+"\x00"+event.WorkloadCounterEpoch)))
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
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
	return syncChurnDirectory(dir)
}

func syncChurnDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (r *Runner) flushChurnEvidence(ctx context.Context) error {
	if r.reporter == nil || !r.reporter.Enabled() {
		return nil
	}
	r.churnEvidence.mu.Lock()
	defer r.churnEvidence.mu.Unlock()
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
		if err := os.Remove(path); err != nil {
			return err
		}
		if err := syncChurnDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}
