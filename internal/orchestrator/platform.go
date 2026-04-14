package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
)

func (m *Manager) RunPlatformLogMonitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}

	m.syncPlatformLogs(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.syncPlatformLogs(ctx)
		}
	}
}

func (m *Manager) syncPlatformLogs(ctx context.Context) {
	workers, err := m.db.ListWorkers("")
	if err != nil {
		slog.Error("Failed to list workers for platform log sync", "error", err)
		return
	}

	for _, worker := range workers {
		if worker.FlyMachineID == "" {
			continue
		}
		if worker.Status == model.WorkerDormant || worker.Status == model.WorkerStopped || worker.Status == model.WorkerFailed {
			continue
		}
		if err := m.syncWorkerPlatformLogs(ctx, worker); err != nil {
			slog.Warn("Failed to sync platform logs", "worker_id", worker.ID, "machine_id", worker.FlyMachineID, "error", err)
		}
	}
}

func (m *Manager) syncWorkerPlatformLogs(ctx context.Context, worker model.Worker) error {
	logs, err := m.platformLogClientForWorker(worker).ListAppLogs(ctx, worker.FlyMachineID, "")
	if err != nil {
		return fmt.Errorf("list app logs: %w", err)
	}

	if logs == nil {
		return nil
	}

	entries := append([]flyapi.AppLogEntry(nil), logs.Data...)
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Attributes.Timestamp.Before(entries[j].Attributes.Timestamp)
	})

	for _, entry := range entries {
		eventType, message, ok := classifyPlatformLog(entry)
		if !ok {
			continue
		}

		details, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal platform log entry: %w", err)
		}

		_, err = m.db.RecordUniqueEventAt(worker.ID, eventType, message, string(details), entry.Attributes.Timestamp.UTC())
		if err != nil {
			return fmt.Errorf("record platform event: %w", err)
		}
	}
	return nil
}

func classifyPlatformLog(entry flyapi.AppLogEntry) (string, string, bool) {
	message := strings.TrimSpace(entry.Attributes.Message)
	if message == "" {
		return "", "", false
	}

	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "oom:") || strings.Contains(lower, "out of memory"):
		return "platform_oom", fmt.Sprintf("Fly log reported OOM: %s", firstMeaningfulLine(message)), true
	case strings.Contains(lower, "no space left on device"):
		return "platform_disk_full", fmt.Sprintf("Fly log reported disk pressure: %s", firstMeaningfulLine(message)), true
	case strings.Contains(lower, "signal: killed"):
		return "platform_killed", fmt.Sprintf("Fly log reported process kill: %s", firstMeaningfulLine(message)), true
	case entry.Attributes.Meta.Event.Provider != "" && entry.Attributes.Meta.Event.Provider != "app" &&
		(strings.Contains(lower, "restart") || strings.Contains(lower, "restarted") || strings.Contains(lower, "starting") || strings.Contains(lower, "started")):
		return "platform_restart", fmt.Sprintf("Fly platform event: %s", firstMeaningfulLine(message)), true
	default:
		return "", "", false
	}
}

func latestPlatformEvent(events []model.Event) *model.Event {
	var latest *model.Event
	for _, event := range events {
		if !isPlatformEventType(event.EventType) {
			continue
		}
		copy := event
		if latest == nil || copy.CreatedAt.After(latest.CreatedAt) {
			latest = &copy
		}
	}
	return latest
}

func isPlatformEventType(eventType string) bool {
	return strings.HasPrefix(strings.TrimSpace(eventType), "platform_")
}
