package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
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

	workersByApp := make(map[string]map[string]model.Worker)
	for _, worker := range workers {
		if worker.FlyMachineID == "" {
			continue
		}
		if worker.Status == model.WorkerDormant || worker.Status == model.WorkerStopped || worker.Status == model.WorkerFailed {
			continue
		}
		appName := strings.TrimSpace(worker.AppName)
		if appName == "" {
			appName = m.appName
		}
		if workersByApp[appName] == nil {
			workersByApp[appName] = make(map[string]model.Worker)
		}
		workersByApp[appName][worker.FlyMachineID] = worker
	}

	for appName, workersByMachine := range workersByApp {
		if err := m.syncAppPlatformLogs(ctx, appName, workersByMachine); err != nil {
			slog.Warn("Failed to sync platform logs", "app", appName, "error", err)
		}
	}
}

func (m *Manager) syncAppPlatformLogs(ctx context.Context, appName string, workersByMachine map[string]model.Worker) error {
	logs, err := m.fetchAppPlatformLogs(ctx, appName)
	if err != nil {
		return fmt.Errorf("fetch app logs: %w", err)
	}

	if len(logs) == 0 {
		return nil
	}

	sort.SliceStable(logs, func(i, j int) bool {
		return logs[i].Timestamp.Before(logs[j].Timestamp)
	})

	for _, logLine := range logs {
		worker, ok := workersByMachine[logLine.Instance]
		if !ok {
			continue
		}

		entry := logLine.entry()
		eventType, message, ok := classifyPlatformLog(entry)
		if !ok {
			continue
		}

		details, err := json.Marshal(logLine)
		if err != nil {
			return fmt.Errorf("marshal platform log entry: %w", err)
		}

		_, err = m.db.RecordUniqueEventAt(worker.ID, eventType, message, string(details), logLine.Timestamp.UTC())
		if err != nil {
			return fmt.Errorf("record platform event: %w", err)
		}
	}
	return nil
}

func (m *Manager) fetchAppPlatformLogs(ctx context.Context, appName string) ([]platformLogLine, error) {
	if strings.TrimSpace(m.platformLogToken) == "" {
		return nil, fmt.Errorf("platform log token is not configured")
	}

	cmd := exec.CommandContext(ctx, "flyctl", "logs", "-a", appName, "--json", "--no-tail")
	cmd.Env = append(os.Environ(), "FLY_API_TOKEN="+m.platformLogToken)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return nil, fmt.Errorf("run fly logs: %s", message)
	}

	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	logs := make([]platformLogLine, 0)
	for {
		var entry platformLogLine
		if err := decoder.Decode(&entry); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "unexpected end of JSON input") {
				break
			}
			return nil, fmt.Errorf("decode fly log line: %w", err)
		}
		logs = append(logs, entry)
	}

	return logs, nil
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
	case strings.Contains(lower, "no space left on device"),
		strings.Contains(lower, "disk is full"),
		strings.Contains(lower, "database is full"),
		strings.Contains(lower, "database or disk is full"):
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

type platformLogLine struct {
	Level     string            `json:"level"`
	Instance  string            `json:"instance"`
	Message   string            `json:"message"`
	Region    string            `json:"region"`
	Timestamp time.Time         `json:"timestamp"`
	Meta      flyapi.AppLogMeta `json:"meta"`
}

func (l platformLogLine) entry() flyapi.AppLogEntry {
	return flyapi.AppLogEntry{
		Attributes: flyapi.AppLogAttributes{
			Timestamp: l.Timestamp,
			Message:   l.Message,
			Level:     l.Level,
			Instance:  l.Instance,
			Region:    l.Region,
			Meta:      l.Meta,
		},
	}
}
