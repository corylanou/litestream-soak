package orchestrator

import (
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

const eventFeedCoalesceWindow = 10 * time.Minute

func coalesceEventFeed(events []model.Event) []model.Event {
	if len(events) == 0 {
		return nil
	}

	collapsed := make([]model.Event, 0, len(events))
	latestByKey := make(map[string]int)

	for _, event := range events {
		normalized := normalizeEventForDisplay(event)
		if !shouldCoalesceEvent(normalized) {
			collapsed = append(collapsed, normalized)
			continue
		}

		key := eventCoalesceKey(normalized)
		if index, ok := latestByKey[key]; ok {
			current := collapsed[index]
			if current.CreatedAt.Sub(normalized.CreatedAt) <= eventFeedCoalesceWindow {
				if current.CollapsedCount == 0 {
					current.CollapsedCount = 1
					current.CollapsedWindowEnd = timePtr(current.CreatedAt)
				}
				current.CollapsedCount++
				current.CollapsedWindowStart = timePtr(normalized.CreatedAt)
				collapsed[index] = current
				continue
			}
		}

		normalized.CollapsedCount = 1
		normalized.CollapsedWindowStart = timePtr(normalized.CreatedAt)
		normalized.CollapsedWindowEnd = timePtr(normalized.CreatedAt)
		collapsed = append(collapsed, normalized)
		latestByKey[key] = len(collapsed) - 1
	}

	for i := range collapsed {
		if collapsed[i].CollapsedCount <= 1 {
			collapsed[i].CollapsedCount = 0
			collapsed[i].CollapsedWindowStart = nil
			collapsed[i].CollapsedWindowEnd = nil
		}
	}

	return collapsed
}

func normalizeEventForDisplay(event model.Event) model.Event {
	copy := event
	if isPlatformEventType(copy.EventType) {
		copy.Message = stablePlatformEventMessage(copy.EventType, copy.Message)
	}
	return copy
}

func stablePlatformEventMessage(eventType, message string) string {
	lower := strings.ToLower(message)
	switch strings.TrimSpace(eventType) {
	case "platform_oom":
		return "Fly log reported OOM: " + normalizePlatformMessage(lower, message)
	case "platform_disk_full":
		return "Fly log reported disk pressure: " + normalizePlatformMessage(lower, message)
	case "platform_killed":
		return "Fly log reported process kill: " + normalizePlatformMessage(lower, message)
	case "platform_restart":
		return "Fly platform event: " + normalizePlatformMessage(lower, message)
	default:
		return strings.TrimSpace(message)
	}
}

func shouldCoalesceEvent(event model.Event) bool {
	return isPlatformEventType(event.EventType)
}

func eventCoalesceKey(event model.Event) string {
	return strings.Join([]string{
		strings.TrimSpace(event.WorkerID),
		strings.TrimSpace(event.EventType),
		strings.TrimSpace(event.Message),
	}, "|")
}

func timePtr(value time.Time) *time.Time {
	copy := value
	return &copy
}
