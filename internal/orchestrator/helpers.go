package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func mustJSON(v any) string {
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(body)
}

func jsonCompact(v any) string {
	body, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(body)
}

func firstMeaningfulLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}

func formatTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "unknown"
	}
	return t.Format(time.RFC3339)
}

func valueOrUnknown(v string) string {
	if strings.TrimSpace(v) == "" {
		return "unknown"
	}
	return v
}
