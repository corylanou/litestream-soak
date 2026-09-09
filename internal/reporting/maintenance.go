package reporting

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type MaintenanceEvidence struct {
	Epoch       string    `json:"maintenance_epoch,omitempty"`
	StartedAt   time.Time `json:"maintenance_observer_started_at,omitempty"`
	Complete    bool      `json:"maintenance_observer_complete"`
	Snapshots   uint64    `json:"maintenance_snapshots_total,omitempty"`
	Compactions uint64    `json:"maintenance_compactions_total,omitempty"`
	Retentions  uint64    `json:"maintenance_retentions_total,omitempty"`
	Errors      uint64    `json:"litestream_log_errors_total,omitempty"`
	LastError   string    `json:"litestream_last_log_error,omitempty"`
}

var maintenanceTextField = regexp.MustCompile(`(?:^|\s)(msg|size|deleted_count)=(?:"([^"]*)"|(\S+))`)
var maintenanceInfoLevel = regexp.MustCompile(`(?:^|\s)level=INFO(?:\s|$)|"level"\s*:\s*"INFO"`)
var maintenanceErrorLevel = regexp.MustCompile(`(?:^|\s)level=(?:ERROR|WARN)(?:\s|$)|"level"\s*:\s*"(?:ERROR|WARN)"`)

var maintenanceKnownLevel = regexp.MustCompile(`(?:^|\s)level=(?:DEBUG|INFO|WARN|ERROR)(?:\s|$)|"level"\s*:\s*"(?:DEBUG|INFO|WARN|ERROR)"`)
var maintenanceAdverseMessage = regexp.MustCompile(`(?i)\b(retry|retrying|retried|retries|failed|failure|failures|timeout|timed out|deadline exceeded|recovered|corrupt|corruption|panic|unavailable)\b`)

func (e *MaintenanceEvidence) ObserveLine(line string) {
	if strings.TrimSpace(line) == "" {
		return
	}
	if !maintenanceKnownLevel.MatchString(line) || (strings.HasPrefix(strings.TrimSpace(line), "{") && !json.Valid([]byte(line))) {
		e.Complete = false
	}
	if maintenanceErrorLevel.MatchString(line) || maintenanceAdverseMessage.MatchString(line) {
		e.Errors++
		e.LastError = strings.ReplaceAll(line, "\x00", "")
		if len(e.LastError) > 1000 {
			e.LastError = e.LastError[:1000] + "...truncated"
		}
		return
	}
	if !maintenanceInfoLevel.MatchString(line) {
		return
	}
	var message string
	var size, deleted float64
	if strings.HasPrefix(strings.TrimSpace(line), "{") {
		var fields struct {
			Message string  `json:"msg"`
			Size    float64 `json:"size"`
			Deleted float64 `json:"deleted_count"`
		}
		if json.Unmarshal([]byte(line), &fields) != nil {
			e.Complete = false
			return
		}
		message = fields.Message
		size = fields.Size
		deleted = fields.Deleted
	} else {
		for _, match := range maintenanceTextField.FindAllStringSubmatch(line, -1) {
			value := match[2]
			if value == "" {
				value = match[3]
			}
			switch match[1] {
			case "msg":
				message = value
			case "size":
				size, _ = strconv.ParseFloat(value, 64)
			case "deleted_count":
				deleted, _ = strconv.ParseFloat(value, 64)
			}
		}
	}
	switch message {
	case "snapshot complete":
		if size > 0 {
			e.Snapshots++
		}
	case "compaction complete":
		if size > 0 {
			e.Compactions++
		}
	case "l0 retention enforced":
		if deleted > 0 {
			e.Retentions++
		}
	}
}
