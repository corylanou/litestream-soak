package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
)

func TestClassifyPlatformLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		entry     flyapi.AppLogEntry
		wantType  string
		wantMatch string
		wantOK    bool
	}{
		{
			name: "oom",
			entry: flyapi.AppLogEntry{
				Attributes: flyapi.AppLogAttributes{
					Message: "OOM: litestream killed in litestream-soak",
				},
			},
			wantType:  "platform_oom",
			wantMatch: "OOM",
			wantOK:    true,
		},
		{
			name: "disk full",
			entry: flyapi.AppLogEntry{
				Attributes: flyapi.AppLogAttributes{
					Message: "write /data/file.tmp: no space left on device",
				},
			},
			wantType:  "platform_disk_full",
			wantMatch: "disk pressure",
			wantOK:    true,
		},
		{
			name: "database or disk full",
			entry: flyapi.AppLogEntry{
				Attributes: flyapi.AppLogAttributes{
					Message: "time=2026-04-14T17:46:09.148Z level=ERROR msg=\"Write failed\" error=\"database or disk is full\"",
				},
			},
			wantType:  "platform_disk_full",
			wantMatch: "disk pressure",
			wantOK:    true,
		},
		{
			name: "platform restart",
			entry: flyapi.AppLogEntry{
				Attributes: flyapi.AppLogAttributes{
					Message: "machine restarted by platform",
					Meta: flyapi.AppLogMeta{
						Event: flyapi.AppLogMetaEvent{Provider: "vm"},
					},
				},
			},
			wantType:  "platform_restart",
			wantMatch: "platform event",
			wantOK:    true,
		},
		{
			name: "ordinary app log",
			entry: flyapi.AppLogEntry{
				Attributes: flyapi.AppLogAttributes{
					Message: "Replay progress dataset=gharchive rows=10000",
					Meta: flyapi.AppLogMeta{
						Event: flyapi.AppLogMetaEvent{Provider: "app"},
					},
				},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotType, gotMessage, gotOK := classifyPlatformLog(tt.entry)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if gotType != tt.wantType {
				t.Fatalf("type = %q, want %q", gotType, tt.wantType)
			}
			if gotMessage == "" {
				t.Fatalf("message should not be empty")
			}
			if tt.wantMatch != "" && !containsFold(gotMessage, tt.wantMatch) {
				t.Fatalf("message = %q, want match %q", gotMessage, tt.wantMatch)
			}
		})
	}
}

func TestLatestPlatformEvent(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	events := []model.Event{
		{EventType: "verification_failed", CreatedAt: now.Add(-2 * time.Minute)},
		{EventType: "platform_oom", Message: "Fly log reported OOM", CreatedAt: now.Add(-1 * time.Minute)},
		{EventType: "platform_restart", Message: "Fly platform event", CreatedAt: now.Add(-30 * time.Second)},
	}

	latest := latestPlatformEvent(events)
	if latest == nil {
		t.Fatalf("latestPlatformEvent() = nil, want event")
	}
	if latest.EventType != "platform_restart" {
		t.Fatalf("EventType = %q, want platform_restart", latest.EventType)
	}
}

func containsFold(value, needle string) bool {
	return strings.Contains(strings.ToLower(value), strings.ToLower(needle))
}
