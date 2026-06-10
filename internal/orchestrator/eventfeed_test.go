package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestCoalesceEventFeedMergesHistoricalPlatformSpam(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 18, 7, 41, 0, time.UTC)
	events := []model.Event{
		{
			WorkerID:  "worker-main-burst-vol",
			EventType: "platform_disk_full",
			Message:   `Fly log reported disk pressure: time=2026-04-14T18:07:41.017Z level=ERROR msg="Write failed" error="database or disk is full"`,
			Details:   `{"line":3}`,
			CreatedAt: now,
		},
		{
			WorkerID:  "worker-main-burst-vol",
			EventType: "platform_disk_full",
			Message:   `Fly log reported disk pressure: time=2026-04-14T18:07:11.017Z level=ERROR msg="Write failed" error="database or disk is full"`,
			Details:   `{"line":2}`,
			CreatedAt: now.Add(-30 * time.Second),
		},
		{
			WorkerID:  "worker-main-burst-vol",
			EventType: "platform_disk_full",
			Message:   `Fly log reported disk pressure: time=2026-04-14T18:06:41.017Z level=ERROR msg="Write failed" error="database or disk is full"`,
			Details:   `{"line":1}`,
			CreatedAt: now.Add(-1 * time.Minute),
		},
		{
			WorkerID:  "worker-main-burst-vol",
			EventType: "verification_failed",
			Message:   "wait for sync: dial unix /data/litestream.sock: connect: connection refused",
			CreatedAt: now.Add(-90 * time.Second),
		},
	}

	collapsed := coalesceEventFeed(events)
	if len(collapsed) != 2 {
		t.Fatalf("len(collapsed) = %d, want 2", len(collapsed))
	}

	platform := collapsed[0]
	if platform.Message != "Fly log reported disk pressure: database or disk is full" {
		t.Fatalf("platform.Message = %q", platform.Message)
	}
	if platform.CollapsedCount != 3 {
		t.Fatalf("platform.CollapsedCount = %d, want 3", platform.CollapsedCount)
	}
	if platform.CollapsedWindowStart == nil || !platform.CollapsedWindowStart.Equal(now.Add(-1*time.Minute)) {
		t.Fatalf("platform.CollapsedWindowStart = %v, want %s", platform.CollapsedWindowStart, now.Add(-1*time.Minute))
	}
	if platform.CollapsedWindowEnd == nil || !platform.CollapsedWindowEnd.Equal(now) {
		t.Fatalf("platform.CollapsedWindowEnd = %v, want %s", platform.CollapsedWindowEnd, now)
	}

	if collapsed[1].EventType != "verification_failed" {
		t.Fatalf("collapsed[1].EventType = %q, want verification_failed", collapsed[1].EventType)
	}
	if collapsed[1].CollapsedCount != 0 {
		t.Fatalf("verification event CollapsedCount = %d, want 0", collapsed[1].CollapsedCount)
	}
}

func TestCoalesceEventFeedAscendingOrderDoesNotCoalesceFarApartEvents(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 14, 18, 7, 41, 0, time.UTC)
	older := now.Add(-20 * time.Minute)

	events := []model.Event{
		{
			WorkerID:  "worker-a",
			EventType: "platform_oom",
			Message:   "out of memory",
			CreatedAt: older,
		},
		{
			WorkerID:  "worker-a",
			EventType: "platform_oom",
			Message:   "out of memory",
			CreatedAt: now,
		},
	}

	collapsed := coalesceEventFeed(events)
	if len(collapsed) != 2 {
		t.Fatalf("coalesceEventFeed() len = %d, want %d", len(collapsed), 2)
	}
}

func TestHandleListEventsDefaultsToCollapsedButSupportsRaw(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-burst-vol",
		Name:          "worker-main-burst-vol",
		Status:        model.WorkerDegraded,
		Source:        "main",
		GitSHA:        "sha-burst",
		ProfileName:   "burst-volume",
		ProfileConfig: "{}",
	})

	base := time.Date(2026, time.April, 14, 18, 7, 41, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if err := db.RecordEventAt(
			"worker-main-burst-vol",
			"platform_disk_full",
			`Fly log reported disk pressure: time=2026-04-14T18:07:41.017Z level=ERROR msg="Write failed" error="database or disk is full"`,
			`{"sample":true}`,
			base.Add(-time.Duration(i)*time.Second),
		); err != nil {
			t.Fatalf("RecordEventAt() error = %v", err)
		}
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)

	collapsedRequest := httptest.NewRequest(http.MethodGet, "/api/events?worker_id=worker-main-burst-vol", nil)
	collapsedRecorder := httptest.NewRecorder()
	api.handleListEvents(collapsedRecorder, collapsedRequest)
	if collapsedRecorder.Code != http.StatusOK {
		t.Fatalf("collapsed status = %d, want 200", collapsedRecorder.Code)
	}

	var collapsed []model.Event
	if err := json.NewDecoder(collapsedRecorder.Body).Decode(&collapsed); err != nil {
		t.Fatalf("decode collapsed response: %v", err)
	}
	if len(collapsed) != 1 {
		t.Fatalf("len(collapsed) = %d, want 1", len(collapsed))
	}
	if collapsed[0].CollapsedCount != 3 {
		t.Fatalf("collapsed[0].CollapsedCount = %d, want 3", collapsed[0].CollapsedCount)
	}
	if collapsed[0].Message != "Fly log reported disk pressure: database or disk is full" {
		t.Fatalf("collapsed[0].Message = %q", collapsed[0].Message)
	}

	rawRequest := httptest.NewRequest(http.MethodGet, "/api/events?worker_id=worker-main-burst-vol&raw=1", nil)
	rawRecorder := httptest.NewRecorder()
	api.handleListEvents(rawRecorder, rawRequest)
	if rawRecorder.Code != http.StatusOK {
		t.Fatalf("raw status = %d, want 200", rawRecorder.Code)
	}

	var raw []model.Event
	if err := json.NewDecoder(rawRecorder.Body).Decode(&raw); err != nil {
		t.Fatalf("decode raw response: %v", err)
	}
	if len(raw) != 3 {
		t.Fatalf("len(raw) = %d, want 3", len(raw))
	}
}
