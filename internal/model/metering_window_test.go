package model

import (
	"testing"
	"time"
)

func TestSyncMeteringWindowsOpensAndClosesPerSource(t *testing.T) {
	db := seriesTestDB(t)
	start := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

	if err := db.SyncMeteringWindows([]string{"main"}, start); err != nil {
		t.Fatalf("SyncMeteringWindows(main) error = %v", err)
	}
	if err := db.SyncMeteringWindows([]string{"main"}, start.Add(time.Hour)); err != nil {
		t.Fatalf("SyncMeteringWindows(main) again error = %v", err)
	}
	windows, err := db.ListMeteringWindows("main")
	if err != nil {
		t.Fatalf("ListMeteringWindows() error = %v", err)
	}
	if len(windows) != 1 || !windows[0].Active || !windows[0].StartedAt.Equal(start) {
		t.Fatalf("windows = %+v, want one active window since %s", windows, start)
	}
	if !windows[0].Contains(start.Add(time.Minute)) || windows[0].Contains(start.Add(-time.Minute)) {
		t.Fatalf("active window Contains() wrong for %+v", windows[0])
	}

	end := start.Add(24 * time.Hour)
	if err := db.SyncMeteringWindows(nil, end); err != nil {
		t.Fatalf("SyncMeteringWindows(nil) error = %v", err)
	}
	windows, err = db.ListMeteringWindows("main")
	if err != nil {
		t.Fatalf("ListMeteringWindows() error = %v", err)
	}
	if len(windows) != 1 || windows[0].Active || windows[0].EndedAt == nil || !windows[0].EndedAt.Equal(end) {
		t.Fatalf("windows = %+v, want one window ended at %s", windows, end)
	}
	if windows[0].Contains(end.Add(time.Minute)) || !windows[0].Contains(end.Add(-time.Minute)) {
		t.Fatalf("closed window Contains() wrong for %+v", windows[0])
	}

	other, err := db.ListMeteringWindows("pr-1")
	if err != nil {
		t.Fatalf("ListMeteringWindows(pr-1) error = %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("pr-1 windows = %+v, want none", other)
	}
}
