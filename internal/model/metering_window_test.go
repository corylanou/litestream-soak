package model

import (
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
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

	other, err := db.ListMeteringWindows("pr-1")
	if err != nil {
		t.Fatalf("ListMeteringWindows(pr-1) error = %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("pr-1 windows = %+v, want none", other)
	}
}

func TestListVerificationsExcludingMeteringRunsSkipsMeteredRows(t *testing.T) {
	db := seriesTestDB(t)
	seriesTestWorker(t, db, "w-mixed", "main")
	base := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	record := func(at time.Time, profileConfig string) {
		t.Helper()
		completed := at.Add(time.Second)
		if err := db.RecordVerification(&Verification{WorkerID: "w-mixed", StartedAt: at, CompletedAt: &completed, Status: "failed", CheckType: "integrity", Run: reporting.WorkerIdentity{WorkerID: "w-mixed", RunID: "run", ProfileConfig: profileConfig}}); err != nil {
			t.Fatal(err)
		}
	}
	record(base, `{"load_mode":"synthetic"}`)
	record(base.Add(time.Minute), `not json`)
	record(base.Add(2*time.Minute), ``)
	for i, malformed := range []string{`{"metering_window":"true"}`, `{"metering_window":1}`, `{"metering_window":false}`, `{"metering_window":null}`, `[true]`} {
		record(base.Add(-time.Duration(i+1)*time.Minute), malformed)
	}
	for i := range 30 {
		record(base.Add(time.Hour+time.Duration(i)*time.Minute), `{"s3_fault_proxy_mode":"observe","metering_window":true}`)
	}

	got, err := db.ListVerificationsExcludingMeteringRuns("w-mixed", 2)
	if err != nil {
		t.Fatalf("ListVerificationsExcludingMeteringRuns() error = %v", err)
	}
	if len(got) != 2 || !got[0].StartedAt.Equal(base.Add(2*time.Minute)) || !got[1].StartedAt.Equal(base.Add(time.Minute)) {
		t.Fatalf("got %d rows %+v, want the two newest non-metered rows", len(got), got)
	}
	all, err := db.ListVerificationsExcludingMeteringRuns("w-mixed", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 8 {
		t.Fatalf("got %d rows, want 8 rows without a boolean true metering marker", len(all))
	}
}
