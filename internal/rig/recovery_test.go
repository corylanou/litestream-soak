package rig

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecoveryScheduleBoundaries(t *testing.T) {
	s := RecoverySchedule(time.Second, 600)
	for _, tt := range []struct {
		at   time.Duration
		name string
		rate int
	}{
		{0, "idle", 0}, {time.Second, "offline", 600}, {2 * time.Second, "latency", 600}, {3 * time.Second, "throttle", 600}, {4 * time.Second, "drain", 600}, {5 * time.Second, "complete", 0},
	} {
		p := s.At(tt.at)
		if p.Name != tt.name || p.WriteRate != tt.rate {
			t.Fatalf("at %s: %+v", tt.at, p)
		}
	}
}

func TestRecoveryFaultsRetainAttemptsAfterReconnect(t *testing.T) {
	elapsed := time.Second
	s := RecoverySchedule(time.Second, 10)
	h := NewRecoveryHandler(s, func() time.Duration { return elapsed }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, step := range []struct {
		at     time.Duration
		status int
	}{{time.Second, 503}, {3 * time.Second, 429}, {4 * time.Second, 204}} {
		elapsed = step.at
		r := httptest.NewRequest("PUT", "http://example.test/object?secret=hidden", nil)
		r.Header.Set("Amz-Sdk-Request", "attempt=2; max=3")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != step.status {
			t.Fatalf("got %d want %d", w.Code, step.status)
		}
	}
	events := h.Events()
	if len(events) != 3 || events[0].Status != 503 || events[2].Status != 204 || events[0].SDKRequest != "attempt=2; max=3" || events[0].Path != "/object" {
		t.Fatalf("events: %+v", events)
	}
	if !h.Engaged("offline") || !h.Engaged("throttle") || h.Engaged("latency") {
		t.Fatal("incorrect engagement")
	}
	events[0].Status = 0
	if h.Events()[0].Status != 503 {
		t.Fatal("mutable evidence")
	}
}

func TestRecoveryLatencyHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h := NewRecoveryHandler(RecoverySchedule(time.Second, 1), func() time.Duration { return 2 * time.Second }, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("forwarded canceled request") }))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "http://example.test", nil).WithContext(ctx))
	if len(h.Events()) != 1 || h.Events()[0].Error == "" {
		t.Fatal("lost cancellation")
	}
}

func TestRecoverySummaryPreservesFailures(t *testing.T) {
	e := RecoveryMeasurements{}
	e.Observe(RecoverySample{ElapsedSeconds: 1, CommittedRows: 100, ReplicatedRows: 20, DiskBytes: 1000, MemoryBytes: 2000})
	e.Observe(RecoverySample{ElapsedSeconds: 3, CommittedRows: 120, ReplicatedRows: 120, DiskBytes: 500, MemoryBytes: 1000})
	if e.MaxLagRows != 80 || e.MaxDiskBytes != 1000 || e.MaxMemoryBytes != 2000 || e.CatchUpRowsPerSecond() != 50 {
		t.Fatalf("%+v rate=%f", e, e.CatchUpRowsPerSecond())
	}
	if (RecoveryMeasurements{}).CatchUpRowsPerSecond() != 0 {
		t.Fatal("empty rate")
	}
}

func TestRecoveryRequestJournalIncludesInjectedFailures(t *testing.T) {
	h := NewRecoveryHandler(RecoverySchedule(time.Second, 1), func() time.Duration { return time.Second }, http.NotFoundHandler())
	var saved []RecoveryRequest
	h.OnRequest = func(e RecoveryRequest) { saved = append(saved, e) }
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("PUT", "http://example.test/object", nil))
	if len(saved) != 1 || saved[0].Status != 503 {
		t.Fatalf("journal: %+v", saved)
	}
}

func TestRecoveryNetDrainRateAccountsForContinuingWrites(t *testing.T) {
	m := RecoveryMeasurements{}
	m.Observe(RecoverySample{ElapsedSeconds: 1, CommittedRows: 100, ReplicatedRows: 20})
	m.Observe(RecoverySample{ElapsedSeconds: 3, CommittedRows: 120, ReplicatedRows: 120})
	if m.NetDrainRowsPerSecond() != 40 {
		t.Fatalf("net rate=%f", m.NetDrainRowsPerSecond())
	}
	if (RecoveryMeasurements{}).NetDrainRowsPerSecond() != 0 {
		t.Fatal("empty net rate")
	}
}

func TestBacklogVerdictRequiresExposureAndBoundaries(t *testing.T) {
	for _, tt := range []struct {
		engaged, validated, writes bool
		failures                   int
		want                       string
	}{
		{true, true, true, 0, "scenario_success"},
		{true, true, true, 1, "recovered_with_incidents"},
		{false, true, true, 1, "inconclusive"},
		{true, false, true, 0, "inconclusive"},
		{true, true, false, 0, "inconclusive"},
	} {
		if got := BacklogVerdict(tt.engaged, tt.validated, tt.writes, tt.failures); got != tt.want {
			t.Fatalf("%+v: %s", tt, got)
		}
	}
}

func TestDrainEvidenceRejectsGrowingBacklog(t *testing.T) {
	for _, tt := range []struct {
		name                        string
		endCommitted, endReplicated int64
		want                        bool
	}{
		{"backlog grows", 160, 70, false}, {"no continuing writes", 100, 100, false}, {"backlog shrinks", 120, 100, true}, {"caught up", 120, 120, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := RecoveryMeasurements{}
			m.Observe(RecoverySample{ElapsedSeconds: 1, CommittedRows: 100, ReplicatedRows: 20})
			m.Observe(RecoverySample{ElapsedSeconds: 2, CommittedRows: tt.endCommitted, ReplicatedRows: tt.endReplicated})
			if m.DrainedWhileWriting() != tt.want {
				t.Fatalf("%+v", m)
			}
		})
	}
}

func TestRequestSummaryRetainsSelfHealedProviderFailure(t *testing.T) {
	events := []RecoveryRequest{{Status: 503, Injected: true}, {Status: 429, Injected: true}, {Status: 408, SDKRequest: "attempt=1; max=3"}, {Status: 200, SDKRequest: "attempt=2; max=3"}, {Error: "context canceled"}}
	s := SummarizeRecoveryRequests(events)
	if s.InjectedFailures != 2 || s.UnexpectedHTTPFailures != 1 || s.SDKRetries != 1 || s.TransportFailures != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestRecoveryLimitsUseActualFixtureMount(t *testing.T) {
	mounts := "tmpfs /data tmpfs rw,size=65536k 0 0\n/dev/root / ext4 rw 0 0\ntmpfs /other tmpfs rw,size=32768k 0 0\n"
	for _, tt := range []struct{ dir, want string }{{"/data/work", "/data"}, {"/other/work", "/other"}, {"/database", "/"}} {
		l := RecoveryLimits(tt.dir, "50000 100000", "134217728", mounts)
		if l["mount"] != tt.want || l["cpu"] != "50000 100000" || l["memory"] != "134217728" {
			t.Fatalf("%+v", l)
		}
	}
	l := RecoveryLimits("/work", "max 100000", "max", "")
	if !strings.Contains(l["cpu"], "unenforced") || !strings.Contains(l["memory"], "unenforced") || !strings.Contains(l["disk"], "unsupported") {
		t.Fatalf("%+v", l)
	}
}

func TestDrainEvidenceAcceptsCaughtUpBoundary(t *testing.T) {
	m := RecoveryMeasurements{}
	m.Observe(RecoverySample{ElapsedSeconds: 1, CommittedRows: 100, ReplicatedRows: 100})
	m.Observe(RecoverySample{ElapsedSeconds: 2, CommittedRows: 120, ReplicatedRows: 120})
	if !m.DrainedWhileWriting() {
		t.Fatal("rejected caught-up boundary under continuing writes")
	}
}
