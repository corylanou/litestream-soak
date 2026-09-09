package orchestrator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestCurrentVerificationActivity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	current := reporting.WorkerIdentity{WorkerID: "worker", RunID: "current", MachineID: "machine", AppName: "app"}
	old := current
	old.RunID, old.MachineID = "old", "old-machine"
	for _, tc := range []struct {
		name       string
		start      reporting.WorkerIdentity
		completion reporting.WorkerIdentity
		expected   bool
		complete   bool
		offset     time.Duration
		check      string
		active     bool
	}{
		{name: "replacement old start", start: old, expected: true},
		{name: "current start", start: current, expected: true, active: true},
		{name: "old completion arrives later", start: current, completion: old, expected: true, complete: true, active: true},
		{name: "different cycle completion", start: current, completion: current, expected: true, complete: true, offset: time.Minute, active: true},
		{name: "different check completion", start: current, completion: current, expected: true, complete: true, check: "other", active: true},
		{name: "matching completion", start: current, completion: current, expected: true, complete: true},
		{name: "legacy start", expected: true},
		{name: "no expected run", start: current},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			worker := model.Worker{ID: "worker", Name: "worker", FlyMachineID: "machine", AppName: "app", Status: model.WorkerRunning, Source: "main", ProfileName: "low-volume", ProfileConfig: "{}"}
			createTestWorker(t, db, worker)
			if tc.expected {
				if err := db.ExpectWorkerRun(current); err != nil {
					t.Fatal(err)
				}
			}
			payload := struct {
				reporting.WorkerEventPayload
				Attributed bool `json:"attributed"`
			}{reporting.WorkerEventPayload{WorkerIdentity: tc.start, ActiveVerification: &reporting.ActiveVerification{StartedAt: now, CheckType: "integrity", Status: "running"}}, tc.start.RunID != ""}
			data, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.RecordEventAt("worker", "verification_started", "start", string(data), now); err != nil {
				t.Fatal(err)
			}
			if tc.complete {
				end := now.Add(2 * time.Minute)
				check := tc.check
				if check == "" {
					check = "integrity"
				}
				if err := db.RecordVerification(&model.Verification{WorkerID: "worker", Run: tc.completion, Attributed: true, StartedAt: now.Add(tc.offset), CompletedAt: &end, Status: "failed", CheckType: check}); err != nil {
					t.Fatal(err)
				}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"machine"}`))
			}))
			defer server.Close()
			api := NewAPI(db, flyapi.NewClientWithBaseURL("app", "", server.URL), nil, nil, nil, nil)
			summary, err := api.buildWorkerSummary(worker)
			if err != nil {
				t.Fatal(err)
			}
			detail, status, err := api.workerDetail("worker")
			if err != nil || status != http.StatusOK {
				t.Fatalf("detail: %d %v", status, err)
			}
			for name, active := range map[string]*reporting.ActiveVerification{"summary": summary.ActiveVerification, "detail": detail.ActiveVerification} {
				if (active != nil) != tc.active {
					t.Errorf("%s active=%v want %v", name, active, tc.active)
				}
			}
			uncertain := tc.name == "legacy start" || tc.name == "no expected run"
			if summary.VerificationActivityUncertain != uncertain || detail.VerificationActivityUncertain != uncertain {
				t.Fatal("uncertainty mismatch")
			}
			bundle, _, err := api.buildIncidentBundle("worker")
			if err != nil {
				t.Fatal(err)
			}
			if (bundle.ActiveVerification != nil) != tc.active || bundle.VerificationActivityUncertain != uncertain {
				t.Fatal("incident mismatch")
			}
			home, err := api.buildHomePageData(httptest.NewRequest(http.MethodGet, "/", nil))
			if err != nil {
				t.Fatal(err)
			}
			if len(home.Workers) != 1 || (home.Workers[0].ActiveVerification != nil) != tc.active || home.Workers[0].VerificationActivityUncertain != uncertain {
				t.Fatalf("home mismatch: %+v", home.Workers)
			}
			var rendered bytes.Buffer
			if err := uiTemplates.ExecuteTemplate(&rendered, "home_body", home); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(rendered.String(), "current activity unknown") != uncertain {
				t.Fatal("home uncertainty rendering mismatch")
			}
			rendered.Reset()
			if err := uiTemplates.ExecuteTemplate(&rendered, "worker", workerPageData{Incident: bundle}); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(rendered.String(), "Some verification evidence cannot be attributed") != uncertain {
				t.Fatal("worker uncertainty rendering mismatch")
			}
			if len(detail.RecentEvents) != 1 {
				t.Fatal("historical start lost")
			}
			if tc.complete && (len(detail.RecentVerifications) != 1 || detail.LatestFailure == nil) {
				t.Fatal("failure evidence lost")
			}
		})
	}
}

func TestVerificationActivityExpectedIdentityOptionalFields(t *testing.T) {
	worker := model.Worker{ID: "worker", AppName: "app", FlyMachineID: "machine"}
	expected := reporting.WorkerIdentity{WorkerID: "worker", RunID: "run", MachineID: "machine"}
	reported := expected
	reported.AppName = "app"
	reported.ProfileHash = "reported-hash"
	event := verificationStartEvent(t, reported, time.Now().UTC(), true)
	active, _ := activeVerificationFromEvents(worker, &expected, []model.Event{event}, nil)
	if active == nil {
		t.Fatal("expected identity does not store app or profile hash; valid report must remain active")
	}
}

func verificationStartEvent(t *testing.T, identity reporting.WorkerIdentity, at time.Time, attributed bool) model.Event {
	t.Helper()
	payload := struct {
		reporting.WorkerEventPayload
		Attributed bool `json:"attributed"`
	}{reporting.WorkerEventPayload{WorkerIdentity: identity, ActiveVerification: &reporting.ActiveVerification{StartedAt: at, CheckType: "integrity"}}, attributed}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return model.Event{WorkerID: identity.WorkerID, EventType: "verification_started", Details: string(data), CreatedAt: at}
}

func TestVerificationActivityCorrelationBoundaries(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	identity := reporting.WorkerIdentity{WorkerID: "worker", RunID: "run", MachineID: "machine", AppName: "app"}
	worker := model.Worker{ID: "worker", FlyMachineID: "machine", AppName: "app"}
	old := identity
	old.RunID = "old"
	for _, tc := range []struct {
		name      string
		events    []model.Event
		expected  reporting.WorkerIdentity
		active    bool
		uncertain bool
	}{
		{"delayed old event", []model.Event{verificationStartEvent(t, old, now.Add(time.Minute), true), verificationStartEvent(t, identity, now, true)}, identity, true, false},
		{"unattributed", []model.Event{verificationStartEvent(t, identity, now, false)}, identity, false, true},
		{"malformed", []model.Event{{EventType: "verification_started", Details: "{"}}, identity, false, true},
		{"missing active payload", []model.Event{{EventType: "verification_started", Details: "{}"}}, identity, false, true},
		{"missing cycle time", []model.Event{verificationStartEvent(t, identity, time.Time{}, true)}, identity, false, true},
		{"expected physical identity stale", []model.Event{verificationStartEvent(t, old, now, true)}, reporting.WorkerIdentity{WorkerID: "worker", RunID: "old", MachineID: "old-machine"}, false, true},
		{"no start", []model.Event{{EventType: "heartbeat"}}, identity, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			active, uncertain := activeVerificationFromEvents(worker, &tc.expected, tc.events, nil)
			if (active != nil) != tc.active || uncertain != tc.uncertain {
				t.Fatalf("active=%v uncertain=%v", active, uncertain)
			}
		})
	}
	t.Run("arrival order does not override latest cycle", func(t *testing.T) {
		events := []model.Event{verificationStartEvent(t, identity, now.Add(-time.Hour), true), verificationStartEvent(t, identity, now, true)}
		active, _ := activeVerificationFromEvents(worker, &identity, events, nil)
		if active == nil || !active.StartedAt.Equal(now) {
			t.Fatal("selected an older cycle")
		}
	})
	t.Run("legacy completion cannot clear attributed cycle", func(t *testing.T) {
		end := now.Add(time.Minute)
		active, _ := activeVerificationFromEvents(worker, &identity, []model.Event{verificationStartEvent(t, identity, now, true)}, []model.Verification{{WorkerID: "worker", Run: identity, StartedAt: now, CompletedAt: &end, CheckType: "integrity", Status: "passed"}})
		if active == nil {
			t.Fatal("unattributed completion cleared cycle")
		}
	})
	t.Run("stale current start remains unfinished", func(t *testing.T) {
		active, _ := activeVerificationFromEvents(worker, &identity, []model.Event{verificationStartEvent(t, identity, now.Add(-3*time.Hour), true)}, nil)
		if active == nil || !active.Stale {
			t.Fatal("stale current evidence lost")
		}
	})
}

func TestVerificationActivityBeyondDisplayWindows(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "start behind noisy events", true: "completion behind unrelated cycles"}[completed], func(t *testing.T) {
			db := openTestDB(t)
			worker := model.Worker{ID: "worker", Name: "worker", Source: "main", Status: model.WorkerRunning, ProfileName: "low-volume", ProfileConfig: "{}"}
			createTestWorker(t, db, worker)
			identity := reporting.WorkerIdentity{WorkerID: "worker", RunID: "current"}
			if err := db.ExpectWorkerRun(identity); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Second)
			start := verificationStartEvent(t, identity, now, true)
			if err := db.RecordEventAt(worker.ID, start.EventType, "start", start.Details, now); err != nil {
				t.Fatal(err)
			}
			if completed {
				end := now.Add(time.Minute)
				if err := db.RecordVerification(&model.Verification{WorkerID: worker.ID, Run: identity, Attributed: true, StartedAt: now, CompletedAt: &end, CheckType: "integrity", Status: "failed"}); err != nil {
					t.Fatal(err)
				}
			}
			for i := 1; i <= 60; i++ {
				if err := db.RecordEventAt(worker.ID, "profile_upload_error", "retry", "{}", now.Add(time.Duration(i)*time.Second)); err != nil {
					t.Fatal(err)
				}
				other := identity
				other.RunID = "old"
				end := now.Add(time.Duration(i) * time.Minute)
				if err := db.RecordVerification(&model.Verification{WorkerID: worker.ID, Run: other, Attributed: true, StartedAt: end, CompletedAt: &end, CheckType: "integrity", Status: "passed", Passed: true}); err != nil {
					t.Fatal(err)
				}
			}
			if completed {
				if err := db.RecordEventAt(worker.ID, start.EventType, "delayed start", start.Details, now.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			api := NewAPI(db, nil, nil, nil, nil, nil)
			summary, err := api.buildWorkerSummary(worker)
			if err != nil {
				t.Fatal(err)
			}
			detail, _, err := api.workerDetail(worker.ID)
			if err != nil {
				t.Fatal(err)
			}
			if (summary.ActiveVerification != nil) == completed || (detail.ActiveVerification != nil) == completed {
				t.Fatalf("completed=%v summary=%v detail=%v", completed, summary.ActiveVerification, detail.ActiveVerification)
			}
			if summary.VerificationActivityUncertain || detail.VerificationActivityUncertain {
				t.Fatal("noise must not make known activity uncertain")
			}
			if len(detail.RecentEvents) != 40 || len(detail.RecentVerifications) != 20 {
				t.Fatal("display limits changed")
			}
		})
	}
}
