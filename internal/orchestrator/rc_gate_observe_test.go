package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestRegisterRCGateObserveProxyRoundTrip(t *testing.T) {
	now := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	db := openTestDB(t)
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	request := RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateRef: "v0.5.0-rc1",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "24h",
		Profiles:     []string{gateLargeDBProfile},
		SourceSuffix: "obsv1",
		ObserveProxy: true,
	}
	gate, created, err := manager.RegisterRCGate(request, now)
	if err != nil || !created || !gate.ObserveProxy {
		t.Fatalf("RegisterRCGate() = %+v, %v, %v", gate, created, err)
	}
	stored, err := db.GetRCGate(gate.ID)
	if err != nil || stored == nil || !stored.ObserveProxy {
		t.Fatalf("stored gate = %+v, %v", stored, err)
	}
	again, created, err := manager.RegisterRCGate(request, now.Add(time.Minute))
	if err != nil || created || again.ID != gate.ID {
		t.Fatalf("repeat RegisterRCGate() = %d, %v, %v, want existing gate %d", again.ID, created, err, gate.ID)
	}
	withoutProxy := request
	withoutProxy.ObserveProxy = false
	if _, _, err := manager.RegisterRCGate(withoutProxy, now.Add(time.Minute)); !errors.Is(err, model.ErrRCGateSourceActive) {
		t.Fatalf("RegisterRCGate() without observe_proxy error = %v, want %v", err, model.ErrRCGateSourceActive)
	}

	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		spec, err := manager.rcGateFleetForSource(source, testGateSoakSHA, litestreamSHA, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("rcGateFleetForSource(%s) error = %v", source, err)
		}
		if len(spec.Workers) != 1 || spec.Workers[0].ProfileName != gateLargeDBProfile {
			t.Fatalf("%s fleet = %+v", source, spec.Workers)
		}
		for _, worker := range spec.Workers {
			if !worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != "observe" || !worker.Workload.S3ObserveProxyRequired {
				t.Fatalf("%s proxy = %v/%q required=%v, want enabled observe required", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode, worker.Workload.S3ObserveProxyRequired)
			}
			env := manager.workerEnv(model.Worker{ID: worker.WorkerID}, worker.Workload)
			if env["S3_OBSERVE_PROXY_REQUIRED"] != "true" || env["S3_FAULT_PROXY_MODE"] != "observe" {
				t.Fatalf("%s env lacks the required observe proxy: %v", worker.Name, env)
			}
			if worker.Workload.MeteringWindow || strings.Contains(worker.Workload.JSON(), `"metering_window"`) {
				t.Fatalf("%s carries the metering window marker: %s", worker.Name, worker.Workload.JSON())
			}
		}
	}
	if gate.CandidateSource != testGateCandidate+"-obsv1" || gate.BaselineSource != testGateBaseline+"-obsv1" {
		t.Fatalf("sources = %s vs %s", gate.CandidateSource, gate.BaselineSource)
	}
}

func TestRegisterRCGateDefaultsToDirectReplication(t *testing.T) {
	now := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	db := openTestDB(t)
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	gate, _, err := manager.RegisterRCGate(RCGateRequest{Repository: "benbjohnson/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA, Duration: "24h"}, now)
	if err != nil || gate.ObserveProxy {
		t.Fatalf("RegisterRCGate() = %+v, %v", gate, err)
	}
	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		spec, err := manager.rcGateFleetForSource(source, testGateSoakSHA, litestreamSHA, now.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if len(spec.Workers) == 0 {
			t.Fatalf("%s fleet is empty", source)
		}
		for _, worker := range spec.Workers {
			if worker.Workload.S3FaultProxyEnabled || worker.Workload.S3FaultProxyMode != "" || worker.Workload.MeteringWindow || worker.Workload.S3ObserveProxyRequired {
				t.Fatalf("%s proxy = %v/%q metering=%v, want direct replication", worker.Name, worker.Workload.S3FaultProxyEnabled, worker.Workload.S3FaultProxyMode, worker.Workload.MeteringWindow)
			}
		}
	}
}

func TestApplyGateWorkloadObserveProxyKeepsFaultModes(t *testing.T) {
	spec := func() FleetSpec {
		return FleetSpec{Workers: []DesiredWorker{
			{Name: "fault", Workload: workload.Config{S3FaultProxyEnabled: true, S3FaultProxyMode: "source-get-reset", S3FaultProxyFailFirstAttempts: 3}},
			{Name: "plain", Workload: workload.Config{LoadMode: "synthetic"}},
			{Name: "already-observing", Workload: workload.Config{S3FaultProxyEnabled: true, S3FaultProxyMode: "observe"}},
		}}
	}
	applied := applyGateWorkload(model.RCGate{ObserveProxy: true}, spec())
	if fault := applied.Workers[0].Workload; !fault.S3FaultProxyEnabled || fault.S3FaultProxyMode != "source-get-reset" || fault.S3FaultProxyFailFirstAttempts != 3 || fault.S3ObserveProxyRequired {
		t.Fatalf("fault worker = %+v, want its fault mode kept", fault)
	}
	if plain := applied.Workers[1].Workload; !plain.S3FaultProxyEnabled || plain.S3FaultProxyMode != "observe" || !plain.S3ObserveProxyRequired || plain.MeteringWindow {
		t.Fatalf("plain worker = %+v, want required observe proxy without metering window", plain)
	}
	if observing := applied.Workers[2].Workload; !observing.S3FaultProxyEnabled || observing.S3FaultProxyMode != "observe" || !observing.S3ObserveProxyRequired {
		t.Fatalf("already-observing worker = %+v, want the proxy made required", observing)
	}
	unchanged := applyGateWorkload(model.RCGate{}, spec())
	if plain := unchanged.Workers[1].Workload; plain.S3FaultProxyEnabled || plain.S3FaultProxyMode != "" || plain.S3ObserveProxyRequired {
		t.Fatalf("plain worker without opt-in = %+v, want direct replication", plain)
	}
	if observing := unchanged.Workers[2].Workload; observing.S3ObserveProxyRequired {
		t.Fatalf("already-observing worker without opt-in = %+v, want fail-open proxy", observing)
	}
}

func TestRegisterRCGateObserveProxyRejectsMeteringWindowSources(t *testing.T) {
	now := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	request := RCGateRequest{Repository: "benbjohnson/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA, Duration: "24h", ObserveProxy: true}
	for name, listed := range map[string]string{
		"candidate listed": "main," + testGateCandidate,
		"baseline listed":  testGateBaseline,
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("SOAK_METERING_WINDOW_SOURCES", listed)
			db := openTestDB(t)
			manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
			_, _, err := manager.RegisterRCGate(request, now)
			if !errors.Is(err, errRCGateInvalid) || !strings.Contains(err.Error(), "metering window source") {
				t.Fatalf("RegisterRCGate() error = %v, want %v about the metering window", err, errRCGateInvalid)
			}
			gates, err := db.ListRCGates()
			if err != nil || len(gates) != 0 {
				t.Fatalf("gates after rejected registration = %d, %v", len(gates), err)
			}

			api := NewAPI(db, nil, nil, nil, manager, nil)
			mux := http.NewServeMux()
			api.RegisterRoutes(mux)
			body, _ := json.Marshal(request)
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/admin/rc-gates", bytes.NewReader(body)))
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("register status = %d body %s, want 400", recorder.Code, recorder.Body.String())
			}

			plain := request
			plain.ObserveProxy = false
			if _, created, err := manager.RegisterRCGate(plain, now); err != nil || !created {
				t.Fatalf("RegisterRCGate() without observe_proxy = %v, %v, want the metering gate to register", created, err)
			}
		})
	}
}

func TestAttachMeteringWindowsHonorsUntil(t *testing.T) {
	hour := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	ended := hour.Add(30 * time.Minute)
	windows := []model.MeteringWindow{
		{Source: "before", StartedAt: hour.Add(-3 * time.Hour), EndedAt: new(hour.Add(-2 * time.Hour))},
		{Source: "overlapping", StartedAt: hour.Add(-time.Hour), EndedAt: &ended},
		{Source: "inside", StartedAt: hour.Add(20 * time.Minute)},
		{Source: "after", StartedAt: hour.Add(time.Hour)},
		{Source: "far", StartedAt: hour.Add(5 * time.Hour)},
	}
	names := func(sources []model.CostMeterSource) []string {
		var out []string
		for _, source := range sources {
			out = append(out, source.Source)
		}
		return out
	}
	unbounded := attachMeteringWindows(nil, windows, hour, time.Time{})
	if got := names(unbounded); !slices.Equal(got, []string{"after", "far", "inside", "overlapping"}) {
		t.Fatalf("unbounded = %v", got)
	}
	oneHour := attachMeteringWindows(nil, windows, hour, hour.Add(time.Hour))
	if got := names(oneHour); !slices.Equal(got, []string{"inside", "overlapping"}) {
		t.Fatalf("[since, until) = %v", got)
	}
	insideHour := attachMeteringWindows(nil, windows, hour, hour.Add(10*time.Minute))
	if got := names(insideHour); !slices.Equal(got, []string{"inside", "overlapping"}) {
		t.Fatalf("until inside the hour = %v, want the whole hour's windows", got)
	}
}

func TestRCGateAPIRoundTripsObserveProxy(t *testing.T) {
	db := openTestDB(t)
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	api := NewAPI(db, nil, nil, nil, manager, nil)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	body, _ := json.Marshal(RCGateRequest{Repository: "benbjohnson/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA, Duration: "24h", ObserveProxy: true})
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/admin/rc-gates", bytes.NewReader(body)))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("register status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var registered map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &registered); err != nil {
		t.Fatal(err)
	}
	if registered["observe_proxy"] != true {
		t.Fatalf("registered gate = %v, want observe_proxy true", registered)
	}

	recorder = httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/rc-gates", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("ledger status = %d body %s", recorder.Code, recorder.Body.String())
	}
	var ledger struct {
		Gates []map[string]any `json:"gates"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &ledger); err != nil {
		t.Fatal(err)
	}
	if len(ledger.Gates) != 1 || ledger.Gates[0]["observe_proxy"] != true {
		t.Fatalf("ledger = %v, want observe_proxy true", ledger.Gates)
	}
}
