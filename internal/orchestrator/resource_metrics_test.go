package orchestrator

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestResourceMetricsRespectObservationStatus(t *testing.T) {
	metrics := NewControlMetrics(openTestDB(t))
	worker := model.Worker{ID: "resource-status", ProfileName: "test", Source: "test"}
	labels := workerMetricLabels(worker)
	for _, status := range []string{"fresh", "stale", "unavailable", "unsupported", ""} {
		t.Run(status, func(t *testing.T) {
			worker.LastRuntimeJSON = mustJSON(reporting.RuntimePayload{
				LitestreamProcess:  reporting.ProcessObservation{Status: status, CollectedAt: time.Now()},
				WorkerProcess:      reporting.ProcessObservation{Status: status, CollectedAt: time.Now()},
				LocalStateStatus:   status,
				LitestreamRSSBytes: 10, WorkerRSSBytes: 20, LitestreamDirSizeBytes: 30,
			})
			metrics.observeWorker(worker)
			for _, gauge := range []float64{
				testutil.ToFloat64(controlWorkerLitestreamRSSBytes.WithLabelValues(labels...)),
				testutil.ToFloat64(controlWorkerRSSBytes.WithLabelValues(labels...)),
				testutil.ToFloat64(controlWorkerLitestreamLocalStateSize.WithLabelValues(labels...)),
			} {
				if status == "fresh" && math.IsNaN(gauge) || status != "fresh" && !math.IsNaN(gauge) {
					t.Fatalf("status=%q value=%v", status, gauge)
				}
			}
		})
	}
}

func TestMissingResourceRuntimeClearsMetrics(t *testing.T) {
	worker := model.Worker{ID: "missing-resource", ProfileName: "test", Source: "test"}
	labels := workerMetricLabels(worker)
	publishControlResourceMetrics(labels, nil)
	if !math.IsNaN(testutil.ToFloat64(controlWorkerLitestreamRSSBytes.WithLabelValues(labels...))) {
		t.Fatal("missing runtime appears measured")
	}
	for _, resource := range []string{"litestream", "worker", "local_state"} {
		resourceLabels := append(append([]string(nil), labels...), resource)
		if testutil.ToFloat64(controlResourceObservationStatus.WithLabelValues(resourceLabels...)) != 0 || !math.IsNaN(testutil.ToFloat64(controlResourceObservationTime.WithLabelValues(resourceLabels...))) {
			t.Fatal("missing validity metadata")
		}
	}
}

func TestResourceHeartbeatAttributionCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		registered, metadata bool
	}{
		{"registered", true, true}, {"unregistered", false, true}, {"legacy", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			worker := model.Worker{ID: "resource-" + tc.name, Name: "resource", Source: "main", ProfileName: "low-volume", ProfileConfig: "{}", Status: model.WorkerRunning}
			createTestWorker(t, db, worker)
			identity := reporting.WorkerIdentity{WorkerID: worker.ID, Name: worker.Name, Source: worker.Source, ProfileName: worker.ProfileName, ProfileConfig: worker.ProfileConfig}
			if tc.registered {
				worker.FlyMachineID = "resource-machine"
				identity = fixtureRun(worker, model.Deployment{ID: 7, Source: "main", GitSHA: "soak", LitestreamSHA: "litestream", WorkloadSHA: "generator"})
				if err := db.ExpectWorkerRun(identity); err != nil {
					t.Fatal(err)
				}
			}
			runtime := reporting.RuntimePayload{DBSizeBytes: 128, LitestreamRSSBytes: 10, LitestreamSnapshotHealthy: true, SnapshotCollectedAt: time.Now().UTC()}
			if tc.metadata {
				runtime.LitestreamProcess = reporting.ProcessObservation{Status: "fresh", PID: 42, StartTicks: "100", CollectedAt: runtime.SnapshotCollectedAt}
			}
			body, err := json.Marshal(reporting.HeartbeatPayload{WorkerIdentity: identity, RuntimePayload: runtime, SentAt: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/heartbeat", bytes.NewReader(body))
			req.SetPathValue("id", worker.ID)
			response := httptest.NewRecorder()
			NewAPI(db, nil, nil, nil, nil, nil).handleHeartbeat(response, req)
			if response.Code != http.StatusAccepted {
				t.Fatalf("heartbeat rejected: %d %s", response.Code, response.Body.String())
			}
			stored, err := db.GetWorker(worker.ID)
			if err != nil {
				t.Fatal(err)
			}
			var observed reporting.RuntimePayload
			if err := json.Unmarshal([]byte(stored.LastRuntimeJSON), &observed); err != nil {
				t.Fatal(err)
			}
			if observed.LitestreamProcess != runtime.LitestreamProcess || observed.LitestreamRSSBytes != 10 || !observed.LitestreamSnapshotHealthy {
				t.Fatalf("operational resource telemetry lost: %+v", observed)
			}
			attributed, quarantined, err := db.ReportAttribution(identity)
			if err != nil || quarantined || attributed != tc.registered {
				t.Fatalf("attribution=%v quarantine=%v err=%v", attributed, quarantined, err)
			}
			labels := workerMetricLabels(*stored)
			value := testutil.ToFloat64(controlWorkerLitestreamRSSBytes.WithLabelValues(labels...))
			if tc.metadata && value != 10 || !tc.metadata && !math.IsNaN(value) {
				t.Fatalf("resource validity conflated with attribution: %v", value)
			}
			if testutil.ToFloat64(controlWorkerDBSize.WithLabelValues(labels...)) != 128 {
				t.Fatal("legacy operational telemetry suppressed")
			}
		})
	}
}
