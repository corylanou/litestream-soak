package orchestrator

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"modernc.org/sqlite"
)

func TestDeploymentObservationDoesNotBlockReports(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	function := fmt.Sprintf("observe_gate_%d", time.Now().UnixNano())
	if err := sqlite.RegisterScalarFunction(function, 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
		once.Do(func() { close(entered) })
		<-release
		return int64(1), nil
	}); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "control.db")
	db, err := model.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { releaseGate(); _ = db.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := NewAPIWithContext(ctx, db, nil, nil, nil, nil, nil)
	if err := db.UpsertReadyDeployment(&model.Deployment{GitSHA: "candidate", Source: "main", Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	setup, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = setup.Close() }()
	if _, err := setup.Exec("ALTER TABLE deployments RENAME TO retained_deployments; CREATE VIEW deployments AS SELECT * FROM retained_deployments WHERE " + function + "()=1"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { api.observeLatestDeploymentState("main"); close(done) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		releaseGate()
		t.Fatal("comparison query did not start")
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		releaseGate()
		<-done
		t.Fatal("report path waits for deployment comparison while retaining its report lock")
	}

	payload := reporting.WorkerEventPayload{WorkerIdentity: reporting.WorkerIdentity{WorkerID: "observer-worker", Name: "observer-worker", Source: "main", ProfileName: "low-volume", ProfileConfig: "{}"}, EventType: "profile_upload_failed", Message: "retained failure", SentAt: time.Now().UTC()}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	reported := make(chan struct{})
	go func() {
		request := httptest.NewRequest(http.MethodPost, "/api/workers/observer-worker/events", bytes.NewReader(body))
		request.SetPathValue("id", "observer-worker")
		response := httptest.NewRecorder()
		api.handleWorkerEvent(response, request)
		if response.Code != http.StatusAccepted {
			t.Errorf("report status = %d", response.Code)
		}
		close(reported)
	}()
	select {
	case <-reported:
	case <-time.After(time.Second):
		releaseGate()
		t.Fatal("report blocked behind gauge refresh")
	}
	unlocked := make(chan struct{})
	go func() { unlock := db.LockWorkerReports("observer-worker"); unlock(); close(unlocked) }()
	select {
	case <-unlocked:
	case <-time.After(time.Second):
		releaseGate()
		t.Fatal("report lock retained by gauge refresh")
	}
	healthCtx, healthCancel := context.WithTimeout(context.Background(), time.Second)
	if err := db.HealthCheck(healthCtx); err != nil {
		healthCancel()
		t.Fatal(err)
	}
	healthCancel()
	events, err := db.ListWorkerEvents("observer-worker", 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.EventType == payload.EventType {
			found = true
		}
	}
	if !found {
		t.Fatal("failure event was not persisted")
	}
	records, err := db.ListRuntimeEvidence("main", 0)
	if err != nil || len(records) != 1 {
		t.Fatalf("runtime evidence count = %d, error=%v", len(records), err)
	}
	releaseGate()
	cancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := api.WaitForBackground(waitCtx); err != nil {
		t.Fatal(err)
	}

}

func TestDeploymentObserverCoalescesBurst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	batches := make(chan []string, 2)
	var calls atomic.Int32
	observer := &deploymentObserver{ctx: ctx, interval: 10 * time.Millisecond, budget: time.Second, refresh: func(ctx context.Context, sources []string) error {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		batches <- sources
		return nil
	}}
	observer.request("first")
	<-entered
	for range 100 {
		observer.request("second")
		observer.request("third")
	}
	if calls.Load() != 1 {
		t.Fatal("concurrent refreshes started")
	}
	close(release)
	<-batches
	select {
	case sources := <-batches:
		if fmt.Sprint(sources) != "[second third]" {
			t.Fatalf("pending sources: %v", sources)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced followup missing")
	}
	if calls.Load() != 2 {
		t.Fatalf("refresh count=%d", calls.Load())
	}
}

func TestDeploymentObserverDeadlineAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	expired := make(chan struct{})
	failures := testutil.ToFloat64(deploymentRefreshFailures)
	deploymentRefreshSuccess.Set(123)
	observer := &deploymentObserver{ctx: ctx, interval: time.Hour, budget: 20 * time.Millisecond, refresh: func(ctx context.Context, _ []string) error { <-ctx.Done(); close(expired); return ctx.Err() }}
	observer.request("main")
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("refresh has no deadline")
	}
	cancel()
	observer.mu.Lock()
	done := observer.done
	observer.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel pending retry")
	}
	if testutil.ToFloat64(deploymentRefreshFailures) <= failures || testutil.ToFloat64(deploymentRefreshHealthy) != 0 || testutil.ToFloat64(deploymentRefreshSuccess) != 123 {
		t.Fatal("refresh failure erased freshness evidence")
	}
	observer.request("after-shutdown")
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.running || len(observer.pending) != 0 {
		t.Fatal("shutdown accepted more work")
	}
}

func TestDeploymentRefreshPreservesCompleteSnapshot(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "history.db")
	db, err := model.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	setup, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = setup.Close() }()
	_, err = setup.Exec(`
 WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<156)
 INSERT INTO workers(id,name,git_sha,profile_name,source,status) SELECT 'worker-'||x,'worker-'||x,'fixture','low-volume',CASE WHEN x%10=0 THEN 'main' ELSE 'pr-'||(x%10) END,'running' FROM n;
 WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<25027)
 INSERT INTO verifications(worker_id,started_at,completed_at,status,check_type,source_checksum,restored_checksum,passed,duration_ms,error_message,run_identity_json)
 SELECT 'worker-'||(1+x%156),datetime('now'),datetime('now'),CASE WHEN x=1 THEN 'failed' ELSE 'passed' END,'incremental','','',x!=1,1,CASE WHEN x=1 THEN 'retained failure' ELSE '' END,'{}' FROM n;
 WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<3428)
 INSERT INTO evidence_runtime(source,deployment_id,identity_json,runtime_json,attributed,received_at,kind) SELECT 'main',0,'{}','{}',0,datetime('now'),'heartbeat' FROM n;
 `)
	if err != nil {
		t.Fatal(err)
	}
	for source := 0; source < 10; source++ {
		name := fmt.Sprintf("pr-%d", source)
		if source == 0 {
			name = "main"
		}
		if err := db.UpsertReadyDeployment(&model.Deployment{GitSHA: "fixture", Source: name, Status: "ready"}); err != nil {
			t.Fatal(err)
		}
	}
	metrics := NewControlMetrics(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := metrics.refreshDeploymentMetrics(ctx, db.WithReadContext(ctx)); err != nil {
		t.Fatal(err)
	}
	previous := metrics.latestDeploymentPublished
	previousInfo := metrics.latestDeployment.labels
	if len(metrics.sourceComparisonInfo) == 0 {
		t.Fatal("retained fixture did not populate source comparisons")
	}
	if err := db.UpsertReadyDeployment(&model.Deployment{GitSHA: "new-fixture", Source: "main", Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	if _, err := setup.Exec(`ALTER TABLE evidence_runtime RENAME TO retained_runtime`); err != nil {
		t.Fatal(err)
	}
	if err := metrics.refreshDeploymentMetrics(ctx, db.WithReadContext(ctx)); err == nil {
		t.Fatal("failed evidence read reported success")
	}
	if metrics.latestDeploymentPublished != previous || fmt.Sprint(metrics.latestDeployment.labels) != fmt.Sprint(previousInfo) {
		t.Fatal("partial refresh published a new rollout")
	}
	if err := metrics.observeSourceComparisons(db); err == nil {
		t.Fatal("failed source query was swallowed")
	}
	if len(metrics.sourceComparisonInfo) == 0 {
		t.Fatal("failed source refresh erased prior gauges")
	}
	var verifications, failures, runtime int
	if err := setup.QueryRow(`SELECT count(*),sum(passed=0) FROM evidence_verifications`).Scan(&verifications, &failures); err != nil {
		t.Fatal(err)
	}
	if err := setup.QueryRow(`SELECT count(*) FROM retained_runtime`).Scan(&runtime); err != nil {
		t.Fatal(err)
	}
	if verifications != 25027 || failures != 1 || runtime != 3428 {
		t.Fatalf("history changed: verifications=%d failures=%d runtime=%d", verifications, failures, runtime)
	}
}

func TestDeploymentObserverRetriesFailureWithoutLosingEvidence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	recovered := make(chan struct{})
	failures := testutil.ToFloat64(deploymentRefreshFailures)
	observer := &deploymentObserver{ctx: ctx, interval: time.Millisecond, budget: time.Second, refresh: func(_ context.Context, sources []string) error {
		if fmt.Sprint(sources) != "[main]" {
			t.Errorf("retry sources=%v", sources)
		}
		if calls.Add(1) == 1 {
			return fmt.Errorf("fixture query failure")
		}
		close(recovered)
		return nil
	}}
	observer.request("main")
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("failed refresh was abandoned")
	}
	observer.mu.Lock()
	done := observer.done
	observer.mu.Unlock()
	<-done
	if testutil.ToFloat64(deploymentRefreshFailures) <= failures || testutil.ToFloat64(deploymentRefreshHealthy) != 1 {
		t.Fatal("recovery lost failure or freshness evidence")
	}
}

func TestDeploymentAlertRefreshContinuesAfterFailedSource(t *testing.T) {
	var mainQueries atomic.Int32
	function := fmt.Sprintf("source_gate_%d", time.Now().UnixNano())
	if err := sqlite.RegisterScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		if args[0] == "bad" {
			return nil, fmt.Errorf("source unavailable")
		}
		if args[0] == "main" {
			mainQueries.Add(1)
		}
		return int64(1), nil
	}); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "sources.db")
	db, err := model.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, source := range []string{"bad", "main"} {
		if err := db.UpsertReadyDeployment(&model.Deployment{GitSHA: "fixture", Source: source, Status: "ready"}); err != nil {
			t.Fatal(err)
		}
	}
	setup, err := sql.Open("sqlite", filename)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = setup.Close() }()
	if _, err := setup.Exec("ALTER TABLE deployments RENAME TO source_deployments; CREATE VIEW deployments AS SELECT * FROM source_deployments WHERE " + function + "(source)=1"); err != nil {
		t.Fatal(err)
	}
	api := &API{db: db, alerts: &AlertDispatcher{}}
	failures := testutil.ToFloat64(deploymentAlertRefreshFailures.WithLabelValues("bad"))
	if err := api.refreshDeploymentState(context.Background(), []string{"bad", "missing", "main"}); err == nil {
		t.Fatal("source failure was swallowed")
	}
	if mainQueries.Load() == 0 || testutil.ToFloat64(deploymentAlertRefreshFailures.WithLabelValues("bad")) <= failures {
		t.Fatal("failed source prevented remaining source or lost failure accounting")
	}
}
