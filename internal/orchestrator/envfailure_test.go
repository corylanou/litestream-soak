package orchestrator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

const tigrisListNoSuchBucket = `wait for sync: sync returned 500: {"error":"sync database: db sync: replica sync: list objects: operation error S3: ListObjectsV2, https response error StatusCode: 404, RequestID: 01H8, api error NoSuchBucket: The specified bucket does not exist"}`

func configureEnvPolicyForTest(t *testing.T, policy EnvironmentalFailurePolicy) {
	t.Helper()
	previous := environmentalFailurePolicy
	ConfigureEnvironmentalFailurePolicy(policy)
	t.Cleanup(func() { environmentalFailurePolicy = previous })
}

func classifyForTest(message string) *reporting.FailureClassification {
	classification := reporting.ClassifyVerificationFailure("integrity", message)
	return &classification
}

func TestIsTransientObjectStoreFailure(t *testing.T) {
	policy := EnvironmentalFailurePolicy{Bucket: "litestream-soak-replicas-shared"}.normalized()

	if !isTransientObjectStoreFailure(classifyForTest(tigrisListNoSuchBucket), policy) {
		t.Fatal("NoSuchBucket/404 on ListObjectsV2 without a bucket mismatch should be transient-environmental")
	}
	slowdown := `restore failed: operation error S3: GetObject, https response error StatusCode: 503, api error SlowDown: Please reduce your request rate`
	if !isTransientObjectStoreFailure(classifyForTest(slowdown), policy) {
		t.Fatal("SlowDown/503 should be transient-environmental")
	}
	otherBucket := tigrisListNoSuchBucket + ` s3://some-other-bucket/prefix/file.ltx`
	if isTransientObjectStoreFailure(classifyForTest(otherBucket), policy) {
		t.Fatal("bucket mismatch must never be environmental")
	}
	if isTransientObjectStoreFailure(classifyForTest(tigrisListNoSuchBucket), EnvironmentalFailurePolicy{}.normalized()) {
		t.Fatal("empty configured bucket must fail closed (never environmental)")
	}
	server500 := `wait for sync: sync returned 500: {"error":"replica sync: list objects: operation error S3: ListObjectsV2, https response error StatusCode: 500, api error InternalError: oops"}`
	if isTransientObjectStoreFailure(classifyForTest(server500), policy) {
		t.Fatal("non-allowlisted errors must stay real failures")
	}
	if isTransientObjectStoreFailure(classifyForTest("validation failed (exit 1)"), policy) {
		t.Fatal("non-object-store failures must stay real failures")
	}
}

func TestEnvironmentalStreakEscalation(t *testing.T) {
	policy := EnvironmentalFailurePolicy{Bucket: "b", EscalateAfterConsecutive: 3, EscalateAfterDuration: 30 * time.Minute}

	now := time.Now().UTC()
	envFailure := func(age time.Duration) model.Verification {
		return model.Verification{Status: "failed", CheckType: "integrity", StartedAt: now.Add(-age), ErrorMessage: tigrisListNoSuchBucket}
	}

	if environmentalStreakEscalated([]model.Verification{envFailure(2 * time.Minute), envFailure(4 * time.Minute)}, now, policy) {
		t.Fatal("3 consecutive (2 prior + current) within window should NOT escalate at threshold 3")
	}
	if !environmentalStreakEscalated([]model.Verification{envFailure(2 * time.Minute), envFailure(4 * time.Minute), envFailure(6 * time.Minute)}, now, policy) {
		t.Fatal("4 consecutive should escalate past threshold 3")
	}
	if !environmentalStreakEscalated([]model.Verification{envFailure(45 * time.Minute)}, now, policy) {
		t.Fatal("a streak spanning more than the duration window should escalate")
	}
	passed := model.Verification{Status: "passed", Passed: true, StartedAt: now.Add(-3 * time.Minute)}
	if environmentalStreakEscalated([]model.Verification{passed, envFailure(5 * time.Minute), envFailure(7 * time.Minute), envFailure(9 * time.Minute)}, now, policy) {
		t.Fatal("a pass resets the streak")
	}
}

func TestBuildFailureClassificationContextEnvironmentalAndEscalation(t *testing.T) {
	configureEnvPolicyForTest(t, EnvironmentalFailurePolicy{Bucket: "litestream-soak-replicas-shared", EscalateAfterConsecutive: 2, EscalateAfterDuration: time.Hour})

	now := time.Now().UTC()
	stats := []model.VerificationStat{
		{ID: 1, WorkerID: "w1", Source: "pr-1322", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-50 * time.Minute), ErrorMessage: tigrisListNoSuchBucket, HasPriorPass: true},
		{ID: 2, WorkerID: "w1", Source: "pr-1322", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-40 * time.Minute), ErrorMessage: tigrisListNoSuchBucket, HasPriorPass: true},
		{ID: 3, WorkerID: "w1", Source: "pr-1322", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-30 * time.Minute), ErrorMessage: tigrisListNoSuchBucket, HasPriorPass: true},
		{ID: 4, WorkerID: "w2", Source: "main", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-20 * time.Minute), ErrorMessage: "validation failed (exit 1)", HasPriorPass: true},
	}

	ctx := buildFailureClassificationContext(stats)

	if got := ctx.categoryForVerificationID(1); got != failureCategoryEnvironmental {
		t.Fatalf("first blip category = %q, want environmental", got)
	}
	if got := ctx.categoryForVerificationID(2); got != failureCategoryEnvironmental {
		t.Fatalf("second blip category = %q, want environmental (at threshold)", got)
	}
	if got := ctx.categoryForVerificationID(3); got != failureCategoryActionable {
		t.Fatalf("third consecutive category = %q, want actionable (escalated past threshold 2)", got)
	}
	if got := ctx.categoryForVerificationID(4); got != failureCategoryActionable {
		t.Fatalf("unrelated failure category = %q, want actionable", got)
	}
	if labels := ctx.environmentalSourceLabels("sync_s3_bucket_missing"); len(labels) == 0 {
		t.Fatal("environmental source labels should include the blip source")
	}
}

func TestDeploymentFailureCategoryEnvironmental(t *testing.T) {
	configureEnvPolicyForTest(t, EnvironmentalFailurePolicy{Bucket: "litestream-soak-replicas-shared", EscalateAfterConsecutive: 2, EscalateAfterDuration: time.Hour})

	now := time.Now().UTC()
	worker := model.Worker{ID: "w1", CreatedAt: now.Add(-24 * time.Hour)}
	blip := model.Verification{WorkerID: "w1", Status: "failed", CheckType: "integrity", StartedAt: now, ErrorMessage: tigrisListNoSuchBucket}

	if got := deploymentFailureCategory(worker, blip, []model.Verification{{WorkerID: "w1", Status: "passed", Passed: true, StartedAt: now.Add(-30 * time.Minute)}}); got != failureCategoryEnvironmental {
		t.Fatalf("category = %q, want environmental", got)
	}

	history := []model.Verification{
		{WorkerID: "w1", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-10 * time.Minute), ErrorMessage: tigrisListNoSuchBucket},
		{WorkerID: "w1", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-20 * time.Minute), ErrorMessage: tigrisListNoSuchBucket},
	}
	if got := deploymentFailureCategory(worker, blip, history); got != failureCategoryActionable {
		t.Fatalf("escalated streak category = %q, want actionable", got)
	}

	if !failureExcludedFromComparison(failureCategoryEnvironmental) {
		t.Fatal("environmental must be excluded from comparison verdicts")
	}
	scorecard := DeploymentScorecard{PassedWorkers: 5, RampUpFailures: 1, EnvironmentalFailures: 2}
	if got := comparisonPassedWorkers(scorecard); got != 8 {
		t.Fatalf("comparisonPassedWorkers = %d, want 8 (env folds into passed side)", got)
	}
}

func postVerificationForTest(t *testing.T, api *API, workerID string, startedAt time.Time, errorMessage string) {
	t.Helper()
	payload := reporting.VerificationPayload{
		WorkerIdentity: reporting.WorkerIdentity{
			WorkerID: workerID, Name: workerID, Source: "pr-1322",
			GitSHA: "abc123", LitestreamSHA: "ls123", ProfileName: "many-dbs-100-dir", ProfileConfig: "{}",
		},
		StartedAt:    startedAt,
		CompletedAt:  startedAt.Add(time.Minute),
		CheckType:    "integrity",
		Status:       "failed",
		Passed:       false,
		Summary:      "verification failed",
		ErrorMessage: errorMessage,
		DurationMS:   60000,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/workers/"+workerID+"/verifications", bytes.NewReader(body))
	request.SetPathValue("id", workerID)
	recorder := httptest.NewRecorder()
	api.handleVerification(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
}

func TestHandleVerificationEnvironmentalKeepsWorkerRunningUntilEscalation(t *testing.T) {
	configureEnvPolicyForTest(t, EnvironmentalFailurePolicy{Bucket: "litestream-soak-replicas-shared", EscalateAfterConsecutive: 2, EscalateAfterDuration: time.Hour})

	db := openTestDB(t)
	workerID := "worker-env-guard"
	createTestWorker(t, db, model.Worker{
		ID: workerID, Name: workerID, Status: model.WorkerRunning, Source: "pr-1322",
		GitSHA: "abc123", LitestreamSHA: "ls123", ProfileName: "many-dbs-100-dir", ProfileConfig: "{}",
	})
	manager := &Manager{db: db, appName: "litestream-soak"}
	api := NewAPI(db, nil, nil, nil, manager, nil)

	now := time.Now().UTC()
	postVerificationForTest(t, api, workerID, now.Add(-20*time.Minute), tigrisListNoSuchBucket)
	postVerificationForTest(t, api, workerID, now.Add(-10*time.Minute), tigrisListNoSuchBucket)

	worker, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if worker.Status != model.WorkerRunning {
		t.Fatalf("worker status after 2 environmental blips = %q, want running", worker.Status)
	}
	events, err := db.ListEvents(20)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	for _, event := range events {
		if event.EventType == "first_failure" {
			t.Fatal("environmental blip must not emit a first_failure transition")
		}
	}
	sawEnvironmentalEvent := false
	for _, event := range events {
		if event.EventType == "verification_failed_environmental" {
			sawEnvironmentalEvent = true
		}
	}
	if !sawEnvironmentalEvent {
		t.Fatal("environmental failures must still be recorded as events")
	}

	postVerificationForTest(t, api, workerID, now, tigrisListNoSuchBucket)

	worker, err = db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker() after escalation error = %v", err)
	}
	if worker.Status != model.WorkerDegraded {
		t.Fatalf("worker status after escalated streak = %q, want degraded", worker.Status)
	}
}

func TestEnvironmentalStreakTreatsAbortedAsNeutral(t *testing.T) {
	policy := EnvironmentalFailurePolicy{Bucket: "b", EscalateAfterConsecutive: 2, EscalateAfterDuration: time.Hour}

	now := time.Now().UTC()
	envFailure := func(age time.Duration) model.Verification {
		return model.Verification{Status: "failed", CheckType: "integrity", StartedAt: now.Add(-age), ErrorMessage: tigrisListNoSuchBucket}
	}
	abortedAt := func(age time.Duration) model.Verification {
		return model.Verification{Status: "aborted", CheckType: "integrity", StartedAt: now.Add(-age)}
	}

	interleaved := []model.Verification{abortedAt(5 * time.Minute), envFailure(10 * time.Minute), abortedAt(15 * time.Minute), envFailure(20 * time.Minute)}
	if !environmentalStreakEscalated(interleaved, now, policy) {
		t.Fatal("aborted checks between environmental failures must not reset the streak (deleted-bucket bypass)")
	}

	stats := []model.VerificationStat{
		{ID: 1, WorkerID: "w1", Source: "pr-1", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-20 * time.Minute), ErrorMessage: tigrisListNoSuchBucket, HasPriorPass: true},
		{ID: 2, WorkerID: "w1", Source: "pr-1", Status: "aborted", CheckType: "integrity", StartedAt: now.Add(-15 * time.Minute), HasPriorPass: true},
		{ID: 3, WorkerID: "w1", Source: "pr-1", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-10 * time.Minute), ErrorMessage: tigrisListNoSuchBucket, HasPriorPass: true},
		{ID: 4, WorkerID: "w1", Source: "pr-1", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-5 * time.Minute), ErrorMessage: tigrisListNoSuchBucket, HasPriorPass: true},
	}
	escalated := escalatedEnvironmentalStatIDs(stats, policy)
	if !escalated[4] {
		t.Fatal("stats-path streak must escalate across interleaved aborts")
	}
}

func TestEnvironmentalVerificationIDsForLifecycle(t *testing.T) {
	policy := EnvironmentalFailurePolicy{Bucket: "litestream-soak-replicas-shared", EscalateAfterConsecutive: 2, EscalateAfterDuration: time.Hour}

	now := time.Now().UTC()
	verifications := []model.Verification{
		{ID: 1, Status: "failed", CheckType: "integrity", StartedAt: now.Add(-30 * time.Minute), ErrorMessage: tigrisListNoSuchBucket},
		{ID: 2, Status: "passed", Passed: true, StartedAt: now.Add(-25 * time.Minute)},
		{ID: 3, Status: "failed", CheckType: "integrity", StartedAt: now.Add(-20 * time.Minute), ErrorMessage: "validation failed (exit 1)"},
	}

	environmental := environmentalVerificationIDs(verifications, policy)
	if !environmental[1] {
		t.Fatal("single blip should be environmental for lifecycle checks")
	}
	if environmental[3] {
		t.Fatal("real failures must never be environmental for lifecycle checks")
	}
}

func TestHandleVerificationEscalatesAcrossAbortStarvedHistory(t *testing.T) {
	configureEnvPolicyForTest(t, EnvironmentalFailurePolicy{Bucket: "litestream-soak-replicas-shared", EscalateAfterConsecutive: 2, EscalateAfterDuration: 12 * time.Hour})

	db := openTestDB(t)
	workerID := "worker-env-abort-starve"
	createTestWorker(t, db, model.Worker{
		ID: workerID, Name: workerID, Status: model.WorkerRunning, Source: "pr-1322",
		GitSHA: "abc123", LitestreamSHA: "ls123", ProfileName: "many-dbs-100-dir", ProfileConfig: "{}",
	})
	now := time.Now().UTC()
	record := func(age time.Duration, status, errorMessage string) {
		t.Helper()
		done := now.Add(-age).Add(time.Minute)
		if err := db.RecordVerification(&model.Verification{
			WorkerID: workerID, StartedAt: now.Add(-age), CompletedAt: &done,
			Status: status, CheckType: "integrity", Passed: false, ErrorMessage: errorMessage,
		}); err != nil {
			t.Fatalf("RecordVerification() error = %v", err)
		}
	}
	record(40*time.Minute, "failed", tigrisListNoSuchBucket)
	record(30*time.Minute, "aborted", "")
	record(20*time.Minute, "aborted", "")
	record(10*time.Minute, "failed", tigrisListNoSuchBucket)

	manager := &Manager{db: db, appName: "litestream-soak"}
	api := NewAPI(db, nil, nil, nil, manager, nil)
	postVerificationForTest(t, api, workerID, now, tigrisListNoSuchBucket)

	worker, err := db.GetWorker(workerID)
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	if worker.Status != model.WorkerDegraded {
		t.Fatalf("worker status = %q, want degraded — interleaved aborts must not starve the escalation guard", worker.Status)
	}
}

func TestRecentEnvironmentalSignaturesAgeOut(t *testing.T) {
	configureEnvPolicyForTest(t, EnvironmentalFailurePolicy{Bucket: "litestream-soak-replicas-shared", EscalateAfterConsecutive: 5, EscalateAfterDuration: 12 * time.Hour})

	now := time.Now().UTC()
	stats := []model.VerificationStat{
		{ID: 1, WorkerID: "w-old", Source: "main", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-9 * time.Hour), ErrorMessage: tigrisListNoSuchBucket, HasPriorPass: true},
		{ID: 2, WorkerID: "w-new", Source: "pr-9", Status: "failed", CheckType: "integrity", StartedAt: now.Add(-30 * time.Minute), ErrorMessage: tigrisListNoSuchBucket, HasPriorPass: true},
	}
	ctx := buildFailureClassificationContext(stats)

	recent := ctx.recentEnvironmentalSignatures(now, 4*time.Hour)
	if len(recent) != 1 || recent[0] != "sync_s3_bucket_missing" {
		t.Fatalf("recent signatures = %v, want just the fresh one", recent)
	}

	staleOnly := buildFailureClassificationContext(stats[:1])
	if got := staleOnly.recentEnvironmentalSignatures(now, 4*time.Hour); len(got) != 0 {
		t.Fatalf("stale-only signatures = %v, want none (banner must age out)", got)
	}
	if got := staleOnly.categoryForVerificationID(1); got != failureCategoryEnvironmental {
		t.Fatalf("stale env failure category = %q, want environmental (KPI tiles still classify)", got)
	}
}
