package orchestrator

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestBuildAttentionItemsFlagsStaleHeartbeats(t *testing.T) {
	t.Parallel()

	staleAt := time.Now().UTC().Add(-30 * time.Minute)
	freshAt := time.Now().UTC().Add(-10 * time.Second)
	workers := []homeWorker{
		{Worker: model.Worker{ID: "w-stale", Name: "w-stale", Status: model.WorkerRunning, LastHeartbeatAt: &staleAt}},
		{Worker: model.Worker{ID: "w-fresh", Name: "w-fresh", Status: model.WorkerRunning, LastHeartbeatAt: &freshAt}},
	}

	items := buildAttentionItems("main", diagnosisSnapshot{}, homeSummary{TotalWorkers: 2, HealthyWorkers: 2}, workers, nil, nil, "", "", failureClassificationContext{})

	var staleItem *attentionItem
	for i := range items {
		if strings.Contains(items[i].Title, "stale heartbeat") {
			staleItem = &items[i]
		}
	}
	if staleItem == nil {
		t.Fatalf("no stale-heartbeat attention item in %+v", items)
	}
	if staleItem.Severity != "warn" {
		t.Fatalf("stale heartbeat severity = %q, want warn", staleItem.Severity)
	}
	if !strings.Contains(staleItem.Detail, "w-stale") {
		t.Fatalf("stale heartbeat detail %q should name w-stale", staleItem.Detail)
	}
	if strings.Contains(staleItem.Detail, "w-fresh") {
		t.Fatalf("stale heartbeat detail %q must not name w-fresh", staleItem.Detail)
	}
}

func TestBuildHomePageDataTreatsStoppedSuccessArchiveAsPassed(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	source := "pr-1312"
	completedAt := time.Now().UTC().Add(-30 * time.Minute)
	for _, workerID := range []string{"worker-pr-1312-low", "worker-pr-1312-high"} {
		createTestWorker(t, db, model.Worker{
			ID:            workerID,
			Name:          workerID,
			Status:        model.WorkerStopped,
			Source:        source,
			GitSHA:        "sha-pr",
			LitestreamSHA: "litestream-pr",
			PRNumber:      1312,
			ProfileName:   "low-volume",
			ProfileConfig: "{}",
		})
		mustRecordVerification(t, db, &model.Verification{
			WorkerID:    workerID,
			StartedAt:   completedAt.Add(-15 * time.Second),
			CompletedAt: &completedAt,
			Status:      "passed",
			CheckType:   "integrity",
			Passed:      true,
		})
	}
	if _, err := db.RecordRunArchive(&model.RunArchive{
		DeploymentID:  1312,
		Source:        source,
		ArchiveType:   "success",
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		Status:        "stable",
		Summary:       "PR #1312 completed a clean soak.",
		Payload:       `{"rollout":{"status":"stable"}}`,
		ArchivedAt:    completedAt,
	}); err != nil {
		t.Fatalf("RecordRunArchive() error = %v", err)
	}

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/ui?source=pr-1312", nil)
	data, err := api.buildHomePageData(request)
	if err != nil {
		t.Fatalf("buildHomePageData() error = %v", err)
	}

	if data.Summary.AttentionWorkers != 0 {
		t.Fatalf("AttentionWorkers = %d, want 0", data.Summary.AttentionWorkers)
	}
	if data.Summary.HealthyWorkers != 2 {
		t.Fatalf("HealthyWorkers = %d, want 2", data.Summary.HealthyWorkers)
	}
	if data.KPIs.FleetHealthPct != 100 {
		t.Fatalf("FleetHealthPct = %d, want 100", data.KPIs.FleetHealthPct)
	}
	if !data.KPIs.FleetPassed {
		t.Fatal("FleetPassed = false, want true")
	}
	if len(data.Attention) != 0 {
		t.Fatalf("Attention = %+v, want none", data.Attention)
	}
	for _, worker := range data.Workers {
		if !worker.CompletedSuccess {
			t.Fatalf("worker %s CompletedSuccess = false, want true", worker.Worker.ID)
		}
		if homeWorkerNeedsAttention(worker) {
			t.Fatalf("worker %s needs attention, want passed", worker.Worker.ID)
		}
	}
	foundSourceCard := false
	for _, card := range data.ActiveSources {
		if card.Source != source {
			continue
		}
		foundSourceCard = true
		if card.Attention != 0 {
			t.Fatalf("source card attention = %d, want 0", card.Attention)
		}
		if !card.Passed {
			t.Fatal("source card Passed = false, want true")
		}
	}
	if !foundSourceCard {
		t.Fatalf("source card for %s not found in %+v", source, data.ActiveSources)
	}

	var body bytes.Buffer
	if err := uiTemplates.ExecuteTemplate(&body, "home_body", data); err != nil {
		t.Fatalf("ExecuteTemplate(home_body) error = %v", err)
	}
	rendered := body.String()
	if !strings.Contains(rendered, "Passed") || !strings.Contains(rendered, "clean soak, torn down") {
		t.Fatalf("rendered home body missing passed state: %s", rendered[:min(500, len(rendered))])
	}
	if strings.Contains(rendered, "row-bad") {
		t.Fatalf("rendered home body includes row-bad for completed success: %s", rendered[:min(500, len(rendered))])
	}
}

func TestHomeBodyRendersPassedComparisonWithoutDeltas(t *testing.T) {
	t.Parallel()

	data := homePageData{
		ReleaseComparison: &DeploymentComparisonResponse{
			BaseSource:     "main",
			HeadSource:     "pr-1312",
			ComparisonKind: "cross_source",
			Verdict:        "passed",
			Summary:        "PR #1312 passed a clean soak and was torn down on success.",
			PassDelta:      -7,
			FailDelta:      -2,
			Head: DeploymentScorecard{
				Deployment: model.Deployment{Source: "pr-1312", PRNumber: 1312},
			},
			Base: &DeploymentScorecard{
				Deployment: model.Deployment{Source: "main"},
			},
		},
	}

	var body bytes.Buffer
	if err := uiTemplates.ExecuteTemplate(&body, "home_body", data); err != nil {
		t.Fatalf("ExecuteTemplate(home_body) error = %v", err)
	}
	rendered := body.String()
	for _, want := range []string{
		"v-better",
		"PR #1312 passed its soak",
		"completed &amp; torn down",
		"badge-good",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered home body missing %q: %s", want, rendered[:min(700, len(rendered))])
		}
	}
	for _, unwanted := range []string{
		"insufficient_data",
		"pass <strong>-7</strong>",
		"fail <strong>-2</strong>",
		"pass delta: -7",
		"fail delta: -2",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("rendered home body includes %q: %s", unwanted, rendered[:min(1200, len(rendered))])
		}
	}
}

func TestBuildHomePageDataKeepsStoppedWorkersWithoutSuccessAttention(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	source := "pr-1313"
	completedAt := time.Now().UTC().Add(-30 * time.Minute)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1313-low",
		Name:          "worker-pr-1313-low",
		Status:        model.WorkerStopped,
		Source:        source,
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		PRNumber:      1313,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    "worker-pr-1313-low",
		StartedAt:   completedAt.Add(-15 * time.Second),
		CompletedAt: &completedAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/ui?source=pr-1313", nil)
	data, err := api.buildHomePageData(request)
	if err != nil {
		t.Fatalf("buildHomePageData() error = %v", err)
	}

	if data.Summary.AttentionWorkers != 1 {
		t.Fatalf("AttentionWorkers = %d, want 1", data.Summary.AttentionWorkers)
	}
	if data.Summary.HealthyWorkers != 0 {
		t.Fatalf("HealthyWorkers = %d, want 0", data.Summary.HealthyWorkers)
	}
	if data.KPIs.FleetHealthPct != 0 {
		t.Fatalf("FleetHealthPct = %d, want 0", data.KPIs.FleetHealthPct)
	}
	if len(data.Attention) == 0 {
		t.Fatal("Attention is empty, want stopped worker attention item")
	}
}

func TestBuildHomePageDataTagsCorrelatedS3TransportFailureEnvironmental(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-main-high",
		Name:          "worker-main-high",
		Status:        model.WorkerRunning,
		Source:        "main",
		GitSHA:        "sha-main",
		LitestreamSHA: "litestream-main",
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1302-high",
		Name:          "worker-pr-1302-high",
		Status:        model.WorkerRunning,
		Source:        "pr-1302",
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		PRNumber:      1302,
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	mainWorker, err := db.GetWorker("worker-main-high")
	if err != nil {
		t.Fatalf("GetWorker(main) error = %v", err)
	}
	prWorker, err := db.GetWorker("worker-pr-1302-high")
	if err != nil {
		t.Fatalf("GetWorker(pr) error = %v", err)
	}

	mainPassAt := mainWorker.CreatedAt.Add(time.Minute).UTC()
	prPassAt := prWorker.CreatedAt.Add(time.Minute).UTC()
	mainFailAt := mainPassAt.Add(10 * time.Minute)
	prFailAt := mainFailAt.Add(5 * time.Minute)
	transportErr := `wait for sync: sync returned 500: sync database: replica sync: operation error S3: PutObject, https response error StatusCode: 0, RequestID: , request send failed: unexpected EOF`
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    mainWorker.ID,
		StartedAt:   mainPassAt.Add(-15 * time.Second),
		CompletedAt: &mainPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:    prWorker.ID,
		StartedAt:   prPassAt.Add(-15 * time.Second),
		CompletedAt: &prPassAt,
		Status:      "passed",
		CheckType:   "integrity",
		Passed:      true,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     mainWorker.ID,
		StartedAt:    mainFailAt.Add(-15 * time.Second),
		CompletedAt:  &mainFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: transportErr,
	})
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     prWorker.ID,
		StartedAt:    prFailAt.Add(-15 * time.Second),
		CompletedAt:  &prFailAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: transportErr,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/ui?source=pr-1302", nil)
	data, err := api.buildHomePageData(request)
	if err != nil {
		t.Fatalf("buildHomePageData() error = %v", err)
	}

	if data.KPIs.Failures24h != 1 {
		t.Fatalf("Failures24h = %d, want 1", data.KPIs.Failures24h)
	}
	if data.KPIs.EnvironmentalFailures24h != 1 {
		t.Fatalf("EnvironmentalFailures24h = %d, want 1", data.KPIs.EnvironmentalFailures24h)
	}
	if data.KPIs.ActionableFailures24h != 0 {
		t.Fatalf("ActionableFailures24h = %d, want 0", data.KPIs.ActionableFailures24h)
	}
	if data.Spotlight == nil {
		t.Fatal("Spotlight = nil, want environmental failure")
	}
	if data.Spotlight.FailureCategory != "environmental" {
		t.Fatalf("FailureCategory = %q, want environmental", data.Spotlight.FailureCategory)
	}
	if data.Spotlight.FailureSeverity != "warn" {
		t.Fatalf("FailureSeverity = %q, want warn", data.Spotlight.FailureSeverity)
	}

	var s3Item *attentionItem
	for i := range data.Attention {
		if strings.Contains(data.Attention[i].Title, "S3 degraded") {
			s3Item = &data.Attention[i]
			break
		}
	}
	if s3Item == nil {
		t.Fatalf("attention items = %+v, want S3 degraded item", data.Attention)
	}
	if s3Item.Severity != "warn" {
		t.Fatalf("S3 item severity = %q, want warn", s3Item.Severity)
	}
	if !strings.Contains(s3Item.Detail, "main branch") || !strings.Contains(s3Item.Detail, "PR #1302") {
		t.Fatalf("S3 item detail = %q, want both fleet names", s3Item.Detail)
	}
}

func TestBuildHomePageDataSplitsRampUpFailures(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1305-low",
		Name:          "worker-pr-1305-low",
		Status:        model.WorkerRunning,
		Source:        "pr-1305",
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		PRNumber:      1305,
		ProfileName:   "low-volume",
		ProfileConfig: "{}",
	})
	worker, err := db.GetWorker("worker-pr-1305-low")
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	failedAt := worker.CreatedAt.Add(30 * time.Minute).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     worker.ID,
		StartedAt:    failedAt.Add(-15 * time.Second),
		CompletedAt:  &failedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: `validation failed (exit 1): unexpected EOF reading stdout`,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/ui?source=pr-1305", nil)
	data, err := api.buildHomePageData(request)
	if err != nil {
		t.Fatalf("buildHomePageData() error = %v", err)
	}

	if data.KPIs.Failures24h != 1 {
		t.Fatalf("Failures24h = %d, want 1", data.KPIs.Failures24h)
	}
	if data.KPIs.RampUpFailures24h != 1 {
		t.Fatalf("RampUpFailures24h = %d, want 1", data.KPIs.RampUpFailures24h)
	}
	if data.KPIs.ActionableFailures24h != 0 {
		t.Fatalf("ActionableFailures24h = %d, want 0", data.KPIs.ActionableFailures24h)
	}
	if data.Spotlight == nil {
		t.Fatal("Spotlight = nil, want ramp-up failure")
	}
	if data.Spotlight.FailureCategory != "ramp-up" {
		t.Fatalf("FailureCategory = %q, want ramp-up", data.Spotlight.FailureCategory)
	}
	if data.Spotlight.FailureSeverity != "warn" {
		t.Fatalf("FailureSeverity = %q, want warn", data.Spotlight.FailureSeverity)
	}
}

func TestBuildHomePageDataEscalatesRampUpAfterDeadline(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	createTestWorker(t, db, model.Worker{
		ID:            "worker-pr-1305-stuck",
		Name:          "worker-pr-1305-stuck",
		Status:        model.WorkerRunning,
		Source:        "pr-1305",
		GitSHA:        "sha-pr",
		LitestreamSHA: "litestream-pr",
		PRNumber:      1305,
		ProfileName:   "high-volume",
		ProfileConfig: "{}",
	})
	worker, err := db.GetWorker("worker-pr-1305-stuck")
	if err != nil {
		t.Fatalf("GetWorker() error = %v", err)
	}
	failedAt := worker.CreatedAt.Add(91 * time.Minute).UTC()
	mustRecordVerification(t, db, &model.Verification{
		WorkerID:     worker.ID,
		StartedAt:    failedAt.Add(-15 * time.Second),
		CompletedAt:  &failedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: `validation failed (exit 1): unexpected EOF reading stdout`,
	})

	api := NewAPI(db, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/ui?source=pr-1305", nil)
	data, err := api.buildHomePageData(request)
	if err != nil {
		t.Fatalf("buildHomePageData() error = %v", err)
	}

	if data.KPIs.RampUpFailures24h != 0 {
		t.Fatalf("RampUpFailures24h = %d, want 0", data.KPIs.RampUpFailures24h)
	}
	if data.KPIs.ActionableFailures24h != 1 {
		t.Fatalf("ActionableFailures24h = %d, want 1", data.KPIs.ActionableFailures24h)
	}
	if data.Spotlight == nil {
		t.Fatal("Spotlight = nil, want actionable failure")
	}
	if data.Spotlight.FailureCategory != "actionable" {
		t.Fatalf("FailureCategory = %q, want actionable", data.Spotlight.FailureCategory)
	}
	if data.Spotlight.FailureSeverity != "bad" {
		t.Fatalf("FailureSeverity = %q, want bad", data.Spotlight.FailureSeverity)
	}
}
