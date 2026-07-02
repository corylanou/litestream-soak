package orchestrator

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

type attentionAction struct {
	Label   string
	URL     string
	CopyURL string
	Kind    string
}

type attentionItem struct {
	Severity string
	Title    string
	Detail   string
	Actions  []attentionAction
}

type homeKPIs struct {
	TotalWorkers             int
	HealthyWorkers           int
	FleetHealthPct           int
	FleetPassed              bool
	PassRatePct              float64
	HasPassRate              bool
	PassRateDelta            float64
	HasPassRateDelta         bool
	Checks24h                int
	Failures24h              int
	ActionableFailures24h    int
	EnvironmentalFailures24h int
	RampUpFailures24h        int
	RolloutUpdated           int
	RolloutTotal             int
	RolloutStatus            string
	HasRollout               bool
	StalestHeartbeatAt       *time.Time
	StalestRuntimeAt         *time.Time
}

type passRateLine struct {
	Label string     `json:"label"`
	Data  []*float64 `json:"data"`
}

type homeChartData struct {
	Labels         []string       `json:"labels"`
	PassRateSeries []passRateLine `json:"pass_rate_series"`
	P50            []*int         `json:"p50"`
	P95            []*int         `json:"p95"`
	Failures       []int          `json:"failures"`
	HasData        bool           `json:"-"`
	HasSourceData  bool           `json:"-"`
}

const homeChartHours = 24

func buildHomeKPIs(summary homeSummary, windowStats, previousStats []model.VerificationStat, rollout *DeploymentRolloutResponse, failureContext failureClassificationContext) homeKPIs {
	kpis := homeKPIs{
		TotalWorkers:       summary.TotalWorkers,
		HealthyWorkers:     summary.HealthyWorkers,
		StalestHeartbeatAt: summary.StalestHeartbeatAt,
		StalestRuntimeAt:   summary.StalestRuntimeAt,
	}
	if summary.TotalWorkers > 0 {
		kpis.FleetHealthPct = int(100.0*float64(summary.HealthyWorkers)/float64(summary.TotalWorkers) + 0.5)
	}
	if summary.CompletedSuccess && summary.AttentionWorkers == 0 {
		kpis.FleetPassed = true
		kpis.FleetHealthPct = 100
	}

	rate, total := passRateSummary(windowStats)
	kpis.Checks24h = total
	if total > 0 {
		kpis.PassRatePct = rate
		kpis.HasPassRate = true
	}
	prevRate, prevTotal := passRateSummary(previousStats)
	if total > 0 && prevTotal > 0 {
		kpis.PassRateDelta = rate - prevRate
		kpis.HasPassRateDelta = true
	}
	for _, stat := range windowStats {
		verification := model.Verification{Status: stat.Status, Passed: stat.Passed}
		if verification.Failed() {
			kpis.Failures24h++
			switch failureContext.categoryForVerificationID(stat.ID) {
			case failureCategoryEnvironmental:
				kpis.EnvironmentalFailures24h++
			case failureCategoryRampUp:
				kpis.RampUpFailures24h++
			default:
				kpis.ActionableFailures24h++
			}
		}
	}

	if rollout != nil {
		kpis.HasRollout = true
		kpis.RolloutUpdated = rollout.UpdatedWorkers
		kpis.RolloutTotal = rollout.TotalWorkers
		kpis.RolloutStatus = rollout.Status
	}
	return kpis
}

func buildHomeChartData(selectedSource string, chartFrom time.Time, selectedStats, mainStats []model.VerificationStat) homeChartData {
	selected := buildChartSeries(selectedStats, chartFrom, homeChartHours)

	data := homeChartData{
		Labels:        selected.Labels,
		P50:           selected.P50,
		P95:           selected.P95,
		Failures:      selected.Failures,
		HasData:       len(selectedStats) > 0,
		HasSourceData: len(selectedStats) > 0,
	}
	data.PassRateSeries = []passRateLine{{Label: sourceHumanLabel(selectedSource), Data: selected.PassRate}}
	if selectedSource != "main" {
		main := buildChartSeries(mainStats, chartFrom, homeChartHours)
		data.PassRateSeries = append(data.PassRateSeries, passRateLine{Label: "main", Data: main.PassRate})
		if len(mainStats) > 0 {
			data.HasData = true
		}
	}
	return data
}

func buildAttentionItems(selectedSource string, diagnosis diagnosisSnapshot, summary homeSummary, workers []homeWorker, rollout *DeploymentRolloutResponse, comparison *DeploymentComparisonResponse, comparisonPromptURL, comparisonJSONURL string, failureContext failureClassificationContext) []attentionItem {
	if summary.Retired {
		// Retired sources are torn down on purpose; stale heartbeats and
		// leftover failure history are not actionable, so the neutral
		// retired banner replaces the attention stack entirely.
		return nil
	}

	items := make([]attentionItem, 0, 4)
	clusterSeverity := "warn"
	if selectedSource != "main" {
		clusterSeverity = "bad"
	}

	if comparison != nil && comparison.Verdict == "worse" {
		items = append(items, attentionItem{
			Severity: "bad",
			Title:    fmt.Sprintf("%s is regressing vs %s", sourceHumanLabel(comparison.HeadSource), sourceHumanLabel(comparison.BaseSource)),
			Detail:   comparison.Summary,
			Actions: []attentionAction{
				{Label: "Copy comparison prompt", CopyURL: comparisonPromptURL, Kind: "primary"},
				{Label: "Comparison JSON", URL: comparisonJSONURL},
			},
		})
	}

	if sources := failureContext.environmentalSourceLabels("s3_transport"); len(sources) >= 2 {
		items = append(items, attentionItem{
			Severity: "warn",
			Title:    "S3 degraded across fleets",
			Detail:   fmt.Sprintf("%s share s3_transport failures in the correlation window", strings.Join(sources, " and ")),
		})
	}

	for index, cluster := range diagnosis.Clusters {
		if index >= 4 {
			items = append(items, attentionItem{
				Severity: clusterSeverity,
				Title:    fmt.Sprintf("%d more failure clusters", len(diagnosis.Clusters)-index),
				Detail:   "Open the diagnosis details below for the full list.",
			})
			break
		}
		detail := cluster.Summary
		if strings.TrimSpace(cluster.Signature) != "" {
			detail = fmt.Sprintf("%d workers · %s", cluster.WorkerCount, shortenText(cluster.Signature, 110))
		}
		item := attentionItem{
			Severity: clusterSeverity,
			Title:    cluster.Headline,
			Detail:   detail,
		}
		if strings.TrimSpace(cluster.RepresentativeWorker.ID) != "" {
			item.Actions = []attentionAction{
				{Label: "Open " + cluster.RepresentativeWorker.Name, URL: "/ui/workers/" + url.PathEscape(cluster.RepresentativeWorker.ID), Kind: "primary"},
				{Label: "Copy prompt", CopyURL: "/api/workers/" + url.PathEscape(cluster.RepresentativeWorker.ID) + "/prompt?mode=triage"},
			}
		}
		items = append(items, item)
	}

	if len(diagnosis.Clusters) == 0 && summary.AttentionWorkers > 0 {
		names := make([]string, 0, 3)
		var firstAttention *model.Worker
		for _, worker := range workers {
			if !homeWorkerNeedsAttention(worker) {
				continue
			}
			if firstAttention == nil {
				attentionWorker := worker.Worker
				firstAttention = &attentionWorker
			}
			if len(names) < 3 {
				names = append(names, fmt.Sprintf("%s (%s)", worker.Worker.Name, worker.Worker.Status))
			}
		}
		detail := strings.Join(names, ", ")
		if summary.AttentionWorkers > len(names) {
			detail += fmt.Sprintf(", +%d more", summary.AttentionWorkers-len(names))
		}
		item := attentionItem{
			Severity: "warn",
			Title:    fmt.Sprintf("%d worker(s) need attention", summary.AttentionWorkers),
			Detail:   detail,
		}
		if firstAttention != nil {
			item.Actions = []attentionAction{
				{Label: "Open " + firstAttention.Name, URL: "/ui/workers/" + url.PathEscape(firstAttention.ID)},
			}
		}
		items = append(items, item)
	}

	staleNames := make([]string, 0, 3)
	staleCount := 0
	var firstStale *model.Worker
	for _, worker := range workers {
		if worker.CompletedSuccess || worker.Worker.Status == model.WorkerStopped {
			continue
		}
		if homeWorkerNeedsAttention(worker) {
			continue
		}
		if heartbeatClass(worker.Worker.LastHeartbeatAt) != "status-bad" {
			continue
		}
		staleCount++
		if firstStale == nil {
			staleWorker := worker.Worker
			firstStale = &staleWorker
		}
		if len(staleNames) < 3 {
			staleNames = append(staleNames, worker.Worker.Name)
		}
	}
	if staleCount > 0 {
		detail := strings.Join(staleNames, ", ")
		if staleCount > len(staleNames) {
			detail += fmt.Sprintf(", +%d more", staleCount-len(staleNames))
		}
		item := attentionItem{
			Severity: "warn",
			Title:    fmt.Sprintf("%d worker(s) have stale heartbeats", staleCount),
			Detail:   detail + " — data from these workers is old; restore visibility before trusting this page",
		}
		if firstStale != nil {
			item.Actions = []attentionAction{
				{Label: "Open " + firstStale.Name, URL: "/ui/workers/" + url.PathEscape(firstStale.ID)},
			}
		}
		items = append(items, item)
	}

	if rollout != nil && rollout.GraceWindowExceeded {
		items = append(items, attentionItem{
			Severity: "warn",
			Title:    "Rollout is beyond the 45m grace window",
			Detail:   fmt.Sprintf("%d/%d workers updated · status %s", rollout.UpdatedWorkers, rollout.TotalWorkers, rollout.Status),
			Actions: []attentionAction{
				{Label: "Rollout JSON", URL: "/api/deployments/latest"},
			},
		})
	}

	return items
}

// filterStatsToActiveWorkers drops verification history that belongs to
// stopped/failed (retired) workers so a live source's KPIs and charts reflect
// only the fleet that is actually running.
func filterStatsToActiveWorkers(stats []model.VerificationStat, summaries []WorkerSummaryResponse) []model.VerificationStat {
	active := make(map[string]bool, len(summaries))
	for _, summary := range summaries {
		if workerRowActive(summary.Worker) {
			active[summary.Worker.ID] = true
		}
	}
	return filterStatsToWorkerSet(stats, active)
}

// filterStatsToActiveWorkerRows is filterStatsToActiveWorkers for a plain
// worker list (used for the main-baseline chart overlay on PR pages).
func filterStatsToActiveWorkerRows(stats []model.VerificationStat, workers []model.Worker) []model.VerificationStat {
	active := make(map[string]bool, len(workers))
	for _, worker := range workers {
		if workerRowActive(worker) {
			active[worker.ID] = true
		}
	}
	return filterStatsToWorkerSet(stats, active)
}

func workerRowActive(worker model.Worker) bool {
	return worker.Status != model.WorkerStopped && worker.Status != model.WorkerFailed
}

func filterStatsToWorkerSet(stats []model.VerificationStat, active map[string]bool) []model.VerificationStat {
	filtered := make([]model.VerificationStat, 0, len(stats))
	for _, stat := range stats {
		if active[stat.WorkerID] {
			filtered = append(filtered, stat)
		}
	}
	return filtered
}

// activeWorkerSummaries returns only summaries whose worker rows are still
// active, so retired workers' leftover failure signatures cannot seed
// diagnosis clusters on live pages.
func activeWorkerSummaries(summaries []WorkerSummaryResponse) []WorkerSummaryResponse {
	filtered := make([]WorkerSummaryResponse, 0, len(summaries))
	for _, summary := range summaries {
		if workerRowActive(summary.Worker) {
			filtered = append(filtered, summary)
		}
	}
	return filtered
}

func splitStatsAt(stats []model.VerificationStat, cutoff time.Time) (before, after []model.VerificationStat) {
	for _, stat := range stats {
		if stat.StartedAt.Before(cutoff) {
			before = append(before, stat)
		} else {
			after = append(after, stat)
		}
	}
	return before, after
}

func filterStatsSince(stats []model.VerificationStat, since time.Time) []model.VerificationStat {
	filtered := make([]model.VerificationStat, 0, len(stats))
	for _, stat := range stats {
		if !stat.StartedAt.Before(since) {
			filtered = append(filtered, stat)
		}
	}
	return filtered
}

type workerDurationSeries struct {
	Labels []string `json:"labels"`
	Values []int    `json:"values"`
	Passed []bool   `json:"passed"`
}

type workerChartData struct {
	WorkerDurations *workerDurationSeries `json:"worker_durations,omitempty"`
}

func buildWorkerChartData(verifications []model.Verification) workerChartData {
	if len(verifications) == 0 {
		return workerChartData{}
	}
	series := &workerDurationSeries{}
	for i := len(verifications) - 1; i >= 0; i-- {
		v := verifications[i]
		if v.Aborted() {
			continue
		}
		series.Labels = append(series.Labels, v.StartedAt.Format(time.RFC3339))
		series.Values = append(series.Values, v.DurationMS)
		series.Passed = append(series.Passed, !v.Failed())
	}
	if len(series.Values) == 0 {
		return workerChartData{}
	}
	return workerChartData{WorkerDurations: series}
}

func buildWorkerTicks(verifications []model.Verification) []model.VerificationTick {
	ticks := make([]model.VerificationTick, 0, len(verifications))
	for i := len(verifications) - 1; i >= 0; i-- {
		v := verifications[i]
		ticks = append(ticks, model.VerificationTick{
			WorkerID:   v.WorkerID,
			StartedAt:  v.StartedAt,
			Status:     v.Status,
			Passed:     v.Passed,
			DurationMS: v.DurationMS,
		})
	}
	return ticks
}

func tickClass(tick model.VerificationTick) string {
	verification := model.Verification{Status: tick.Status, Passed: tick.Passed}
	switch {
	case verification.Aborted():
		return "tick-aborted"
	case verification.Failed():
		return "tick-fail"
	default:
		return "tick-pass"
	}
}

func tickLabel(tick model.VerificationTick) string {
	verification := model.Verification{Status: tick.Status, Passed: tick.Passed}
	switch {
	case verification.Aborted():
		return "aborted"
	case verification.Failed():
		return "fail"
	default:
		return "pass"
	}
}
