package orchestrator

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/workload"
)

type homePageData struct {
	GeneratedAt              time.Time
	SelectedSource           string
	SelectedSourceLabel      string
	ScopeSummary             string
	Summary                  homeSummary
	Diagnosis                diagnosisSnapshot
	Coverage                 coverageSnapshot
	LatestDeployment         *DeploymentRolloutResponse
	ReleaseComparison        *DeploymentComparisonResponse
	ActiveSources            []homeSourceCard
	LatestRolloutURL         string
	LatestRolloutPromptURL   string
	LatestRolloutPromptLabel string
	ComparisonJSONURL        string
	ComparisonPromptURL      string
	ComparisonPromptLabel    string
	Spotlight                *FailureResponse
	FailureQueue             []FailureResponse
	Workers                  []homeWorker
	Events                   []model.Event
	Attention                []attentionItem
	KPIs                     homeKPIs
	ChartData                homeChartData
}

type homeSummary struct {
	TotalWorkers         int
	HealthyWorkers       int
	AttentionWorkers     int
	CompletedSuccess     bool
	Retired              bool
	ActiveVerifications  int
	RecentFailures       int
	LatestHeartbeatAt    *time.Time
	StalestHeartbeatAt   *time.Time
	StalestRuntimeAt     *time.Time
	LatestVerificationAt *time.Time
}

type homeWorker struct {
	Worker                   model.Worker
	LatestVerification       *model.Verification
	ActiveVerification       *reporting.ActiveVerification
	Workload                 workload.Config
	RuntimeSnapshotStatus    string
	CurrentFailureStage      string
	CurrentFailureSignature  string
	CurrentFailureCategory   string
	CurrentFailureSeverity   string
	CurrentProbableSubsystem string
	CompletedSuccess         bool
	Ticks                    []model.VerificationTick
}

type homeSourceCard struct {
	Source     string
	Label      string
	Summary    string
	Status     string
	Selected   bool
	ViewURL    string
	CompareURL string
	Total      int
	Attention  int
	Passed     bool
	Retired    bool
}

type workerPageData struct {
	GeneratedAt time.Time
	Incident    *IncidentBundle
	Ticks       []model.VerificationTick
	ChartData   workerChartData
}

type helpPageData struct {
	GeneratedAt time.Time
	Diagnosis   diagnosisSnapshot
	Coverage    coverageSnapshot
	PromptModes []promptModeInfo
}

func (a *API) handleHome(w http.ResponseWriter, r *http.Request) {
	data, err := a.buildHomePageData(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	renderHTML(w, "home", data)
}

func (a *API) handleHomePartial(w http.ResponseWriter, r *http.Request) {
	data, err := a.buildHomePageData(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	renderHTML(w, "home_body", data)
}

func (a *API) buildHomePageData(r *http.Request) (homePageData, error) {
	requestedSource := "main"
	baseSource := ""
	headSource := ""
	if r != nil {
		requestedSource = firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("source")), "main")
		baseSource = strings.TrimSpace(r.URL.Query().Get("base_source"))
		headSource = strings.TrimSpace(r.URL.Query().Get("head_source"))
	}
	if requestedSource != "main" && baseSource == "" && headSource == "" {
		baseSource = "main"
		headSource = requestedSource
	}
	rolloutSource := firstNonEmpty(headSource, requestedSource, "main")
	comparisonJSONURL := "/api/deployments/compare/latest"
	latestRolloutURL := "/api/deployments/latest"
	if headSource != "" || baseSource != "" {
		query := url.Values{}
		if baseSource != "" {
			query.Set("base_source", baseSource)
		}
		if headSource != "" {
			query.Set("head_source", headSource)
		}
		comparisonJSONURL += "?" + query.Encode()
	}
	if rolloutSource != "main" {
		latestRolloutURL += "?source=" + url.QueryEscape(rolloutSource)
	}

	summaries, err := a.listWorkerSummaries("", requestedSource)
	if err != nil {
		return homePageData{}, err
	}

	now := time.Now().UTC()
	chartFrom := now.Truncate(time.Hour).Add(-(homeChartHours - 1) * time.Hour)
	sourceStats, err := a.db.ListVerificationStatsSince(requestedSource, now.Add(-48*time.Hour))
	if err != nil {
		return homePageData{}, err
	}
	sourceStats = filterStatsToActiveWorkers(sourceStats, summaries)
	previousStats, windowStats := splitStatsAt(sourceStats, now.Add(-24*time.Hour))
	allWindowStats, err := a.db.ListVerificationStatsSince("", now.Add(-24*time.Hour))
	if err != nil {
		return homePageData{}, err
	}
	failureContext := buildFailureClassificationContext(allWindowStats)
	var mainStats []model.VerificationStat
	if requestedSource != "main" {
		mainStats, err = a.db.ListVerificationStatsSince("main", chartFrom)
		if err != nil {
			return homePageData{}, err
		}
	}
	ticksByWorker, err := a.db.ListVerificationTicks(20, now.Add(-7*24*time.Hour))
	if err != nil {
		return homePageData{}, err
	}

	latestDeployment, err := a.db.GetLatestDeployment(rolloutSource)
	if err != nil {
		return homePageData{}, err
	}

	failures, err := a.db.ListRecentFailedVerifications(16)
	if err != nil {
		return homePageData{}, err
	}

	events, err := a.db.ListEvents(12)
	if err != nil {
		return homePageData{}, err
	}
	events = coalesceEventFeed(events)

	successArchivesBySource, err := listSuccessArchivesBySource(a.db)
	if err != nil {
		return homePageData{}, err
	}
	_, sourceHasSuccessArchive := successArchivesBySource[requestedSource]
	sourceCompletedSuccess := sourceHasSuccessArchive && allWorkerSummariesStopped(summaries)
	sourceRetired := requestedSource != "main" && !sourceCompletedSuccess && allWorkerSummariesTerminal(summaries)

	workerCards := make([]homeWorker, 0, len(summaries))
	summary := homeSummary{
		TotalWorkers:     len(summaries),
		CompletedSuccess: sourceCompletedSuccess,
		Retired:          sourceRetired,
		RecentFailures:   len(failures),
	}

	for _, workerSummary := range summaries {
		card := homeWorker{
			Worker:                   workerSummary.Worker,
			LatestVerification:       workerSummary.LastVerification,
			ActiveVerification:       workerSummary.ActiveVerification,
			Workload:                 workerSummary.Workload,
			RuntimeSnapshotStatus:    workerSummary.RuntimeSnapshotStatus,
			CurrentFailureStage:      workerSummary.CurrentFailureStage,
			CurrentFailureSignature:  workerSummary.CurrentFailureSignature,
			CurrentProbableSubsystem: workerSummary.CurrentProbableSubsystem,
			CompletedSuccess:         sourceHasSuccessArchive && workerSummary.Worker.Status == model.WorkerStopped,
			Ticks:                    ticksByWorker[workerSummary.Worker.ID],
		}
		if card.CurrentFailureSignature != "" {
			failureID := 0
			if workerSummary.LastVerification != nil && workerSummary.LastVerification.Failed() {
				failureID = workerSummary.LastVerification.ID
			} else if workerSummary.LatestFailure != nil {
				failureID = workerSummary.LatestFailure.ID
			}
			card.CurrentFailureCategory = failureContext.categoryForVerificationID(failureID)
			card.CurrentFailureSeverity = failureSeverityForCategory(card.CurrentFailureCategory)
		}

		if !sourceRetired && homeWorkerNeedsAttention(card) {
			// Retired sources are torn down on purpose; leftover failed
			// workers there are history, not something to act on.
			summary.AttentionWorkers++
		} else {
			summary.HealthyWorkers++
		}
		if workerSummary.ActiveVerification != nil {
			summary.ActiveVerifications++
		}
		if workerSummary.Worker.LastHeartbeatAt != nil && !workerSummary.Worker.LastHeartbeatAt.IsZero() {
			summary.LatestHeartbeatAt = maxTime(summary.LatestHeartbeatAt, *workerSummary.Worker.LastHeartbeatAt)
			summary.StalestHeartbeatAt = minTime(summary.StalestHeartbeatAt, *workerSummary.Worker.LastHeartbeatAt)
		}
		if workerSummary.Worker.LastRuntimeAt != nil && !workerSummary.Worker.LastRuntimeAt.IsZero() {
			summary.StalestRuntimeAt = minTime(summary.StalestRuntimeAt, *workerSummary.Worker.LastRuntimeAt)
		}
		if workerSummary.LastVerification != nil {
			if observedAt, ok := verificationObservedAt(*workerSummary.LastVerification); ok {
				summary.LatestVerificationAt = maxTime(summary.LatestVerificationAt, observedAt)
			}
		}

		workerCards = append(workerCards, card)
	}

	sort.SliceStable(workerCards, func(i, j int) bool {
		left := workerCards[i]
		right := workerCards[j]
		if homeWorkerRank(left) != homeWorkerRank(right) {
			return homeWorkerRank(left) < homeWorkerRank(right)
		}
		return heartbeatUnix(left.Worker.LastHeartbeatAt) > heartbeatUnix(right.Worker.LastHeartbeatAt)
	})

	failureCards := make([]FailureResponse, 0, len(failures))
	for _, verification := range failures {
		vf := classifyVerification(&verification)
		category := failureContext.categoryForVerificationID(verification.ID)
		card := FailureResponse{
			Verification:      verification,
			FailureStage:      vf.Stage,
			FailureSignature:  vf.Signature,
			FailureCategory:   category,
			FailureSeverity:   failureSeverityForCategory(category),
			ProbableSubsystem: vf.probableSubsystem(),
		}
		worker, err := a.db.GetWorker(verification.WorkerID)
		if err == nil {
			card.Worker = worker
			if requestedSource != "" && worker.Source != requestedSource {
				continue
			}
		}
		failureCards = append(failureCards, card)
	}
	summary.RecentFailures = len(failureCards)

	var spotlight *FailureResponse
	queue := make([]FailureResponse, 0)
	if len(failureCards) > 0 {
		spotlight = &failureCards[0]
		if len(failureCards) > 1 {
			queue = failureCards[1:]
		}
	}

	var rollout *DeploymentRolloutResponse
	if latestDeployment != nil {
		progress, err := a.buildDeploymentRollout(*latestDeployment)
		if err != nil {
			return homePageData{}, err
		}
		rollout = &progress
	}

	releaseComparison, err := a.buildRequestedDeploymentComparison(requestedSource, baseSource, headSource)
	if err != nil {
		return homePageData{}, err
	}

	latestRolloutPromptURL := rolloutPromptURL(rolloutSource, rollout)
	latestRolloutPromptLabel := promptActionLabelForMode(defaultPromptModeForRolloutValue(rollout))
	comparisonPromptURL := comparisonPromptURL(requestedSource, baseSource, headSource, releaseComparison)
	comparisonPromptLabel := comparisonPromptActionLabelForMode(defaultPromptModeForComparisonValue(releaseComparison))

	activeSources, err := a.buildHomeSourceCards(requestedSource, successArchivesBySource)
	if err != nil {
		return homePageData{}, err
	}

	diagnosis := buildDiagnosisSnapshot(summaries)

	attention := buildAttentionItems(requestedSource, diagnosis, summary, workerCards, rollout, releaseComparison, comparisonPromptURL, comparisonJSONURL, failureContext)
	kpis := buildHomeKPIs(summary, windowStats, previousStats, rollout, failureContext)
	chartData := buildHomeChartData(requestedSource, chartFrom, filterStatsSince(sourceStats, chartFrom), mainStats)

	return homePageData{
		GeneratedAt:              now,
		SelectedSource:           requestedSource,
		SelectedSourceLabel:      sourceHumanLabel(requestedSource),
		ScopeSummary:             buildHomeScopeSummary(requestedSource, rolloutSource, releaseComparison),
		Summary:                  summary,
		Diagnosis:                diagnosis,
		Coverage:                 buildCoverageSnapshot(summaries),
		LatestDeployment:         rollout,
		ReleaseComparison:        releaseComparison,
		ActiveSources:            activeSources,
		LatestRolloutURL:         latestRolloutURL,
		LatestRolloutPromptURL:   latestRolloutPromptURL,
		LatestRolloutPromptLabel: latestRolloutPromptLabel,
		ComparisonJSONURL:        comparisonJSONURL,
		ComparisonPromptURL:      comparisonPromptURL,
		ComparisonPromptLabel:    comparisonPromptLabel,
		Spotlight:                spotlight,
		FailureQueue:             queue,
		Workers:                  workerCards,
		Events:                   events,
		Attention:                attention,
		KPIs:                     kpis,
		ChartData:                chartData,
	}, nil
}

// allWorkerSummariesTerminal reports whether every worker has reached a
// terminal state (stopped or failed) — the shared definition of a retired
// source for both the summary banner and the tab cards.
func allWorkerSummariesTerminal(summaries []WorkerSummaryResponse) bool {
	if len(summaries) == 0 {
		return false
	}
	for _, summary := range summaries {
		if summary.Worker.Status != model.WorkerStopped && summary.Worker.Status != model.WorkerFailed {
			return false
		}
	}
	return true
}

func allWorkerSummariesStopped(summaries []WorkerSummaryResponse) bool {
	if len(summaries) == 0 {
		return false
	}
	for _, summary := range summaries {
		if summary.Worker.Status != model.WorkerStopped {
			return false
		}
	}
	return true
}

func allWorkersStopped(workers []model.Worker) bool {
	if len(workers) == 0 {
		return false
	}
	for _, worker := range workers {
		if worker.Status != model.WorkerStopped {
			return false
		}
	}
	return true
}

func homeWorkerNeedsAttention(worker homeWorker) bool {
	return activeWorkerNeedsAttention(worker.Worker.Status, worker.RuntimeSnapshotStatus)
}

// activeWorkerNeedsAttention mirrors workerNeedsAttention except that stopped
// workers are neutral: stopping is an intentional terminal state (clean pass,
// teardown, retirement), never an alert.
func activeWorkerNeedsAttention(status model.WorkerStatus, runtimeStatus string) bool {
	if status == model.WorkerStopped {
		return false
	}
	return workerNeedsAttention(status, runtimeStatus)
}

func (a *API) buildHomeSourceCards(selectedSource string, successArchivesBySource map[string]model.RunArchive) ([]homeSourceCard, error) {
	workers, err := a.db.ListWorkersFiltered("", "")
	if err != nil {
		return nil, err
	}

	cards := assembleHomeSourceCards(selectedSource, workers, successArchivesBySource)
	for i := range cards {
		if latestDeployment, err := a.db.GetLatestDeployment(cards[i].Source); err == nil && latestDeployment != nil {
			if rollout, err := a.buildDeploymentRollout(*latestDeployment); err == nil {
				cards[i].Status = rollout.Status
			}
		}
	}
	return cards, nil
}

// assembleHomeSourceCards builds the source tab bar. Counts cover active
// (non-stopped) workers only. A non-main source whose workers are all
// stopped/failed is retired: it keeps a tab only while it is the selected
// source (so a stale bookmark renders a neutral retired state instead of a
// missing page) or when it passed a clean soak (success archive present).
func assembleHomeSourceCards(selectedSource string, workers []model.Worker, successArchivesBySource map[string]model.RunArchive) []homeSourceCard {
	type sourceCounts struct {
		total     int
		attention int
	}

	workersBySource := make(map[string][]model.Worker)
	for _, worker := range workers {
		source := firstNonEmpty(strings.TrimSpace(worker.Source), "main")
		workersBySource[source] = append(workersBySource[source], worker)
	}

	countsBySource := make(map[string]sourceCounts)
	for source, sourceWorkers := range workersBySource {
		counts := sourceCounts{}
		for _, worker := range sourceWorkers {
			if worker.Status == model.WorkerStopped || worker.Status == model.WorkerFailed {
				continue
			}
			counts.total++
			runtimeStatus := reporting.SnapshotStatus(extractReportedRuntime(worker, nil))
			if activeWorkerNeedsAttention(worker.Status, runtimeStatus) {
				counts.attention++
			}
		}
		countsBySource[source] = counts
	}

	cards := make([]homeSourceCard, 0, len(countsBySource))
	for source, counts := range countsBySource {
		_, sourceHasSuccessArchive := successArchivesBySource[source]
		sourcePassed := sourceHasSuccessArchive && allWorkersStopped(workersBySource[source])
		sourceRetired := source != "main" && counts.total == 0 && !sourcePassed
		if source != "main" && counts.total == 0 && source != selectedSource {
			continue
		}
		summary := fmt.Sprintf("%d workers, %d need attention", counts.total, counts.attention)
		switch {
		case sourcePassed:
			summary = fmt.Sprintf("%d workers, passed clean soak", len(workersBySource[source]))
		case sourceRetired:
			summary = "retired — fleet torn down, workers stopped"
		}
		card := homeSourceCard{
			Source:    source,
			Label:     sourceHumanLabel(source),
			Summary:   summary,
			Selected:  source == selectedSource,
			ViewURL:   "/ui?source=" + url.QueryEscape(source),
			Total:     counts.total,
			Attention: counts.attention,
			Passed:    sourcePassed,
			Retired:   sourceRetired,
		}
		if source != "main" {
			card.CompareURL = fmt.Sprintf("/ui?source=%s&base_source=main&head_source=%s", url.QueryEscape(source), url.QueryEscape(source))
		}
		cards = append(cards, card)
	}

	sortHomeSourceCards(cards)

	return cards
}

// sortHomeSourceCards orders source tabs identically on every page: main
// first, then PR sources in ascending PR-number order, then anything else
// alphabetically. Selection never reorders tabs.
func sortHomeSourceCards(cards []homeSourceCard) {
	sort.SliceStable(cards, func(i, j int) bool {
		left := cards[i]
		right := cards[j]
		if (left.Source == "main") != (right.Source == "main") {
			return left.Source == "main"
		}
		leftPR := sourcePRNumber(left.Source)
		rightPR := sourcePRNumber(right.Source)
		if (leftPR > 0) != (rightPR > 0) {
			return leftPR > 0
		}
		if leftPR > 0 && leftPR != rightPR {
			return leftPR < rightPR
		}
		return left.Label < right.Label
	})
}

func buildHomeScopeSummary(selectedSource, rolloutSource string, comparison *DeploymentComparisonResponse) string {
	selectedLabel := sourceHumanLabel(selectedSource)
	rolloutLabel := sourceHumanLabel(rolloutSource)
	if comparison != nil && comparison.ComparisonKind == "cross_source" {
		return fmt.Sprintf("You are viewing %s. Latest Rollout below is the latest %s rollout. Source Comparison is comparing %s against %s.", selectedLabel, rolloutLabel, sourceHumanLabel(comparison.HeadSource), sourceHumanLabel(comparison.BaseSource))
	}
	return fmt.Sprintf("You are viewing %s. Latest Rollout below is the latest %s rollout. Release Comparison shows the current %s rollout versus the previous %s rollout.", selectedLabel, rolloutLabel, selectedLabel, selectedLabel)
}

func rolloutPromptURL(source string, rollout *DeploymentRolloutResponse) string {
	query := url.Values{}
	if source != "" && source != "main" {
		query.Set("source", source)
	}
	query.Set("mode", defaultPromptModeForRolloutValue(rollout))
	return "/api/deployments/latest/prompt?" + query.Encode()
}

func comparisonPromptURL(source, baseSource, headSource string, comparison *DeploymentComparisonResponse) string {
	query := url.Values{}
	if source != "" && baseSource == "" && headSource == "" {
		query.Set("source", source)
	}
	if baseSource != "" {
		query.Set("base_source", baseSource)
	}
	if headSource != "" {
		query.Set("head_source", headSource)
	}
	query.Set("mode", defaultPromptModeForComparisonValue(comparison))
	return "/api/deployments/compare/latest/prompt?" + query.Encode()
}

func defaultPromptModeForRolloutValue(rollout *DeploymentRolloutResponse) string {
	if rollout == nil {
		return string(promptModeTriage)
	}
	return defaultPromptModeForRollout(*rollout)
}

func defaultPromptModeForComparisonValue(comparison *DeploymentComparisonResponse) string {
	if comparison == nil {
		return string(promptModeTriage)
	}
	return defaultPromptModeForComparison(*comparison)
}

func promptActionLabelForMode(mode string) string {
	if mode == string(promptModeHealthy) {
		return "Copy healthy baseline prompt"
	}
	return "Copy rollout prompt"
}

func comparisonPromptActionLabelForMode(mode string) string {
	if mode == string(promptModeHealthy) {
		return "Copy healthy comparison prompt"
	}
	return "Copy comparison prompt"
}

func workerPromptURL(worker model.Worker, failureSignature, runtimeSnapshotStatus string) string {
	query := url.Values{}
	if strings.TrimSpace(failureSignature) != "" || activeWorkerNeedsAttention(worker.Status, runtimeSnapshotStatus) {
		query.Set("mode", string(promptModeTriage))
	} else {
		query.Set("mode", string(promptModeHealthy))
	}
	return "/api/workers/" + url.PathEscape(worker.ID) + "/prompt?" + query.Encode()
}

func (a *API) handleWorkerPage(w http.ResponseWriter, r *http.Request) {
	bundle, status, err := a.buildIncidentBundle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	renderHTML(w, "worker", workerPageData{
		GeneratedAt: time.Now().UTC(),
		Incident:    bundle,
		Ticks:       buildWorkerTicks(bundle.RecentVerifications),
		ChartData:   buildWorkerChartData(bundle.RecentVerifications),
	})
}

func (a *API) handleHelpPage(w http.ResponseWriter, r *http.Request) {
	source := "main"
	if r != nil {
		source = firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("source")), "main")
	}

	summaries, err := a.listWorkerSummaries("", source)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	renderHTML(w, "help", helpPageData{
		GeneratedAt: time.Now().UTC(),
		Diagnosis:   buildDiagnosisSnapshot(summaries),
		Coverage:    buildCoverageSnapshot(summaries),
		PromptModes: buildPromptModes(string(promptModeTriage)),
	})
}
