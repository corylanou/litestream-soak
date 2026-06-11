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
	HomeAction               *homeActionPlan
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
	CurrentProbableSubsystem string
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
}

type homeActionPlan struct {
	Headline    string
	Summary     string
	WorkerName  string
	WorkerURL   string
	PromptURL   string
	IncidentURL string
	CompareURL  string
	Steps       []string
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
	previousStats, windowStats := splitStatsAt(sourceStats, now.Add(-24*time.Hour))
	var mainStats []model.VerificationStat
	if requestedSource != "main" {
		mainStats, err = a.db.ListVerificationStatsSince("main", chartFrom)
		if err != nil {
			return homePageData{}, err
		}
	}
	ticksByWorker, err := a.db.ListVerificationTicks(20)
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

	workerCards := make([]homeWorker, 0, len(summaries))
	summary := homeSummary{
		TotalWorkers:   len(summaries),
		RecentFailures: len(failures),
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
			Ticks:                    ticksByWorker[workerSummary.Worker.ID],
		}

		if workerNeedsAttention(workerSummary.Worker.Status, workerSummary.RuntimeSnapshotStatus) {
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
		card := FailureResponse{
			Verification:      verification,
			FailureStage:      inferFailureStage(&verification),
			FailureSignature:  inferFailureSignature(&verification),
			ProbableSubsystem: inferProbableSubsystem(inferFailureStage(&verification), inferFailureSignature(&verification)),
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

	activeSources, err := a.buildHomeSourceCards(requestedSource)
	if err != nil {
		return homePageData{}, err
	}

	diagnosis := buildDiagnosisSnapshot(summaries)

	attention := buildAttentionItems(requestedSource, diagnosis, summary, workerCards, rollout, releaseComparison, comparisonPromptURL, comparisonJSONURL)
	kpis := buildHomeKPIs(summary, windowStats, previousStats, rollout)
	chartData := buildHomeChartData(requestedSource, chartFrom, filterStatsSince(sourceStats, chartFrom), mainStats)

	return homePageData{
		GeneratedAt:              now,
		SelectedSource:           requestedSource,
		SelectedSourceLabel:      sourceHumanLabel(requestedSource),
		ScopeSummary:             buildHomeScopeSummary(requestedSource, rolloutSource, releaseComparison),
		HomeAction:               buildHomeActionPlan(requestedSource, diagnosis),
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

func (a *API) buildHomeSourceCards(selectedSource string) ([]homeSourceCard, error) {
	workers, err := a.db.ListWorkersFiltered("", "")
	if err != nil {
		return nil, err
	}

	type sourceCounts struct {
		total     int
		attention int
	}

	countsBySource := make(map[string]sourceCounts)
	for _, worker := range workers {
		source := firstNonEmpty(strings.TrimSpace(worker.Source), "main")
		runtimeStatus := reporting.SnapshotStatus(extractReportedRuntime(worker, nil))
		counts := countsBySource[source]
		counts.total++
		if workerNeedsAttention(worker.Status, runtimeStatus) {
			counts.attention++
		}
		countsBySource[source] = counts
	}

	cards := make([]homeSourceCard, 0, len(countsBySource))
	for source, counts := range countsBySource {
		card := homeSourceCard{
			Source:    source,
			Label:     sourceHumanLabel(source),
			Summary:   fmt.Sprintf("%d workers, %d need attention", counts.total, counts.attention),
			Selected:  source == selectedSource,
			ViewURL:   "/ui?source=" + url.QueryEscape(source),
			Total:     counts.total,
			Attention: counts.attention,
		}
		if source != "main" {
			card.CompareURL = fmt.Sprintf("/ui?source=%s&base_source=main&head_source=%s", url.QueryEscape(source), url.QueryEscape(source))
		}
		if latestDeployment, err := a.db.GetLatestDeployment(source); err == nil && latestDeployment != nil {
			if rollout, err := a.buildDeploymentRollout(*latestDeployment); err == nil {
				card.Status = rollout.Status
			}
		}
		cards = append(cards, card)
	}

	sort.SliceStable(cards, func(i, j int) bool {
		left := cards[i]
		right := cards[j]
		switch {
		case left.Source == selectedSource && right.Source != selectedSource:
			return true
		case right.Source == selectedSource && left.Source != selectedSource:
			return false
		case left.Source == "main" && right.Source != "main":
			return true
		case right.Source == "main" && left.Source != "main":
			return false
		default:
			return left.Label < right.Label
		}
	})

	return cards, nil
}

func buildHomeScopeSummary(selectedSource, rolloutSource string, comparison *DeploymentComparisonResponse) string {
	selectedLabel := sourceHumanLabel(selectedSource)
	rolloutLabel := sourceHumanLabel(rolloutSource)
	if comparison != nil && comparison.ComparisonKind == "cross_source" {
		return fmt.Sprintf("You are viewing %s. Latest Rollout below is the latest %s rollout. Source Comparison is comparing %s against %s.", selectedLabel, rolloutLabel, sourceHumanLabel(comparison.HeadSource), sourceHumanLabel(comparison.BaseSource))
	}
	return fmt.Sprintf("You are viewing %s. Latest Rollout below is the latest %s rollout. Release Comparison shows the current %s rollout versus the previous %s rollout.", selectedLabel, rolloutLabel, selectedLabel, selectedLabel)
}

func buildHomeActionPlan(selectedSource string, diagnosis diagnosisSnapshot) *homeActionPlan {
	if len(diagnosis.Clusters) == 0 {
		return nil
	}

	cluster := diagnosis.Clusters[0]
	workerID := cluster.RepresentativeWorker.ID
	if strings.TrimSpace(workerID) == "" {
		return nil
	}

	plan := &homeActionPlan{
		Headline:    fmt.Sprintf("Open %s and hand that incident to AI.", cluster.RepresentativeWorker.Name),
		Summary:     fmt.Sprintf("This is the fastest path to debug %s in %s.", valueOrUnknown(cluster.Signature), sourceHumanLabel(selectedSource)),
		WorkerName:  cluster.RepresentativeWorker.Name,
		WorkerURL:   "/ui/workers/" + url.PathEscape(workerID),
		PromptURL:   "/api/workers/" + url.PathEscape(workerID) + "/prompt?mode=triage",
		IncidentURL: "/api/workers/" + url.PathEscape(workerID) + "/incident",
		Steps: []string{
			fmt.Sprintf("Open %s.", cluster.RepresentativeWorker.Name),
			"Copy the AI prompt and give it to your debugging agent.",
			fmt.Sprintf("Ask the agent to explain %s during %s and propose the next commands to run.", valueOrUnknown(cluster.Signature), valueOrUnknown(cluster.Stage)),
		},
	}
	if selectedSource != "main" {
		plan.CompareURL = fmt.Sprintf("/ui?source=%s&base_source=main&head_source=%s", url.QueryEscape(selectedSource), url.QueryEscape(selectedSource))
		plan.Steps = append(plan.Steps, "Check the Source Comparison card against main before deciding whether the PR is better or worse.")
	}
	return plan
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
	if strings.TrimSpace(failureSignature) != "" || workerNeedsAttention(worker.Status, runtimeSnapshotStatus) {
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
