package orchestrator

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

type promptMode string

const (
	promptModeHealthy    promptMode = "healthy"
	promptModeTriage     promptMode = "triage"
	promptModeLitestream promptMode = "litestream"
	promptModeHarness    promptMode = "harness"
)

type promptModeInfo struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Summary     string `json:"summary"`
	Recommended bool   `json:"recommended,omitempty"`
}

type incidentGuide struct {
	Headline              string   `json:"headline,omitempty"`
	Summary               string   `json:"summary,omitempty"`
	ProbableSubsystem     string   `json:"probable_subsystem,omitempty"`
	RecommendedPromptMode string   `json:"recommended_prompt_mode,omitempty"`
	WhyLikely             []string `json:"why_likely,omitempty"`
	NextSteps             []string `json:"next_steps,omitempty"`
}

type diagnosisWorkerRef struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ProfileName string `json:"profile_name,omitempty"`
}

type diagnosisCluster struct {
	Key                  string               `json:"key,omitempty"`
	Headline             string               `json:"headline,omitempty"`
	Summary              string               `json:"summary,omitempty"`
	Stage                string               `json:"stage,omitempty"`
	Signature            string               `json:"signature,omitempty"`
	ProbableSubsystem    string               `json:"probable_subsystem,omitempty"`
	Confidence           string               `json:"confidence,omitempty"`
	WorkerCount          int                  `json:"worker_count,omitempty"`
	RepresentativeWorker diagnosisWorkerRef   `json:"representative_worker"`
	Workers              []diagnosisWorkerRef `json:"workers,omitempty"`
	AffectedProfiles     []string             `json:"affected_profiles,omitempty"`
	AffectedDatasets     []string             `json:"affected_datasets,omitempty"`
	AffectedLoadModes    []string             `json:"affected_load_modes,omitempty"`
	WhyLikely            []string             `json:"why_likely,omitempty"`
	NextSteps            []string             `json:"next_steps,omitempty"`
}

type diagnosisSnapshot struct {
	Headline          string             `json:"headline,omitempty"`
	Summary           string             `json:"summary,omitempty"`
	ProbableSubsystem string             `json:"probable_subsystem,omitempty"`
	Confidence        string             `json:"confidence,omitempty"`
	AffectedWorkers   int                `json:"affected_workers,omitempty"`
	DominantStage     string             `json:"dominant_stage,omitempty"`
	DominantSignature string             `json:"dominant_signature,omitempty"`
	AffectedProfiles  []string           `json:"affected_profiles,omitempty"`
	AffectedDatasets  []string           `json:"affected_datasets,omitempty"`
	AffectedLoadModes []string           `json:"affected_load_modes,omitempty"`
	WhyLikely         []string           `json:"why_likely,omitempty"`
	NextSteps         []string           `json:"next_steps,omitempty"`
	Clusters          []diagnosisCluster `json:"clusters,omitempty"`
}

type coverageCount struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

type coverageSnapshot struct {
	LoadModes      []coverageCount `json:"load_modes,omitempty"`
	ReplayDatasets []coverageCount `json:"replay_datasets,omitempty"`
	Profiles       []coverageCount `json:"profiles,omitempty"`
	RuntimeStates  []coverageCount `json:"runtime_states,omitempty"`
}

type promptEventSummary struct {
	CreatedAt             time.Time                        `json:"created_at"`
	EventType             string                           `json:"event_type"`
	Message               string                           `json:"message"`
	CollapsedCount        int                              `json:"collapsed_count,omitempty"`
	CollapsedWindowStart  *time.Time                       `json:"collapsed_window_start,omitempty"`
	CollapsedWindowEnd    *time.Time                       `json:"collapsed_window_end,omitempty"`
	CheckType             string                           `json:"check_type,omitempty"`
	VerificationStatus    string                           `json:"verification_status,omitempty"`
	Passed                *bool                            `json:"passed,omitempty"`
	FailureStage          string                           `json:"failure_stage,omitempty"`
	FailureSignature      string                           `json:"failure_signature,omitempty"`
	FailureClassification *reporting.FailureClassification `json:"failure_classification,omitempty"`
	RuntimeSnapshotStatus string                           `json:"runtime_snapshot_status,omitempty"`
	RuntimeSnapshotError  string                           `json:"runtime_snapshot_error,omitempty"`
	RunID                 string                           `json:"run_id,omitempty"`
	VerificationSteps     []reporting.VerificationStep     `json:"verification_steps,omitempty"`
	FailureDebugCaptured  bool                             `json:"failure_debug_captured,omitempty"`
}

func parsePromptMode(raw string, recommended string) promptMode {
	switch promptMode(strings.ToLower(strings.TrimSpace(raw))) {
	case promptModeHealthy, promptModeTriage, promptModeLitestream, promptModeHarness:
		return promptMode(strings.ToLower(strings.TrimSpace(raw)))
	}

	switch promptMode(strings.ToLower(strings.TrimSpace(recommended))) {
	case promptModeHealthy, promptModeTriage, promptModeLitestream, promptModeHarness:
		return promptMode(strings.ToLower(strings.TrimSpace(recommended)))
	default:
		return promptModeTriage
	}
}

func buildPromptModes(recommended string) []promptModeInfo {
	mode := parsePromptMode("", recommended)
	return []promptModeInfo{
		{
			ID:          string(promptModeHealthy),
			Label:       "Healthy baseline",
			Summary:     "Explain why the worker or rollout looks healthy and what future regressions should be compared against.",
			Recommended: mode == promptModeHealthy,
		},
		{
			ID:          string(promptModeTriage),
			Label:       "Fast triage",
			Summary:     "Classify the likely subsystem, rank hypotheses, and give next commands.",
			Recommended: mode == promptModeTriage,
		},
		{
			ID:          string(promptModeLitestream),
			Label:       "Litestream deep dive",
			Summary:     "Assume the issue may be in Litestream sync, restore, or replication behavior.",
			Recommended: mode == promptModeLitestream,
		},
		{
			ID:          string(promptModeHarness),
			Label:       "Harness sanity check",
			Summary:     "Assume Litestream may be fine and look for runtime, config, or worker-harness issues.",
			Recommended: mode == promptModeHarness,
		},
	}
}

func buildIncidentGuide(bundle *IncidentBundle) incidentGuide {
	stage := bundle.FailureStage
	signature := bundle.FailureSignature
	subsystem := inferProbableSubsystem(stage, signature)
	recommendedMode := recommendedPromptModeForSubsystem(subsystem)

	headline := "Worker currently passing verification"
	summary := "No active failure is present on this worker."
	if bundle.ActiveFailure {
		headline = fmt.Sprintf("Active incident likely in %s", subsystem)
		summary = fmt.Sprintf("The latest verification is failing during %s with signature %s.", valueOrUnknown(stage), valueOrUnknown(signature))
	} else if bundle.LatestFailure != nil {
		headline = fmt.Sprintf("Worker recovered; use this as the current healthy baseline")
		summary = fmt.Sprintf("This worker is running now, but its latest recorded failure happened during %s with signature %s.", valueOrUnknown(stage), valueOrUnknown(signature))
		recommendedMode = promptModeHealthy
		subsystem = "Healthy baseline"
	} else {
		recommendedMode = promptModeHealthy
		subsystem = "Healthy baseline"
	}

	why := make([]string, 0, 4)
	if bundle.ActiveFailure {
		why = append(why, fmt.Sprintf("The latest verification is still failing, and the worker status is %s.", bundle.Worker.Status))
	} else {
		why = append(why, fmt.Sprintf("The worker status is %s and the latest verification is not actively failing.", bundle.Worker.Status))
	}
	if stage != "" {
		why = append(why, fmt.Sprintf("The failure stage is classified as %s.", stage))
	}
	if signature != "" {
		why = append(why, fmt.Sprintf("The dominant error signature is %s.", signature))
	}
	if dataset := bundle.Workload.MetricReplayDataset(); dataset != "none" {
		why = append(why, fmt.Sprintf("This worker is exercising the %s replay dataset.", dataset))
	} else {
		why = append(why, fmt.Sprintf("This worker is running a %s synthetic workload.", bundle.Workload.MetricPattern()))
	}
	if bundle.ReportedRuntime != nil {
		if bundle.ReportedRuntime.LitestreamSnapshotHealthy {
			why = append(why, fmt.Sprintf("The latest Litestream runtime snapshot refreshed at %s.", bundle.ReportedRuntime.SnapshotCollectedAt.Format(timeFormatRFC3339)))
		} else if bundle.ReportedRuntime.LitestreamSnapshotError == reporting.LegacyRuntimeTelemetryError {
			why = append(why, fmt.Sprintf("This worker is still reporting legacy runtime telemetry as of %s, so runtime fields like db_status and last_sync_age_seconds should be treated as advisory.", bundle.ReportedRuntime.SnapshotCollectedAt.Format(timeFormatRFC3339)))
		} else {
			why = append(why, fmt.Sprintf("The latest Litestream runtime snapshot is unhealthy: %s.", valueOrUnknown(bundle.ReportedRuntime.LitestreamSnapshotError)))
		}
	}
	if bundle.LatestPlatformEvent != nil {
		why = append(why, fmt.Sprintf("The latest platform signal is %s at %s: %s.", bundle.LatestPlatformEvent.EventType, bundle.LatestPlatformEvent.CreatedAt.Format(timeFormatRFC3339), bundle.LatestPlatformEvent.Message))
	}

	nextSteps := incidentNextSteps(subsystem, bundle)

	return incidentGuide{
		Headline:              headline,
		Summary:               summary,
		ProbableSubsystem:     subsystem,
		RecommendedPromptMode: string(recommendedMode),
		WhyLikely:             why,
		NextSteps:             nextSteps,
	}
}

func buildDiagnosisSnapshot(summaries []WorkerSummaryResponse) diagnosisSnapshot {
	clusters := buildDiagnosisClusters(summaries)
	if len(clusters) == 0 {
		return diagnosisSnapshot{
			Headline:          "All workers are currently passing verification",
			Summary:           "Use Grafana to inspect last-failure history and workload shape, or open the control-plane help page to onboard a new operator.",
			ProbableSubsystem: "None",
			NextSteps: []string{
				"Check Fleet Last Failure Classes in Grafana for the most recent historical issue.",
				"Use /api/worker-summaries to review workload coverage across synthetic, replay, and mixed workers.",
				"Open /ui/help for the operator workflow and AI debugging guide.",
			},
		}
	}

	totalAffectedWorkers := 0
	profiles := make([]string, 0)
	datasets := make([]string, 0)
	loadModes := make([]string, 0)
	runtimeStates := make(map[string]int)
	for _, cluster := range clusters {
		totalAffectedWorkers += cluster.WorkerCount
		profiles = append(profiles, cluster.AffectedProfiles...)
		datasets = append(datasets, cluster.AffectedDatasets...)
		loadModes = append(loadModes, cluster.AffectedLoadModes...)
	}
	for _, summary := range summaries {
		runtimeStates[summary.RuntimeSnapshotStatus]++
	}

	topCluster := clusters[0]
	headline := fmt.Sprintf("%d workers currently point to %s", totalAffectedWorkers, topCluster.ProbableSubsystem)
	summary := fmt.Sprintf("The dominant live cluster is %s during %s. Start with %s, then use Grafana to confirm whether the issue is clustered by profile or workload shape.", topCluster.Signature, valueOrUnknown(topCluster.Stage), topCluster.RepresentativeWorker.Name)
	if len(clusters) > 1 {
		headline = fmt.Sprintf("%d active failure clusters across %d workers", len(clusters), totalAffectedWorkers)
		summary = fmt.Sprintf("The dominant live cluster is %s during %s, pointing to %s. There are %d additional active cluster(s) that should be reviewed before treating this as one isolated issue.", topCluster.Signature, valueOrUnknown(topCluster.Stage), topCluster.ProbableSubsystem, len(clusters)-1)
	}

	why := []string{
		fmt.Sprintf("%d workers are currently failing verification across %d active cluster(s).", totalAffectedWorkers, len(clusters)),
		fmt.Sprintf("%s is the dominant current signature across %d worker(s).", topCluster.Signature, topCluster.WorkerCount),
		fmt.Sprintf("The dominant cluster spans profiles %s.", strings.Join(topCluster.AffectedProfiles, ", ")),
	}
	if len(topCluster.AffectedDatasets) > 0 {
		why = append(why, fmt.Sprintf("The dominant cluster touches replay datasets %s.", strings.Join(topCluster.AffectedDatasets, ", ")))
	}
	if len(clusters) > 1 {
		why = append(why, fmt.Sprintf("%d additional cluster(s) are active, so the fleet currently has more than one failure family.", len(clusters)-1))
	}
	if legacyCount := runtimeStates[reporting.RuntimeSnapshotStatusLegacy]; legacyCount > 0 {
		why = append(why, fmt.Sprintf("%d worker(s) are still reporting legacy runtime telemetry, so their runtime fields should be treated as advisory until the fleet is refreshed.", legacyCount))
	}

	nextSteps := append([]string{}, topCluster.NextSteps...)
	if legacyCount := runtimeStates[reporting.RuntimeSnapshotStatusLegacy]; legacyCount > 0 {
		nextSteps = append([]string{
			"Refresh the worker fleet image before treating runtime fields as ground truth on legacy workers.",
		}, nextSteps...)
	}

	return diagnosisSnapshot{
		Headline:          headline,
		Summary:           summary,
		ProbableSubsystem: topCluster.ProbableSubsystem,
		Confidence:        topCluster.Confidence,
		AffectedWorkers:   totalAffectedWorkers,
		DominantStage:     topCluster.Stage,
		DominantSignature: topCluster.Signature,
		AffectedProfiles:  uniqueSortedStrings(profiles),
		AffectedDatasets:  uniqueSortedStrings(datasets),
		AffectedLoadModes: uniqueSortedStrings(loadModes),
		WhyLikely:         why,
		NextSteps:         nextSteps,
		Clusters:          clusters,
	}
}

func relatedDiagnosisClusters(diagnosis diagnosisSnapshot, workerID, failureSignature, probableSubsystem string) []diagnosisCluster {
	clusters := make([]diagnosisCluster, 0, 3)
	seen := make(map[string]struct{})

	appendCluster := func(cluster diagnosisCluster) {
		if len(clusters) >= 3 {
			return
		}
		if _, ok := seen[cluster.Key]; ok {
			return
		}
		seen[cluster.Key] = struct{}{}
		clusters = append(clusters, cluster)
	}

	for _, cluster := range diagnosis.Clusters {
		if diagnosisClusterHasWorker(cluster, workerID) {
			appendCluster(cluster)
		}
	}
	for _, cluster := range diagnosis.Clusters {
		if failureSignature != "" && cluster.Signature == failureSignature {
			appendCluster(cluster)
		}
	}
	for _, cluster := range diagnosis.Clusters {
		if probableSubsystem != "" && cluster.ProbableSubsystem == probableSubsystem {
			appendCluster(cluster)
		}
	}
	for _, cluster := range diagnosis.Clusters {
		appendCluster(cluster)
	}

	return clusters
}

func diagnosisClusterHasWorker(cluster diagnosisCluster, workerID string) bool {
	for _, worker := range cluster.Workers {
		if worker.ID == workerID {
			return true
		}
	}
	return false
}

func buildDiagnosisClusters(summaries []WorkerSummaryResponse) []diagnosisCluster {
	type clusterAccumulator struct {
		stage      string
		signature  string
		subsystem  string
		workers    []diagnosisWorkerRef
		profiles   []string
		datasets   []string
		loadModes  []string
		example    WorkerSummaryResponse
		latestSeen time.Time
	}

	clustersByKey := make(map[string]*clusterAccumulator)
	for _, summary := range summaries {
		if strings.TrimSpace(summary.CurrentFailureSignature) == "" {
			continue
		}

		key := diagnosisClusterKey(summary)
		cluster, ok := clustersByKey[key]
		if !ok {
			cluster = &clusterAccumulator{
				stage:     summary.CurrentFailureStage,
				signature: summary.CurrentFailureSignature,
				subsystem: summary.CurrentProbableSubsystem,
				example:   summary,
			}
			clustersByKey[key] = cluster
		}

		cluster.workers = append(cluster.workers, diagnosisWorkerRef{
			ID:          summary.Worker.ID,
			Name:        workerName(&summary.Worker, summary.Worker.ID),
			ProfileName: summary.Worker.ProfileName,
		})
		cluster.profiles = append(cluster.profiles, summary.Worker.ProfileName)
		cluster.loadModes = append(cluster.loadModes, summary.Workload.MetricLoadMode())
		if dataset := summary.Workload.MetricReplayDataset(); dataset != "none" {
			cluster.datasets = append(cluster.datasets, dataset)
		}
		if summary.LastVerification != nil && summary.LastVerification.StartedAt.After(cluster.latestSeen) {
			cluster.latestSeen = summary.LastVerification.StartedAt
			cluster.example = summary
		}
	}

	clusters := make([]diagnosisCluster, 0, len(clustersByKey))
	for key, cluster := range clustersByKey {
		profiles := uniqueSortedStrings(cluster.profiles)
		datasets := uniqueSortedStrings(cluster.datasets)
		loadModes := uniqueSortedStrings(cluster.loadModes)
		workerCount := len(cluster.workers)
		confidence := diagnosisConfidence(workerCount, len(profiles), len(loadModes), len(datasets), cluster.signature)

		why := []string{
			fmt.Sprintf("%d worker(s) currently share this stage/signature pair.", workerCount),
			fmt.Sprintf("The cluster is classified as %s.", cluster.subsystem),
		}
		if len(profiles) > 0 {
			why = append(why, fmt.Sprintf("Affected profiles: %s.", strings.Join(profiles, ", ")))
		}
		if len(datasets) > 0 {
			why = append(why, fmt.Sprintf("Replay datasets in this cluster: %s.", strings.Join(datasets, ", ")))
		}
		if len(loadModes) > 0 {
			why = append(why, fmt.Sprintf("Load modes in this cluster: %s.", strings.Join(loadModes, ", ")))
		}

		clusters = append(clusters, diagnosisCluster{
			Key:               key,
			Headline:          fmt.Sprintf("%d workers share %s", workerCount, cluster.signature),
			Summary:           fmt.Sprintf("This cluster is failing during %s and currently points to %s.", valueOrUnknown(cluster.stage), cluster.subsystem),
			Stage:             cluster.stage,
			Signature:         cluster.signature,
			ProbableSubsystem: cluster.subsystem,
			Confidence:        confidence,
			WorkerCount:       workerCount,
			RepresentativeWorker: diagnosisWorkerRef{
				ID:          cluster.example.Worker.ID,
				Name:        workerName(&cluster.example.Worker, cluster.example.Worker.ID),
				ProfileName: cluster.example.Worker.ProfileName,
			},
			Workers:           sortDiagnosisWorkers(cluster.workers),
			AffectedProfiles:  profiles,
			AffectedDatasets:  datasets,
			AffectedLoadModes: loadModes,
			WhyLikely:         why,
			NextSteps:         diagnosisNextSteps(cluster.subsystem, cluster.example),
		})
	}

	sort.Slice(clusters, func(i, j int) bool {
		if clusters[i].WorkerCount != clusters[j].WorkerCount {
			return clusters[i].WorkerCount > clusters[j].WorkerCount
		}
		if confidenceRank(clusters[i].Confidence) != confidenceRank(clusters[j].Confidence) {
			return confidenceRank(clusters[i].Confidence) > confidenceRank(clusters[j].Confidence)
		}
		return clusters[i].Headline < clusters[j].Headline
	})

	return clusters
}

func buildCoverageSnapshot(summaries []WorkerSummaryResponse) coverageSnapshot {
	loadModes := make(map[string]int)
	datasets := make(map[string]int)
	profiles := make(map[string]int)
	runtimeStates := make(map[string]int)

	for _, summary := range summaries {
		loadModes[summary.Workload.MetricLoadMode()]++
		profiles[summary.Worker.ProfileName]++
		if dataset := summary.Workload.MetricReplayDataset(); dataset != "none" {
			datasets[dataset]++
		}
		runtimeStates[firstNonEmpty(summary.RuntimeSnapshotStatus, reporting.RuntimeSnapshotStatusMissing)]++
	}

	return coverageSnapshot{
		LoadModes:      sortedCoverageCounts(loadModes),
		ReplayDatasets: sortedCoverageCounts(datasets),
		Profiles:       sortedCoverageCounts(profiles),
		RuntimeStates:  sortedCoverageCounts(runtimeStates),
	}
}

func buildPrompt(bundle *IncidentBundle, mode promptMode) string {
	task := promptTask(mode)
	returnFormat := promptReturnFormat(mode)
	intro := "You are diagnosing a Litestream soak incident."
	if mode == promptModeHealthy {
		intro = "You are reviewing a healthy Litestream soak baseline."
	}

	sections := []string{
		intro,
		"",
		"<mode>",
		string(mode),
		"</mode>",
		"",
		"<task>",
		task,
		"</task>",
		"",
		"<return_format>",
		returnFormat,
		"</return_format>",
	}

	if bundle.Guide.ProbableSubsystem != "" {
		sections = append(
			sections,
			"",
			"<operator_diagnosis>",
			fmt.Sprintf("probable_subsystem: %s", bundle.Guide.ProbableSubsystem),
			fmt.Sprintf("summary: %s", bundle.Guide.Summary),
			"why_likely:",
			bulletLines(bundle.Guide.WhyLikely),
			"next_steps:",
			bulletLines(bundle.Guide.NextSteps),
			"</operator_diagnosis>",
		)
	}

	if len(bundle.TriageCommands) > 0 {
		sections = append(
			sections,
			"",
			"<control_plane_access>",
			"The control-plane API uses HTTP basic auth in production.",
			`Export SOAK_BASIC_AUTH_USERNAME and SOAK_BASIC_AUTH_PASSWORD or source .envrc before running the curl commands below.`,
			`Use the provided incident and diagnosis curl commands as written so you can compare the worker-specific evidence against the live fleet diagnosis.`,
			"</control_plane_access>",
		)
	}

	sections = append(
		sections,
		"",
		"<worker_debug_tools>",
		"Standard worker images include curl, jq, rg, procps, iproute2/ss, sqlite3, /usr/bin/time, lsof, strace, file, netcat-openbsd, dnsutils, and s3cmd.",
		"Use /api/workers/{id}/debug-snapshot when it exists before asking an operator to SSH. The snapshot is failure-triggered, bounded, and may be absent for successful checks or repeated same-signature failures.",
		"</worker_debug_tools>",
	)

	if bundle.Diagnosis.Headline != "" {
		sections = append(
			sections,
			"",
			"<fleet_diagnosis>",
			fmt.Sprintf("headline: %s", bundle.Diagnosis.Headline),
			fmt.Sprintf("summary: %s", bundle.Diagnosis.Summary),
			fmt.Sprintf("probable_subsystem: %s", valueOrUnknown(bundle.Diagnosis.ProbableSubsystem)),
			fmt.Sprintf("confidence: %s", valueOrUnknown(bundle.Diagnosis.Confidence)),
			fmt.Sprintf("affected_workers: %d", bundle.Diagnosis.AffectedWorkers),
			fmt.Sprintf("dominant_stage: %s", valueOrUnknown(bundle.Diagnosis.DominantStage)),
			fmt.Sprintf("dominant_signature: %s", valueOrUnknown(bundle.Diagnosis.DominantSignature)),
			"why_likely:",
			bulletLines(bundle.Diagnosis.WhyLikely),
			"next_steps:",
			bulletLines(bundle.Diagnosis.NextSteps),
			"</fleet_diagnosis>",
		)
	}

	if len(bundle.RelatedClusters) > 0 {
		sections = append(
			sections,
			"",
			"<related_clusters>",
			mustJSON(bundle.RelatedClusters),
			"</related_clusters>",
		)
	}

	sections = append(
		sections,
		"",
		"<summary>",
		fmt.Sprintf("generated_at: %s", bundle.GeneratedAt.Format(timeFormatRFC3339)),
		fmt.Sprintf("worker_id: %s", bundle.Worker.ID),
		fmt.Sprintf("status: %s", bundle.Worker.Status),
		fmt.Sprintf("soak_git_sha: %s", valueOrUnknown(bundle.Worker.GitSHA)),
		fmt.Sprintf("litestream_sha_under_test: %s", valueOrUnknown(bundle.Worker.LitestreamSHA)),
		fmt.Sprintf("last_heartbeat_at: %s", formatTime(bundle.Worker.LastHeartbeatAt)),
		fmt.Sprintf("load_mode: %s", valueOrUnknown(bundle.Workload.MetricLoadMode())),
		fmt.Sprintf("replay_dataset: %s", valueOrUnknown(bundle.Workload.MetricReplayDataset())),
		fmt.Sprintf("failure_stage: %s", valueOrUnknown(bundle.FailureStage)),
		fmt.Sprintf("failure_signature: %s", valueOrUnknown(bundle.FailureSignature)),
		fmt.Sprintf("current_runtime_snapshot_status: %s", valueOrUnknown(bundle.RuntimeSnapshotStatus)),
		"</summary>",
	)

	if bundle.ReportedRuntime != nil {
		sections = append(
			sections,
			"",
			"<reported_runtime>",
			mustJSON(bundle.ReportedRuntime),
			"</reported_runtime>",
		)
	}

	if bundle.FailureDebug != nil {
		sections = append(
			sections,
			"",
			"<failure_debug_snapshot>",
			"This worker captured a bounded failure-time snapshot automatically. Prefer this over ad-hoc SSH when ranking hypotheses.",
			"It includes process table, per-process FD counts and FD type breakdowns, Litestream socket summary, disk/cgroup state, verification substep timings, run identity, recent process log tails, and object storage prefix summary when available.",
			mustJSON(bundle.FailureDebug),
			"</failure_debug_snapshot>",
		)
	} else {
		sections = append(
			sections,
			"",
			"<failure_debug_snapshot>",
			"No failure-time debug snapshot is attached. Use the triage commands if you need live process, FD, disk, or socket evidence.",
			"</failure_debug_snapshot>",
		)
	}

	if bundle.LatestPlatformEvent != nil {
		sections = append(
			sections,
			"",
			"<latest_platform_event>",
			mustJSON(bundle.LatestPlatformEvent),
			"</latest_platform_event>",
		)
	}

	sections = append(
		sections,
		"",
		"<runtime_interpretation>",
		"The current <reported_runtime> block is the highest-priority runtime evidence for this worker.",
		"Historical event summaries below are normalized per event timestamp so you can see whether older verification events came from healthy, legacy, or unhealthy runtime snapshots.",
		"If current_runtime_snapshot_status is unhealthy, treat the current snapshot error as stronger evidence than older successful runtime fields embedded in historical verification payloads.",
		"</runtime_interpretation>",
	)

	sections = append(
		sections,
		"",
		"<triage_commands>",
		strings.Join(bundle.TriageCommands, "\n"),
		"</triage_commands>",
		"",
		"<worker>",
		mustJSON(bundle.Worker),
		"</worker>",
	)

	if bundle.LatestFailure != nil {
		sections = append(
			sections,
			"",
			"<latest_failed_verification>",
			mustJSON(bundle.LatestFailure),
			"</latest_failed_verification>",
		)
	}

	sections = append(
		sections,
		"",
		"<recent_verifications>",
		mustJSON(bundle.RecentVerifications),
		"</recent_verifications>",
		"",
		"<recent_event_summaries>",
		mustJSON(buildPromptEventSummaries(bundle.RecentEvents)),
		"</recent_event_summaries>",
		"",
		"<workload>",
		mustJSON(bundle.Workload),
		"</workload>",
	)

	if bundle.Machine != nil {
		sections = append(
			sections,
			"",
			"<machine>",
			mustJSON(bundle.Machine),
			"</machine>",
		)
	}
	if bundle.MachineError != "" {
		sections = append(
			sections,
			"",
			"<machine_error>",
			bundle.MachineError,
			"</machine_error>",
		)
	}

	return strings.Join(sections, "\n")
}

func buildPromptEventSummaries(events []model.Event) []promptEventSummary {
	summaries := make([]promptEventSummary, 0, len(events))
	for _, event := range events {
		summary := promptEventSummary{
			CreatedAt:            event.CreatedAt,
			EventType:            event.EventType,
			Message:              event.Message,
			CollapsedCount:       event.CollapsedCount,
			CollapsedWindowStart: event.CollapsedWindowStart,
			CollapsedWindowEnd:   event.CollapsedWindowEnd,
		}

		if strings.HasPrefix(event.EventType, "verification_") || event.EventType == "first_failure" {
			var payload reporting.VerificationPayload
			if err := json.Unmarshal([]byte(event.Details), &payload); err == nil {
				summary.CheckType = payload.CheckType
				summary.VerificationStatus = payload.Status
				summary.Passed = boolPtr(payload.Passed)

				runtime := payload.RuntimePayload.Normalize(event.CreatedAt.UTC())
				summary.RuntimeSnapshotStatus = reporting.SnapshotStatus(&runtime)
				summary.RuntimeSnapshotError = runtime.LitestreamSnapshotError
				summary.RunID = payload.RunID
				summary.VerificationSteps = payload.Steps
				summary.FailureDebugCaptured = payload.FailureDebug != nil

				verification := model.Verification{
					WorkerID:     payload.WorkerID,
					StartedAt:    payload.StartedAt,
					Status:       payload.Status,
					CheckType:    payload.CheckType,
					Passed:       payload.Passed,
					DurationMS:   payload.DurationMS,
					ErrorMessage: payload.ErrorMessage,
				}
				summary.FailureStage = inferFailureStage(&verification)
				summary.FailureSignature = inferFailureSignature(&verification)
				if payload.FailureClassification != nil {
					summary.FailureClassification = payload.FailureClassification
				} else {
					classification := reporting.ClassifyVerificationFailure(verification.CheckType, verification.ErrorMessage)
					summary.FailureClassification = &classification
				}
			}
		} else if event.EventType == "worker_failed" {
			var payload reporting.WorkerEventPayload
			if err := json.Unmarshal([]byte(event.Details), &payload); err == nil {
				summary.RunID = payload.RunID
				summary.FailureDebugCaptured = payload.FailureDebug != nil
				runtime := payload.RuntimePayload.Normalize(event.CreatedAt.UTC())
				summary.RuntimeSnapshotStatus = reporting.SnapshotStatus(&runtime)
				summary.RuntimeSnapshotError = runtime.LitestreamSnapshotError
			}
		}

		summaries = append(summaries, summary)
	}
	return summaries
}

func boolPtr(value bool) *bool {
	v := value
	return &v
}

func promptTask(mode promptMode) string {
	switch mode {
	case promptModeHealthy:
		return "Explain why this worker, rollout, or archived run currently looks healthy enough to use as a baseline. Call out the strongest health evidence, anything unusual but not yet failing, and what future regressions should be compared against."
	case promptModeLitestream:
		return "Assume the most likely issue is in Litestream sync, restore, or replication behavior. Use both the worker incident evidence and the fleet diagnosis to explain whether the failure is in the control socket, restore path, replica object fetch path, or restore correctness."
	case promptModeHarness:
		return "Assume Litestream may be innocent. Use both the worker incident evidence and the fleet diagnosis to look for worker-harness issues, runtime conditions, bad timeouts, bad config, Fly machine problems, or dataset-specific behavior."
	default:
		return "Classify the likely subsystem first, rank the top hypotheses, and use the fleet diagnosis to decide whether this looks shared across workers or isolated before recommending next commands or log locations. Do not jump straight to code changes."
	}
}

func promptReturnFormat(mode promptMode) string {
	switch mode {
	case promptModeHealthy:
		return strings.Join([]string{
			"1. Why this looks healthy enough to use as a baseline",
			"2. The strongest evidence for that, using verification results, runtime snapshots, and rollout/comparison status",
			"3. Anything unusual or worth watching even though it is not failing yet",
			"4. What future failures or regressions should be compared against",
			"5. Exact next checks or dashboards to revisit after the next release",
		}, "\n")
	case promptModeLitestream:
		return strings.Join([]string{
			"1. Most likely Litestream subsystem",
			"2. Evidence for that subsystem, prioritizing the failure_debug_snapshot and verification step timings",
			"3. Competing hypotheses",
			"4. Whether this looks shared across the fleet or isolated, using the fleet diagnosis and related clusters",
			"5. Exact next commands or files to inspect, preferring the auth-safe control-plane and Fly commands already provided",
		}, "\n")
	case promptModeHarness:
		return strings.Join([]string{
			"1. Most likely non-Litestream cause",
			"2. Evidence for that cause, prioritizing process FD/socket snapshots, cgroup state, child exit evidence, and step timings",
			"3. What would falsify this hypothesis",
			"4. Whether the workload shape or fleet clustering is a clue",
			"5. Exact next commands or files to inspect, preferring the auth-safe control-plane and Fly commands already provided",
		}, "\n")
	default:
		return strings.Join([]string{
			"1. Likely subsystem",
			"2. Top three hypotheses ranked",
			"3. Evidence for each, using failure_debug_snapshot before older repeated failure messages",
			"4. Whether this looks shared across the fleet or isolated, with evidence",
			"5. Fastest next commands or logs, preferring the auth-safe control-plane and Fly commands already provided",
			"6. Whether to investigate Litestream, runtime, S3, or the harness first",
		}, "\n")
	}
}

func inferProbableSubsystem(stage, signature string) string {
	text := strings.ToLower(stage + " " + signature)
	switch {
	case strings.Contains(text, "db_sync_executor") || strings.Contains(text, "db sync executor"):
		return "Litestream DB sync executor"
	case strings.Contains(text, "sync") || strings.Contains(text, "litestream_sync_socket_refused") || strings.Contains(text, "litestream_sync_timeout") || strings.Contains(text, "litestream_sync_fd_exhausted"):
		return "Litestream sync/control socket"
	case strings.Contains(text, "restore") || strings.Contains(text, "replica_") || strings.Contains(text, "ltx"):
		return "Replication or restore path"
	case strings.Contains(text, "integrity") || strings.Contains(text, "sqlite_index_mismatch") || strings.Contains(text, "validation_failed") || strings.Contains(text, "validation"):
		return "Restore correctness / integrity validation"
	case strings.Contains(text, "pause load") || strings.Contains(text, "checkpoint"):
		return "Soak harness or worker runtime"
	default:
		return "Needs operator triage"
	}
}

func recommendedPromptModeForSubsystem(subsystem string) promptMode {
	switch subsystem {
	case "Healthy baseline":
		return promptModeHealthy
	case "Litestream DB sync executor", "Litestream sync/control socket", "Replication or restore path", "Restore correctness / integrity validation":
		return promptModeLitestream
	case "Soak harness or worker runtime":
		return promptModeHarness
	default:
		return promptModeTriage
	}
}

func incidentNextSteps(subsystem string, bundle *IncidentBundle) []string {
	steps := make([]string, 0, 6)
	if subsystem == "Healthy baseline" {
		steps = append(steps,
			"Capture this prompt as a clean baseline before the next deploy or workload change.",
			"Use the latest verification result, runtime snapshot, and comparison verdict as the reference point for future regressions.",
			"After the next release, compare any worker that needs attention against this healthy baseline before blaming Litestream or the harness.",
		)
		if len(bundle.TriageCommands) > 0 {
			steps = append(steps, fmt.Sprintf("If you want a live follow-up snapshot, start with %s.", bundle.TriageCommands[0]))
		}
		return steps
	}

	if bundle.LatestPlatformEvent != nil {
		switch bundle.LatestPlatformEvent.EventType {
		case "platform_oom":
			steps = append(steps,
				"Treat the recent OOM as first-class evidence. Check memory pressure and process survival before assuming a logic bug.",
				"Inspect the worker page event timeline and Fly logs around the OOM timestamp before retrying the workload.",
			)
		case "platform_disk_full":
			steps = append(steps,
				"Treat disk pressure as the current blocker. Check volume usage and Litestream temp/LTX growth before changing application logic.",
				"Compare this worker against other profiles to see whether the failure is tied to dataset or write rate.",
			)
		case "platform_restart", "platform_killed":
			steps = append(steps,
				"Treat the recent platform restart or kill as first-class evidence before investigating higher-level verification failures.",
				"Inspect the worker event timeline and Fly logs around the restart timestamp to see what died first.",
			)
		}
	}
	if bundle.ReportedRuntime != nil && bundle.ReportedRuntime.LitestreamSnapshotError == reporting.LegacyRuntimeTelemetryError {
		steps = append(steps,
			"Treat runtime fields on this page as advisory for this worker because it is still emitting legacy telemetry without snapshot-health metadata.",
			"Use the verification failure, machine logs, and fleet diagnosis first; refresh the worker before trusting db_status or sync-age fields.",
		)
	}
	switch subsystem {
	case "Litestream DB sync executor":
		steps = append(steps,
			"Start with failure_debug_snapshot.sync_status_before_sync and sync_status_after_sync_failure to identify the active Litestream sync operation and phase.",
			"Inspect failure_debug_snapshot.litestream_goroutines_on_sync_failure for the goroutine holding or waiting on DB sync, checkpoint, or replica sync paths.",
			"Compare executor_waiter_count and executor_wait_seconds across related workers before treating this as a socket or process-liveness problem.",
		)
	case "Litestream sync/control socket":
		steps = append(steps,
			"Check whether the Litestream process is running and whether /data/litestream.sock exists on the worker.",
			"Inspect the worker logs around the failed verification window for Litestream startup, crash, or timeout messages.",
			"Use Grafana to see whether sync failures are clustered on high-load or GH Archive workers.",
		)
	case "Replication or restore path":
		steps = append(steps,
			"Inspect restore-related log lines and object-fetch errors in the incident bundle and worker logs.",
			"Check whether the same restore failure is happening across multiple workers or only one worker prefix in object storage.",
			"Confirm whether the failure is a timeout, missing LTX file, or restore-plan failure.",
		)
	case "Restore correctness / integrity validation":
		steps = append(steps,
			"Confirm that restore completed before validation failed, then inspect the exact integrity-check output.",
			"Compare this worker against other workers using the same workload shape to see whether the mismatch repeats.",
			"Capture the incident bundle and validation output before making code changes.",
		)
	default:
		steps = append(steps,
			"Inspect the worker logs and incident bundle first.",
			"Check Grafana to see whether the problem is clustered by workload shape.",
			"Use the AI prompt bundle to rank likely subsystems before changing code.",
		)
	}

	if len(bundle.TriageCommands) > 0 {
		steps = append(steps, fmt.Sprintf("Run the triage commands listed on this page, starting with %s.", bundle.TriageCommands[0]))
	}
	return steps
}

func buildRolloutPrompt(rollout DeploymentRolloutResponse, comparison *DeploymentComparisonResponse, mode promptMode) string {
	task := promptTask(mode)
	returnFormat := promptReturnFormat(mode)
	intro := "You are reviewing a Litestream soak rollout."
	if mode == promptModeHealthy {
		intro = "You are reviewing a healthy Litestream soak rollout baseline."
	}

	sections := []string{
		intro,
		"",
		"<mode>",
		string(mode),
		"</mode>",
		"",
		"<task>",
		task,
		"</task>",
		"",
		"<return_format>",
		returnFormat,
		"</return_format>",
		"",
		"<rollout_summary>",
		fmt.Sprintf("status: %s", rollout.Status),
		fmt.Sprintf("summary: %s", rollout.Summary),
		fmt.Sprintf("next_action: %s", valueOrUnknown(rollout.NextAction)),
		fmt.Sprintf("grace_window_exceeded: %t", rollout.GraceWindowExceeded),
		"</rollout_summary>",
		"",
		"<latest_rollout>",
		mustJSON(rollout),
		"</latest_rollout>",
	}

	if comparison != nil {
		sections = append(
			sections,
			"",
			"<release_comparison>",
			mustJSON(comparison),
			"</release_comparison>",
		)
	}

	return strings.Join(sections, "\n")
}

func buildComparisonPrompt(comparison DeploymentComparisonResponse, mode promptMode) string {
	task := promptTask(mode)
	returnFormat := promptReturnFormat(mode)
	intro := "You are reviewing a Litestream soak release comparison."
	if mode == promptModeHealthy {
		intro = "You are reviewing a healthy Litestream soak comparison baseline."
	}

	sections := []string{
		intro,
		"",
		"<mode>",
		string(mode),
		"</mode>",
		"",
		"<task>",
		task,
		"</task>",
		"",
		"<return_format>",
		returnFormat,
		"</return_format>",
		"",
		"<comparison_summary>",
		fmt.Sprintf("verdict: %s", comparison.Verdict),
		fmt.Sprintf("summary: %s", comparison.Summary),
		fmt.Sprintf("base_source: %s", comparison.BaseSource),
		fmt.Sprintf("head_source: %s", comparison.HeadSource),
		"</comparison_summary>",
		"",
		"<release_comparison>",
		mustJSON(comparison),
		"</release_comparison>",
	}

	return strings.Join(sections, "\n")
}

func buildRunArchivePrompt(archive model.RunArchive, mode promptMode) string {
	task := promptTask(mode)
	returnFormat := promptReturnFormat(mode)
	intro := "You are reviewing an archived Litestream soak run."
	if mode == promptModeHealthy {
		intro = "You are reviewing an archived healthy Litestream soak baseline."
	}

	sections := []string{
		intro,
		"",
		"<mode>",
		string(mode),
		"</mode>",
		"",
		"<task>",
		task,
		"</task>",
		"",
		"<return_format>",
		returnFormat,
		"</return_format>",
		"",
		"<run_archive>",
		mustJSON(archive),
		"</run_archive>",
		"",
		"<archived_payload>",
		prettyRawJSON(archive.Payload),
		"</archived_payload>",
	}

	return strings.Join(sections, "\n")
}

func prettyRawJSON(raw string) string {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return raw
	}
	return mustJSON(value)
}

func diagnosisNextSteps(subsystem string, example WorkerSummaryResponse) []string {
	steps := []string{
		fmt.Sprintf("Open %s in the control plane first.", example.Worker.Name),
		"Compare the failing workers against Grafana Fleet Workload Shapes and Fleet Last Failure Classes.",
	}
	switch subsystem {
	case "Litestream DB sync executor":
		steps = append(steps, "Inspect sync-status and pprof evidence before treating this as a socket liveness issue.")
	case "Litestream sync/control socket":
		steps = append(steps, "Inspect Litestream process and socket health before assuming the replay dataset is bad.")
	case "Replication or restore path":
		steps = append(steps, "Inspect restore/object-fetch errors before treating this as a data-integrity problem.")
	case "Restore correctness / integrity validation":
		steps = append(steps, "Confirm that restore completed and focus on validate/integrity output next.")
	default:
		steps = append(steps, "Use the worker incident bundle and AI prompt to narrow the subsystem.")
	}
	return steps
}

func sortedCoverageCounts(values map[string]int) []coverageCount {
	items := make([]coverageCount, 0, len(values))
	for label, count := range values {
		if strings.TrimSpace(label) == "" {
			continue
		}
		items = append(items, coverageCount{Label: label, Count: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Label < items[j].Label
	})
	return items
}

func diagnosisClusterKey(summary WorkerSummaryResponse) string {
	return strings.Join([]string{
		summary.Worker.Source,
		summary.Worker.GitSHA,
		summary.Worker.LitestreamSHA,
		summary.CurrentFailureStage,
		summary.CurrentFailureSignature,
		summary.CurrentProbableSubsystem,
	}, "|")
}

func diagnosisConfidence(workerCount, profileCount, loadModeCount, datasetCount int, signature string) string {
	score := 0
	switch {
	case workerCount >= 4:
		score += 4
	case workerCount == 3:
		score += 3
	case workerCount == 2:
		score += 2
	case workerCount == 1:
		score++
	}

	if profileCount >= 2 {
		score++
	}
	if loadModeCount >= 2 {
		score++
	}
	if datasetCount >= 2 {
		score++
	}
	if strings.TrimSpace(signature) != "" {
		score++
	}

	switch {
	case score >= 5:
		return "high"
	case score >= 3:
		return "medium"
	default:
		return "low"
	}
}

func confidenceRank(value string) int {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "high":
		return 3
	case "medium":
		return 2
	case "low":
		return 1
	default:
		return 0
	}
}

func sortDiagnosisWorkers(workers []diagnosisWorkerRef) []diagnosisWorkerRef {
	items := append([]diagnosisWorkerRef(nil), workers...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name != items[j].Name {
			return items[i].Name < items[j].Name
		}
		return items[i].ID < items[j].ID
	})
	return items
}

func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{})
	items := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		items = append(items, value)
	}
	sort.Strings(items)
	return items
}

func topCurrentFailure(summaries []WorkerSummaryResponse, key func(WorkerSummaryResponse) string) (string, int) {
	counts := make(map[string]int)
	topValue := ""
	topCount := 0
	for _, summary := range summaries {
		value := strings.TrimSpace(key(summary))
		if value == "" {
			continue
		}
		counts[value]++
		if counts[value] > topCount {
			topValue = value
			topCount = counts[value]
		}
	}
	return topValue, topCount
}

func bulletLines(values []string) string {
	if len(values) == 0 {
		return "- none"
	}
	lines := make([]string, 0, len(values))
	for _, value := range values {
		lines = append(lines, "- "+value)
	}
	return strings.Join(lines, "\n")
}

const timeFormatRFC3339 = "2006-01-02T15:04:05Z07:00"

func activeFailure(verification *model.Verification) bool {
	if verification == nil {
		return false
	}
	return !verification.Passed || strings.EqualFold(verification.Status, "failed")
}
