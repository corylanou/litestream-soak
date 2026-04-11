package orchestrator

import (
	"fmt"
	"sort"
	"strings"

	"github.com/corylanou/litestream-soak/internal/model"
)

type promptMode string

const (
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

type diagnosisSnapshot struct {
	Headline          string   `json:"headline,omitempty"`
	Summary           string   `json:"summary,omitempty"`
	ProbableSubsystem string   `json:"probable_subsystem,omitempty"`
	AffectedWorkers   int      `json:"affected_workers,omitempty"`
	DominantStage     string   `json:"dominant_stage,omitempty"`
	DominantSignature string   `json:"dominant_signature,omitempty"`
	AffectedProfiles  []string `json:"affected_profiles,omitempty"`
	AffectedDatasets  []string `json:"affected_datasets,omitempty"`
	AffectedLoadModes []string `json:"affected_load_modes,omitempty"`
	WhyLikely         []string `json:"why_likely,omitempty"`
	NextSteps         []string `json:"next_steps,omitempty"`
}

type coverageCount struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

type coverageSnapshot struct {
	LoadModes      []coverageCount `json:"load_modes,omitempty"`
	ReplayDatasets []coverageCount `json:"replay_datasets,omitempty"`
	Profiles       []coverageCount `json:"profiles,omitempty"`
}

func parsePromptMode(raw string, recommended string) promptMode {
	switch promptMode(strings.ToLower(strings.TrimSpace(raw))) {
	case promptModeTriage, promptModeLitestream, promptModeHarness:
		return promptMode(strings.ToLower(strings.TrimSpace(raw)))
	}

	switch promptMode(strings.ToLower(strings.TrimSpace(recommended))) {
	case promptModeTriage, promptModeLitestream, promptModeHarness:
		return promptMode(strings.ToLower(strings.TrimSpace(recommended)))
	default:
		return promptModeTriage
	}
}

func buildPromptModes(recommended string) []promptModeInfo {
	mode := parsePromptMode("", recommended)
	return []promptModeInfo{
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
	if bundle.ActiveFailure && bundle.LatestFailure != nil {
		headline = fmt.Sprintf("Active incident likely in %s", subsystem)
		summary = fmt.Sprintf("The latest verification is failing during %s with signature %s.", valueOrUnknown(stage), valueOrUnknown(signature))
	} else if bundle.LatestFailure != nil {
		headline = fmt.Sprintf("Worker recovered; latest recorded failure points to %s", subsystem)
		summary = fmt.Sprintf("This worker is running now, but its latest recorded failure happened during %s with signature %s.", valueOrUnknown(stage), valueOrUnknown(signature))
	}

	why := make([]string, 0, 4)
	if bundle.ActiveFailure {
		why = append(why, fmt.Sprintf("The latest verification is still failing, and the worker status is %s.", bundle.Worker.Status))
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
	active := make([]WorkerSummaryResponse, 0)
	for _, summary := range summaries {
		if strings.TrimSpace(summary.CurrentFailureSignature) != "" {
			active = append(active, summary)
		}
	}

	if len(active) == 0 {
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

	signature, signatureCount := topCurrentFailure(active, func(summary WorkerSummaryResponse) string {
		return summary.CurrentFailureSignature
	})
	stage, _ := topCurrentFailure(active, func(summary WorkerSummaryResponse) string {
		return summary.CurrentFailureStage
	})
	subsystem := inferProbableSubsystem(stage, signature)

	profiles := uniqueSortedStrings(func() []string {
		values := make([]string, 0, len(active))
		for _, summary := range active {
			values = append(values, summary.Worker.ProfileName)
		}
		return values
	}())
	datasets := uniqueSortedStrings(func() []string {
		values := make([]string, 0, len(active))
		for _, summary := range active {
			dataset := summary.Workload.MetricReplayDataset()
			if dataset != "none" {
				values = append(values, dataset)
			}
		}
		return values
	}())
	loadModes := uniqueSortedStrings(func() []string {
		values := make([]string, 0, len(active))
		for _, summary := range active {
			values = append(values, summary.Workload.MetricLoadMode())
		}
		return values
	}())

	why := []string{
		fmt.Sprintf("%d workers are currently failing verification.", len(active)),
		fmt.Sprintf("%s is the dominant current signature across %d worker(s).", signature, signatureCount),
		fmt.Sprintf("The current cluster spans profiles %s.", strings.Join(profiles, ", ")),
	}
	if len(datasets) > 0 {
		why = append(why, fmt.Sprintf("The affected replay datasets are %s.", strings.Join(datasets, ", ")))
	}

	return diagnosisSnapshot{
		Headline:          fmt.Sprintf("%d workers currently point to %s", len(active), subsystem),
		Summary:           fmt.Sprintf("The dominant live failure is %s during %s. Start with one affected worker, then use Grafana to confirm whether the issue is clustered by profile or workload shape.", signature, valueOrUnknown(stage)),
		ProbableSubsystem: subsystem,
		AffectedWorkers:   len(active),
		DominantStage:     stage,
		DominantSignature: signature,
		AffectedProfiles:  profiles,
		AffectedDatasets:  datasets,
		AffectedLoadModes: loadModes,
		WhyLikely:         why,
		NextSteps:         diagnosisNextSteps(subsystem, active[0]),
	}
}

func buildCoverageSnapshot(summaries []WorkerSummaryResponse) coverageSnapshot {
	loadModes := make(map[string]int)
	datasets := make(map[string]int)
	profiles := make(map[string]int)

	for _, summary := range summaries {
		loadModes[summary.Workload.MetricLoadMode()]++
		profiles[summary.Worker.ProfileName]++
		if dataset := summary.Workload.MetricReplayDataset(); dataset != "none" {
			datasets[dataset]++
		}
	}

	return coverageSnapshot{
		LoadModes:      sortedCoverageCounts(loadModes),
		ReplayDatasets: sortedCoverageCounts(datasets),
		Profiles:       sortedCoverageCounts(profiles),
	}
}

func buildPrompt(bundle *IncidentBundle, mode promptMode) string {
	task := promptTask(mode)
	returnFormat := promptReturnFormat(mode)

	sections := []string{
		"You are diagnosing a Litestream soak incident.",
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

	sections = append(
		sections,
		"",
		"<summary>",
		fmt.Sprintf("generated_at: %s", bundle.GeneratedAt.Format(timeFormatRFC3339)),
		fmt.Sprintf("worker_id: %s", bundle.Worker.ID),
		fmt.Sprintf("status: %s", bundle.Worker.Status),
		fmt.Sprintf("last_heartbeat_at: %s", formatTime(bundle.Worker.LastHeartbeatAt)),
		fmt.Sprintf("load_mode: %s", valueOrUnknown(bundle.Workload.MetricLoadMode())),
		fmt.Sprintf("replay_dataset: %s", valueOrUnknown(bundle.Workload.MetricReplayDataset())),
		fmt.Sprintf("failure_stage: %s", valueOrUnknown(bundle.FailureStage)),
		fmt.Sprintf("failure_signature: %s", valueOrUnknown(bundle.FailureSignature)),
		"</summary>",
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
		"<recent_events>",
		mustJSON(bundle.RecentEvents),
		"</recent_events>",
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

func promptTask(mode promptMode) string {
	switch mode {
	case promptModeLitestream:
		return "Assume the most likely issue is in Litestream sync, restore, or replication behavior. Use the incident evidence to explain whether the failure is in the control socket, restore path, replica object fetch path, or restore correctness."
	case promptModeHarness:
		return "Assume Litestream may be innocent. Use the incident evidence to look for worker-harness issues, runtime conditions, bad timeouts, bad config, Fly machine problems, or dataset-specific behavior."
	default:
		return "Classify the likely subsystem first, rank the top hypotheses, and recommend the fastest next commands or log locations. Do not jump straight to code changes."
	}
}

func promptReturnFormat(mode promptMode) string {
	switch mode {
	case promptModeLitestream:
		return strings.Join([]string{
			"1. Most likely Litestream subsystem",
			"2. Evidence for that subsystem",
			"3. Competing hypotheses",
			"4. Exact next commands or files to inspect",
			"5. Whether this looks shared across workers or isolated",
		}, "\n")
	case promptModeHarness:
		return strings.Join([]string{
			"1. Most likely non-Litestream cause",
			"2. Evidence for that cause",
			"3. What would falsify this hypothesis",
			"4. Exact next commands or files to inspect",
			"5. Whether the workload shape itself is a clue",
		}, "\n")
	default:
		return strings.Join([]string{
			"1. Likely subsystem",
			"2. Top three hypotheses ranked",
			"3. Evidence for each",
			"4. Fastest next commands or logs",
			"5. Whether to investigate Litestream, runtime, S3, or the harness first",
		}, "\n")
	}
}

func inferProbableSubsystem(stage, signature string) string {
	text := strings.ToLower(stage + " " + signature)
	switch {
	case strings.Contains(text, "sync") || strings.Contains(text, "litestream_sync_socket_refused") || strings.Contains(text, "litestream_sync_timeout"):
		return "Litestream sync/control socket"
	case strings.Contains(text, "restore") || strings.Contains(text, "replica_") || strings.Contains(text, "ltx"):
		return "Replication or restore path"
	case strings.Contains(text, "integrity") || strings.Contains(text, "sqlite_index_mismatch"):
		return "Restore correctness / integrity validation"
	case strings.Contains(text, "pause load") || strings.Contains(text, "checkpoint"):
		return "Soak harness or worker runtime"
	default:
		return "Needs operator triage"
	}
}

func recommendedPromptModeForSubsystem(subsystem string) promptMode {
	switch subsystem {
	case "Litestream sync/control socket", "Replication or restore path", "Restore correctness / integrity validation":
		return promptModeLitestream
	case "Soak harness or worker runtime":
		return promptModeHarness
	default:
		return promptModeTriage
	}
}

func incidentNextSteps(subsystem string, bundle *IncidentBundle) []string {
	steps := make([]string, 0, 6)
	switch subsystem {
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

func diagnosisNextSteps(subsystem string, example WorkerSummaryResponse) []string {
	steps := []string{
		fmt.Sprintf("Open %s in the control plane first.", example.Worker.Name),
		"Compare the failing workers against Grafana Fleet Workload Shapes and Fleet Last Failure Classes.",
	}
	switch subsystem {
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
