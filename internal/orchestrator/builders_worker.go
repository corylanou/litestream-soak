package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func (a *API) listWorkerSummaries(status, source string) ([]WorkerSummaryResponse, error) {
	workers, err := a.db.ListWorkersFiltered(status, source)
	if err != nil {
		return nil, err
	}

	summaries := make([]WorkerSummaryResponse, 0, len(workers))
	for _, worker := range workers {
		summary, err := a.buildWorkerSummary(worker)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}

	return summaries, nil
}

func (a *API) buildWorkerSummary(worker model.Worker) (WorkerSummaryResponse, error) {
	reportedRuntime := extractReportedRuntime(worker, nil)
	summary := WorkerSummaryResponse{
		Worker:                  worker,
		Workload:                resolveWorkerWorkload(worker),
		RuntimeSnapshotStatus:   reporting.SnapshotStatus(reportedRuntime),
		LitestreamMetricsStatus: reporting.LitestreamMetricsStatus(reportedRuntime),
		TriageCommands:          buildTriageCommands(worker, worker.FlyMachineID != ""),
	}

	verifications, err := a.db.ListVerifications(worker.ID, 20)
	if err != nil {
		return summary, err
	}
	if len(verifications) > 0 {
		verification := verifications[0]
		if observedAt, ok := verificationObservedAt(verification); ok && !observedAt.Before(worker.CreatedAt.UTC()) {
			summary.LastVerification = &verification
		}
	}
	latestConclusive := latestVerificationInWindow(verifications, worker.CreatedAt.UTC(), nil)
	if activeFailure(latestConclusive) {
		vf := classifyVerification(latestConclusive)
		summary.CurrentFailureStage = vf.Stage
		summary.CurrentFailureSignature = vf.Signature
		summary.CurrentFailureClassification = vf.Classification
		summary.CurrentProbableSubsystem = vf.probableSubsystem()
	}

	latestFailure, err := a.db.GetLatestFailedVerification(worker.ID)
	if err != nil {
		return summary, err
	}
	if latestFailure != nil {
		vf := classifyVerification(latestFailure)
		summary.LatestFailure = latestFailure
		summary.LatestFailureStage = vf.Stage
		summary.LatestFailureSignature = vf.Signature
		summary.LatestFailureClassification = vf.Classification
		summary.LatestProbableSubsystem = vf.probableSubsystem()
		recovery := failureRecovery(verifications, *latestFailure)
		summary.Recovery = &recovery
	}

	events, err := a.db.ListWorkerEvents(worker.ID, 40)
	if err == nil {
		events = coalesceEventFeed(events)
		summary.LatestPlatformEvent = latestPlatformEvent(events)
		summary.ActiveVerification, summary.VerificationActivityUncertain, err = a.currentVerificationActivity(worker, events, verifications)
		if err != nil {
			return summary, err
		}
	}

	return summary, nil
}

func (a *API) workerDetail(workerID string) (*WorkerDetailResponse, int, error) {
	worker, err := a.db.GetWorker(workerID)
	if err != nil {
		return nil, http.StatusNotFound, err
	}

	verifications, err := a.db.ListVerifications(workerID, 20)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	events, err := a.db.ListWorkerEvents(workerID, 40)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	events = coalesceEventFeed(events)

	active, uncertain, err := a.currentVerificationActivity(*worker, events, verifications)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	response := &WorkerDetailResponse{
		Worker:                        *worker,
		Workload:                      resolveWorkerWorkload(*worker),
		ReportedRuntime:               extractReportedRuntime(*worker, events),
		LatestPlatformEvent:           latestPlatformEvent(events),
		ActiveVerification:            active,
		VerificationActivityUncertain: uncertain,
		RecentVerifications:           verifications,
		RecentEvents:                  events,
		TriageCommands:                buildTriageCommands(*worker, false),
	}
	response.RuntimeSnapshotStatus = reporting.SnapshotStatus(response.ReportedRuntime)
	response.LitestreamMetricsStatus = reporting.LitestreamMetricsStatus(response.ReportedRuntime)

	for _, verification := range verifications {
		if !activeFailure(&verification) {
			continue
		}
		verificationCopy := verification
		vf := classifyVerification(&verificationCopy)
		response.LatestFailure = &verificationCopy
		response.FailureStage = vf.Stage
		response.FailureSignature = vf.Signature
		response.FailureClassification = vf.Classification
		response.ProbableSubsystem = vf.probableSubsystem()
		break
	}

	if worker.FlyMachineID != "" {
		flyClient := a.fly
		if appName := strings.TrimSpace(worker.AppName); appName != "" {
			flyClient = a.fly.ForApp(appName)
		}

		machine, err := flyClient.GetMachine(context.Background(), worker.FlyMachineID)
		if err != nil {
			response.MachineError = flyapi.DiagnosticError(err)
		} else {
			response.Machine = machine.Diagnostic()
			response.TriageCommands = buildTriageCommands(*worker, true)
		}
	}

	return response, http.StatusOK, nil
}

func (a *API) buildIncidentBundle(workerID string) (*IncidentBundle, int, error) {
	detail, status, err := a.workerDetail(workerID)
	if err != nil {
		return nil, status, err
	}

	var latestFailure *model.Verification
	activeFailureDetected := false
	for i, verification := range detail.RecentVerifications {
		if i == 0 && activeFailure(&verification) {
			activeFailureDetected = true
		}
		if activeFailure(&verification) {
			verificationCopy := verification
			latestFailure = &verificationCopy
			break
		}
	}

	summaries, err := a.listWorkerSummaries("", detail.Worker.Source)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	diagnosis := buildDiagnosisSnapshot(summaries)
	failure := classifyVerification(latestFailure)
	probableSubsystem := failure.probableSubsystem()
	reportedRuntime := extractReportedRuntime(detail.Worker, detail.RecentEvents)
	failureDebug := latestFailureDebugSnapshot(detail.RecentEvents)

	bundle := &IncidentBundle{
		GeneratedAt:                   time.Now().UTC(),
		Worker:                        detail.Worker,
		Workload:                      detail.Workload,
		LatestFailure:                 latestFailure,
		LatestPlatformEvent:           detail.LatestPlatformEvent,
		ActiveVerification:            detail.ActiveVerification,
		VerificationActivityUncertain: detail.VerificationActivityUncertain,
		ActiveFailure:                 activeFailureDetected,
		FailureStage:                  failure.Stage,
		FailureSignature:              failure.Signature,
		FailureClassification:         failure.Classification,
		ProbableSubsystem:             probableSubsystem,
		RuntimeSnapshotStatus:         reporting.SnapshotStatus(reportedRuntime),
		LitestreamMetricsStatus:       reporting.LitestreamMetricsStatus(reportedRuntime),
		ReportedRuntime:               reportedRuntime,
		FailureDebug:                  failureDebug,
		Diagnosis:                     diagnosis,
		RelatedClusters:               relatedDiagnosisClusters(diagnosis, detail.Worker.ID, failure.Signature, probableSubsystem),
		RecentVerifications:           detail.RecentVerifications,
		RecentEvents:                  detail.RecentEvents,
		Machine:                       detail.Machine,
		MachineError:                  detail.MachineError,
		TriageCommands:                buildTriageCommands(detail.Worker, detail.Machine != nil),
	}
	bundle.Guide = buildIncidentGuide(bundle)
	bundle.PromptModes = buildPromptModes(bundle.Guide.RecommendedPromptMode)
	bundle.Prompt = buildPrompt(bundle, parsePromptMode("", bundle.Guide.RecommendedPromptMode))
	return bundle, http.StatusOK, nil
}

func latestFailureDebugSnapshot(events []model.Event) *reporting.FailureDebugSnapshot {
	for _, event := range events {
		if event.EventType != "verification_failed" && event.EventType != "worker_failed" && event.EventType != "first_failure" {
			continue
		}
		var verificationPayload reporting.VerificationPayload
		if err := json.Unmarshal([]byte(event.Details), &verificationPayload); err == nil && verificationPayload.FailureDebug != nil {
			return verificationPayload.FailureDebug
		}
		var eventPayload reporting.WorkerEventPayload
		if err := json.Unmarshal([]byte(event.Details), &eventPayload); err == nil && eventPayload.FailureDebug != nil {
			return eventPayload.FailureDebug
		}
	}
	return nil
}

func (a *API) currentVerificationActivity(worker model.Worker, events []model.Event, verifications []model.Verification) (*reporting.ActiveVerification, bool, error) {
	expected, err := a.db.ExpectedWorkerRun(worker.ID)
	if err != nil {
		return nil, true, err
	}
	if expected == nil || expected.RunID == "" {
		return nil, true, nil
	}
	start, err := a.db.LatestRunVerificationStart(*expected)
	if err != nil {
		return nil, true, err
	}
	if start != nil {
		events = append(append([]model.Event(nil), events...), *start)
	}
	active, uncertain := activeVerificationFromEvents(worker, expected, events, nil)
	if active == nil {
		return nil, uncertain, nil
	}
	completion, err := a.db.RunVerificationCompletion(*expected, active.StartedAt, active.CheckType)
	if err != nil {
		return nil, true, err
	}
	if completion != nil {
		verifications = append(append([]model.Verification(nil), verifications...), *completion)
	}
	active, uncertain = activeVerificationFromEvents(worker, expected, events, verifications)
	return active, uncertain, nil
}

func activeVerificationFromEvents(worker model.Worker, expected *reporting.WorkerIdentity, events []model.Event, verifications []model.Verification) (*reporting.ActiveVerification, bool) {
	if expected == nil || expected.RunID == "" || expected.WorkerID != worker.ID ||
		(worker.FlyMachineID != "" && expected.MachineID != worker.FlyMachineID) ||
		(worker.AppName != "" && expected.AppName != "" && expected.AppName != worker.AppName) {
		return nil, true
	}
	var latest *reporting.ActiveVerification
	uncertain := false
	for _, event := range events {
		if event.EventType != "verification_started" {
			continue
		}
		var payload struct {
			reporting.WorkerEventPayload
			Attributed bool `json:"attributed"`
		}
		if err := json.Unmarshal([]byte(event.Details), &payload); err != nil || payload.ActiveVerification == nil {
			uncertain = true
			continue
		}
		if !payload.Attributed || payload.RunID == "" {
			uncertain = true
			continue
		}
		if event.WorkerID != worker.ID || (worker.AppName != "" && payload.AppName != worker.AppName) || !sameVerificationRun(payload.WorkerIdentity, *expected) {
			continue
		}
		active := *payload.ActiveVerification
		if active.StartedAt.IsZero() || active.CheckType == "" {
			uncertain = true
			continue
		}
		if active.Status != "" && active.Status != "running" {
			uncertain = true
			continue
		}
		if latest == nil || active.StartedAt.After(latest.StartedAt) {
			if active.ObservedAt.IsZero() {
				active.ObservedAt = event.CreatedAt.UTC()
			}
			active.Status = "running"
			latest = &active
		}
	}
	if latest == nil {
		return nil, uncertain
	}
	for _, verification := range verifications {
		if verification.Attributed && verification.WorkerID == worker.ID && (worker.AppName == "" || verification.Run.AppName == worker.AppName) && sameVerificationRun(verification.Run, *expected) &&
			verification.StartedAt.Equal(latest.StartedAt) && verification.CheckType == latest.CheckType &&
			(verification.CompletedAt != nil || verification.Status != "running") {
			return nil, uncertain
		}
	}
	latest.AgeSeconds = time.Since(latest.StartedAt).Seconds()
	latest.Stale = latest.AgeSeconds > (2 * time.Hour).Seconds()
	return latest, uncertain
}

func sameVerificationRun(identity, expected reporting.WorkerIdentity) bool {
	return identity.RunID == expected.RunID && identity.WorkerID == expected.WorkerID &&
		identity.MachineID == expected.MachineID && (expected.AppName == "" || identity.AppName == expected.AppName) &&
		identity.DeploymentID == expected.DeploymentID && identity.GitSHA == expected.GitSHA &&
		identity.LitestreamSHA == expected.LitestreamSHA && identity.WorkloadSHA == expected.WorkloadSHA &&
		(expected.ProfileHash == "" || identity.ProfileHash == expected.ProfileHash) && identity.ImageRef == expected.ImageRef &&
		identity.Source == expected.Source && identity.ProfileName == expected.ProfileName &&
		identity.ProfileConfig == expected.ProfileConfig && identity.WorkloadID == expected.WorkloadID &&
		(expected.VolumeID == "" || identity.VolumeID == expected.VolumeID)
}

func extractReportedRuntime(worker model.Worker, events []model.Event) *reporting.RuntimePayload {
	var observedAt time.Time
	if worker.LastRuntimeAt != nil {
		observedAt = worker.LastRuntimeAt.UTC()
	}
	if runtime := parseRuntimeJSON(worker.LastRuntimeJSON, observedAt); runtime != nil {
		return runtime
	}

	for _, event := range events {
		if !strings.HasPrefix(event.EventType, "verification_") {
			continue
		}

		var payload reporting.VerificationPayload
		if err := json.Unmarshal([]byte(event.Details), &payload); err != nil {
			continue
		}

		runtime := payload.Normalize(event.CreatedAt.UTC())
		return &runtime
	}

	return nil
}

func parseRuntimeJSON(raw string, observedAt time.Time) *reporting.RuntimePayload {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	var runtime reporting.RuntimePayload
	if err := json.Unmarshal([]byte(raw), &runtime); err != nil {
		return nil
	}
	normalized := runtime.Normalize(observedAt)
	return &normalized
}

func failureRecovery(verifications []model.Verification, latestFailure model.Verification) FailureRecovery {
	recovery := FailureRecovery{}
	failureAt, ok := verificationObservedAt(latestFailure)
	if !ok {
		return recovery
	}
	if len(verifications) > 0 {
		recovery.StillFailing = activeFailure(&verifications[0])
	}
	for i, verification := range verifications {
		if verification.ID == latestFailure.ID && i > 0 {
			nextVerification := verifications[i-1]
			recovery.FailedThenNextPassed = nextVerification.Succeeded()
		}
		observedAt, ok := verificationObservedAt(verification)
		if !ok || !observedAt.After(failureAt) {
			continue
		}
		if verification.Succeeded() {
			if recovery.LastPassAfterFailureAt == nil || observedAt.After(*recovery.LastPassAfterFailureAt) {
				passAt := observedAt
				recovery.LastPassAfterFailureAt = &passAt
			}
		}
	}
	return recovery
}

func latestVerificationInWindow(verifications []model.Verification, since time.Time, until *time.Time) *model.Verification {
	pending := false
	for i := range verifications {
		if verifications[i].Aborted() {
			continue
		}
		observedAt, ok := verificationObservedAt(verifications[i])
		if !ok {
			continue
		}
		if observedAt.Before(since) {
			continue
		}
		if until != nil && !until.IsZero() && !observedAt.Before(*until) {
			continue
		}
		if verifications[i].Pending() {
			pending = true
			continue
		}
		if pending && verifications[i].Succeeded() {
			return nil
		}
		verification := verifications[i]
		return &verification
	}
	return nil
}

func buildTriageCommands(worker model.Worker, hasMachine bool) []string {
	commands := make([]string, 0, 6)
	appName := strings.TrimSpace(worker.AppName)
	if appName == "" {
		appName = "litestream-soak"
	}

	if worker.FlyMachineID != "" {
		commands = append(commands, fmt.Sprintf("fly machine status %s -a %s", worker.FlyMachineID, appName))
		if worker.Status == model.WorkerDormant {
			commands = append(commands, fmt.Sprintf("fly machine start %s -a %s", worker.FlyMachineID, appName))
		}
		commands = append(commands, fmt.Sprintf("fly logs -a %s -i %s", appName, worker.FlyMachineID))
	}
	if hasMachine {
		commands = append(commands, fmt.Sprintf("fly ssh console -a %s", appName))
	}
	commands = append(commands,
		fmt.Sprintf(`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/workers/%s/incident | jq .`, worker.ID),
		fmt.Sprintf(`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/workers/%s/debug-snapshot | jq .`, worker.ID),
		`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/diagnosis | jq .`,
	)

	return commands
}
