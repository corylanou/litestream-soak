package orchestrator

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

type RunIncident struct {
	IncidentEventID string                   `json:"incident_event_id,omitempty"`
	WorkloadEventID string                   `json:"workload_event_id,omitempty"`
	Operation       string                   `json:"workload_operation,omitempty"`
	ErrorKind       string                   `json:"workload_error_kind,omitempty"`
	RuntimeID       int                      `json:"runtime_id,omitempty"`
	VerificationID  int                      `json:"verification_id,omitempty"`
	EventID         int                      `json:"event_id,omitempty"`
	Run             reporting.WorkerIdentity `json:"run"`
	At              time.Time                `json:"at"`
	Kind            string                   `json:"kind"`
	Classification  string                   `json:"classification"`
	Message         string                   `json:"message"`
	Attributed      bool                     `json:"attributed"`
}

type WorkerRunEvidence struct {
	WorkerID                  string        `json:"worker_id"`
	Profile                   string        `json:"profile"`
	Region                    string        `json:"region"`
	WorkloadSHA               string        `json:"workload_sha"`
	ProfileHash               string        `json:"profile_hash"`
	CurrentHealth             string        `json:"current_health"`
	VerificationCount         int           `json:"verification_count"`
	UnexpectedFailures        int           `json:"unexpected_failures"`
	ExpectedInjections        int           `json:"expected_injections"`
	UnattributedObservations  int           `json:"unattributed_observations"`
	PendingObservations       int           `json:"pending_observations"`
	IncompleteObservations    int           `json:"incomplete_observations"`
	WorkloadAttempts          uint64        `json:"workload_attempts"`
	WorkloadMutations         uint64        `json:"workload_mutations"`
	WorkloadBusy              uint64        `json:"workload_busy"`
	WorkloadErrors            uint64        `json:"workload_errors"`
	WorkloadProgress          uint64        `json:"workload_progress"`
	MaintenanceSnapshots      uint64        `json:"maintenance_snapshots"`
	MaintenanceCompactions    uint64        `json:"maintenance_compactions"`
	MaintenanceRetentions     uint64        `json:"maintenance_retentions"`
	MaintenanceObservations   int           `json:"maintenance_observations"`
	FirstVerification         *time.Time    `json:"first_verification,omitempty"`
	LastVerification          *time.Time    `json:"last_verification,omitempty"`
	VerifiedSpanSeconds       float64       `json:"verified_span_seconds"`
	MaxVerificationGapSeconds float64       `json:"max_verification_gap_seconds"`
	HistoryComplete           bool          `json:"history_complete"`
	CoverageComplete          bool          `json:"coverage_complete"`
	Eligible                  bool          `json:"eligible"`
	EligibilityReasons        []string      `json:"eligibility_reasons"`
	ReleaseScoringReason      string        `json:"release_scoring_reason"`
	Incidents                 []RunIncident `json:"incidents"`
	lastRun                   string
	lastTXID                  uint64
	lastRuntimeAt             time.Time
}

type ReliabilityFinding struct {
	Profile        string `json:"profile"`
	Region         string `json:"region"`
	Classification string `json:"classification"`
	BaseFailures   int    `json:"base_failures"`
	HeadFailures   int    `json:"head_failures"`
}

func buildRunReliability(db *model.DB, deployment model.Deployment, end *time.Time) ([]WorkerRunEvidence, error) {
	records, err := db.ListEvidenceVerifications(deploymentScorecardSource(deployment))
	if err != nil {
		return nil, err
	}
	workers, err := db.ListWorkersForSource(deploymentScorecardSource(deployment))
	if err != nil {
		return nil, err
	}
	byWorker := make(map[string]*WorkerRunEvidence)
	ensure := func(id, profile, region string) *WorkerRunEvidence {
		if profile == "" && region == "" {
			var existing *WorkerRunEvidence
			for _, candidate := range byWorker {
				if candidate.WorkerID == id {
					if existing != nil {
						existing = nil
						break
					}
					existing = candidate
				}
			}
			if existing != nil {
				return existing
			}
		}
		evidenceKey := id + "\x00" + profile + "\x00" + region
		e := byWorker[evidenceKey]
		if e == nil {
			e = &WorkerRunEvidence{WorkerID: id, Profile: profile, Region: region, CurrentHealth: "unknown", HistoryComplete: true, Incidents: []RunIncident{}}
			byWorker[evidenceKey] = e
		}
		return e
	}
	for _, w := range workers {
		ensure(w.ID, w.ProfileName, w.Region)
	}
	for _, record := range records {
		v := record.Verification
		if v.Run.DeploymentID != 0 && v.Run.DeploymentID != deployment.ID {
			continue
		}
		at := v.StartedAt
		if v.CompletedAt != nil {
			at = *v.CompletedAt
		}
		if v.Run.DeploymentID == 0 && (at.Before(deployment.StartedAt) || (end != nil && at.After(*end))) {
			continue
		}
		e := ensure(v.WorkerID, v.Run.ProfileName, v.Run.Region)
		attributed := v.MatchesDeployment(model.Worker{ID: v.WorkerID}, deployment)
		if !attributed {
			e.UnattributedObservations++
			e.HistoryComplete = false
		} else {
			e.WorkloadSHA = v.Run.WorkloadSHA
			e.ProfileHash = v.Run.ProfileHash
			e.HistoryComplete = e.HistoryComplete && record.HistoryComplete
		}
		pending := strings.EqualFold(v.Status, "pending")
		if pending {
			if attributed {
				e.CurrentHealth = "pending"
				if e.lastRun != v.Run.RunID {
					e.lastRun = ""
					e.lastTXID = 0
				}
			}
			e.PendingObservations++
			continue
		}
		pending = strings.EqualFold(v.Status, "running") || v.Aborted() || v.CompletedAt == nil
		if pending {
			e.IncompleteObservations++
			e.Incidents = append(e.Incidents, RunIncident{VerificationID: v.ID, Run: v.Run, At: at, Kind: v.Status, Classification: "unavailable", Message: v.ErrorMessage, Attributed: attributed})
			continue
		}
		if !v.Succeeded() {
			e.Incidents = append(e.Incidents, RunIncident{VerificationID: v.ID, Run: v.Run, At: at, Kind: classifyVerification(&v).Signature, Classification: "unexpected", Message: v.ErrorMessage, Attributed: attributed})
			if attributed {
				e.UnexpectedFailures++
			}
		}
		if !attributed {
			continue
		}
		e.VerificationCount++
		e.CurrentHealth = "failed"
		if v.Succeeded() {
			e.CurrentHealth = "passed"
		}
		if e.FirstVerification == nil {
			first := at
			e.FirstVerification = &first
		}
		if e.LastVerification != nil {
			e.MaxVerificationGapSeconds = max(e.MaxVerificationGapSeconds, at.Sub(*e.LastVerification).Seconds())
		}
		last := at
		e.LastVerification = &last
		if record.Runtime.LitestreamSnapshotHealthy && !record.Runtime.SnapshotCollectedAt.IsZero() && record.Runtime.SnapshotCollectedAt.After(e.lastRuntimeAt) && record.Runtime.SnapshotCollectedAt.Sub(at).Abs() <= time.Minute {
			if e.lastRun == v.Run.RunID && record.Runtime.DBTXID > e.lastTXID {
				e.WorkloadProgress += record.Runtime.DBTXID - e.lastTXID
			}
			e.lastRun = v.Run.RunID
			e.lastTXID = record.Runtime.DBTXID
			e.lastRuntimeAt = record.Runtime.SnapshotCollectedAt
		}
	}
	if err := applyRuntimeEvidence(db, deployment, end, ensure); err != nil {
		return nil, err
	}
	events, err := db.ListEvidenceEvents(deploymentScorecardSource(deployment))
	if err != nil {
		return nil, err
	}
	seenEvents := make(map[string]bool)
	for _, event := range events {
		if strings.HasPrefix(event.EventType, "verification_") {
			continue
		}
		kind := incidentEventClass(event.EventType)
		if kind == "" {
			continue
		}
		var payload struct {
			WorkloadEventID string `json:"workload_event_id"`
			IncidentEventID string `json:"incident_event_id"`
			Operation       string `json:"workload_operation"`
			ErrorKind       string `json:"workload_error_kind"`
			reporting.WorkerEventPayload
			Attributed bool `json:"attributed"`
		}
		_ = json.Unmarshal([]byte(event.Details), &payload)
		if payload.DeploymentID == 0 && (event.CreatedAt.Before(deployment.StartedAt) || (end != nil && event.CreatedAt.After(*end))) {
			continue
		}
		eventID := firstNonEmpty(payload.WorkloadEventID, payload.IncidentEventID)
		if eventID != "" {
			key := event.WorkerID + "\x00" + payload.RunID + "\x00" + eventID
			if seenEvents[key] {
				continue
			}
			seenEvents[key] = true
		}
		if payload.DeploymentID != 0 && payload.DeploymentID != deployment.ID {
			continue
		}
		e := ensure(event.WorkerID, payload.ProfileName, payload.Region)
		attributed := payload.Attributed && (model.Verification{Attributed: true, Run: payload.WorkerIdentity}).MatchesDeployment(model.Worker{ID: event.WorkerID}, deployment)
		e.Incidents = append(e.Incidents, RunIncident{IncidentEventID: payload.IncidentEventID, WorkloadEventID: payload.WorkloadEventID, Operation: payload.Operation, ErrorKind: payload.ErrorKind, EventID: event.ID, Run: payload.WorkerIdentity, At: event.CreatedAt, Kind: event.EventType, Classification: kind, Message: event.Message, Attributed: attributed})
		if !attributed {
			e.UnattributedObservations++
			continue
		}
		switch kind {
		case "expected_injection":
			e.ExpectedInjections++
		case "maintenance":
			e.MaintenanceObservations++
			switch event.EventType {
			case "maintenance_snapshot_completed":
				e.MaintenanceSnapshots++
			case "maintenance_compaction_completed":
				e.MaintenanceCompactions++
			case "maintenance_retention_completed":
				e.MaintenanceRetentions++
			}
		case "unavailable":
			e.IncompleteObservations++
		default:
			var counters workloadEvidenceCounters
			_ = json.Unmarshal([]byte(event.Details), &counters)
			countedWorkload := event.EventType == "workload_error" && counters.Present
			countedLog := event.EventType == "litestream_log_error" && payload.Epoch != "" && payload.Errors > 0
			if !countedWorkload && !countedLog {
				e.UnexpectedFailures++
			}
		}
	}
	result := make([]WorkerRunEvidence, 0, len(byWorker))
	for _, e := range byWorker {
		if e.FirstVerification == nil {
			e.HistoryComplete = false
		}
		if e.FirstVerification != nil && e.LastVerification != nil {
			e.VerifiedSpanSeconds = e.LastVerification.Sub(*e.FirstVerification).Seconds()
		}
		e.ReleaseScoringReason = "included"
		if !workerIncludedInReleaseQuality(model.Worker{Region: e.Region, ProfileName: e.Profile}) {
			e.ReleaseScoringReason = "excluded from legacy release score; retained in reliability (#187, #192)"
		}
		evaluateRunEligibility(e, deployment.StartedAt, end, time.Hour, 24*time.Hour)
		result = append(result, *e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].WorkerID < result[j].WorkerID })
	return result, nil
}

func incidentEventClass(kind string) string {
	switch kind {
	case "fault_injection_engaged":
		return "expected_injection"
	case "maintenance_snapshot_completed", "maintenance_compaction_completed", "maintenance_retention_completed":
		return "maintenance"
	}
	for _, part := range []string{"missing", "unavailable", "quarantined", "aborted", "pending"} {
		if strings.Contains(kind, part) {
			return "unavailable"
		}
	}
	for _, part := range []string{"fail", "error", "retry", "restart", "oom", "disk_full", "recovered", "crash", "killed", "timeout"} {
		if strings.Contains(kind, part) {
			return "unexpected"
		}
	}
	return ""
}

func evaluateRunEligibility(e *WorkerRunEvidence, start time.Time, end *time.Time, maxGap, minimumSpan time.Duration) {
	if end == nil {
		now := time.Now().UTC()
		end = &now
	}
	e.EligibilityReasons = nil
	if !e.HistoryComplete || e.UnattributedObservations > 0 {
		e.EligibilityReasons = append(e.EligibilityReasons, "history incomplete or attribution unproven")
	}
	if e.IncompleteObservations > 0 {
		e.EligibilityReasons = append(e.EligibilityReasons, "pending, aborted, or unavailable observations")
	}
	if e.VerificationCount < 2 || e.VerifiedSpanSeconds < minimumSpan.Seconds() {
		e.EligibilityReasons = append(e.EligibilityReasons, "insufficient measured verification span or count")
	}
	if e.FirstVerification == nil || e.FirstVerification.Sub(start) > maxGap || e.MaxVerificationGapSeconds > maxGap.Seconds() || (end != nil && (e.LastVerification == nil || end.Sub(*e.LastVerification) > maxGap)) {
		e.EligibilityReasons = append(e.EligibilityReasons, "verification coverage gap")
	}
	if e.WorkloadProgress == 0 && e.WorkloadMutations == 0 {
		e.EligibilityReasons = append(e.EligibilityReasons, "workload progress unproven")
	}
	if e.MaintenanceSnapshots == 0 || e.MaintenanceCompactions == 0 || e.MaintenanceRetentions == 0 {
		e.EligibilityReasons = append(e.EligibilityReasons, "snapshot, compaction, or positive retention exposure unobserved")
	}
	if e.CurrentHealth == "pending" {
		e.EligibilityReasons = append(e.EligibilityReasons, "latest verification is pending")
	}
	e.CoverageComplete = len(e.EligibilityReasons) == 0
	if e.UnexpectedFailures > 0 {
		e.EligibilityReasons = append(e.EligibilityReasons, "unexpected incidents remain in complete-run history")
	}
	if e.ExpectedInjections > 0 {
		e.EligibilityReasons = append(e.EligibilityReasons, "fault injection exposure is not a clean soak")
	}
	if e.CurrentHealth != "passed" {
		e.EligibilityReasons = append(e.EligibilityReasons, "current verification health is not passed")
	}
	e.Eligible = len(e.EligibilityReasons) == 0
}

func reliabilityFinding(base, head int, covered bool) string {
	if !covered {
		return "inconclusive"
	}
	switch {
	case base > 0 && head == 0:
		return "fixed"
	case base == 0 && head > 0:
		return "new"
	case head > base:
		return "regressed"
	case head < base:
		return "improved"
	default:
		return "unchanged"
	}
}

func compareRunReliability(base, head []WorkerRunEvidence) []ReliabilityFinding {
	key := func(e WorkerRunEvidence) string { return e.Profile + "\x00" + e.Region }
	before := make(map[string]WorkerRunEvidence)
	after := make(map[string]WorkerRunEvidence)
	for _, e := range base {
		if prior, ok := before[key(e)]; ok {
			e.UnexpectedFailures += prior.UnexpectedFailures
			e.CoverageComplete = false
		}
		before[key(e)] = e
	}
	for _, e := range head {
		if prior, ok := after[key(e)]; ok {
			e.UnexpectedFailures += prior.UnexpectedFailures
			e.CoverageComplete = false
		}
		after[key(e)] = e
	}
	for k := range before {
		if _, ok := after[k]; !ok {
			after[k] = WorkerRunEvidence{Profile: before[k].Profile, Region: before[k].Region}
		}
	}
	findings := make([]ReliabilityFinding, 0, len(after))
	for k, h := range after {
		b, exists := before[k]
		covered := exists && b.CoverageComplete && h.CoverageComplete && b.WorkloadSHA != "" && b.WorkloadSHA == h.WorkloadSHA && b.ProfileHash != "" && b.ProfileHash == h.ProfileHash && b.VerificationCount == h.VerificationCount && math.Abs(b.VerifiedSpanSeconds-h.VerifiedSpanSeconds) <= 60
		findings = append(findings, ReliabilityFinding{Profile: h.Profile, Region: h.Region, Classification: reliabilityFinding(b.UnexpectedFailures, h.UnexpectedFailures, covered), BaseFailures: b.UnexpectedFailures, HeadFailures: h.UnexpectedFailures})
	}
	sort.Slice(findings, func(i, j int) bool {
		return findings[i].Profile+findings[i].Region < findings[j].Profile+findings[j].Region
	})
	return findings
}

func applyReliabilityVerdict(comparison *DeploymentComparisonResponse) {
	if len(comparison.Head.Reliability) == 0 {
		return
	}
	if comparison.Verdict == "worse" || comparison.Verdict == "mixed" {
		comparison.Verdict = "worse"
		comparison.Summary = "Observed regressions remain disqualifying even when another worker improves; complete-run coverage may still be inconclusive."
		return
	}
	for _, finding := range comparison.ReliabilityFindings {
		if finding.Classification == "new" || finding.Classification == "regressed" {
			comparison.Verdict = "worse"
			comparison.Summary = "Complete-run reliability contains new or regressed incidents; recovered current health does not cancel them."
			return
		}
	}
	for _, finding := range comparison.ReliabilityFindings {
		if finding.Classification == "inconclusive" {
			comparison.Verdict = "insufficient_data"
			comparison.Summary = "Complete-run comparison is inconclusive. Review retained incidents and coverage eligibility reasons for every profile and region."
			return
		}
	}
	for _, e := range comparison.Head.Reliability {
		if !e.Eligible {
			comparison.Verdict = "insufficient_data"
			comparison.Summary = "A clean soak is not proven. Review complete-run incidents and measured coverage separately from current health."
			return
		}
	}
}

func decodeEvidenceReport(body io.Reader, payload any) (json.RawMessage, error) {
	var raw json.RawMessage
	if err := json.NewDecoder(body).Decode(&raw); err != nil {
		return nil, err
	}
	if trimmed := strings.TrimSpace(string(raw)); len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("evidence report must be a JSON object")
	}
	if err := json.Unmarshal(raw, payload); err != nil {
		return nil, err
	}
	return raw, nil
}

func attributedReportJSON(raw json.RawMessage, workerID string, attributed bool) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	fields["worker_id"], _ = json.Marshal(workerID)
	fields["attributed"], _ = json.Marshal(attributed)
	return json.Marshal(fields)
}
