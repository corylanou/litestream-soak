package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/workload"
)

type workloadEvidenceCounters struct {
	Epoch     string `json:"workload_counter_epoch"`
	Present   bool   `json:"workload_counters_present"`
	Attempts  uint64 `json:"workload_attempts_total"`
	Mutations uint64 `json:"workload_mutations_total"`
	Busy      uint64 `json:"workload_busy_total"`
	Errors    uint64 `json:"workload_errors_total"`
}

func applyRuntimeEvidence(db *model.DB, deployment model.Deployment, end *time.Time, ensure func(string, string, string) *WorkerRunEvidence) error {
	records, err := db.ListRuntimeEvidence(deploymentScorecardSource(deployment), deployment.ID, model.EvidenceWindow{DeploymentID: deployment.ID, Start: deployment.StartedAt, End: end})
	if err != nil {
		return err
	}
	previous := make(map[string]workloadEvidenceCounters)
	lastRun := make(map[string]string)
	lastHeartbeatAt := make(map[string]time.Time)
	heartbeatCounters := make(map[string]workloadEvidenceCounters)
	heartbeatMaintenance := make(map[string]reporting.MaintenanceEvidence)
	seenEvents := make(map[string]bool)
	maintenance := make(map[string]reporting.MaintenanceEvidence)
	for _, record := range records {
		if record.Run.DeploymentID == 0 && (record.ReceivedAt.Before(deployment.StartedAt) || (end != nil && record.ReceivedAt.After(*end))) {
			continue
		}
		var eventIdentity struct {
			ID         string `json:"workload_event_id"`
			IncidentID string `json:"incident_event_id"`
		}
		if err := json.Unmarshal(record.RuntimeJSON, &eventIdentity); err != nil {
			return err
		}
		if eventIdentity.ID == "" {
			eventIdentity.ID = eventIdentity.IncidentID
		}
		if record.Kind == "event" && eventIdentity.ID != "" {
			key := record.Run.WorkerID + "\x00" + record.Run.RunID + "\x00" + eventIdentity.ID
			if seenEvents[key] {
				continue
			}
			seenEvents[key] = true
		}
		e := ensure(record.Run.WorkerID, record.Run.ProfileName, record.Run.Region)
		attributed := (model.Verification{Attributed: record.Attributed, Run: record.Run}).MatchesDeployment(model.Worker{ID: record.Run.WorkerID}, deployment)
		incident := func(kind, class, message string) {
			e.Incidents = append(e.Incidents, RunIncident{RuntimeID: record.ID, Run: record.Run, At: record.ReceivedAt, Kind: kind, Classification: class, Message: message, Attributed: attributed})
		}
		if !attributed {
			e.UnattributedObservations++
			var original struct {
				Message   string `json:"message"`
				EventType string `json:"event_type"`
			}
			_ = json.Unmarshal(record.RuntimeJSON, &original)
			message := "Runtime observation has no proven run attribution"
			if original.Message != "" {
				message += ": " + original.EventType + ": " + original.Message
			}
			incident("runtime_attribution_unproven", "unavailable", message)
			continue
		}
		var runtime reporting.RuntimePayload
		if err := json.Unmarshal(record.RuntimeJSON, &runtime); err != nil {
			return err
		}
		observeProfilingEvidence(e, record, runtime.ProfilingEvidence)
		if record.Kind != "event" && reporting.SnapshotStatus(&runtime) != reporting.RuntimeSnapshotStatusHealthy {
			e.IncompleteObservations++
			incident("runtime_unavailable", "unavailable", runtime.LitestreamSnapshotError)
		}
		observedAt := runtime.SnapshotCollectedAt
		if observedAt.IsZero() {
			observedAt = record.ReceivedAt
		}
		forwardHeartbeat := record.Kind == "heartbeat" && observedAt.After(lastHeartbeatAt[e.WorkerID])
		if forwardHeartbeat {
			lastHeartbeatAt[e.WorkerID] = observedAt
		}
		observeMaintenanceEvidence(e, record, runtime.MaintenanceEvidence, maintenance, heartbeatMaintenance, forwardHeartbeat, incident)
		var counters workloadEvidenceCounters
		if err := json.Unmarshal(record.RuntimeJSON, &counters); err != nil {
			return err
		}
		if !counters.Present {
			if record.Kind != "event" && churnEvidenceRequired(record.Run.ProfileConfig) {
				e.IncompleteObservations++
				incident("workload_counters_missing", "unavailable", "Churn workload counters are unavailable")
			}
			continue
		}
		if counters.Epoch == "" {
			e.IncompleteObservations++
			incident("workload_epoch_missing", "unavailable", "Workload counter epoch is unavailable")
		}
		runKey := record.Run.WorkerID + "\x00" + record.Run.RunID + "\x00" + record.Run.MachineID + "\x00" + counters.Epoch
		prior := previous[runKey]
		if forwardHeartbeat {
			if lastRun[e.WorkerID] != "" && lastRun[e.WorkerID] != runKey {
				e.IncompleteObservations++
				incident("workload_run_replaced", "unavailable", "Previous process ending was not observed; replacement does not prove a clean history")
			}
			lastRun[e.WorkerID] = runKey
			heartbeatPrior := heartbeatCounters[runKey]
			if counters.Attempts < heartbeatPrior.Attempts || counters.Mutations < heartbeatPrior.Mutations || counters.Errors < heartbeatPrior.Errors || counters.Busy < heartbeatPrior.Busy {
				e.IncompleteObservations++
				incident("workload_counters_reset", "unavailable", "Counters decreased in a later heartbeat within an epoch")
			}
			heartbeatCounters[runKey] = counters
		}
		if counters.Errors < counters.Busy || counters.Attempts < counters.Errors {
			e.IncompleteObservations++
			incident("workload_counters_invalid", "unavailable", "Reported counters are inconsistent")
		}
		if counters.Errors > prior.Errors {
			count := counters.Errors - prior.Errors
			e.UnexpectedFailures += int(min(count, uint64(int(^uint(0)>>1)-e.UnexpectedFailures)))
			incident("workload_errors", "unexpected", fmt.Sprintf("%d additional workload errors observed; busy errors are included", count))
		}
		e.WorkloadAttempts += max(counters.Attempts, prior.Attempts) - prior.Attempts
		e.WorkloadMutations += max(counters.Mutations, prior.Mutations) - prior.Mutations
		e.WorkloadBusy += max(counters.Busy, prior.Busy) - prior.Busy
		e.WorkloadErrors += max(counters.Errors, prior.Errors) - prior.Errors
		previous[runKey] = workloadEvidenceCounters{Present: true, Attempts: max(counters.Attempts, prior.Attempts), Mutations: max(counters.Mutations, prior.Mutations), Busy: max(counters.Busy, prior.Busy), Errors: max(counters.Errors, prior.Errors)}
	}
	return nil
}

func churnEvidenceRequired(config string) bool {
	parsed, err := workload.ParseConfig(config)
	if err != nil {
		return true
	}
	mode := strings.TrimSpace(parsed.LoadMode)
	return mode == "queue" || mode == "cache"
}

func observeMaintenanceEvidence(e *WorkerRunEvidence, record model.RuntimeEvidence, current reporting.MaintenanceEvidence, previous map[string]reporting.MaintenanceEvidence, heartbeatPrevious map[string]reporting.MaintenanceEvidence, forwardHeartbeat bool, incident func(string, string, string)) {
	if current.Epoch == "" {
		return
	}
	key := record.Run.WorkerID + "\x00" + record.Run.RunID + "\x00" + current.Epoch
	prior := previous[key]
	if !current.Complete || current.StartedAt.IsZero() {
		e.IncompleteObservations++
		incident("maintenance_observer_incomplete", "unavailable", "Maintenance log observation is incomplete")
	}
	if forwardHeartbeat {
		heartbeatPrior := heartbeatPrevious[key]
		if current.Snapshots < heartbeatPrior.Snapshots || current.Compactions < heartbeatPrior.Compactions || current.Retentions < heartbeatPrior.Retentions || current.Errors < heartbeatPrior.Errors {
			e.IncompleteObservations++
			incident("maintenance_counters_reset", "unavailable", "Maintenance observations decreased in a later heartbeat")
		}
		heartbeatPrevious[key] = current
	}
	e.MaintenanceSnapshots += max(current.Snapshots, prior.Snapshots) - prior.Snapshots
	e.MaintenanceCompactions += max(current.Compactions, prior.Compactions) - prior.Compactions
	e.MaintenanceRetentions += max(current.Retentions, prior.Retentions) - prior.Retentions
	e.MaintenanceObservations = int(min(e.MaintenanceSnapshots+e.MaintenanceCompactions+e.MaintenanceRetentions, uint64(^uint(0)>>1)))
	if current.Errors > prior.Errors {
		delta := current.Errors - prior.Errors
		e.UnexpectedFailures += int(min(delta, uint64(int(^uint(0)>>1)-e.UnexpectedFailures)))
		incident("litestream_log_error", "unexpected", fmt.Sprintf("%d additional warning/error log observations; latest: %s", delta, current.LastError))
	}
	previous[key] = reporting.MaintenanceEvidence{Snapshots: max(current.Snapshots, prior.Snapshots), Compactions: max(current.Compactions, prior.Compactions), Retentions: max(current.Retentions, prior.Retentions), Errors: max(current.Errors, prior.Errors)}
}
