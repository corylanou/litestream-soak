package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
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
	records, err := db.ListRuntimeEvidence(deploymentScorecardSource(deployment), deployment.ID)
	if err != nil {
		return err
	}
	previous := make(map[string]workloadEvidenceCounters)
	lastRun := make(map[string]string)
	for _, record := range records {
		if record.ReceivedAt.Before(deployment.StartedAt) || (end != nil && record.ReceivedAt.After(*end)) {
			continue
		}
		e := ensure(record.Run.WorkerID, record.Run.ProfileName, record.Run.Region)
		attributed := (model.Verification{Attributed: record.Attributed, Run: record.Run}).MatchesDeployment(model.Worker{ID: record.Run.WorkerID}, deployment)
		incident := func(kind, class, message string) {
			e.Incidents = append(e.Incidents, RunIncident{RuntimeID: record.ID, Run: record.Run, At: record.ReceivedAt, Kind: kind, Classification: class, Message: message, Attributed: attributed})
		}
		if !attributed {
			e.UnattributedObservations++
			incident("runtime_attribution_unproven", "unavailable", "Runtime observation has no proven run attribution")
			continue
		}
		var runtime reporting.RuntimePayload
		if err := json.Unmarshal(record.RuntimeJSON, &runtime); err != nil {
			return err
		}
		if reporting.SnapshotStatus(&runtime) != reporting.RuntimeSnapshotStatusHealthy {
			e.IncompleteObservations++
			incident("runtime_unavailable", "unavailable", runtime.LitestreamSnapshotError)
		}
		var counters workloadEvidenceCounters
		if err := json.Unmarshal(record.RuntimeJSON, &counters); err != nil {
			return err
		}
		if !counters.Present {
			if strings.Contains(record.Run.ProfileName, "queue") || strings.Contains(record.Run.ProfileName, "cache") || strings.Contains(record.Run.ProfileConfig, "churn") {
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
		if lastRun[e.WorkerID] != "" && lastRun[e.WorkerID] != runKey {
			e.IncompleteObservations++
			incident("workload_run_replaced", "unavailable", "Previous run ending was not observed; replacement does not prove a clean history")
		}
		lastRun[e.WorkerID] = runKey
		if counters.Attempts < prior.Attempts || counters.Mutations < prior.Mutations || counters.Errors < prior.Errors || counters.Busy < prior.Busy {
			e.IncompleteObservations++
			incident("workload_counters_reset", "unavailable", "Counters decreased within a run; previous errors are retained")
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
