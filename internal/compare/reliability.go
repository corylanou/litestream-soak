package compare

import (
	"fmt"

	"github.com/corylanou/litestream-soak/internal/orchestrator"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func attachRunEvidence(o *Observation) {
	r := o.Request
	e := orchestrator.WorkerRunEvidence{WorkerID: r.ReplicaPrefix, Profile: r.Scenario, Region: r.Contract.Region, WorkloadSHA: r.Contract.GeneratorSHA, ProfileHash: r.Contract.ConfigSHA256, CurrentHealth: "unavailable", ProfileCapability: "unknown", WorkloadAttempts: o.WorkloadAttempts, WorkloadMutations: uint64(max(0, o.CompletedOperations)), WorkloadProgress: uint64(max(0, o.CompletedOperations)), DetectionLimitations: []string{"bounded local evidence is not the durable fleet journal or restart history", "one end-boundary verification does not establish continuous soak coverage", "quiet logs cannot exclude unlogged retries"}}
	if !r.Litestream {
		e.ProfileCapability = "disabled"
	} else if o.Metrics["allocation_bytes_per_operation"].Value != nil {
		e.ProfileCapability = "observed"
	}
	if o.Correctness == "pass" || o.Correctness == "fail" {
		e.CurrentHealth = "failed"
		if o.Correctness == "pass" {
			e.CurrentHealth = "passed"
		}
		e.VerificationCount = 1
		if !o.VerifiedAt.IsZero() {
			e.FirstVerification = &o.VerifiedAt
			e.LastVerification = &o.VerifiedAt
		}
	} else {
		e.IncompleteObservations++
	}

	if o.Maintenance != nil {
		e.MaintenanceSnapshots = o.Maintenance.Snapshots
		e.MaintenanceCompactions = o.Maintenance.Compactions
		e.MaintenanceRetentions = o.Maintenance.Retentions
		e.MaintenanceObservations = 1
		if !o.Maintenance.Complete {
			e.IncompleteObservations++
		}
	}
	identity := reporting.WorkerIdentity{WorkerID: r.ReplicaPrefix, RunID: r.ReplicaPrefix, WorkloadSHA: r.Contract.GeneratorSHA, ValidatorID: r.Contract.OracleSHA, GitSHA: r.Contract.GeneratorSHA, LitestreamSHA: r.SHA, ProfileName: r.Scenario, ProfileHash: r.Contract.ConfigSHA256, Region: r.Contract.Region}
	for i, incident := range o.Incidents {
		e.Incidents = append(e.Incidents, orchestrator.RunIncident{IncidentEventID: fmt.Sprintf("%s/%d", r.ReplicaPrefix, i), Run: identity, At: o.FinishedAt, Kind: incident.Category, Classification: "unexpected", Message: incident.Detail, Attributed: true})
	}
	if o.Error != "" {
		e.IncompleteObservations++
		e.Incidents = append(e.Incidents, orchestrator.RunIncident{IncidentEventID: r.ReplicaPrefix + "/execution", Run: identity, At: o.FinishedAt, Kind: "execution", Classification: "unexpected", Message: o.Error, Attributed: true})
	}
	e.UnexpectedFailures = len(e.Incidents)
	e = orchestrator.EvaluateRunEvidence(e, o.StartedAt, o.FinishedAt)
	o.RunEvidence = &e
	o.Reliability = "unavailable"
	if e.UnexpectedFailures > 0 {
		o.Reliability = "fail"
	}
	if o.CapabilityNotes == nil {
		o.CapabilityNotes = map[string]string{}
	}
	o.CapabilityNotes["reliability"] = "shared complete-run eligibility applied; bounded local artifacts do not establish durable fleet history, restart coverage, or continuous verification span"
}
