package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func observeProfilingEvidence(e *WorkerRunEvidence, record model.RuntimeEvidence, current reporting.ProfilingEvidence) {
	if record.Kind == "event" && current.ProfileCapability == "" {
		return
	}
	e.ProfileCapability = current.ProfileCapability
	e.ProfileRecords = current.ProfileRecords
	if e.profileIncidentIDs == nil {
		e.profileIncidentIDs = make(map[string]bool)
	}
	unavailable := func(message string) {
		e.IncompleteObservations++
		e.Incidents = append(e.Incidents, RunIncident{RuntimeID: record.ID, Run: record.Run, At: record.ReceivedAt, Kind: "profiling_evidence_unavailable", Classification: "unavailable", Message: message, Attributed: record.Attributed})
	}
	if current.ProfileCapability == "" {
		unavailable("Profiling detection capability is unreported")
		return
	}
	if current.ProfileCapability == "snapshot-only" || (current.ProfileCapability != "pending" && current.ProfileCapability != "disabled" && !current.ProfileHistoryComplete) || current.ProfileReadError != "" {
		message := "Profiling history is legacy, truncated, or not continuously observed: " + current.ProfileReadError
		if len(current.ProfileStatusCounts) > 0 {
			counts, _ := json.Marshal(current.ProfileStatusCounts)
			message += "; retained status counts: " + string(counts)
		}
		unavailable(message)
	}
	var recordedUploadFailures uint64
	for _, profile := range current.ProfileRecords {
		recordedUploadFailures += profile.UploadFailureCount
	}
	for reason, count := range current.ProfileStatusCounts {
		neutral := strings.HasSuffix(reason, "-disabled") || reason == "cpu-rate-limited" || reason == "cpu-skipped-shutdown" || reason == "cancelled"
		if count == 0 || neutral {
			continue
		}
		if reason == "upload-failed" && count <= recordedUploadFailures {
			continue
		}
		unavailable("Retained profile status evidence: " + reason + ": " + fmt.Sprint(count) + " observations; original run attribution or complete diagnostics unproven")
	}
	add := func(incident reporting.ProfileIncident, run reporting.WorkerIdentity, attributed bool) {
		if e.profileIncidentIDs[incident.ID] {
			return
		}
		e.profileIncidentIDs[incident.ID] = true
		class := "unexpected"
		if strings.Contains(incident.Kind, "unavailable") {
			class = "unavailable"
		}
		e.Incidents = append(e.Incidents, RunIncident{WorkloadEventID: incident.ID, RuntimeID: record.ID, Run: run, At: incident.At, Kind: incident.Kind, Classification: class, Message: incident.Message, Attributed: attributed})
		if !attributed {
			e.UnattributedObservations++
		} else if class == "unavailable" {
			e.IncompleteObservations++
		} else {
			e.UnexpectedFailures++
		}
	}
	for _, incident := range current.ProfileIncidents {
		attributed := record.Attributed && incident.Run.RunID == record.Run.RunID && incident.Run.WorkerID == record.Run.WorkerID && incident.Run.MachineID == record.Run.MachineID && incident.Run.DeploymentID == record.Run.DeploymentID
		add(incident, incident.Run, attributed)
	}
	for _, profile := range current.ProfileRecords {
		attributed := record.Attributed && profile.RunID == record.Run.RunID && profile.WorkerID == record.Run.WorkerID && profile.MachineID == record.Run.MachineID && profile.DeploymentID == record.Run.DeploymentID
		original := record.Run
		original.RunID = profile.RunID
		original.WorkerID = profile.WorkerID
		original.MachineID = profile.MachineID
		original.DeploymentID = profile.DeploymentID
		for _, incident := range profile.Incidents() {
			add(incident, original, attributed)
		}
	}
}
