package reporting

import (
	"fmt"
	"time"
)

type ProfilingEvidence struct {
	ProfileIncidents       []ProfileIncident       `json:"profile_incidents,omitempty"`
	ProfileCapability      string                  `json:"profile_capability,omitempty"`
	ProfileEpoch           string                  `json:"profile_epoch,omitempty"`
	ProfileHistoryComplete bool                    `json:"profile_history_complete"`
	ProfileReadError       string                  `json:"profile_read_error,omitempty"`
	ProfileStatusCounts    map[string]uint64       `json:"profile_status_counts,omitempty"`
	ProfileRecords         []ProfileRecordEvidence `json:"profile_records,omitempty"`
}

type ProfileUploadFailureEvidence struct {
	At      time.Time `json:"at"`
	Attempt uint64    `json:"attempt"`
	Stage   string    `json:"stage"`
	Error   string    `json:"error"`
}

type ProfileRecordEvidence struct {
	DeploymentID            int                            `json:"deployment_id"`
	WorkerID                string                         `json:"worker_id"`
	MachineID               string                         `json:"machine_id"`
	RunID                   string                         `json:"run_id"`
	Artifact                string                         `json:"artifact"`
	Status                  string                         `json:"status"`
	Error                   string                         `json:"error,omitempty"`
	Sampling                string                         `json:"sampling,omitempty"`
	CapturedAt              time.Time                      `json:"captured_at"`
	Upload                  string                         `json:"upload"`
	UploadError             string                         `json:"upload_error,omitempty"`
	UploadAttempts          uint64                         `json:"upload_attempts"`
	UploadFailureCount      uint64                         `json:"upload_failure_count"`
	UploadFailuresDropped   uint64                         `json:"upload_failures_dropped"`
	UploadHistoryIncomplete bool                           `json:"upload_history_incomplete"`
	UploadFailures          []ProfileUploadFailureEvidence `json:"upload_failures,omitempty"`
}

type ProfileIncident struct {
	ID      string         `json:"id"`
	Kind    string         `json:"kind"`
	Message string         `json:"message"`
	At      time.Time      `json:"at"`
	Run     WorkerIdentity `json:"run"`
}

func (r ProfileRecordEvidence) Incidents() []ProfileIncident {
	var result []ProfileIncident
	key := "profile:" + r.RunID + ":" + r.Artifact
	if r.Error != "" {
		result = append(result, ProfileIncident{ID: key + ":capture", Kind: "profile_capture_error", Message: r.Error, At: r.CapturedAt})
	}
	for _, failure := range r.UploadFailures {
		result = append(result, ProfileIncident{ID: fmt.Sprintf("%s:upload:%d", key, failure.Attempt), Kind: "profile_upload_error", Message: failure.Stage + ": " + failure.Error, At: failure.At})
	}
	if r.UploadHistoryIncomplete || r.UploadFailuresDropped > 0 || r.UploadFailureCount > uint64(len(r.UploadFailures)) {
		result = append(result, ProfileIncident{ID: key + ":history", Kind: "profile_history_unavailable", Message: fmt.Sprintf("Profile upload history incomplete: %d failures, %d details dropped", r.UploadFailureCount, r.UploadFailuresDropped), At: r.CapturedAt})
	}
	return result
}
