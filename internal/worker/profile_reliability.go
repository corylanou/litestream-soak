package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/google/uuid"
)

type profileEvidenceState struct {
	mu                         sync.Mutex
	epoch                      string
	active, complete, observed bool
	seen                       map[string]bool
	incidents                  map[string]reporting.ProfileIncident
}

func (r *Runner) observeProfiling(cancelRun context.CancelCauseFunc) {
	state := &r.profileEvidence
	state.epoch = uuid.NewString()
	state.active = true
	entries, err := os.ReadDir(filepath.Join(r.cfg.DataDir, "profiles"))
	state.complete = os.IsNotExist(err) || (err == nil && len(entries) == 0)
	state.seen = make(map[string]bool)
	state.incidents = make(map[string]reporting.ProfileIncident)
	emit := func(id, kind, message string, at time.Time, identity reporting.WorkerIdentity) {
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.seen[id] {
			return
		}
		incident := reporting.ProfileIncident{ID: id, Kind: kind, Message: redactLogIncident(message), At: at, Run: identity}
		if len(state.incidents) >= evidenceOutboxMaxEvents {
			state.complete = false
			cancelRun(fmt.Errorf("profiling evidence snapshot capacity exhausted"))
		} else {
			state.incidents[id] = incident
		}
		event := reporting.WorkerEventPayload{WorkerIdentity: identity, EventType: kind, Message: redactLogIncident(message), SentAt: at, WorkloadEvent: reporting.WorkloadEvent{WorkloadEventID: id}}
		if err := r.persistRunEvidence(event); err != nil {
			state.complete = false
			cancelRun(fmt.Errorf("persist profiling evidence: %w", err))
			return
		}
		state.seen[id] = true
		r.requestEvidenceFlush()
	}
	r.profiles.onRecord = func(record *profileRecord) {
		state.mu.Lock()
		state.observed = true
		state.mu.Unlock()
		body, _ := json.Marshal(record)
		var evidence reporting.ProfileRecordEvidence
		_ = json.Unmarshal(body, &evidence)
		identity := workerIdentity(r.cfg)
		identity.RunID = record.RunID
		identity.MachineID = record.MachineID
		identity.WorkerID = record.WorkerID
		identity.DeploymentID = record.DeploymentID
		for _, incident := range evidence.Incidents() {
			emit(incident.ID, incident.Kind, incident.Message, incident.At, identity)
		}
	}
	r.profiles.onStatus = func(phase, reason string) {
		if reason == "upload-failed" || strings.HasSuffix(reason, "-disabled") || reason == "cpu-rate-limited" || reason == "cpu-skipped-shutdown" || (phase == "collector" && reason == "cancelled") {
			return
		}
		kind := "profile_observation_unavailable"
		if strings.Contains(reason, "failed") {
			kind = "profile_capture_error"
		}
		emit("profile:"+state.epoch+":"+uuid.NewString(), kind, phase+": "+reason, time.Now().UTC(), workerIdentity(r.cfg))
	}
}

func (r *Runner) profilingSnapshot() reporting.ProfilingEvidence {
	state := &r.profileEvidence
	state.mu.Lock()
	continuous := state.active && state.complete
	result := reporting.ProfilingEvidence{ProfileEpoch: state.epoch, ProfileHistoryComplete: state.active && state.complete, ProfileCapability: "snapshot-only"}
	for _, incident := range state.incidents {
		result.ProfileIncidents = append(result.ProfileIncidents, incident)
	}
	if state.active {
		result.ProfileCapability = "pending"
		if state.observed {
			result.ProfileCapability = "observed"
		}
	}
	state.mu.Unlock()
	if !r.cfg.PprofCaptureEnabled {
		result.ProfileCapability = "disabled"
		result.ProfileHistoryComplete = true
	}
	dir := filepath.Join(r.cfg.DataDir, "profiles")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return result
	}
	if err != nil {
		result.ProfileReadError = err.Error()
		result.ProfileHistoryComplete = false
		return result
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			result.ProfileReadError = err.Error()
			result.ProfileHistoryComplete = false
			continue
		}
		if entry.Name() == "status.json" {
			var status profileStatus
			if err := json.Unmarshal(body, &status); err != nil {
				result.ProfileReadError = err.Error()
				result.ProfileHistoryComplete = false
				continue
			}
			result.ProfileStatusCounts = make(map[string]uint64)
			for reason, count := range status.Counts {
				result.ProfileStatusCounts[redactLogIncident(reason)] = count
			}
			var count uint64
			for reason, n := range status.Counts {
				if !strings.HasSuffix(reason, "-disabled") && reason != "cpu-rate-limited" && reason != "cpu-skipped-shutdown" && reason != "cancelled" {
					count += n
				}
			}
			if !continuous && count > uint64(len(status.Recent)) {
				result.ProfileHistoryComplete = false
				result.ProfileReadError = "Profile status detail history is truncated"
			}
			continue
		}
		var record reporting.ProfileRecordEvidence
		if err := json.Unmarshal(body, &record); err != nil {
			result.ProfileReadError = err.Error()
			result.ProfileHistoryComplete = false
			continue
		}
		var history struct {
			Attempts *uint64 `json:"upload_attempts"`
		}
		_ = json.Unmarshal(body, &history)
		if history.Attempts == nil {
			record.UploadHistoryIncomplete = true
		}
		record.Error = redactLogIncident(record.Error)
		record.UploadError = redactLogIncident(record.UploadError)
		for i := range record.UploadFailures {
			record.UploadFailures[i].Error = redactLogIncident(record.UploadFailures[i].Error)
		}
		if record.RunID != r.cfg.RunID || record.MachineID != r.cfg.MachineID || record.UploadHistoryIncomplete || record.UploadFailuresDropped > 0 {
			result.ProfileHistoryComplete = false
		}
		result.ProfileRecords = append(result.ProfileRecords, record)
	}
	if len(result.ProfileRecords) > pprofMaxEvidenceFiles {
		result.ProfileHistoryComplete = false
		result.ProfileReadError = "Profile evidence storage exceeds its observation bound"
	}
	return result
}

func (c *pprofCapturer) publishProfileStatus(phase, reason string) {
	if c.onStatus != nil {
		c.onStatus(phase, reason)
	}
}
