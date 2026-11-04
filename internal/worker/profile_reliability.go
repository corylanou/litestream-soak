package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/google/uuid"
)

// Profiling incidents are observations about the profiler, not about the
// workload, so they never stop the run. The first profileIncidentsRetained
// distinct incidents are kept in the snapshot and persisted one event each;
// after that each kind is coalesced into one summary incident carrying the
// count, the first and last time, and the latest message, persisted at most
// once per profileIncidentSummaryInterval. An outbox that has no room left
// drops the durable copy and counts the drop; the snapshot still carries the
// incident. Any other persistence failure still stops the run.
const (
	profileIncidentsRetained       = 256
	profileIncidentSummaryInterval = 15 * time.Minute
)

type profileIncidentSummary struct {
	count         uint64
	first, last   time.Time
	latest        string
	run           reporting.WorkerIdentity
	persisted     uint64
	lastPersisted time.Time
}

type profileEvidenceState struct {
	mu                         sync.Mutex
	epoch                      string
	active, complete, observed bool
	seen                       map[string]bool
	incidents                  map[string]reporting.ProfileIncident
	coalesced                  map[string]*profileIncidentSummary
	dropped                    uint64
	summaryInterval            time.Duration
}

func profileIncidentSummaryID(epoch, kind string) string {
	return "profile:" + epoch + ":summary:" + kind
}

func (s *profileIncidentSummary) incident(id, kind string) reporting.ProfileIncident {
	return reporting.ProfileIncident{ID: id, Kind: kind, Message: fmt.Sprintf("%d %s incidents coalesced since %s, latest at %s: %s", s.count, kind, s.first.UTC().Format(time.RFC3339), s.last.UTC().Format(time.RFC3339), s.latest), At: s.last, Run: s.run}
}

func (r *Runner) observeProfiling(cancelRun context.CancelCauseFunc) {
	state := &r.profileEvidence
	state.epoch = uuid.NewString()
	state.active = true
	entries, err := os.ReadDir(filepath.Join(r.cfg.DataDir, "profiles"))
	state.complete = os.IsNotExist(err) || (err == nil && len(entries) == 0)
	state.seen = make(map[string]bool)
	state.incidents = make(map[string]reporting.ProfileIncident)
	state.coalesced = make(map[string]*profileIncidentSummary)
	if state.summaryInterval == 0 {
		state.summaryInterval = profileIncidentSummaryInterval
	}
	persist := func(id, kind, message string, at time.Time, identity reporting.WorkerIdentity) bool {
		event := reporting.WorkerEventPayload{WorkerIdentity: identity, EventType: kind, Message: message, SentAt: at, WorkloadEvent: reporting.WorkloadEvent{WorkloadEventID: id}}
		err := r.persistRunEvidence(event)
		switch {
		case err == nil:
			r.requestEvidenceFlush()
			return true
		case errors.Is(err, errEvidenceOutboxFull):
			state.dropped++
			return false
		default:
			state.complete = false
			cancelRun(fmt.Errorf("persist profiling evidence: %w", err))
			return false
		}
	}
	emit := func(id, kind, message string, at time.Time, identity reporting.WorkerIdentity, dedupe bool) {
		state.mu.Lock()
		defer state.mu.Unlock()
		if dedupe && state.seen[id] {
			return
		}
		message = redactLogIncident(message)
		if len(state.incidents)-len(state.coalesced) < profileIncidentsRetained {
			state.incidents[id] = reporting.ProfileIncident{ID: id, Kind: kind, Message: message, At: at, Run: identity}
			if dedupe {
				state.seen[id] = true
			}
			persist(id, kind, message, at, identity)
			return
		}
		if dedupe {
			state.seen[id] = true
		}
		summary := state.coalesced[kind]
		if summary == nil {
			summary = &profileIncidentSummary{first: at, run: identity}
			state.coalesced[kind] = summary
		}
		summary.count++
		summary.last = at
		summary.latest = message
		summaryID := profileIncidentSummaryID(state.epoch, kind)
		incident := summary.incident(summaryID, kind)
		state.incidents[summaryID] = incident
		if summary.persisted == 0 || at.Sub(summary.lastPersisted) >= state.summaryInterval {
			if persist(fmt.Sprintf("%s:%d", summaryID, summary.persisted+1), kind, incident.Message, at, identity) {
				summary.persisted++
				summary.lastPersisted = at
			}
		}
	}
	r.profiles.onRecord = func(record *profileRecord) {
		state.mu.Lock()
		state.observed = true
		state.mu.Unlock()
		body, _ := json.Marshal(record)
		var evidence reporting.ProfileRecordEvidence
		_ = json.Unmarshal(body, &evidence)
		identity := r.identity
		identity.RunID = record.RunID
		identity.MachineID = record.MachineID
		identity.WorkerID = record.WorkerID
		identity.DeploymentID = record.DeploymentID
		for _, incident := range evidence.Incidents() {
			emit(incident.ID, incident.Kind, incident.Message, incident.At, identity, true)
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
		emit("profile:"+state.epoch+":"+uuid.NewString(), kind, phase+": "+reason, time.Now().UTC(), r.identity, false)
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
	for kind, summary := range state.coalesced {
		if result.ProfileIncidentsCoalesced == nil {
			result.ProfileIncidentsCoalesced = make(map[string]uint64)
		}
		result.ProfileIncidentsCoalesced[kind] = summary.count
	}
	result.ProfileEvidenceDropped = state.dropped
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
