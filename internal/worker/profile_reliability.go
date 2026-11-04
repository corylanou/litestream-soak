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
// after that the incidents of one kind from one run are coalesced into a
// summary carrying the count, the first and last time, and the latest
// message. The summary is published in windows of
// profileIncidentSummaryInterval: each window is one incident with its own
// id, persisted when it opens and kept live in the snapshot until the next
// window replaces it, so a consumer that records an id once still gets a
// fresh count every window.
// An outbox that has no room left drops the durable copy and counts the
// drop; the snapshot still carries the incident. Any other persistence
// failure still stops the run. Ids that dedupe repeated record incidents are
// remembered up to profileIncidentsSeenMax, oldest first out.
const (
	profileIncidentsRetained       = 256
	profileIncidentSummaryInterval = 15 * time.Minute
	profileIncidentsSeenMax        = 4096
)

type profileIncidentSummary struct {
	kind          string
	run           reporting.WorkerIdentity
	count         uint64
	first, last   time.Time
	latest        string
	windows       uint64
	windowID      string
	lastPersisted time.Time
}

type profileEvidenceState struct {
	mu                         sync.Mutex
	epoch                      string
	active, complete, observed bool
	seen                       map[string]bool
	seenOrder                  []string
	incidents                  map[string]reporting.ProfileIncident
	coalesced                  map[string]*profileIncidentSummary
	dropped                    uint64
	summaryInterval            time.Duration
}

func profileIncidentSummaryKey(kind string, run reporting.WorkerIdentity) string {
	return strings.Join([]string{kind, run.RunID, run.WorkerID, run.MachineID, fmt.Sprint(run.DeploymentID)}, "|")
}

func profileIncidentSummaryID(epoch, kind string, run reporting.WorkerIdentity, window uint64) string {
	return fmt.Sprintf("profile:%s:summary:%s:%s:%v:%d", epoch, kind, run.RunID, run.DeploymentID, window)
}

func (s *profileIncidentSummary) incident(id string) reporting.ProfileIncident {
	return reporting.ProfileIncident{ID: id, Kind: s.kind, Message: fmt.Sprintf("%d %s incidents coalesced since %s, latest at %s: %s", s.count, s.kind, s.first.UTC().Format(time.RFC3339), s.last.UTC().Format(time.RFC3339), s.latest), At: s.last, Run: s.run}
}

func (s *profileEvidenceState) markSeen(id string) {
	if s.seen[id] {
		return
	}
	s.seen[id] = true
	s.seenOrder = append(s.seenOrder, id)
	if len(s.seenOrder) > profileIncidentsSeenMax {
		delete(s.seen, s.seenOrder[0])
		s.seenOrder = s.seenOrder[1:]
	}
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
		if dedupe {
			state.markSeen(id)
		}
		if len(state.incidents)-len(state.coalesced) < profileIncidentsRetained {
			state.incidents[id] = reporting.ProfileIncident{ID: id, Kind: kind, Message: message, At: at, Run: identity}
			persist(id, kind, message, at, identity)
			return
		}
		key := profileIncidentSummaryKey(kind, identity)
		summary := state.coalesced[key]
		if summary == nil {
			summary = &profileIncidentSummary{kind: kind, run: identity, first: at}
			state.coalesced[key] = summary
		}
		summary.count++
		summary.last = at
		summary.latest = message
		if summary.windows == 0 || at.Sub(summary.lastPersisted) >= state.summaryInterval {
			delete(state.incidents, summary.windowID)
			summary.windows++
			summary.windowID = profileIncidentSummaryID(state.epoch, kind, identity, summary.windows)
			summary.lastPersisted = at
			persist(summary.windowID, kind, summary.incident(summary.windowID).Message, at, identity)
		}
		state.incidents[summary.windowID] = summary.incident(summary.windowID)
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
	for _, summary := range state.coalesced {
		if result.ProfileIncidentsCoalesced == nil {
			result.ProfileIncidentsCoalesced = make(map[string]uint64)
		}
		result.ProfileIncidentsCoalesced[summary.kind] += summary.count
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
