package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

const (
	gateStressProfile    = "gate-stress"
	gateManyDB500Profile = "gate-manydb-500"
	gateLargeDBProfile   = "gate-large-db"
	gateRemoteProfile    = "gate-remote"

	rcGateCandidatePrefix = "rc"
	rcGateBaselinePrefix  = "rcbase"
	rcGateMaxDuration     = 7 * 24 * time.Hour
	rcGateTailDuration    = 7 * 24 * time.Hour

	runArchiveTypeRCGateTail = "rc_gate_tail"

	rcGateVerdictPass         = "pass"
	rcGateVerdictFail         = "fail"
	rcGateVerdictIncomplete   = "incomplete"
	rcGateVerdictRegression   = "regression"
	rcGateVerdictBothFailing  = "both_failing"
	rcGateSideMissing         = "missing"
	rcGateSideNoCoverage      = "no_coverage"
	rcGateSideNotScheduled    = "not_scheduled"
	rcGateSidePassing         = "passing"
	rcGateSideFailing         = "failing"
	rcGateVerificationHistory = 2000
)

var (
	gateSourceRe           = regexp.MustCompile(`^(rc|rcbase)-[0-9a-f]{8}$`)
	fullCommitSHARe        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	errRCGateNotRegistered = errors.New("rc gate is not registered")
	errRCGatePinned        = errors.New("rc gate source is pinned to a different build")
	errRCGateInvalid       = errors.New("invalid rc gate request")
)

type RCGateRequest struct {
	Repository   string `json:"repository"`
	CandidateRef string `json:"candidate_ref,omitempty"`
	CandidateSHA string `json:"candidate_sha"`
	BaselineSHA  string `json:"baseline_sha"`
	Duration     string `json:"duration"`
	Tail         bool   `json:"tail"`
}

type RCGateSideResult struct {
	Source   string `json:"source"`
	WorkerID string `json:"worker_id,omitempty"`
	Status   string `json:"status"`
	Passed   int    `json:"passed"`
	Failed   int    `json:"failed"`
}

type RCGateScenarioResult struct {
	Scenario  string           `json:"scenario"`
	Profile   string           `json:"profile"`
	Candidate RCGateSideResult `json:"candidate"`
	Baseline  RCGateSideResult `json:"baseline"`
	Verdict   string           `json:"verdict"`
}

type RCGateLedgerEntry struct {
	model.RCGate
	Duration      string                 `json:"duration"`
	TotalMachines int                    `json:"total_machines"`
	TotalVolumeGB int                    `json:"total_volume_gb"`
	Verdict       string                 `json:"verdict"`
	Scenarios     []RCGateScenarioResult `json:"scenarios"`
}

type RCGateLedgerResponse struct {
	Gates []RCGateLedgerEntry `json:"gates"`
}

type gateScenario struct {
	name    string
	profile string
	covers  func(workload.Config) bool
}

var gateScenarios = []gateScenario{
	{name: "overload-truncate0", profile: gateStressProfile, covers: func(cfg workload.Config) bool {
		return cfg.TruncatePageN != nil && *cfg.TruncatePageN == 0
	}},
	{name: "pinned-reader", profile: gateStressProfile, covers: func(cfg workload.Config) bool {
		return strings.TrimSpace(cfg.PinnedReaderHold) != ""
	}},
	{name: "manydb-500", profile: gateManyDB500Profile},
	{name: "large-db", profile: gateLargeDBProfile},
	{name: "remote", profile: gateRemoteProfile},
}

func isGateSource(source string) bool {
	return gateSourceRe.MatchString(strings.TrimSpace(source))
}

func gateSourceName(prefix, sha string) string {
	return prefix + "-" + sha[:8]
}

func GateFleetForSource(source, gitSHA, litestreamSHA string) FleetSpec {
	workers := []DesiredWorker{
		{
			Name:         "worker-gate-stress",
			ProfileName:  gateStressProfile,
			Region:       "ord",
			VolumeSizeGB: 10,
			Workload: workload.Config{
				LoadMode:         "synthetic",
				WriteRate:        600,
				Pattern:          "constant",
				PayloadSize:      2048,
				ReadRatio:        0.1,
				Workers:          8,
				InitialSize:      "50MB",
				VerifyInterval:   "30m",
				VerifyType:       "integrity",
				SnapshotInterval: "10m",
				SyncInterval:     "1s",
				S3PartSize:       "16MB",
				S3Concurrency:    8,
				TruncatePageN:    intPtr(0),
				VolumeSizeGB:     10,
				MemoryMB:         2048,
				CPUs:             2,
			},
		},
		{
			Name:         "worker-gate-manydb-500",
			ProfileName:  gateManyDB500Profile,
			Region:       "ord",
			VolumeSizeGB: 15,
			Workload: workload.Config{
				LoadMode:                "many-db",
				WriteRate:               20,
				Pattern:                 "constant",
				PayloadSize:             512,
				Workers:                 3,
				InitialSize:             "5MB",
				VerifyInterval:          "30m",
				VerifyType:              "integrity",
				SnapshotInterval:        "10m",
				SyncInterval:            "1s",
				NumDatabases:            500,
				ActivePercent:           2,
				ActiveRotateInterval:    "30m",
				ActiveSetSeed:           1,
				ConfigMode:              "dir",
				VerifySampleSize:        5,
				VerifyChangedLimit:      100,
				ReplicationLagThreshold: 0,
				VolumeSizeGB:            15,
				MemoryMB:                3072,
				CPUs:                    2,
			},
		},
		{
			Name:         "worker-gate-large-db",
			ProfileName:  gateLargeDBProfile,
			Region:       "ord",
			VolumeSizeGB: 40,
			Workload: workload.Config{
				LoadMode:                   "synthetic",
				WriteRate:                  10,
				Pattern:                    "constant",
				PayloadSize:                1024,
				ReadRatio:                  0.2,
				Workers:                    2,
				InitialSize:                "6GB",
				VerifyInterval:             "2h",
				VerifyType:                 "integrity",
				VerifySourceSnapshotBudget: "20m",
				VerifyRestoreBudget:        "45m",
				VerifyIntegrityCheckBudget: "20m",
				VerifyCompareBudget:        "30m",
				SnapshotInterval:           "6h",
				SyncInterval:               "1s",
				VolumeSizeGB:               40,
				MemoryMB:                   4096,
				CPUs:                       2,
			},
		},
		{
			Name:         "worker-gate-remote",
			ProfileName:  gateRemoteProfile,
			Region:       "ams",
			VolumeSizeGB: 10,
			Workload: workload.Config{
				LoadMode:         "synthetic",
				WriteRate:        10,
				Pattern:          "constant",
				PayloadSize:      1024,
				ReadRatio:        0.2,
				Workers:          1,
				InitialSize:      "5MB",
				VerifyInterval:   "30m",
				VerifyType:       "integrity",
				SnapshotInterval: "10m",
				SyncInterval:     "1s",
				VolumeSizeGB:     10,
				MemoryMB:         1024,
				CPUs:             1,
			},
		},
	}
	source = strings.TrimSpace(source)
	for i := range workers {
		workers[i].Source = source
		workers[i].GitSHA = gitSHA
		workers[i].LitestreamSHA = litestreamSHA
		workers[i].Name = workerNameForSource(source, workers[i].Name)
		workers[i].WorkerID = workers[i].Name
	}
	return FleetSpec{Workers: workers}
}

func (m *Manager) RegisterRCGate(request RCGateRequest, now time.Time) (model.RCGate, error) {
	repository := strings.TrimSpace(request.Repository)
	if _, _, ok := splitGitHubRepository(repository); !ok {
		return model.RCGate{}, fmt.Errorf("%w: repository must use owner/name form", errRCGateInvalid)
	}
	candidateSHA := strings.TrimSpace(request.CandidateSHA)
	baselineSHA := strings.TrimSpace(request.BaselineSHA)
	if !fullCommitSHARe.MatchString(candidateSHA) || !fullCommitSHARe.MatchString(baselineSHA) {
		return model.RCGate{}, fmt.Errorf("%w: candidate_sha and baseline_sha must be 40-character lowercase hex commit SHAs", errRCGateInvalid)
	}
	if candidateSHA == baselineSHA {
		return model.RCGate{}, fmt.Errorf("%w: candidate and baseline must be different commits", errRCGateInvalid)
	}
	duration, err := time.ParseDuration(strings.TrimSpace(request.Duration))
	if err != nil || duration <= 0 || duration > rcGateMaxDuration {
		return model.RCGate{}, fmt.Errorf("%w: duration must be a positive duration no longer than %s", errRCGateInvalid, rcGateMaxDuration)
	}

	now = now.UTC()
	gate := model.RCGate{
		Repository:      repository,
		CandidateRef:    strings.TrimSpace(request.CandidateRef),
		CandidateSource: gateSourceName(rcGateCandidatePrefix, candidateSHA),
		CandidateSHA:    candidateSHA,
		BaselineSource:  gateSourceName(rcGateBaselinePrefix, baselineSHA),
		BaselineSHA:     baselineSHA,
		Duration:        duration,
		Tail:            request.Tail,
		Status:          model.RCGateRunning,
		StartedAt:       now,
		ExpiresAt:       now.Add(duration),
	}
	if gate.Tail {
		gate.TailExpiresAt = new(now.Add(rcGateTailDuration))
	}
	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		for _, desired := range GateFleetForSource(source, "", litestreamSHA).Workers {
			resource := model.RCGateResource{
				Source:     source,
				WorkerName: desired.Name,
				Profile:    desired.ProfileName,
				Region:     normalizedWorkerRegion(desired.Region),
				CPUs:       desired.Workload.CPUs,
				MemoryMB:   desired.Workload.MemoryMB,
				VolumeGB:   effectiveDesiredVolumeSize(desired, desired.Workload),
				StartedAt:  gate.StartedAt,
				ExpiresAt:  gate.ExpiresAt,
			}
			if gate.Tail && desired.ProfileName == gateStressProfile {
				resource.Tail = true
				resource.ExpiresAt = *gate.TailExpiresAt
			}
			gate.Resources = append(gate.Resources, resource)
		}
	}
	if err := m.db.CreateRCGate(&gate); err != nil {
		return model.RCGate{}, err
	}
	_ = m.db.RecordEvent("", "rc_gate_registered", fmt.Sprintf("Registered RC gate %s vs %s for %s", gate.CandidateSource, gate.BaselineSource, duration), fmt.Sprintf("gate_id=%d tail=%t", gate.ID, gate.Tail))
	return gate, nil
}

func (m *Manager) pinRCGateDeployment(source, litestreamSHA, imageRef string) error {
	gate, err := m.db.ActiveRCGateForSource(source)
	if err != nil {
		return fmt.Errorf("load rc gate for %s: %w", source, err)
	}
	if gate == nil {
		return fmt.Errorf("%w for source %s", errRCGateNotRegistered, source)
	}
	pinnedSHA, _ := gate.LitestreamSHAFor(source)
	if !strings.EqualFold(pinnedSHA, strings.TrimSpace(litestreamSHA)) {
		return fmt.Errorf("%w: %s requires litestream %s, got %s", errRCGatePinned, source, pinnedSHA, litestreamSHA)
	}
	pinned, err := m.db.PinRCGateImage(gate.ID, source, imageRef)
	if err != nil {
		return fmt.Errorf("pin rc gate image: %w", err)
	}
	if pinned != imageRef {
		return fmt.Errorf("%w: %s is running %s, refusing %s", errRCGatePinned, source, pinned, imageRef)
	}
	return nil
}

func (m *Manager) RunRCGateLoop(ctx context.Context, interval time.Duration) {
	if err := m.evaluateRCGates(ctx, time.Now().UTC()); err != nil {
		slog.Error("Failed to evaluate rc gates", "error", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.evaluateRCGates(ctx, time.Now().UTC()); err != nil {
				slog.Error("Failed to evaluate rc gates", "error", err)
			}
		}
	}
}

func (m *Manager) evaluateRCGates(ctx context.Context, now time.Time) error {
	gates, err := m.db.ActiveRCGates()
	if err != nil {
		return fmt.Errorf("list active rc gates: %w", err)
	}
	var gateErrors []error
	for _, gate := range gates {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(gateErrors, err)...)
		}
		if err := m.evaluateRCGate(ctx, gate, now); err != nil {
			gateErrors = append(gateErrors, fmt.Errorf("rc gate %d: %w", gate.ID, err))
		}
	}
	return errors.Join(gateErrors...)
}

func (m *Manager) evaluateRCGate(ctx context.Context, gate model.RCGate, now time.Time) error {
	switch {
	case gate.Status == model.RCGateRunning && !now.Before(gate.ExpiresAt):
		keep := func(model.Worker) bool { return false }
		next := model.RCGateCompleted
		if gate.Tail {
			keep = func(worker model.Worker) bool { return worker.ProfileName == gateStressProfile }
			next = model.RCGateTail
		}
		summary := fmt.Sprintf("RC gate %s vs %s reached its %s TTL; archiving evidence and destroying gate workers.", gate.CandidateSource, gate.BaselineSource, gate.Duration)
		if gate.Tail {
			summary = fmt.Sprintf("RC gate %s vs %s reached its %s TTL; archiving evidence and destroying gate workers except %s, which continues its tail.", gate.CandidateSource, gate.BaselineSource, gate.Duration, gateStressProfile)
		}
		return m.teardownRCGateSources(ctx, gate, runArchiveTypeTeardown, "rc_gate_ttl", summary, keep, next, now)
	case gate.Status == model.RCGateTail && gate.TailExpiresAt != nil && !now.Before(*gate.TailExpiresAt):
		summary := fmt.Sprintf("RC gate %s vs %s finished its %s tail; archiving evidence and destroying remaining gate workers.", gate.CandidateSource, gate.BaselineSource, rcGateTailDuration)
		return m.teardownRCGateSources(ctx, gate, runArchiveTypeRCGateTail, "rc_gate_tail", summary, func(model.Worker) bool { return false }, model.RCGateCompleted, now)
	default:
		return nil
	}
}

func (m *Manager) teardownRCGateSources(ctx context.Context, gate model.RCGate, archiveType, reason, summary string, keep func(model.Worker) bool, next model.RCGateStatus, now time.Time) error {
	complete := true
	var teardownErrors []error
	for _, source := range gate.Sources() {
		result, err := m.archiveAndDestroySourceWorkers(ctx, source, archiveType, reason, "rc_gate", summary, keep)
		switch {
		case errors.Is(err, errSourceNotFound), errors.Is(err, errSourceDeploymentNotFound):
			continue
		case err != nil:
			complete = false
			teardownErrors = append(teardownErrors, fmt.Errorf("%s: %w", source, err))
		case !result.Complete:
			complete = false
			teardownErrors = append(teardownErrors, fmt.Errorf("%s: %d worker(s) failed to tear down", source, result.FailedWorkers))
		}
	}
	if !complete {
		return errors.Join(teardownErrors...)
	}
	var completedAt *time.Time
	if next == model.RCGateCompleted {
		completedAt = &now
	}
	if err := m.db.UpdateRCGateStatus(gate.ID, next, completedAt); err != nil {
		return fmt.Errorf("update rc gate status: %w", err)
	}
	_ = m.db.RecordEvent("", "rc_gate_"+string(next), summary, fmt.Sprintf("gate_id=%d", gate.ID))
	return nil
}

func buildRCGateLedgerEntry(db *model.DB, gate model.RCGate) (RCGateLedgerEntry, error) {
	entry := RCGateLedgerEntry{
		RCGate:   gate,
		Duration: gate.Duration.String(),
	}
	for _, resource := range gate.Resources {
		entry.TotalMachines++
		entry.TotalVolumeGB += resource.VolumeGB
	}
	failing, incomplete := false, false
	for _, scenario := range gateScenarios {
		candidate, err := rcGateSide(db, gate, gate.CandidateSource, scenario)
		if err != nil {
			return RCGateLedgerEntry{}, err
		}
		baseline, err := rcGateSide(db, gate, gate.BaselineSource, scenario)
		if err != nil {
			return RCGateLedgerEntry{}, err
		}
		result := RCGateScenarioResult{
			Scenario:  scenario.name,
			Profile:   scenario.profile,
			Candidate: candidate,
			Baseline:  baseline,
			Verdict:   rcGateScenarioVerdict(candidate.Status, baseline.Status),
		}
		switch result.Verdict {
		case rcGateVerdictRegression, rcGateVerdictBothFailing:
			failing = true
		case rcGateVerdictIncomplete:
			incomplete = true
		}
		entry.Scenarios = append(entry.Scenarios, result)
	}
	switch {
	case failing:
		entry.Verdict = rcGateVerdictFail
	case incomplete:
		entry.Verdict = rcGateVerdictIncomplete
	default:
		entry.Verdict = rcGateVerdictPass
	}
	return entry, nil
}

func rcGateSide(db *model.DB, gate model.RCGate, source string, scenario gateScenario) (RCGateSideResult, error) {
	side := RCGateSideResult{Source: source, Status: rcGateSideMissing}
	workers, err := db.ListWorkersFiltered("", source)
	if err != nil {
		return side, err
	}
	var worker *model.Worker
	for i := range workers {
		if workers[i].ProfileName == scenario.profile {
			worker = &workers[i]
			break
		}
	}
	if worker == nil {
		return side, nil
	}
	side.WorkerID = worker.ID
	if scenario.covers != nil && !scenario.covers(resolveWorkerWorkload(*worker)) {
		side.Status = rcGateSideNotScheduled
		return side, nil
	}
	verifications, err := db.ListVerifications(worker.ID, rcGateVerificationHistory)
	if err != nil {
		return side, err
	}
	for _, verification := range verifications {
		if verification.StartedAt.Before(gate.StartedAt) {
			continue
		}
		switch {
		case verification.Succeeded():
			side.Passed++
		case verification.Failed():
			side.Failed++
		}
	}
	switch {
	case side.Failed > 0:
		side.Status = rcGateSideFailing
	case side.Passed > 0:
		side.Status = rcGateSidePassing
	default:
		side.Status = rcGateSideNoCoverage
	}
	return side, nil
}

func rcGateScenarioVerdict(candidate, baseline string) string {
	covered := func(status string) bool { return status == rcGateSidePassing || status == rcGateSideFailing }
	switch {
	case !covered(candidate) || !covered(baseline):
		return rcGateVerdictIncomplete
	case candidate == rcGateSideFailing && baseline == rcGateSideFailing:
		return rcGateVerdictBothFailing
	case candidate == rcGateSideFailing:
		return rcGateVerdictRegression
	default:
		return rcGateVerdictPass
	}
}

func (a *API) handleListRCGates(w http.ResponseWriter, r *http.Request) {
	db := a.db.WithReadContext(r.Context())
	gates, err := db.ListRCGates()
	if err != nil {
		respondError(w, r, http.StatusInternalServerError, err, "failed to load rc gates")
		return
	}
	response := RCGateLedgerResponse{Gates: make([]RCGateLedgerEntry, 0, len(gates))}
	for _, gate := range gates {
		entry, err := buildRCGateLedgerEntry(db, gate)
		if err != nil {
			respondError(w, r, http.StatusInternalServerError, err, "failed to build rc gate results")
			return
		}
		response.Gates = append(response.Gates, entry)
	}
	writeAPIJSON(w, response)
}

func (a *API) handleRegisterRCGate(w http.ResponseWriter, r *http.Request) {
	if a.manager == nil {
		respondError(w, r, http.StatusInternalServerError, nil, "rc gate manager unavailable")
		return
	}
	var request RCGateRequest
	defer func() { _ = r.Body.Close() }()
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		respondError(w, r, http.StatusBadRequest, err, "invalid payload")
		return
	}
	gate, err := a.manager.RegisterRCGate(request, time.Now().UTC())
	switch {
	case errors.Is(err, errRCGateInvalid):
		respondError(w, r, http.StatusBadRequest, err, err.Error())
		return
	case errors.Is(err, model.ErrRCGateSourceActive):
		respondError(w, r, http.StatusConflict, err, err.Error())
		return
	case err != nil:
		respondError(w, r, http.StatusInternalServerError, err, "failed to register rc gate")
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeAPIJSON(w, gate)
}
