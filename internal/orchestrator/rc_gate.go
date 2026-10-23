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

	rcGateCandidatePrefix    = "rc"
	rcGateBaselinePrefix     = "rcbase"
	rcGateMaxDuration        = 7 * 24 * time.Hour
	rcGateProvisionTimeout   = 2 * time.Hour
	rcGateStressPhase        = "6h"
	rcGateUpstreamRepository = "benbjohnson/litestream"
	rcGateTailDuration       = 7 * 24 * time.Hour

	runArchiveTypeRCGateTail = "rc_gate_tail"

	rcGateVerdictPass         = "pass"
	rcGateVerdictFail         = "fail"
	rcGateVerdictIncomplete   = "incomplete"
	rcGateVerdictRegression   = "regression"
	rcGateVerdictBothFailing  = "both_failing"
	rcGateSideMissing         = "missing"
	rcGateSideNoCoverage      = "no_coverage"
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
	errRCGateClosed        = errors.New("rc gate does not permit provisioning")
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
	name     string
	profile  string
	identity string
}

var gateScenarios = []gateScenario{
	{name: "overload-truncate0", profile: gateStressProfile, identity: "overload-truncate0"},
	{name: "pinned-reader", profile: gateStressProfile, identity: "pinned-reader"},
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
				LoadMode:            "synthetic",
				WriteRate:           600,
				Pattern:             "constant",
				PayloadSize:         2048,
				ReadRatio:           0.1,
				Workers:             8,
				InitialSize:         "50MB",
				VerifyInterval:      "30m",
				VerifyType:          "integrity",
				SnapshotInterval:    "10m",
				SyncInterval:        "1s",
				S3PartSize:          "16MB",
				S3Concurrency:       8,
				TruncatePageN:       intPtr(0),
				StressScenarios:     "overload-truncate0,pinned-reader",
				StressScenarioPhase: rcGateStressPhase,
				VolumeSizeGB:        10,
				MemoryMB:            2048,
				CPUs:                2,
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

func (m *Manager) RegisterRCGate(request RCGateRequest, now time.Time) (model.RCGate, bool, error) {
	repository := strings.TrimSpace(request.Repository)
	if repository != rcGateUpstreamRepository {
		return model.RCGate{}, false, fmt.Errorf("%w: repository must be %s", errRCGateInvalid, rcGateUpstreamRepository)
	}
	candidateSHA := strings.TrimSpace(request.CandidateSHA)
	baselineSHA := strings.TrimSpace(request.BaselineSHA)
	if !fullCommitSHARe.MatchString(candidateSHA) || !fullCommitSHARe.MatchString(baselineSHA) {
		return model.RCGate{}, false, fmt.Errorf("%w: candidate_sha and baseline_sha must be 40-character lowercase hex commit SHAs", errRCGateInvalid)
	}
	if candidateSHA == baselineSHA {
		return model.RCGate{}, false, fmt.Errorf("%w: candidate and baseline must be different commits", errRCGateInvalid)
	}
	duration, err := time.ParseDuration(strings.TrimSpace(request.Duration))
	if err != nil || duration <= 0 || duration > rcGateMaxDuration {
		return model.RCGate{}, false, fmt.Errorf("%w: duration must be a positive duration no longer than %s", errRCGateInvalid, rcGateMaxDuration)
	}

	now = now.UTC()
	gate := model.RCGate{
		Repository:        repository,
		CandidateRef:      strings.TrimSpace(request.CandidateRef),
		CandidateSource:   gateSourceName(rcGateCandidatePrefix, candidateSHA),
		CandidateSHA:      candidateSHA,
		BaselineSource:    gateSourceName(rcGateBaselinePrefix, baselineSHA),
		BaselineSHA:       baselineSHA,
		Duration:          duration,
		Tail:              request.Tail,
		Status:            model.RCGateRunning,
		StartedAt:         now,
		ExpiresAt:         now.Add(duration),
		ProvisionDeadline: now.Add(rcGateProvisionTimeout),
	}
	if gate.Tail {
		gate.TailExpiresAt = new(now.Add(rcGateTailDuration))
	}
	for _, source := range gate.Sources() {
		litestreamSHA, _ := gate.LitestreamSHAFor(source)
		for _, desired := range GateFleetForSource(source, "", litestreamSHA).Workers {
			_, deadline := rcGateProfileWindow(gate, desired.ProfileName)
			gate.Resources = append(gate.Resources, model.RCGateResource{
				Source:     source,
				WorkerName: desired.Name,
				Profile:    desired.ProfileName,
				Region:     normalizedWorkerRegion(desired.Region),
				CPUs:       desired.Workload.CPUs,
				MemoryMB:   desired.Workload.MemoryMB,
				VolumeGB:   effectiveDesiredVolumeSize(desired, desired.Workload),
				Tail:       gate.Tail && desired.ProfileName == gateStressProfile,
				StartedAt:  gate.StartedAt,
				ExpiresAt:  deadline,
			})
		}
	}
	if err := m.db.CreateRCGate(&gate); err != nil {
		if !errors.Is(err, model.ErrRCGateSourceActive) {
			return model.RCGate{}, false, err
		}
		existing, lookupErr := m.db.ActiveRCGateForSource(gate.CandidateSource)
		if lookupErr != nil {
			return model.RCGate{}, false, errors.Join(err, lookupErr)
		}
		if existing != nil && existing.Repository == gate.Repository && existing.CandidateSHA == gate.CandidateSHA &&
			existing.BaselineSHA == gate.BaselineSHA && existing.Duration == gate.Duration && existing.Tail == gate.Tail {
			return *existing, false, nil
		}
		return model.RCGate{}, false, err
	}
	_ = m.db.RecordEvent("", "rc_gate_registered", fmt.Sprintf("Registered RC gate %s vs %s for %s", gate.CandidateSource, gate.BaselineSource, duration), fmt.Sprintf("gate_id=%d tail=%t", gate.ID, gate.Tail))
	return gate, true, nil
}

func rcGateProfileWindow(gate model.RCGate, profile string) (bool, time.Time) {
	known := false
	for _, desired := range GateFleetForSource(gate.CandidateSource, "", "").Workers {
		if desired.ProfileName == profile {
			known = true
		}
	}
	if !known {
		return false, time.Time{}
	}
	tailProfile := gate.Tail && gate.TailExpiresAt != nil && profile == gateStressProfile
	switch {
	case gate.Status == model.RCGateRunning && tailProfile:
		return true, *gate.TailExpiresAt
	case gate.Status == model.RCGateRunning:
		return true, gate.ExpiresAt
	case gate.Status == model.RCGateTail && tailProfile:
		return true, *gate.TailExpiresAt
	default:
		return false, time.Time{}
	}
}

func (m *Manager) rcGateProfilePermitted(gate model.RCGate, profile string, now time.Time) bool {
	allowed, deadline := rcGateProfileWindow(gate, profile)
	if !allowed || !now.Before(deadline) {
		return false
	}
	return gate.PairedReadyAt != nil || now.Before(gate.ProvisionDeadline)
}

func (m *Manager) checkRCGateProvisioning(source, profile, imageRef, litestreamSHA string, now time.Time) error {
	if !isGateSource(source) {
		return nil
	}
	gate, err := m.db.ActiveRCGateForSource(source)
	if err != nil {
		return fmt.Errorf("load rc gate for %s: %w", source, err)
	}
	if gate == nil {
		return fmt.Errorf("%w: %s has no open gate", errRCGateClosed, source)
	}
	if !m.rcGateProfilePermitted(*gate, profile, now) {
		return fmt.Errorf("%w: %s %s is outside its gate window (status %s)", errRCGateClosed, source, profile, gate.Status)
	}
	pinnedImage, _ := gate.PinnedBuildFor(source)
	pinnedLitestream, _ := gate.LitestreamSHAFor(source)
	if pinnedImage == "" || strings.TrimSpace(imageRef) != pinnedImage || !strings.EqualFold(strings.TrimSpace(litestreamSHA), pinnedLitestream) {
		return fmt.Errorf("%w: %s must run %s with litestream %s", errRCGatePinned, source, firstNonEmpty(pinnedImage, "an unpinned build"), pinnedLitestream)
	}
	return nil
}

func (m *Manager) rcGateFleetForSource(source, gitSHA, litestreamSHA string, now time.Time) (FleetSpec, error) {
	gate, err := m.db.ActiveRCGateForSource(source)
	if err != nil {
		return FleetSpec{}, fmt.Errorf("load rc gate for %s: %w", source, err)
	}
	if gate == nil {
		return FleetSpec{}, fmt.Errorf("%w: %s has no open gate", errRCGateClosed, source)
	}
	spec := GateFleetForSource(source, gitSHA, litestreamSHA)
	permitted := spec.Workers[:0]
	for _, desired := range spec.Workers {
		if m.rcGateProfilePermitted(*gate, desired.ProfileName, now) {
			permitted = append(permitted, desired)
		}
	}
	spec.Workers = permitted
	return spec, nil
}

func (m *Manager) pinRCGateDeployment(source, gitSHA, litestreamSHA, imageRef string, now time.Time) (string, error) {
	gate, err := m.db.ActiveRCGateForSource(source)
	if err != nil {
		return "", fmt.Errorf("load rc gate for %s: %w", source, err)
	}
	if gate == nil {
		return "", fmt.Errorf("%w for source %s", errRCGateNotRegistered, source)
	}
	pinnedSHA, _ := gate.LitestreamSHAFor(source)
	if !strings.EqualFold(pinnedSHA, strings.TrimSpace(litestreamSHA)) {
		return "", fmt.Errorf("%w: %s requires litestream %s, got %s", errRCGatePinned, source, pinnedSHA, litestreamSHA)
	}
	if currentImage, _ := gate.PinnedBuildFor(source); currentImage == "" && (gate.Status != model.RCGateRunning || !now.Before(gate.ProvisionDeadline)) {
		return "", fmt.Errorf("%w: %s missed its launch window", errRCGateClosed, source)
	}
	pinnedImage, pinnedGit, err := m.db.PinRCGateBuild(gate.ID, source, imageRef, gitSHA)
	if err != nil {
		return "", fmt.Errorf("pin rc gate build: %w", err)
	}
	if pinnedImage == "" || pinnedGit != gitSHA {
		return "", fmt.Errorf("%w: %s is running %s, refusing %s", errRCGatePinned, source, firstNonEmpty(pinnedImage, "nothing"), imageRef)
	}
	return pinnedImage, nil
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
	destroyAll := func(model.Worker) bool { return false }
	if gate.Status == model.RCGateRunning && gate.PairedReadyAt == nil {
		ready, err := m.rcGatePairedReady(gate)
		if err != nil {
			return err
		}
		switch {
		case ready:
			if err := m.db.MarkRCGatePairedReady(gate.ID, now); err != nil {
				return fmt.Errorf("mark rc gate paired ready: %w", err)
			}
			gate.PairedReadyAt = &now
		case !now.Before(gate.ProvisionDeadline):
			summary := fmt.Sprintf("RC gate %s vs %s did not provision both sides within %s; archiving evidence and destroying all gate workers.", gate.CandidateSource, gate.BaselineSource, rcGateProvisionTimeout)
			return m.teardownRCGateSources(ctx, gate, runArchiveTypeTeardown, "rc_gate_unpaired", summary, destroyAll, model.RCGateAborted, now)
		default:
			return nil
		}
	}
	switch {
	case gate.Status == model.RCGateRunning && !now.Before(gate.ExpiresAt):
		keep := destroyAll
		next := model.RCGateCompleted
		summary := fmt.Sprintf("RC gate %s vs %s reached its %s TTL; archiving evidence and destroying gate workers.", gate.CandidateSource, gate.BaselineSource, gate.Duration)
		if gate.Tail {
			keep = func(worker model.Worker) bool { return worker.ProfileName == gateStressProfile }
			next = model.RCGateTail
			summary = fmt.Sprintf("RC gate %s vs %s reached its %s TTL; archiving evidence and destroying gate workers except %s, which continues its tail.", gate.CandidateSource, gate.BaselineSource, gate.Duration, gateStressProfile)
		}
		return m.teardownRCGateSources(ctx, gate, runArchiveTypeTeardown, "rc_gate_ttl", summary, keep, next, now)
	case gate.Status == model.RCGateTail && gate.TailExpiresAt != nil && !now.Before(*gate.TailExpiresAt):
		summary := fmt.Sprintf("RC gate %s vs %s finished its %s tail; archiving evidence and destroying remaining gate workers.", gate.CandidateSource, gate.BaselineSource, rcGateTailDuration)
		return m.teardownRCGateSources(ctx, gate, runArchiveTypeRCGateTail, "rc_gate_tail", summary, destroyAll, model.RCGateCompleted, now)
	default:
		return nil
	}
}

func (m *Manager) rcGatePairedReady(gate model.RCGate) (bool, error) {
	for _, source := range gate.Sources() {
		pinnedImage, pinnedGit := gate.PinnedBuildFor(source)
		pinnedLitestream, _ := gate.LitestreamSHAFor(source)
		if pinnedImage == "" {
			return false, nil
		}
		workers, err := m.db.ListWorkersFiltered("", source)
		if err != nil {
			return false, fmt.Errorf("list %s workers: %w", source, err)
		}
		for _, desired := range GateFleetForSource(source, pinnedGit, pinnedLitestream).Workers {
			ready := false
			for _, worker := range workers {
				if worker.ProfileName == desired.ProfileName && worker.Status == model.WorkerRunning &&
					worker.GitSHA == pinnedGit && worker.LitestreamSHA == pinnedLitestream && rcGateOwnsWorker(gate, worker) {
					ready = true
					break
				}
			}
			if !ready {
				return false, nil
			}
		}
	}
	return true, nil
}

func (m *Manager) teardownRCGateSources(ctx context.Context, gate model.RCGate, archiveType, reason, summary string, keep func(model.Worker) bool, next model.RCGateStatus, now time.Time) error {
	var teardownErrors []error
	for _, source := range gate.Sources() {
		result, err := m.archiveAndDestroySourceWorkers(ctx, source, archiveType, reason, "rc_gate", summary, keep, true)
		switch {
		case errors.Is(err, errSourceNotFound):
			continue
		case errors.Is(err, errSourceDeploymentNotFound):
			teardownErrors = append(teardownErrors, m.destroyUnarchivedRCGateWorkers(ctx, source, keep))
		case err != nil:
			teardownErrors = append(teardownErrors, fmt.Errorf("%s: %w", source, err))
		case !result.Complete:
			teardownErrors = append(teardownErrors, fmt.Errorf("%s: %d worker(s) failed to tear down", source, result.FailedWorkers))
		}
	}
	if err := errors.Join(teardownErrors...); err != nil {
		return err
	}
	var (
		completedAt *time.Time
		results     string
	)
	if next == model.RCGateCompleted || next == model.RCGateAborted {
		completedAt = &now
		snapshot, err := buildRCGateResults(m.db, gate, now)
		if err != nil {
			return fmt.Errorf("snapshot rc gate results: %w", err)
		}
		body, err := json.Marshal(snapshot)
		if err != nil {
			return fmt.Errorf("marshal rc gate results: %w", err)
		}
		results = string(body)
	}
	if err := m.db.UpdateRCGateStatus(gate.ID, next, completedAt, results); err != nil {
		return fmt.Errorf("update rc gate status: %w", err)
	}
	_ = m.db.RecordEvent("", "rc_gate_"+string(next), summary, fmt.Sprintf("gate_id=%d", gate.ID))
	return nil
}

func (m *Manager) destroyUnarchivedRCGateWorkers(ctx context.Context, source string, keep func(model.Worker) bool) error {
	unlock, err := m.lockSource(ctx, source)
	if err != nil {
		return err
	}
	defer unlock()
	workers, err := m.db.ListWorkersFiltered("", source)
	if err != nil {
		return fmt.Errorf("list %s workers: %w", source, err)
	}
	var destroyErrors []error
	for _, worker := range workers {
		if keep(worker) {
			continue
		}
		if err := m.DestroyWorker(ctx, worker.ID); err != nil {
			destroyErrors = append(destroyErrors, fmt.Errorf("%s: %w", worker.ID, err))
		}
	}
	return errors.Join(destroyErrors...)
}

type rcGateResults struct {
	Verdict   string                 `json:"verdict"`
	Scenarios []RCGateScenarioResult `json:"scenarios"`
	ClosedAt  time.Time              `json:"closed_at"`
}

func buildRCGateLedgerEntry(db *model.DB, gate model.RCGate, now time.Time) (RCGateLedgerEntry, error) {
	entry := RCGateLedgerEntry{
		RCGate:   gate,
		Duration: gate.Duration.String(),
	}
	for _, resource := range gate.Resources {
		entry.TotalMachines++
		entry.TotalVolumeGB += resource.VolumeGB
	}
	var results rcGateResults
	if gate.Closed() && strings.TrimSpace(gate.Results) != "" {
		if err := json.Unmarshal([]byte(gate.Results), &results); err != nil {
			return RCGateLedgerEntry{}, fmt.Errorf("decode rc gate %d results: %w", gate.ID, err)
		}
	} else {
		end := now
		if gate.CompletedAt != nil {
			end = *gate.CompletedAt
		}
		var err error
		results, err = buildRCGateResults(db, gate, end)
		if err != nil {
			return RCGateLedgerEntry{}, err
		}
	}
	entry.Verdict = results.Verdict
	entry.Scenarios = results.Scenarios
	return entry, nil
}

func buildRCGateResults(db *model.DB, gate model.RCGate, end time.Time) (rcGateResults, error) {
	results := rcGateResults{ClosedAt: end.UTC()}
	failing, incomplete := false, false
	for _, scenario := range gateScenarios {
		candidate, err := rcGateSide(db, gate, gate.CandidateSource, scenario, end)
		if err != nil {
			return rcGateResults{}, err
		}
		baseline, err := rcGateSide(db, gate, gate.BaselineSource, scenario, end)
		if err != nil {
			return rcGateResults{}, err
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
		results.Scenarios = append(results.Scenarios, result)
	}
	switch {
	case failing:
		results.Verdict = rcGateVerdictFail
	case incomplete:
		results.Verdict = rcGateVerdictIncomplete
	default:
		results.Verdict = rcGateVerdictPass
	}
	return results, nil
}

func rcGateSide(db *model.DB, gate model.RCGate, source string, scenario gateScenario, end time.Time) (RCGateSideResult, error) {
	side := RCGateSideResult{Source: source, Status: rcGateSideMissing}
	pinnedImage, pinnedGit := gate.PinnedBuildFor(source)
	pinnedLitestream, _ := gate.LitestreamSHAFor(source)
	if pinnedImage == "" {
		return side, nil
	}
	deployment, err := db.GetDeploymentByVersion(source, pinnedGit, pinnedLitestream)
	if err != nil || deployment == nil || deployment.ImageRef != pinnedImage {
		return side, nil
	}
	workers, err := db.ListWorkersFiltered("", source)
	if err != nil {
		return side, err
	}
	var worker *model.Worker
	for i := range workers {
		if workers[i].ProfileName == scenario.profile && rcGateOwnsWorker(gate, workers[i]) {
			worker = &workers[i]
			break
		}
	}
	if worker == nil {
		return side, nil
	}
	side.WorkerID = worker.ID
	verifications, err := db.ListVerifications(worker.ID, rcGateVerificationHistory)
	if err != nil {
		return side, err
	}
	for _, verification := range verifications {
		if verification.StartedAt.Before(gate.StartedAt) || verification.StartedAt.After(end) ||
			verification.Run.Scenario != scenario.identity || !verification.MatchesDeployment(*worker, *deployment) {
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

func rcGateOwnsWorker(gate model.RCGate, worker model.Worker) bool {
	return !worker.CreatedAt.Before(gate.StartedAt.Truncate(time.Second))
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
	now := time.Now().UTC()
	response := RCGateLedgerResponse{Gates: make([]RCGateLedgerEntry, 0, len(gates))}
	for _, gate := range gates {
		entry, err := buildRCGateLedgerEntry(db, gate, now)
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
	gate, created, err := a.manager.RegisterRCGate(request, time.Now().UTC())
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
	if created {
		w.WriteHeader(http.StatusCreated)
	}
	writeAPIJSON(w, gate)
}
