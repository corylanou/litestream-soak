package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

const (
	gateStressProfile        = "gate-stress"
	gateManyDB500Profile     = "gate-manydb-500"
	gateLargeDBProfile       = "gate-large-db"
	gateRemoteProfile        = "gate-remote"
	gateUpgradeLaneProfile   = "gate-upgrade-lane"
	gateUpgradeFollowProfile = "gate-upgrade-follow"

	rcGateCandidatePrefix      = "rc"
	rcGateBaselinePrefix       = "rcbase"
	upgradeGateCandidatePrefix = "up"
	upgradeGateBaselinePrefix  = "upbase"
	upgradeGateDefaultDuration = 24 * time.Hour
	upgradeGateDefaultWarmup   = 2 * time.Hour
	rcGateMaxDuration          = 7 * 24 * time.Hour
	rcGateProvisionTimeout     = 2 * time.Hour
	rcGateStressPhase          = "6h"
	rcGateUpstreamRepository   = "benbjohnson/litestream"
	rcGateTailDuration         = 7 * 24 * time.Hour

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

	gateRoleCandidate = "candidate"
	gateRoleBaseline  = "baseline"
	gateModeFollow    = "follow"
	upgradePhasePre   = "pre-upgrade"
	upgradePhasePost  = "post-upgrade"
)

var (
	gateSourceRe           = regexp.MustCompile(`^(rc|rcbase|up|upbase)-[0-9a-f]{8}$`)
	fullCommitSHARe        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	errRCGateNotRegistered = errors.New("rc gate is not registered")
	errRCGatePinned        = errors.New("rc gate source is pinned to a different build")
	errRCGateInvalid       = errors.New("invalid rc gate request")
	errRCGateClosed        = errors.New("rc gate does not permit provisioning")
)

type RCGateRequest struct {
	Kind          string `json:"kind,omitempty"`
	Repository    string `json:"repository"`
	CandidateRef  string `json:"candidate_ref,omitempty"`
	CandidateSHA  string `json:"candidate_sha"`
	BaselineSHA   string `json:"baseline_sha"`
	Duration      string `json:"duration"`
	UpgradeWarmup string `json:"upgrade_warmup,omitempty"`
	Tail          bool   `json:"tail"`
}

type RCGateSideResult struct {
	Source          string `json:"source"`
	WorkerID        string `json:"worker_id,omitempty"`
	Status          string `json:"status"`
	Passed          int    `json:"passed"`
	Failed          int    `json:"failed"`
	RotationEnabled bool   `json:"rotation_enabled,omitempty"`
}

type RCGateModeResult struct {
	Mode      string           `json:"mode"`
	Candidate RCGateSideResult `json:"candidate"`
	Baseline  RCGateSideResult `json:"baseline"`
	Verdict   string           `json:"verdict"`
}

type RCGateScenarioResult struct {
	Scenario  string             `json:"scenario"`
	Profile   string             `json:"profile"`
	Candidate RCGateSideResult   `json:"candidate"`
	Baseline  RCGateSideResult   `json:"baseline"`
	Modes     []RCGateModeResult `json:"modes,omitempty"`
	Verdict   string             `json:"verdict"`
}

type RCGateLedgerEntry struct {
	model.RCGate
	Duration      string                 `json:"duration"`
	UpgradeWarmup string                 `json:"upgrade_warmup,omitempty"`
	TotalMachines int                    `json:"total_machines"`
	TotalVolumeGB int                    `json:"total_volume_gb"`
	Verdict       string                 `json:"verdict"`
	Scenarios     []RCGateScenarioResult `json:"scenarios"`
	HeldWorkers   []RCGateHeldWorker     `json:"held_workers,omitempty"`
}

type RCGateLedgerResponse struct {
	Gates []RCGateLedgerEntry `json:"gates"`
}

// gateSideFilter selects which verifications count for one side of a scenario:
// the source role, an optional profile override, an upgrade phase, which check
// types count, and optionally a single restore mode. An empty checkType counts
// only current-boundary verifications: follow checks and historical
// pre-upgrade restores are scored by their own scenarios so they can never
// stand in for current coverage.
type gateSideFilter struct {
	role      string
	profile   string
	phase     string
	checkType string
	mode      string
}

const (
	gateCheckCurrent    = ""
	gateCheckAny        = "*"
	gateCheckFollow     = "follow"
	gateCheckHistorical = "pre-upgrade-restore"
)

var gateNonCurrentChecks = []string{gateCheckFollow, gateCheckHistorical}

type gateScenario struct {
	name          string
	profile       string
	identity      string
	txidOnly      bool
	requiredModes []string
	candidate     gateSideFilter
	baseline      gateSideFilter
}

var (
	rcGateScenarios = []gateScenario{
		{name: "overload-truncate0", profile: gateStressProfile, identity: "overload-truncate0"},
		{name: "pinned-reader", profile: gateStressProfile, identity: "pinned-reader"},
		{name: "manydb-500", profile: gateManyDB500Profile, txidOnly: true},
		{name: "large-db", profile: gateLargeDBProfile},
		{name: "remote", profile: gateRemoteProfile},
	}
	// The upgrade gate scores the lane worker's post-upgrade restores against
	// its own pre-upgrade restores, its historical pre-upgrade txid restores
	// against the pre-upgrade txid restores, its post-upgrade follow checks
	// against the baseline follow worker, and the plain follow workers against
	// each other.
	upgradeGateScenarios = []gateScenario{
		{
			name:      "upgrade-lane",
			profile:   gateUpgradeLaneProfile,
			candidate: gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePost},
			baseline:  gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePre},
		},
		{
			name:          "upgrade-lane-historical",
			profile:       gateUpgradeLaneProfile,
			requiredModes: []string{"txid"},
			candidate:     gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePost, checkType: gateCheckHistorical},
			baseline:      gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePre, mode: "txid"},
		},
		{
			name:          "upgrade-lane-follow",
			profile:       gateUpgradeLaneProfile,
			requiredModes: []string{gateModeFollow},
			candidate:     gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePost, checkType: gateCheckFollow},
			baseline:      gateSideFilter{role: gateRoleBaseline, profile: gateUpgradeFollowProfile, checkType: gateCheckFollow},
		},
		{
			name:          "follow-l9",
			profile:       gateUpgradeFollowProfile,
			requiredModes: append(slices.Clone(gateRotatingRestoreModes), gateModeFollow),
			candidate:     gateSideFilter{role: gateRoleCandidate, checkType: gateCheckAny},
			baseline:      gateSideFilter{role: gateRoleBaseline, checkType: gateCheckAny},
		},
	}
	gateRotatingRestoreModes = []string{"txid", "timestamp", "latest"}
)

func gateScenariosFor(kind model.RCGateKind) []gateScenario {
	if kind == model.RCGateKindUpgrade {
		return upgradeGateScenarios
	}
	return rcGateScenarios
}

func (f gateSideFilter) sourceFor(gate model.RCGate) string {
	if f.role == gateRoleBaseline {
		return gate.BaselineSource
	}
	return gate.CandidateSource
}

// accepts applies the side filter. A phase filter also requires the report's
// active Litestream SHA to be the build that phase must have run (baseline
// before the switch, candidate after), so a mislabelled phase never counts.
func (f gateSideFilter) accepts(gate model.RCGate, verification model.Verification) bool {
	if f.phase != "" {
		if verification.Run.UpgradePhase != f.phase {
			return false
		}
		want := gate.CandidateSHA
		if f.phase == upgradePhasePre {
			want = gate.BaselineSHA
		}
		if !strings.EqualFold(strings.TrimSpace(verification.Run.ActiveLitestreamSHA), want) {
			return false
		}
	}
	if f.mode != "" && firstNonEmpty(strings.TrimSpace(verification.RestoreMode), "txid") != f.mode {
		return false
	}
	switch f.checkType {
	case gateCheckAny:
		return true
	case gateCheckCurrent:
		return !slices.Contains(gateNonCurrentChecks, verification.CheckType)
	default:
		return verification.CheckType == f.checkType
	}
}

func isGateSource(source string) bool {
	return gateSourceRe.MatchString(strings.TrimSpace(source))
}

func gateSourceName(prefix, sha string) string {
	return prefix + "-" + sha[:8]
}

func gateKindForSource(source string) model.RCGateKind {
	prefix, _, _ := strings.Cut(strings.TrimSpace(source), "-")
	if prefix == upgradeGateCandidatePrefix || prefix == upgradeGateBaselinePrefix {
		return model.RCGateKindUpgrade
	}
	return model.RCGateKindRC
}

func gateSourcePrefixes(kind model.RCGateKind) (candidate, baseline string) {
	if kind == model.RCGateKindUpgrade {
		return upgradeGateCandidatePrefix, upgradeGateBaselinePrefix
	}
	return rcGateCandidatePrefix, rcGateBaselinePrefix
}

// upgradeGateWorkers is the upgrade gate fleet: the candidate source runs the
// lane worker (baseline build first, candidate build after the warm-up) and a
// plain candidate follow worker; the baseline source runs only the follow
// worker, so the two follow workers are a like-for-like pair.
func upgradeGateWorkers(source string) []DesiredWorker {
	levels := func(cfg *workload.Config) {
		cfg.L1CompactionInterval = "30s"
		cfg.L2CompactionInterval = "5m"
		cfg.L3CompactionInterval = "1h"
		cfg.L0Retention = "10m"
		cfg.L0RetentionCheckInterval = "1m"
		cfg.ReplicaLevelReporting = true
		cfg.FollowCheck = true
		cfg.CompactionProgressCheck = true
		cfg.CompactionL0ObjectBound = 2000
		cfg.CompactionL1ObjectBound = 6000
	}
	lane := DesiredWorker{
		Name:         "worker-gate-upgrade-lane",
		ProfileName:  gateUpgradeLaneProfile,
		Region:       "ord",
		VolumeSizeGB: 10,
		Workload: workload.Config{
			LoadMode:                    "synthetic",
			WriteRate:                   50,
			Pattern:                     "constant",
			PayloadSize:                 1024,
			ReadRatio:                   0.1,
			Workers:                     2,
			InitialSize:                 "20MB",
			VerifyInterval:              "30m",
			VerifyType:                  "integrity",
			SnapshotInterval:            "24h",
			SyncInterval:                "1s",
			UpgradeLane:                 true,
			UpgradeWarmup:               upgradeGateDefaultWarmup.String(),
			UpgradePostSnapshotInterval: "1h",
			VolumeSizeGB:                10,
			MemoryMB:                    1024,
			CPUs:                        1,
		},
	}
	follow := DesiredWorker{
		Name:         "worker-gate-upgrade-follow",
		ProfileName:  gateUpgradeFollowProfile,
		Region:       "ord",
		VolumeSizeGB: 10,
		Workload: workload.Config{
			LoadMode:         "synthetic",
			WriteRate:        50,
			Pattern:          "constant",
			PayloadSize:      1024,
			ReadRatio:        0.1,
			Workers:          2,
			InitialSize:      "20MB",
			VerifyInterval:   "30m",
			VerifyType:       "integrity",
			SnapshotInterval: "15m",
			SyncInterval:     "1s",
			VolumeSizeGB:     10,
			MemoryMB:         1024,
			CPUs:             1,
		},
	}
	levels(&lane.Workload)
	levels(&follow.Workload)
	if strings.HasPrefix(strings.TrimSpace(source), upgradeGateBaselinePrefix+"-") {
		return []DesiredWorker{follow}
	}
	return []DesiredWorker{lane, follow}
}

func GateFleetForSource(source, gitSHA, litestreamSHA string) FleetSpec {
	source = strings.TrimSpace(source)
	workers := rcGateWorkers()
	if gateKindForSource(source) == model.RCGateKindUpgrade {
		workers = upgradeGateWorkers(source)
	}
	for i := range workers {
		workers[i].Source = source
		workers[i].GitSHA = gitSHA
		workers[i].LitestreamSHA = litestreamSHA
		workers[i].Name = workerNameForSource(source, workers[i].Name)
		workers[i].WorkerID = workers[i].Name
	}
	return FleetSpec{Workers: withMeteringWindowObserveProxy(workers)}
}

func rcGateWorkers() []DesiredWorker {
	return []DesiredWorker{
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
}

func parseRCGateKind(raw string) (model.RCGateKind, error) {
	switch kind := model.RCGateKind(strings.TrimSpace(raw)); kind {
	case "", model.RCGateKindRC:
		return model.RCGateKindRC, nil
	case model.RCGateKindUpgrade:
		return kind, nil
	default:
		return "", fmt.Errorf("%w: kind must be %s or %s", errRCGateInvalid, model.RCGateKindRC, model.RCGateKindUpgrade)
	}
}

func (m *Manager) RegisterRCGate(request RCGateRequest, now time.Time) (model.RCGate, bool, error) {
	kind, err := parseRCGateKind(request.Kind)
	if err != nil {
		return model.RCGate{}, false, err
	}
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
	durationText := strings.TrimSpace(request.Duration)
	if durationText == "" && kind == model.RCGateKindUpgrade {
		durationText = upgradeGateDefaultDuration.String()
	}
	duration, err := time.ParseDuration(durationText)
	if err != nil || duration <= 0 || duration > rcGateMaxDuration {
		return model.RCGate{}, false, fmt.Errorf("%w: duration must be a positive duration no longer than %s", errRCGateInvalid, rcGateMaxDuration)
	}
	var warmup time.Duration
	if kind == model.RCGateKindUpgrade {
		if request.Tail {
			return model.RCGate{}, false, fmt.Errorf("%w: upgrade gates do not support tail", errRCGateInvalid)
		}
		warmup = upgradeGateDefaultWarmup
		if text := strings.TrimSpace(request.UpgradeWarmup); text != "" {
			if warmup, err = time.ParseDuration(text); err != nil || warmup <= 0 {
				return model.RCGate{}, false, fmt.Errorf("%w: upgrade_warmup must be a positive duration", errRCGateInvalid)
			}
		}
		if warmup >= duration {
			return model.RCGate{}, false, fmt.Errorf("%w: upgrade_warmup %s must be shorter than duration %s", errRCGateInvalid, warmup, duration)
		}
	} else if strings.TrimSpace(request.UpgradeWarmup) != "" {
		return model.RCGate{}, false, fmt.Errorf("%w: upgrade_warmup is only valid for upgrade gates", errRCGateInvalid)
	}

	now = now.UTC()
	candidatePrefix, baselinePrefix := gateSourcePrefixes(kind)
	gate := model.RCGate{
		Kind:              kind,
		Repository:        repository,
		CandidateRef:      strings.TrimSpace(request.CandidateRef),
		CandidateSource:   gateSourceName(candidatePrefix, candidateSHA),
		CandidateSHA:      candidateSHA,
		BaselineSource:    gateSourceName(baselinePrefix, baselineSHA),
		BaselineSHA:       baselineSHA,
		Duration:          duration,
		UpgradeWarmup:     warmup,
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
		if existing != nil && existing.Kind == gate.Kind && existing.Repository == gate.Repository && existing.CandidateSHA == gate.CandidateSHA &&
			existing.BaselineSHA == gate.BaselineSHA && existing.Duration == gate.Duration && existing.UpgradeWarmup == gate.UpgradeWarmup && existing.Tail == gate.Tail {
			return *existing, false, nil
		}
		return model.RCGate{}, false, err
	}
	_ = m.db.RecordEvent("", "rc_gate_registered", fmt.Sprintf("Registered %s gate %s vs %s for %s", gate.Kind, gate.CandidateSource, gate.BaselineSource, duration), fmt.Sprintf("gate_id=%d tail=%t upgrade_warmup=%s", gate.ID, gate.Tail, gate.UpgradeWarmup))
	return gate, true, nil
}

func gateHasProfile(gate model.RCGate, profile string) bool {
	for _, source := range gate.Sources() {
		for _, desired := range GateFleetForSource(source, "", "").Workers {
			if desired.ProfileName == profile {
				return true
			}
		}
	}
	return false
}

// applyGateWorkload overrides the fleet defaults with the per-gate settings an
// operator chose at registration, so the attested profile config matches what
// the worker actually runs.
func applyGateWorkload(gate model.RCGate, spec FleetSpec) FleetSpec {
	for i := range spec.Workers {
		if spec.Workers[i].Workload.UpgradeLane && gate.UpgradeWarmup > 0 {
			spec.Workers[i].Workload.UpgradeWarmup = gate.UpgradeWarmup.String()
		}
	}
	return spec
}

func rcGateProfileWindow(gate model.RCGate, profile string) (bool, time.Time) {
	if !gateHasProfile(gate, profile) {
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
	spec := applyGateWorkload(*gate, GateFleetForSource(source, gitSHA, litestreamSHA))
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
		var latest *time.Time
		paired := true
		for _, source := range gate.Sources() {
			readyAt := gate.ReadyAtFor(source)
			if readyAt == nil {
				ready, err := m.rcGateSideReady(gate, source)
				if err != nil {
					return err
				}
				if ready {
					if err := m.db.MarkRCGateSideReady(gate.ID, source, now); err != nil {
						return fmt.Errorf("mark rc gate side ready: %w", err)
					}
					readyAt = &now
				}
			}
			if readyAt == nil {
				paired = false
				continue
			}
			if latest == nil || readyAt.After(*latest) {
				latest = readyAt
			}
		}
		switch {
		case paired && latest.Before(gate.ProvisionDeadline) && latest.Before(gate.ExpiresAt):
			if err := m.db.MarkRCGatePairedReady(gate.ID, *latest); err != nil {
				return fmt.Errorf("mark rc gate paired ready: %w", err)
			}
			gate.PairedReadyAt = latest
		case !now.Before(gate.ProvisionDeadline) || !now.Before(gate.ExpiresAt):
			summary := fmt.Sprintf("RC gate %s vs %s did not have both sides ready before its launch deadline or expiry; archiving evidence and destroying all gate workers.", gate.CandidateSource, gate.BaselineSource)
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

func (m *Manager) rcGateSideReady(gate model.RCGate, source string) (bool, error) {
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
		if err := m.DestroyWorker(ctx, worker.ID); err != nil && !errors.Is(err, errWorkerEvidenceHeld) {
			destroyErrors = append(destroyErrors, fmt.Errorf("%s: %w", worker.ID, err))
		}
	}
	return errors.Join(destroyErrors...)
}

type rcGateResults struct {
	Verdict     string                 `json:"verdict"`
	Scenarios   []RCGateScenarioResult `json:"scenarios"`
	HeldWorkers []RCGateHeldWorker     `json:"held_workers,omitempty"`
	ClosedAt    time.Time              `json:"closed_at"`
}

func buildRCGateLedgerEntry(db *model.DB, gate model.RCGate, now time.Time) (RCGateLedgerEntry, error) {
	entry := RCGateLedgerEntry{
		RCGate:   gate,
		Duration: gate.Duration.String(),
	}
	if gate.UpgradeWarmup > 0 {
		entry.UpgradeWarmup = gate.UpgradeWarmup.String()
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
	entry.HeldWorkers = results.HeldWorkers
	return entry, nil
}

func buildRCGateResults(db *model.DB, gate model.RCGate, end time.Time) (rcGateResults, error) {
	results := rcGateResults{ClosedAt: end.UTC()}
	failing, incomplete := false, false
	for _, scenario := range gateScenariosFor(gate.Kind) {
		candidateFilter, baselineFilter := scenario.candidate, scenario.baseline
		if candidateFilter.role == "" {
			candidateFilter.role = gateRoleCandidate
		}
		if baselineFilter.role == "" {
			baselineFilter.role = gateRoleBaseline
		}
		candidate, candidateModes, err := rcGateSide(db, gate, scenario, candidateFilter, end)
		if err != nil {
			return rcGateResults{}, err
		}
		baseline, baselineModes, err := rcGateSide(db, gate, scenario, baselineFilter, end)
		if err != nil {
			return rcGateResults{}, err
		}
		result := RCGateScenarioResult{
			Scenario:  scenario.name,
			Profile:   scenario.profile,
			Candidate: candidate,
			Baseline:  baseline,
			Modes:     rcGateModeResults(rcGateRequiredModes(scenario, candidate, baseline), candidate, baseline, candidateModes, baselineModes),
			Verdict:   rcGateScenarioVerdict(candidate.Status, baseline.Status),
		}
		if result.Verdict != rcGateVerdictIncomplete && len(result.Modes) > 0 {
			result.Verdict = rcGateModesVerdict(result.Modes)
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
	held, err := rcGateHeldWorkers(db, gate)
	if err != nil {
		return rcGateResults{}, err
	}
	results.HeldWorkers = held
	return results, nil
}

func rcGateSide(db *model.DB, gate model.RCGate, scenario gateScenario, filter gateSideFilter, end time.Time) (RCGateSideResult, map[string]*RCGateSideResult, error) {
	source := filter.sourceFor(gate)
	profile := firstNonEmpty(filter.profile, scenario.profile)
	side := RCGateSideResult{Source: source, Status: rcGateSideMissing}
	modes := make(map[string]*RCGateSideResult)
	pinnedImage, pinnedGit := gate.PinnedBuildFor(source)
	pinnedLitestream, _ := gate.LitestreamSHAFor(source)
	if pinnedImage == "" {
		return side, modes, nil
	}
	deployment, err := db.GetDeploymentByVersion(source, pinnedGit, pinnedLitestream)
	if err != nil || deployment == nil || deployment.ImageRef != pinnedImage {
		return side, modes, nil
	}
	workers, err := db.ListWorkersFiltered("", source)
	if err != nil {
		return side, modes, err
	}
	var worker *model.Worker
	for i := range workers {
		if workers[i].ProfileName == profile && rcGateOwnsWorker(gate, workers[i]) {
			worker = &workers[i]
			break
		}
	}
	if worker == nil {
		return side, modes, nil
	}
	side.WorkerID = worker.ID
	verifications, err := db.ListVerificationsExcludingMeteringRuns(worker.ID, rcGateVerificationHistory)
	if err != nil {
		return side, modes, err
	}
	for _, verification := range verifications {
		if verification.StartedAt.Before(gate.StartedAt) || verification.StartedAt.After(end) ||
			verification.Run.Scenario != scenario.identity || !verification.MatchesDeployment(*worker, *deployment) || !filter.accepts(gate, verification) {
			continue
		}
		mode := strings.TrimSpace(verification.RestoreMode)
		if mode == "" {
			mode = "txid"
		}
		if strings.TrimSpace(verification.RestoreMode) != "" && verification.RestoreModeFallback != "rotation_disabled" {
			side.RotationEnabled = true
		}
		if modes[mode] == nil {
			modes[mode] = &RCGateSideResult{Source: source, WorkerID: worker.ID}
		}
		switch {
		case verification.Succeeded():
			side.Passed++
			modes[mode].Passed++
		case verification.Failed():
			side.Failed++
			modes[mode].Failed++
		}
	}
	side.Status = rcGateSideStatus(side)
	for _, mode := range modes {
		mode.Status = rcGateSideStatus(*mode)
	}
	return side, modes, nil
}

func rcGateSideStatus(side RCGateSideResult) string {
	switch {
	case side.Failed > 0:
		return rcGateSideFailing
	case side.Passed > 0:
		return rcGateSidePassing
	default:
		return rcGateSideNoCoverage
	}
}

func rcGateRequiredModes(scenario gateScenario, candidate, baseline RCGateSideResult) []string {
	if len(scenario.requiredModes) > 0 {
		return scenario.requiredModes
	}
	if scenario.txidOnly || (!candidate.RotationEnabled && !baseline.RotationEnabled) {
		return []string{"txid"}
	}
	return gateRotatingRestoreModes
}

func rcGateModeResults(required []string, candidate, baseline RCGateSideResult, candidateModes, baselineModes map[string]*RCGateSideResult) []RCGateModeResult {
	names := slices.Clone(required)
	for _, modes := range []map[string]*RCGateSideResult{candidateModes, baselineModes} {
		for name := range modes {
			if !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	results := make([]RCGateModeResult, 0, len(names))
	for _, name := range names {
		side := func(base RCGateSideResult, modes map[string]*RCGateSideResult) RCGateSideResult {
			if mode := modes[name]; mode != nil {
				return *mode
			}
			status := rcGateSideNoCoverage
			if base.Status == rcGateSideMissing {
				status = rcGateSideMissing
			}
			return RCGateSideResult{Source: base.Source, WorkerID: base.WorkerID, Status: status}
		}
		result := RCGateModeResult{Mode: name, Candidate: side(candidate, candidateModes), Baseline: side(baseline, baselineModes)}
		result.Verdict = rcGateScenarioVerdict(result.Candidate.Status, result.Baseline.Status)
		results = append(results, result)
	}
	return results
}

func rcGateModesVerdict(modes []RCGateModeResult) string {
	for _, verdict := range []string{rcGateVerdictRegression, rcGateVerdictBothFailing, rcGateVerdictIncomplete} {
		for _, mode := range modes {
			if mode.Verdict == verdict {
				return verdict
			}
		}
	}
	return rcGateVerdictPass
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
