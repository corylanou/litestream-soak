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
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/workload"
)

const (
	gateStressProfile          = "gate-stress"
	gateManyDB500Profile       = "gate-manydb-500"
	gateLargeDBProfile         = "gate-large-db"
	gateRemoteProfile          = "gate-remote"
	gateUpgradeLaneProfile     = "gate-upgrade-lane"
	gateUpgradeLaneNoL9Profile = "gate-upgrade-lane-nol9"
	gateUpgradeFollowProfile   = "gate-upgrade-follow"

	// upgradeLaneVariantNoL9 empties the replica's L9 level at the switch so
	// the candidate build starts on a legacy L0-L3-only replica.
	upgradeLaneVariantNoL9 = "nol9"

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
	rcGateWorkerEventHistory  = 2000

	gateRoleCandidate = "candidate"
	gateRoleBaseline  = "baseline"
	gateModeFollow    = "follow"
	upgradePhasePre   = "pre-upgrade"
	upgradePhasePost  = "post-upgrade"

	rcGateReasonDiskExhausted       = "disk_exhausted"
	rcGateReasonWorkerCrashLoop     = "worker_crash_loop"
	rcGateReasonWorkerDegraded      = "worker_degraded"
	rcGateReasonHarnessInconclusive = "harness_inconclusive"

	rcGateCrashLoopEvents        = 2
	rcGateDiskExhaustedUsedPct   = 98.0
	rcGateFlyNotRestartingMarker = "not restarting"
	rcGateStaleWorkerMessage     = "worker missed heartbeat deadline"
	rcGateStaleGrace             = 30 * time.Minute
	rcGateStaleBurstWorkers      = 3
	rcGateStaleBurstWindow       = 90 * time.Second
	rcGatePlannedRestartWindow   = 10 * time.Minute
)

var (
	rcGateDiskFullEventTypes = []string{
		"platform_disk_full",
		reporting.WorkerEventDiskFullNoProgress,
		reporting.WorkerEventDiskFullRecoveryFailed,
	}
	rcGateCrashEventTypes = []string{"platform_restart", "platform_oom", "platform_killed", "worker_failed"}
	// Planned restarts the worker reports as worker_failed on images that predate
	// the phaseEnded check in internal/worker/runner.go.
	rcGatePlannedLifecycleMarkers = []string{"stress scenario phase ended", "upgrade warm-up ended"}
	// Fatal runner errors raised before Litestream is launched (internal/worker/runner.go).
	rcGatePreStartFailurePrefixes = []string{"populate:", "prepare disk-full recovery reserve:", "write litestream config:", "start litestream:"}
)

var (
	// gateSourceRe accepts the legacy <prefix>-<sha8> names and the per-gate
	// <prefix>-<sha8>-<suffix> names, so two gates for the same commits can be
	// open at once (a rerun of one profile while the original gate is still
	// running) without colliding on sources.
	gateSourceRe           = regexp.MustCompile(`^(rc|rcbase|up|upbase)(-nol9)?-[0-9a-f]{8}(-[0-9a-z]{1,8})?$`)
	gateSourceSuffixRe     = regexp.MustCompile(`^[0-9a-z]{1,8}$`)
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
	LaneVariant   string `json:"lane_variant,omitempty"`
	Tail          bool   `json:"tail"`
	// Profiles limits the gate to these fleet profiles; empty runs them all.
	Profiles []string `json:"profiles,omitempty"`
	// SourceSuffix is appended to both source names (rc-<sha8>-<suffix>) so
	// the gate's sources are distinct from any other gate's for the same
	// commits; empty keeps the legacy rc-<sha8> names.
	SourceSuffix string `json:"source_suffix,omitempty"`
}

type RCGateSideResult struct {
	Source          string `json:"source"`
	WorkerID        string `json:"worker_id,omitempty"`
	Status          string `json:"status"`
	Passed          int    `json:"passed"`
	Failed          int    `json:"failed"`
	RotationEnabled bool   `json:"rotation_enabled,omitempty"`
	Reason          string `json:"reason,omitempty"`
	ReasonDetail    string `json:"reason_detail,omitempty"`
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

// The no-L9 variant runs a single lane worker whose replica loses its L9
// level at the switch, plus the baseline follow worker, so its scenarios are
// the lane trio only: current restores, historical restores, and follow.
var upgradeGateNoL9Scenarios = []gateScenario{
	{
		name:      "upgrade-lane-nol9",
		profile:   gateUpgradeLaneNoL9Profile,
		candidate: gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePost},
		baseline:  gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePre},
	},
	{
		name:          "upgrade-lane-nol9-historical",
		profile:       gateUpgradeLaneNoL9Profile,
		requiredModes: []string{"txid"},
		candidate:     gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePost, checkType: gateCheckHistorical},
		baseline:      gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePre, mode: "txid"},
	},
	{
		name:          "upgrade-lane-nol9-follow",
		profile:       gateUpgradeLaneNoL9Profile,
		requiredModes: []string{gateModeFollow},
		candidate:     gateSideFilter{role: gateRoleCandidate, phase: upgradePhasePost, checkType: gateCheckFollow},
		baseline:      gateSideFilter{role: gateRoleBaseline, profile: gateUpgradeFollowProfile, checkType: gateCheckFollow},
	},
}

func gateScenariosFor(gate model.RCGate) []gateScenario {
	switch {
	case gate.Kind == model.RCGateKindUpgrade && gate.LaneVariant == upgradeLaneVariantNoL9:
		return upgradeGateNoL9Scenarios
	case gate.Kind == model.RCGateKindUpgrade:
		return upgradeGateScenarios
	default:
		return rcGateScenarios
	}
}

func parseLaneVariant(raw string, kind model.RCGateKind) (string, error) {
	variant := strings.TrimSpace(raw)
	switch {
	case variant == "":
		return "", nil
	case kind != model.RCGateKindUpgrade:
		return "", fmt.Errorf("%w: lane_variant is only valid for upgrade gates", errRCGateInvalid)
	case variant == upgradeLaneVariantNoL9:
		return variant, nil
	default:
		return "", fmt.Errorf("%w: lane_variant must be empty or %s", errRCGateInvalid, upgradeLaneVariantNoL9)
	}
}

// gateLaneVariantForSource reads the variant segment out of a gate source
// name: up-nol9-<cand8> is the no-L9 lane, up-<cand8> the standard lane.
func gateLaneVariantForSource(source string) string {
	parts := strings.Split(strings.TrimSpace(source), "-")
	if len(parts) == 3 && parts[1] == upgradeLaneVariantNoL9 {
		return upgradeLaneVariantNoL9
	}
	return ""
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

func gateSourceName(prefix, sha, suffix string) string {
	name := prefix + "-" + sha[:8]
	if suffix != "" {
		name += "-" + suffix
	}
	return name
}

// gateFleetForSource is GateFleetForSource narrowed to the profiles the gate
// selected; every per-gate fleet decision (resources, provisioning window,
// readiness) goes through it so an unselected profile is never provisioned
// or waited for.
func gateFleetForSource(gate model.RCGate, source, gitSHA, litestreamSHA string) FleetSpec {
	spec := GateFleetForSource(source, gitSHA, litestreamSHA)
	selected := spec.Workers[:0]
	for _, desired := range spec.Workers {
		if gate.SelectsProfile(desired.ProfileName) {
			selected = append(selected, desired)
		}
	}
	spec.Workers = selected
	return spec
}

// normalizeGateProfiles validates a requested profile subset against the
// kind's fleet and returns it in fleet order; nil means the whole fleet.
func normalizeGateProfiles(kind model.RCGateKind, laneVariant string, requested []string) ([]string, error) {
	wanted := make(map[string]bool, len(requested))
	for _, profile := range requested {
		if profile = strings.TrimSpace(profile); profile != "" {
			wanted[profile] = true
		}
	}
	if len(wanted) == 0 {
		return nil, nil
	}
	candidatePrefix, baselinePrefix := gateSourcePrefixes(kind, laneVariant)
	var selected []string
	for _, source := range []string{candidatePrefix + "-00000000", baselinePrefix + "-00000000"} {
		for _, desired := range GateFleetForSource(source, "", "").Workers {
			if wanted[desired.ProfileName] && !slices.Contains(selected, desired.ProfileName) {
				selected = append(selected, desired.ProfileName)
			}
		}
	}
	for profile := range wanted {
		if !slices.Contains(selected, profile) {
			return nil, fmt.Errorf("%w: profile %q is not part of the %s gate fleet", errRCGateInvalid, profile, kind)
		}
	}
	return selected, nil
}

func gateKindForSource(source string) model.RCGateKind {
	prefix, _, _ := strings.Cut(strings.TrimSpace(source), "-")
	if prefix == upgradeGateCandidatePrefix || prefix == upgradeGateBaselinePrefix {
		return model.RCGateKindUpgrade
	}
	return model.RCGateKindRC
}

func gateSourcePrefixes(kind model.RCGateKind, laneVariant string) (candidate, baseline string) {
	switch {
	case kind == model.RCGateKindUpgrade && laneVariant != "":
		return upgradeGateCandidatePrefix + "-" + laneVariant, upgradeGateBaselinePrefix + "-" + laneVariant
	case kind == model.RCGateKindUpgrade:
		return upgradeGateCandidatePrefix, upgradeGateBaselinePrefix
	default:
		return rcGateCandidatePrefix, rcGateBaselinePrefix
	}
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
	if gateLaneVariantForSource(source) == upgradeLaneVariantNoL9 {
		lane.Name = "worker-gate-upgrade-lane-nol9"
		lane.ProfileName = gateUpgradeLaneNoL9Profile
		lane.Workload.UpgradeDeleteL9AtSwitch = true
		return []DesiredWorker{lane}
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
	laneVariant, err := parseLaneVariant(request.LaneVariant, kind)
	if err != nil {
		return model.RCGate{}, false, err
	}
	profiles, err := normalizeGateProfiles(kind, laneVariant, request.Profiles)
	if err != nil {
		return model.RCGate{}, false, err
	}
	if request.Tail && profiles != nil && !slices.Contains(profiles, gateStressProfile) {
		return model.RCGate{}, false, fmt.Errorf("%w: tail requires the %s profile", errRCGateInvalid, gateStressProfile)
	}
	suffix := strings.TrimSpace(request.SourceSuffix)
	if suffix != "" && !gateSourceSuffixRe.MatchString(suffix) {
		return model.RCGate{}, false, fmt.Errorf("%w: source_suffix must be 1 to 8 lowercase letters or digits", errRCGateInvalid)
	}

	now = now.UTC()
	candidatePrefix, baselinePrefix := gateSourcePrefixes(kind, laneVariant)
	gate := model.RCGate{
		Kind:              kind,
		LaneVariant:       laneVariant,
		Repository:        repository,
		CandidateRef:      strings.TrimSpace(request.CandidateRef),
		CandidateSource:   gateSourceName(candidatePrefix, candidateSHA, suffix),
		CandidateSHA:      candidateSHA,
		BaselineSource:    gateSourceName(baselinePrefix, baselineSHA, suffix),
		BaselineSHA:       baselineSHA,
		Duration:          duration,
		UpgradeWarmup:     warmup,
		Tail:              request.Tail,
		Profiles:          profiles,
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
		for _, desired := range gateFleetForSource(gate, source, "", litestreamSHA).Workers {
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
		if existing != nil && existing.Kind == gate.Kind && existing.LaneVariant == gate.LaneVariant && existing.Repository == gate.Repository && existing.CandidateSHA == gate.CandidateSHA &&
			existing.BaselineSHA == gate.BaselineSHA && existing.Duration == gate.Duration && existing.UpgradeWarmup == gate.UpgradeWarmup && existing.Tail == gate.Tail &&
			slices.Equal(existing.Profiles, gate.Profiles) {
			return *existing, false, nil
		}
		return model.RCGate{}, false, err
	}
	_ = m.db.RecordEvent("", "rc_gate_registered", fmt.Sprintf("Registered %s gate %s vs %s for %s", gate.Kind, gate.CandidateSource, gate.BaselineSource, duration), fmt.Sprintf("gate_id=%d tail=%t upgrade_warmup=%s lane_variant=%s profiles=%s", gate.ID, gate.Tail, gate.UpgradeWarmup, gate.LaneVariant, firstNonEmpty(strings.Join(gate.Profiles, ","), "all")))
	return gate, true, nil
}

func gateHasProfile(gate model.RCGate, profile string) bool {
	for _, source := range gate.Sources() {
		for _, desired := range gateFleetForSource(gate, source, "", "").Workers {
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
	spec := applyGateWorkload(*gate, gateFleetForSource(*gate, source, gitSHA, litestreamSHA))
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
	for _, desired := range gateFleetForSource(gate, source, pinnedGit, pinnedLitestream).Workers {
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
	for _, scenario := range gateScenariosFor(gate) {
		if !gate.SelectsProfile(scenario.profile) {
			continue
		}
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
	observed := 0
	for _, verification := range verifications {
		if verification.StartedAt.Before(gate.StartedAt) || verification.StartedAt.After(end) || !verification.MatchesDeployment(*worker, *deployment) {
			continue
		}
		observed++
		if verification.Run.Scenario != scenario.identity || !filter.accepts(gate, verification) {
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
	side.Reason, side.ReasonDetail, err = rcGateSideFault(db, gate, *worker, observed, end)
	if err != nil {
		return side, modes, err
	}
	side.Status = rcGateSideStatus(side)
	for _, mode := range modes {
		mode.Reason, mode.ReasonDetail = side.Reason, side.ReasonDetail
		mode.Status = rcGateSideStatus(*mode)
	}
	return side, modes, nil
}

func rcGateSideStatus(side RCGateSideResult) string {
	switch {
	case side.Failed > 0 || rcGateHardFault(side.Reason):
		return rcGateSideFailing
	case side.Passed > 0:
		return rcGateSidePassing
	default:
		return rcGateSideNoCoverage
	}
}

func rcGateHardFault(reason string) bool {
	return reason != "" && reason != rcGateReasonHarnessInconclusive
}

func rcGateSideFault(db *model.DB, gate model.RCGate, worker model.Worker, observedVerifications int, end time.Time) (string, string, error) {
	events, err := db.ListWorkerEvents(worker.ID, rcGateWorkerEventHistory)
	if err != nil {
		return "", "", fmt.Errorf("list %s events: %w", worker.ID, err)
	}
	windowStart := gate.StartedAt.Truncate(time.Second)
	inWindow := events[:0]
	for _, event := range events {
		if event.CreatedAt.Before(windowStart) || event.CreatedAt.After(end) {
			continue
		}
		inWindow = append(inWindow, event)
	}
	reason, detail, err := rcGateWorkerFault(db, worker, inWindow, end)
	if err != nil {
		return "", "", err
	}
	if reason == "" {
		return "", "", nil
	}
	if observedVerifications == 0 {
		if evidence, ok := rcGatePreStartEvidence(worker, inWindow); ok {
			return rcGateReasonHarnessInconclusive, reason + " before litestream started (" + evidence + "): " + detail, nil
		}
	}
	return reason, detail, nil
}

// rcGatePreStartEvidence reports affirmative evidence that Litestream never ran
// on the worker: the control plane never stored a healthy Litestream snapshot
// (last_runtime_at stays NULL), and either the last persisted heartbeat sample
// still shows Litestream PID 0 with no IPC snapshot, uptime, or TXID while the
// harness process itself was alive, or the worker reported a fatal error from a
// pre-start step. Without that evidence a fault is a hard failure.
func rcGatePreStartEvidence(worker model.Worker, events []model.Event) (string, bool) {
	if worker.LastRuntimeAt != nil {
		return "", false
	}
	for _, event := range events {
		if event.EventType != "worker_failed" {
			continue
		}
		for _, prefix := range rcGatePreStartFailurePrefixes {
			if strings.HasPrefix(strings.TrimSpace(event.Message), prefix) {
				return "worker_failed event " + strings.TrimSuffix(prefix, ":"), true
			}
		}
	}
	runtime := parseRuntimeJSON(worker.LastRuntimeJSON, time.Time{})
	if runtime == nil || runtime.LitestreamSnapshotHealthy || runtime.LitestreamProcess.PID != 0 ||
		runtime.LitestreamUptimeSeconds > 0 || runtime.DBTXID > 0 || runtime.ReplicatedTXID > 0 {
		return "", false
	}
	harnessAlive := runtime.WorkerProcess.PID > 0 || runtime.UptimeSeconds > 0 || strings.TrimSpace(runtime.LitestreamSnapshotError) != ""
	if !harnessAlive {
		return "", false
	}
	evidence := "last heartbeat reported litestream pid 0 with no ipc snapshot"
	if !runtime.SnapshotCollectedAt.IsZero() {
		evidence += " at " + runtime.SnapshotCollectedAt.UTC().Format(time.RFC3339)
	}
	return evidence, true
}

func rcGateWorkerFault(db *model.DB, worker model.Worker, events []model.Event, end time.Time) (string, string, error) {
	degraded := worker.Status == model.WorkerDegraded || worker.Status == model.WorkerFailed
	planned := rcGatePlannedRestartTimes(worker, events)
	var (
		diskDetail, crashDetail, staleDetail string
		crashes                              int
		terminalCrash, staleEventSeen        bool
	)
	for _, event := range events {
		switch {
		case slices.Contains(rcGateDiskFullEventTypes, event.EventType):
			diskDetail = firstNonEmpty(event.Message, event.EventType)
		case slices.Contains(rcGateCrashEventTypes, event.EventType):
			if rcGatePlannedLifecycleEvent(event) || (event.EventType == "platform_restart" && rcGateNearPlannedRestart(event.CreatedAt, planned)) {
				continue
			}
			crashes += max(1, event.Occurrences)
			crashDetail = firstNonEmpty(event.Message, event.EventType)
			if strings.Contains(strings.ToLower(event.Message), rcGateFlyNotRestartingMarker) {
				terminalCrash = true
			}
		case event.EventType == "worker_stale":
			staleEventSeen = true
			detail, fault, err := rcGateStaleFault(db, worker, event, end)
			if err != nil {
				return "", "", err
			}
			if fault {
				staleDetail = detail
			}
		}
	}
	if degraded && worker.ErrorMessage == rcGateStaleWorkerMessage {
		// A stale-only degradation is judged by its worker_stale event; without
		// one in the window it counts only while the worker is still silent.
		degraded = !staleEventSeen && rcGateCurrentlyStale(worker, end)
	}
	if diskDetail == "" && degraded && reporting.IsDiskCapacityMessage(worker.ErrorMessage) {
		diskDetail = worker.ErrorMessage
	}
	crashLoop := crashes >= rcGateCrashLoopEvents || terminalCrash
	if diskDetail == "" && (degraded || crashLoop || staleDetail != "") {
		if runtime := parseRuntimeJSON(worker.LastRuntimeJSON, time.Time{}); runtime != nil && runtime.DataDiskUsedPercent >= rcGateDiskExhaustedUsedPct {
			diskDetail = fmt.Sprintf("data disk %.1f%% used", runtime.DataDiskUsedPercent)
		}
	}
	switch {
	case diskDetail != "":
		return rcGateReasonDiskExhausted, diskDetail, nil
	case crashLoop:
		return rcGateReasonWorkerCrashLoop, fmt.Sprintf("%d crash events in the gate window; last: %s", crashes, crashDetail), nil
	case degraded:
		return rcGateReasonWorkerDegraded, firstNonEmpty(worker.ErrorMessage, string(worker.Status)), nil
	case staleDetail != "":
		return rcGateReasonWorkerDegraded, staleDetail, nil
	default:
		return "", "", nil
	}
}

// rcGateStaleFault decides whether one worker_stale event is a worker fault. A
// stale burst that hits several workers in the same minute is a control-plane
// restart, and a worker whose heartbeats resumed within the grace period only
// missed a deadline, so neither counts. Staleness counts when the worker never
// heartbeated again or stayed silent past the grace period.
func rcGateStaleFault(db *model.DB, worker model.Worker, event model.Event, end time.Time) (string, bool, error) {
	burst, err := db.CountWorkersWithEventBetween("worker_stale", event.CreatedAt.Add(-rcGateStaleBurstWindow), event.CreatedAt.Add(rcGateStaleBurstWindow))
	if err != nil {
		return "", false, fmt.Errorf("count stale workers: %w", err)
	}
	if burst >= rcGateStaleBurstWorkers {
		return "", false, nil
	}
	resumedAt, resumed, err := db.FirstHeartbeatAfter(worker.Source, worker.ID, event.CreatedAt)
	if err != nil {
		return "", false, fmt.Errorf("find heartbeat after stale: %w", err)
	}
	if !resumed && worker.LastHeartbeatAt != nil && worker.LastHeartbeatAt.After(event.CreatedAt) {
		resumedAt, resumed = worker.LastHeartbeatAt.UTC(), true
	}
	switch {
	case !resumed && end.Sub(event.CreatedAt) < rcGateStaleGrace:
		return "", false, nil
	case !resumed:
		return fmt.Sprintf("%s; no heartbeat since %s", firstNonEmpty(event.Message, event.EventType), event.CreatedAt.UTC().Format(time.RFC3339)), true, nil
	case resumedAt.Sub(event.CreatedAt) > rcGateStaleGrace:
		return fmt.Sprintf("%s; heartbeats resumed after %s", firstNonEmpty(event.Message, event.EventType), resumedAt.Sub(event.CreatedAt).Round(time.Minute)), true, nil
	default:
		return "", false, nil
	}
}

func rcGateCurrentlyStale(worker model.Worker, end time.Time) bool {
	return worker.LastHeartbeatAt == nil || end.Sub(*worker.LastHeartbeatAt) >= rcGateStaleGrace
}

func rcGatePlannedLifecycleEvent(event model.Event) bool {
	if event.EventType != "worker_failed" {
		return false
	}
	message := strings.ToLower(event.Message)
	for _, marker := range rcGatePlannedLifecycleMarkers {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

// rcGatePlannedRestartTimes lists the moments the worker restarted on purpose:
// every planned lifecycle event it reported, plus each wall-clock stress phase
// boundary inside the window for the gate-stress worker, whose Litestream and
// harness restart between scenario phases.
func rcGatePlannedRestartTimes(worker model.Worker, events []model.Event) []time.Time {
	var planned []time.Time
	var earliest, latest time.Time
	for _, event := range events {
		if rcGatePlannedLifecycleEvent(event) {
			planned = append(planned, event.CreatedAt)
		}
		if earliest.IsZero() || event.CreatedAt.Before(earliest) {
			earliest = event.CreatedAt
		}
		if event.CreatedAt.After(latest) {
			latest = event.CreatedAt
		}
	}
	if worker.ProfileName != gateStressProfile || earliest.IsZero() {
		return planned
	}
	phase, err := time.ParseDuration(rcGateStressPhase)
	if err != nil || phase <= 0 {
		return planned
	}
	for boundary := earliest.UTC().Truncate(phase); !boundary.After(latest.Add(phase)); boundary = boundary.Add(phase) {
		planned = append(planned, boundary)
	}
	return planned
}

func rcGateNearPlannedRestart(at time.Time, planned []time.Time) bool {
	for _, moment := range planned {
		if delta := at.Sub(moment); delta >= -rcGatePlannedRestartWindow && delta <= rcGatePlannedRestartWindow {
			return true
		}
	}
	return false
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
			result := RCGateSideResult{Source: base.Source, WorkerID: base.WorkerID, Reason: base.Reason, ReasonDetail: base.ReasonDetail}
			result.Status = rcGateSideStatus(result)
			if base.Status == rcGateSideMissing {
				result.Status = rcGateSideMissing
			}
			return result
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
