package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const (
	UpgradePhasePre  = "pre-upgrade"
	UpgradePhasePost = "post-upgrade"

	defaultLitestreamBaseBinary  = "/opt/soak/base/litestream-base"
	defaultLitestreamBaseSHAFile = "/opt/soak/base/litestream-base.sha"
	upgradeLaneStateFile         = "upgrade-lane.json"
)

var errUpgradeWarmupEnded = errors.New("upgrade warm-up ended")

// upgradeLaneState is persisted on the worker volume so a machine restart
// after the switch never runs the base build against candidate-written state,
// and so the warm-up is measured from the lane's first start.
type upgradeLaneState struct {
	Phase           string     `json:"phase"`
	StartedAt       time.Time  `json:"started_at"`
	SwitchedAt      *time.Time `json:"switched_at,omitempty"`
	BaseSHA         string     `json:"base_litestream_sha"`
	CandidateSHA    string     `json:"candidate_litestream_sha"`
	PreUpgradeTXID  uint64     `json:"pre_upgrade_txid,omitempty"`
	PreRestoreModes []string   `json:"pre_restore_modes,omitempty"`
	SwitchBlockedAt *time.Time `json:"switch_blocked_at,omitempty"`
	// SwitchBlockedReason and BlockedEventDeliveredAt keep the blocked event
	// resendable: the control plane scores the block from that event, so it is
	// retried, across restarts, until one delivery is on record.
	SwitchBlockedReason     string              `json:"switch_blocked_reason,omitempty"`
	BlockedEventDeliveredAt *time.Time          `json:"blocked_event_delivered_at,omitempty"`
	L9Purge                 *replicaL9Purge     `json:"l9_purge,omitempty"`
	PreservedChain          *preservedChain     `json:"preserved_chain,omitempty"`
	BaseProofFailure        *baseProofFailure   `json:"base_proof_failure,omitempty"`
	LaneFault               *laneFault          `json:"lane_fault,omitempty"`
	FirstCandidateL9        *firstL9Observation `json:"first_candidate_l9,omitempty"`
}

// firstL9Observation records when the candidate build's first L9 snapshot
// was seen on a replica whose L9 level was emptied at the switch.
type firstL9Observation struct {
	ObservedAt time.Time `json:"observed_at"`
	MaxTXID    string    `json:"max_txid"`
	Objects    int       `json:"objects"`
}

func upgradeLaneStatePath(dataDir string) string {
	return filepath.Join(dataDir, upgradeLaneStateFile)
}

func readUpgradeLaneState(dataDir string) (upgradeLaneState, error) {
	var state upgradeLaneState
	body, err := os.ReadFile(upgradeLaneStatePath(dataDir))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("read upgrade lane state: %w", err)
	}
	if err := json.Unmarshal(body, &state); err != nil {
		return state, fmt.Errorf("decode upgrade lane state: %w", err)
	}
	return state, nil
}

func writeUpgradeLaneState(dataDir string, state upgradeLaneState) error {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	body, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(upgradeLaneStatePath(dataDir), append(body, '\n')); err != nil {
		return fmt.Errorf("upgrade lane state: %w", err)
	}
	return nil
}

// upgradeLaneStateMu serializes every read-modify-write of the lane state:
// the verification loop, the switch decision and the event delivery all
// update the same file from different goroutines.
var upgradeLaneStateMu sync.Mutex

var errUpgradeSwitchBlocked = errors.New("upgrade lane switch is blocked")

// updateUpgradeLaneState applies change to the persisted state under the lock
// and writes it back when change reports a difference. A recorded block is
// sticky: no update can clear or move it, undo its delivery record, or leave
// the pre-upgrade phase.
func updateUpgradeLaneState(dataDir string, change func(*upgradeLaneState) bool) (upgradeLaneState, error) {
	upgradeLaneStateMu.Lock()
	defer upgradeLaneStateMu.Unlock()
	prev, err := readUpgradeLaneState(dataDir)
	if err != nil {
		return prev, err
	}
	next := prev
	next.PreRestoreModes = slices.Clone(prev.PreRestoreModes)
	if !change(&next) {
		return prev, nil
	}
	if prev.SwitchBlockedAt != nil {
		if next.Phase != prev.Phase {
			return prev, fmt.Errorf("%w since %s", errUpgradeSwitchBlocked, prev.SwitchBlockedAt.Format(time.RFC3339))
		}
		next.SwitchBlockedAt, next.SwitchBlockedReason = prev.SwitchBlockedAt, prev.SwitchBlockedReason
		if prev.BlockedEventDeliveredAt != nil {
			next.BlockedEventDeliveredAt = prev.BlockedEventDeliveredAt
		}
	}
	if err := writeUpgradeLaneState(dataDir, next); err != nil {
		return prev, err
	}
	return next, nil
}

// writeFileDurable writes and fsyncs a file so the phase marker survives a
// crash right after the switch; the caller renames it into place and syncs
// the directory.
func writeFileDurable(path string, body []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// The switch waits for the pre-upgrade phase to hold the baseline the gate
// scores against: a passing restore in every rotating mode and a saved txid.
// The warm-up is the earliest the switch can happen; past it the lane waits
// until upgradeEvidenceGrace after the warm-up's end, measured from the
// persisted start so a restart never extends it. A lane that still has no
// baseline then stays on the base build for good: it records the block,
// reports it once as upgradeSwitchBlockedEvent, and keeps verifying, so
// the base build's failures stay pre-upgrade failures and nothing is
// attributed to a candidate that never ran.
var (
	upgradeEvidencePoll           = 5 * time.Second
	upgradeEvidenceGrace          = func(warmup time.Duration) time.Duration { return max(warmup, 30*time.Minute) }
	upgradeBlockedEventBackoff    = 5 * time.Second
	upgradeBlockedEventMaxBackoff = 5 * time.Minute
)

const upgradeSwitchBlockedEvent = "upgrade_switch_blocked"

func recordUpgradePreEvidence(dataDir string, txid uint64, mode string) error {
	_, err := updateUpgradeLaneState(dataDir, func(state *upgradeLaneState) bool {
		if state.Phase != UpgradePhasePre {
			return false
		}
		changed := false
		if txid > state.PreUpgradeTXID {
			state.PreUpgradeTXID, changed = txid, true
		}
		if slices.Contains(restoreModeRotation, mode) && !slices.Contains(state.PreRestoreModes, mode) {
			state.PreRestoreModes, changed = append(state.PreRestoreModes, mode), true
		}
		return changed
	})
	return err
}

func (s upgradeLaneState) missingPreRestoreModes() []string {
	var missing []string
	for _, mode := range restoreModeRotation {
		if !slices.Contains(s.PreRestoreModes, mode) {
			missing = append(missing, mode)
		}
	}
	return missing
}

func (s upgradeLaneState) preEvidenceComplete() bool {
	return s.PreUpgradeTXID != 0 && len(s.missingPreRestoreModes()) == 0
}

// nextPreUpgradeRestoreMode is the first rotating mode the pre-upgrade phase
// has no passing restore in yet, so the baseline fills by cycle, not by
// where a cycle happens to land on the wall clock.
func nextPreUpgradeRestoreMode(dataDir string) string {
	state, err := readUpgradeLaneState(dataDir)
	if err != nil {
		return ""
	}
	if missing := state.missingPreRestoreModes(); len(missing) > 0 {
		return missing[0]
	}
	return ""
}

// awaitUpgradeSwitch ends the pre-upgrade phase with errUpgradeWarmupEnded at
// the warm-up's end when the baseline evidence is complete, or as soon as it
// completes before the grace runs out. remaining is the time left until the
// warm-up's end and is negative once it has passed. Past the grace it blocks
// the switch durably and then delivers the blocked event, without ending the
// phase.
func awaitUpgradeSwitch(ctx context.Context, cfg Config, remaining time.Duration, cancel context.CancelCauseFunc) {
	deadline := time.Now().Add(remaining + upgradeEvidenceGrace(cfg.UpgradeWarmup))
	select {
	case <-ctx.Done():
		return
	case <-time.After(max(remaining, 0)):
	}
	for {
		state, err := readUpgradeLaneState(cfg.DataDir)
		if err == nil && state.SwitchBlockedAt == nil && !state.preEvidenceComplete() && !time.Now().Before(deadline) {
			state, err = blockUpgradeSwitch(cfg)
			if err != nil {
				slog.Error("Failed to record the blocked upgrade switch", "error", err)
			}
		}
		if err == nil && state.SwitchBlockedAt != nil {
			deliverUpgradeSwitchBlocked(ctx, cfg)
			return
		}
		if err == nil && state.preEvidenceComplete() {
			cancel(errUpgradeWarmupEnded)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(upgradeEvidencePoll):
		}
	}
}

// baseProofFailure is the base build's failed restore of the preserved chain,
// kept until it has been reported as a failed pre-upgrade txid restore: that
// report is what puts the failure on the baseline side of the gate.
type baseProofFailure struct {
	TXID        string     `json:"txid"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt time.Time  `json:"completed_at"`
	Error       string     `json:"error"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
}

// blockUpgradeSwitchForBaseProof keeps the lane on the base build because
// that build could not restore the chain it wrote. The candidate never runs,
// so this is the same blocked switch as a baseline that never completed.
func blockUpgradeSwitchForBaseProof(cfg Config, record preservedChain) (upgradeLaneState, error) {
	return updateUpgradeLaneState(cfg.DataDir, func(state *upgradeLaneState) bool {
		if state.Phase != UpgradePhasePre {
			return false
		}
		if state.BaseProofFailure == nil {
			state.BaseProofFailure = &baseProofFailure{TXID: record.TXID, StartedAt: record.StartedAt, CompletedAt: record.CompletedAt, Error: record.Error}
		}
		if state.SwitchBlockedAt == nil {
			blocked := time.Now().UTC()
			state.SwitchBlockedAt = &blocked
			state.SwitchBlockedReason = fmt.Sprintf("upgrade lane stays on the base build: the base build could not restore the replica it wrote (%s); the candidate build was never started", record.Error)
			slog.Error(state.SwitchBlockedReason)
		}
		return true
	})
}

// blockUpgradeSwitch records the block unless the evidence completed, or the
// block was recorded, since the caller last looked.
func blockUpgradeSwitch(cfg Config) (upgradeLaneState, error) {
	return updateUpgradeLaneState(cfg.DataDir, func(state *upgradeLaneState) bool {
		if state.Phase != UpgradePhasePre || state.SwitchBlockedAt != nil || state.preEvidenceComplete() {
			return false
		}
		blocked := time.Now().UTC()
		state.SwitchBlockedAt = &blocked
		state.SwitchBlockedReason = fmt.Sprintf("upgrade lane stays on the base build: the pre-upgrade baseline was incomplete %s after the %s warm-up (no passing restore in mode(s) %s, pre-upgrade txid %s)", upgradeEvidenceGrace(cfg.UpgradeWarmup), cfg.UpgradeWarmup, firstNonEmpty(strings.Join(state.missingPreRestoreModes(), ","), "none"), formatTXID(state.PreUpgradeTXID))
		slog.Error(state.SwitchBlockedReason)
		return true
	})
}

// deliverUpgradeSwitchBlocked posts the blocked event, and the base build's
// failed restore of the preserved chain when that is what blocked the switch,
// until each has one delivery on record, backing off between attempts. It
// runs again after a restart, so a failed POST never leaves the gate without
// the reports it scores from.
func deliverUpgradeSwitchBlocked(ctx context.Context, cfg Config) {
	reporter := NewReporter(cfg)
	if !reporter.Enabled() {
		return
	}
	backoff := upgradeBlockedEventBackoff
	for {
		state, err := readUpgradeLaneState(cfg.DataDir)
		proofPending := state.BaseProofFailure != nil && state.BaseProofFailure.DeliveredAt == nil
		if err == nil && (state.SwitchBlockedAt == nil || (state.BlockedEventDeliveredAt != nil && !proofPending)) {
			return
		}
		if err == nil && proofPending {
			err = sendBaseProofFailure(ctx, cfg, *state.BaseProofFailure)
		}
		if err == nil && state.BlockedEventDeliveredAt == nil {
			err = sendUpgradeSwitchBlocked(ctx, cfg, reporter, state)
		}
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		slog.Warn("Failed to report the blocked upgrade switch; retrying", "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, upgradeBlockedEventMaxBackoff)
	}
}

func sendUpgradeSwitchBlocked(ctx context.Context, cfg Config, reporter *Reporter, state upgradeLaneState) error {
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := reporter.SendEvent(sendCtx, reporting.WorkerEventPayload{EventType: upgradeSwitchBlockedEvent, Message: state.SwitchBlockedReason, SentAt: *state.SwitchBlockedAt}); err != nil {
		return err
	}
	_, err := updateUpgradeLaneState(cfg.DataDir, func(state *upgradeLaneState) bool {
		if state.BlockedEventDeliveredAt != nil {
			return false
		}
		delivered := time.Now().UTC()
		state.BlockedEventDeliveredAt = &delivered
		return true
	})
	return err
}

// sendBaseProofFailure reports the failed proof as a pre-upgrade txid restore
// of the base build, so the ledger counts it as a baseline failure.
func sendBaseProofFailure(ctx context.Context, cfg Config, failure baseProofFailure) error {
	base, err := cfg.ForUpgradePhase(UpgradePhasePre)
	if err != nil {
		return err
	}
	classification := reporting.ClassifyVerificationFailure(base.VerifyType, failure.Error)
	classification.RestoreMode = restoreModeTXID
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := NewReporter(base).SendVerification(sendCtx, reporting.VerificationPayload{
		StartedAt:             failure.StartedAt,
		CompletedAt:           failure.CompletedAt,
		CheckType:             base.VerifyType,
		Status:                "failed",
		Summary:               fmt.Sprintf("base build could not restore %s txid %s", reporting.PreservedChainRestoreSubject, failure.TXID),
		ErrorMessage:          failure.Error,
		DurationMS:            int(failure.CompletedAt.Sub(failure.StartedAt).Milliseconds()),
		FailureClassification: &classification,
		RestoreMode:           restoreModeTXID,
	}); err != nil {
		return err
	}
	_, err = updateUpgradeLaneState(cfg.DataDir, func(state *upgradeLaneState) bool {
		if state.BaseProofFailure == nil || state.BaseProofFailure.DeliveredAt != nil {
			return false
		}
		delivered := time.Now().UTC()
		state.BaseProofFailure.DeliveredAt = &delivered
		return true
	})
	return err
}

func resolveLitestreamBaseSHA(fromEnv string) string {
	if v := strings.TrimSpace(fromEnv); v != "" {
		return v
	}
	shaFile := strings.TrimSpace(os.Getenv("LITESTREAM_BASE_SHA_FILE"))
	if shaFile == "" {
		shaFile = defaultLitestreamBaseSHAFile
	}
	body, err := os.ReadFile(shaFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}

func (c Config) validateUpgradeLane(stat func(string) (os.FileInfo, error)) error {
	if !c.UpgradeLane {
		return nil
	}
	if c.ManyDBEnabled() {
		return errors.New("UPGRADE_LANE does not support many-database profiles")
	}
	if c.UpgradeWarmup <= 0 {
		return errors.New("UPGRADE_LANE requires a positive UPGRADE_WARMUP")
	}
	info, err := stat(c.LitestreamBaseBinary)
	if err != nil || info.IsDir() {
		return fmt.Errorf("UPGRADE_LANE requires the base litestream binary at %s: %v", c.LitestreamBaseBinary, err)
	}
	if c.LitestreamBaseSHA == "" {
		return errors.New("UPGRADE_LANE requires LITESTREAM_BASE_SHA or the image's litestream-base.sha")
	}
	if c.LitestreamSHA == "" {
		return errors.New("UPGRADE_LANE requires the image's pinned LITESTREAM_SHA")
	}
	if strings.EqualFold(c.LitestreamBaseSHA, c.LitestreamSHA) {
		return fmt.Errorf("UPGRADE_LANE base and candidate builds are the same commit %s", c.LitestreamSHA)
	}
	return nil
}

// litestreamCommand is the Litestream executable every subprocess in this
// phase runs: the base build before the switch, the image's build after.
func (c Config) litestreamCommand() string {
	if c.litestreamBinary != "" {
		return c.litestreamBinary
	}
	return "litestream"
}

func (c Config) UpgradePhase() string {
	return c.upgradePhase
}

func (c Config) ActiveLitestreamSHA() string {
	return c.activeLitestreamSHA
}

// ForUpgradePhase derives the config one lane phase runs with. The attested
// workload identity stays the lane's, so both phases report the same profile
// hash and differ only in upgrade_phase and active_litestream_sha.
func (c Config) ForUpgradePhase(phase string) (Config, error) {
	if !c.UpgradeLane {
		return Config{}, errors.New("upgrade lane is not configured")
	}
	base := c
	next := c
	next.identityBase = &base
	next.upgradePhase = phase
	switch phase {
	case UpgradePhasePre:
		next.litestreamBinary = c.LitestreamBaseBinary
		next.activeLitestreamSHA = c.LitestreamBaseSHA
		next.FollowCheck = false
	case UpgradePhasePost:
		next.litestreamBinary = "litestream"
		next.activeLitestreamSHA = c.LitestreamSHA
		if c.UpgradePostSnapshotInterval > 0 {
			next.SnapshotInterval = c.UpgradePostSnapshotInterval
		}
	default:
		return Config{}, fmt.Errorf("unknown upgrade phase %q", phase)
	}
	return next, nil
}

// RunUpgradeLane runs the lane until the context ends. A lane that halted on
// a harness failure of its own stays up and idle: exiting would have the
// platform restart the machine into the same failure until its restart limit.
func RunUpgradeLane(ctx context.Context, cfg Config, run func(context.Context, Config) error) error {
	err := runUpgradeLane(ctx, cfg, time.Now, newReplicaL9Purger(), run)
	if !errors.Is(err, errUpgradeLaneHalted) {
		return err
	}
	slog.Error("Upgrade lane halted; idling until the worker is stopped", "error", err)
	backoff := upgradeBlockedEventBackoff
	for {
		if deliverErr := deliverLaneFault(ctx, cfg); deliverErr == nil {
			break
		} else {
			slog.Warn("Failed to report the upgrade lane harness failure; retrying", "error", deliverErr, "retry_in", backoff)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, upgradeBlockedEventMaxBackoff)
	}
	<-ctx.Done()
	return nil
}

// laneFault is a harness failure that ended the lane: the replica copy or
// the L9 purge could not be done. It is terminal. The lane reports it once
// and never retries the work, across restarts too.
type laneFault struct {
	At          time.Time  `json:"at"`
	Error       string     `json:"error"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
}

var errUpgradeLaneHalted = errors.New("upgrade lane halted on a harness failure")

// failUpgradeLane records the fault, reports it, and returns the halted
// error every later start of this lane returns as well.
func failUpgradeLane(ctx context.Context, cfg Config, cause error) error {
	state, err := updateUpgradeLaneState(cfg.DataDir, func(state *upgradeLaneState) bool {
		if state.LaneFault != nil {
			return false
		}
		state.LaneFault = &laneFault{At: time.Now().UTC(), Error: cause.Error()}
		return true
	})
	if err != nil {
		return errors.Join(cause, err)
	}
	return haltedUpgradeLane(ctx, cfg, state)
}

func haltedUpgradeLane(ctx context.Context, cfg Config, state upgradeLaneState) error {
	if err := deliverLaneFault(ctx, cfg); err != nil {
		slog.Warn("Failed to report upgrade lane harness failure", "error", err)
	}
	return fmt.Errorf("%w: %s", errUpgradeLaneHalted, state.LaneFault.Error)
}

// deliverLaneFault tells the control plane, once, that the lane itself, not
// Litestream, stopped the switch, so the gate shows a harness failure rather
// than a silent missing worker.
func deliverLaneFault(ctx context.Context, cfg Config) error {
	state, err := readUpgradeLaneState(cfg.DataDir)
	if err != nil {
		return err
	}
	if state.LaneFault == nil || state.LaneFault.DeliveredAt != nil {
		return nil
	}
	reporter := NewReporter(cfg)
	if !reporter.Enabled() {
		return nil
	}
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := reporter.SendEvent(reportCtx, reporting.WorkerEventPayload{
		EventType: "worker_failed",
		Message:   "upgrade lane harness failure: " + state.LaneFault.Error,
		SentAt:    state.LaneFault.At,
	}); err != nil {
		return err
	}
	_, err = updateUpgradeLaneState(cfg.DataDir, func(state *upgradeLaneState) bool {
		if state.LaneFault == nil || state.LaneFault.DeliveredAt != nil {
			return false
		}
		delivered := time.Now().UTC()
		state.LaneFault.DeliveredAt = &delivered
		return true
	})
	return err
}

// recordFirstCandidateL9 notes the first L9 object seen after a purged switch,
// so the evidence says when (or whether) the candidate wrote its first
// snapshot. It returns the observation on record, if any.
func recordFirstCandidateL9(dataDir string, l9 reporting.ReplicaLevelSummary, now time.Time) (*firstL9Observation, error) {
	recorded := false
	state, err := updateUpgradeLaneState(dataDir, func(state *upgradeLaneState) bool {
		if state.FirstCandidateL9 != nil || state.L9Purge == nil || l9.ObjectCount == 0 {
			return false
		}
		state.FirstCandidateL9 = &firstL9Observation{ObservedAt: now.UTC(), MaxTXID: formatTXID(l9.MaxTXIDDecimal), Objects: l9.ObjectCount}
		recorded = true
		return true
	})
	if err != nil {
		return nil, err
	}
	if recorded {
		slog.Info("Candidate build wrote its first L9 snapshot", "max_txid", state.FirstCandidateL9.MaxTXID, "objects", l9.ObjectCount)
	}
	return state.FirstCandidateL9, nil
}

// purgeUntilClean runs the L9 purge up to replicaL9PurgeAttempts times,
// persisting every attempt, and returns nil only once a successful record is
// durable. The candidate build must not start on a replica that still holds
// base-written L9 objects; a persistent failure is a harness failure.
func purgeUntilClean(ctx context.Context, cfg Config, clock func() time.Time, purger replicaL9Purger, state *upgradeLaneState) error {
	var attemptErr error
	for attempt := 1; attempt <= replicaL9PurgeAttempts; attempt++ {
		if ctx.Err() != nil {
			return errors.Join(attemptErr, ctx.Err())
		}
		slog.Info("Deleting replica L9 objects before the candidate build starts", "prefix", replicaOwnPrefix(cfg), "attempt", attempt)
		purge, err := purger.purgeReplicaL9(ctx, cfg, clock)
		purge.Attempt = attempt
		if state.L9Purge != nil {
			purge.Attempt = state.L9Purge.Attempt + 1
		}
		purge.Succeeded = err == nil
		updated, writeErr := updateUpgradeLaneState(cfg.DataDir, func(stored *upgradeLaneState) bool {
			stored.L9Purge = &purge
			return true
		})
		if writeErr != nil {
			return errors.Join(err, writeErr)
		}
		*state = updated
		if err == nil {
			slog.Info("Replica L9 level emptied", "deleted", len(purge.Deleted), "listed", purge.Listed, "attempt", purge.Attempt)
			return nil
		}
		attemptErr = err
		slog.Warn("Replica L9 purge attempt failed; the candidate build will not start on a dirty replica", "attempt", purge.Attempt, "error", err)
		if attempt < replicaL9PurgeAttempts {
			select {
			case <-ctx.Done():
				return errors.Join(attemptErr, ctx.Err())
			case <-time.After(replicaL9PurgeRetryDelay):
			}
		}
	}
	return fmt.Errorf("%d attempt(s) failed, replica may still hold base-written L9 objects: %w", replicaL9PurgeAttempts, attemptErr)
}

func runUpgradeLane(ctx context.Context, cfg Config, clock func() time.Time, purger replicaL9Purger, run func(context.Context, Config) error) error {
	if !cfg.UpgradeLane {
		return errors.New("upgrade lane is not configured")
	}
	started := clock().UTC()
	state, err := updateUpgradeLaneState(cfg.DataDir, func(state *upgradeLaneState) bool {
		if state.Phase != "" {
			return false
		}
		*state = upgradeLaneState{Phase: UpgradePhasePre, StartedAt: started, BaseSHA: cfg.LitestreamBaseSHA, CandidateSHA: cfg.LitestreamSHA}
		return true
	})
	if err != nil {
		return err
	}
	if state.LaneFault != nil {
		return haltedUpgradeLane(ctx, cfg, state)
	}
	if state.BaseSHA != cfg.LitestreamBaseSHA || state.CandidateSHA != cfg.LitestreamSHA {
		return fmt.Errorf("upgrade lane state on this volume is for %s -> %s, not %s -> %s", state.BaseSHA, state.CandidateSHA, cfg.LitestreamBaseSHA, cfg.LitestreamSHA)
	}

	// The loop runs the pre-upgrade phase again only after the base build
	// failed to restore the chain it wrote: the switch is then blocked for
	// good and the base build keeps running, as for an incomplete baseline.
	for state.Phase == UpgradePhasePre {
		pre, err := cfg.ForUpgradePhase(UpgradePhasePre)
		if err != nil {
			return err
		}
		now := clock().UTC()
		end := state.StartedAt.Add(cfg.UpgradeWarmup)
		slog.Info("Starting upgrade lane pre-upgrade phase", "litestream_sha", pre.activeLitestreamSHA, "binary", pre.litestreamCommand(), "switch_at", end)
		phaseCtx, cancel := context.WithCancelCause(ctx)
		switchDecided := make(chan struct{})
		go func() {
			defer close(switchDecided)
			awaitUpgradeSwitch(phaseCtx, cfg, end.Sub(now), cancel)
		}()
		err = run(phaseCtx, pre)
		cause := context.Cause(phaseCtx)
		ended := errors.Is(cause, errUpgradeWarmupEnded)
		cancel(nil)
		<-switchDecided
		switch {
		case ctx.Err() != nil:
			return nil
		case !ended && err != nil:
			return fmt.Errorf("pre-upgrade phase: %w", err)
		case !ended:
			return nil
		}
		// The base process has exited and the lane is still pre-upgrade.
		// The chain is copied and the base build restores it before the
		// switch is committed, so a base build that cannot restore its own
		// replica blocks the switch like any other missing baseline and the
		// candidate, which never ran, takes no fault. A copy that cannot be
		// made is the lane's own failure. The successful record is written
		// with the switch itself: a lane that is post-upgrade without one
		// (an older worker, or an unsupported backend) is never copied after
		// the candidate has written to its replica.
		var chain *preservedChain
		if purger.chain != nil && preservedChainSupported(cfg) {
			record, err := preserveUntilVerified(ctx, cfg, clock, purger.chain)
			if errors.Is(err, errBaseProofFailed) {
				if state, err = blockUpgradeSwitchForBaseProof(cfg, record); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return failUpgradeLane(ctx, cfg, fmt.Errorf("upgrade lane pre-upgrade chain preservation: %w", err))
			}
			chain = &record
		}
		switched := clock().UTC()
		state, err = updateUpgradeLaneState(cfg.DataDir, func(state *upgradeLaneState) bool {
			state.Phase = UpgradePhasePost
			state.SwitchedAt = &switched
			state.PreservedChain = chain
			return true
		})
		if err != nil {
			return err
		}
		slog.Info("Upgrade lane switching Litestream build", "from", cfg.LitestreamBaseSHA, "to", cfg.LitestreamSHA, "pre_upgrade_txid", formatTXID(state.PreUpgradeTXID))
	}

	// The purge runs only here: the base process has exited (the Runner
	// stops Litestream before returning) and the candidate has not started.
	// Only a durable successful record lets the candidate start: a failed
	// attempt is recorded and retried while the base build is still stopped,
	// a crash between the switch and a successful purge repeats the purge on
	// restart, and a successful record is never acted on again, so L9
	// objects the candidate writes are never deleted.
	if cfg.UpgradeDeleteL9AtSwitch && !state.L9Purge.succeeded() {
		if err := purgeUntilClean(ctx, cfg, clock, purger, &state); err != nil {
			return failUpgradeLane(ctx, cfg, fmt.Errorf("upgrade lane L9 purge: %w", err))
		}
	}

	post, err := cfg.ForUpgradePhase(UpgradePhasePost)
	if err != nil {
		return err
	}
	post.upgradePreTXID = state.PreUpgradeTXID
	if state.PreservedChain.succeeded() {
		post.upgradePreTXID = state.PreservedChain.txid()
		post.upgradePreservedChain = state.PreservedChain.Prefix
	}
	slog.Info("Starting upgrade lane post-upgrade phase", "litestream_sha", post.activeLitestreamSHA, "binary", post.litestreamCommand(), "pre_upgrade_txid", formatTXID(post.upgradePreTXID), "preserved_chain", post.upgradePreservedChain)
	return run(ctx, post)
}
