package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
	Phase            string              `json:"phase"`
	StartedAt        time.Time           `json:"started_at"`
	SwitchedAt       *time.Time          `json:"switched_at,omitempty"`
	BaseSHA          string              `json:"base_litestream_sha"`
	CandidateSHA     string              `json:"candidate_litestream_sha"`
	PreUpgradeTXID   uint64              `json:"pre_upgrade_txid,omitempty"`
	L9Purge          *replicaL9Purge     `json:"l9_purge,omitempty"`
	FirstCandidateL9 *firstL9Observation `json:"first_candidate_l9,omitempty"`
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

// recordUpgradePreTXID remembers the newest verified pre-upgrade boundary so
// post-upgrade cycles can keep restoring a txid written by the base build.
func recordUpgradePreTXID(dataDir string, txid uint64) error {
	state, err := readUpgradeLaneState(dataDir)
	if err != nil {
		return err
	}
	if state.Phase != UpgradePhasePre || txid <= state.PreUpgradeTXID {
		return nil
	}
	state.PreUpgradeTXID = txid
	return writeUpgradeLaneState(dataDir, state)
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

func RunUpgradeLane(ctx context.Context, cfg Config, run func(context.Context, Config) error) error {
	return runUpgradeLane(ctx, cfg, time.Now, newReplicaL9Purger(), run)
}

// recordFirstCandidateL9 notes the first L9 object seen after a purged switch,
// so the evidence says when (or whether) the candidate wrote its first
// snapshot. It returns the observation on record, if any.
func recordFirstCandidateL9(dataDir string, l9 reporting.ReplicaLevelSummary, now time.Time) (*firstL9Observation, error) {
	state, err := readUpgradeLaneState(dataDir)
	if err != nil {
		return nil, err
	}
	if state.FirstCandidateL9 != nil || state.L9Purge == nil || l9.ObjectCount == 0 {
		return state.FirstCandidateL9, nil
	}
	state.FirstCandidateL9 = &firstL9Observation{ObservedAt: now.UTC(), MaxTXID: formatTXID(l9.MaxTXIDDecimal), Objects: l9.ObjectCount}
	if err := writeUpgradeLaneState(dataDir, state); err != nil {
		return nil, err
	}
	slog.Info("Candidate build wrote its first L9 snapshot", "max_txid", state.FirstCandidateL9.MaxTXID, "objects", l9.ObjectCount)
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
		state.L9Purge = &purge
		if writeErr := writeUpgradeLaneState(cfg.DataDir, *state); writeErr != nil {
			return errors.Join(err, writeErr)
		}
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

// reportLaneHarnessFailure tells the control plane that the lane itself,
// not Litestream, stopped the switch, so the gate shows a harness failure
// rather than a silent missing worker.
func reportLaneHarnessFailure(ctx context.Context, cfg Config, err error) {
	reporter := NewReporter(cfg)
	if !reporter.Enabled() {
		return
	}
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if sendErr := reporter.SendEvent(reportCtx, reporting.WorkerEventPayload{
		EventType: "worker_failed",
		Message:   "upgrade lane harness failure: " + err.Error(),
		SentAt:    time.Now().UTC(),
	}); sendErr != nil {
		slog.Warn("Failed to report upgrade lane harness failure", "error", sendErr)
	}
}

func runUpgradeLane(ctx context.Context, cfg Config, clock func() time.Time, purger replicaL9Purger, run func(context.Context, Config) error) error {
	if !cfg.UpgradeLane {
		return errors.New("upgrade lane is not configured")
	}
	state, err := readUpgradeLaneState(cfg.DataDir)
	if err != nil {
		return err
	}
	now := clock().UTC()
	if state.Phase == "" {
		state = upgradeLaneState{Phase: UpgradePhasePre, StartedAt: now, BaseSHA: cfg.LitestreamBaseSHA, CandidateSHA: cfg.LitestreamSHA}
		if err := writeUpgradeLaneState(cfg.DataDir, state); err != nil {
			return err
		}
	}
	if state.BaseSHA != cfg.LitestreamBaseSHA || state.CandidateSHA != cfg.LitestreamSHA {
		return fmt.Errorf("upgrade lane state on this volume is for %s -> %s, not %s -> %s", state.BaseSHA, state.CandidateSHA, cfg.LitestreamBaseSHA, cfg.LitestreamSHA)
	}

	if state.Phase == UpgradePhasePre {
		pre, err := cfg.ForUpgradePhase(UpgradePhasePre)
		if err != nil {
			return err
		}
		end := state.StartedAt.Add(cfg.UpgradeWarmup)
		slog.Info("Starting upgrade lane pre-upgrade phase", "litestream_sha", pre.activeLitestreamSHA, "binary", pre.litestreamCommand(), "switch_at", end)
		phaseCtx, cancel := context.WithCancelCause(ctx)
		timer := time.AfterFunc(max(end.Sub(now), 0), func() { cancel(errUpgradeWarmupEnded) })
		err = run(phaseCtx, pre)
		timer.Stop()
		ended := errors.Is(context.Cause(phaseCtx), errUpgradeWarmupEnded)
		cancel(nil)
		switch {
		case ctx.Err() != nil:
			return nil
		case !ended && err != nil:
			return fmt.Errorf("pre-upgrade phase: %w", err)
		case !ended:
			return nil
		}
		state, err = readUpgradeLaneState(cfg.DataDir)
		if err != nil {
			return err
		}
		switched := clock().UTC()
		state.Phase = UpgradePhasePost
		state.SwitchedAt = &switched
		if err := writeUpgradeLaneState(cfg.DataDir, state); err != nil {
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
			err = fmt.Errorf("upgrade lane L9 purge: %w", err)
			reportLaneHarnessFailure(ctx, cfg, err)
			return err
		}
	}

	post, err := cfg.ForUpgradePhase(UpgradePhasePost)
	if err != nil {
		return err
	}
	post.upgradePreTXID = state.PreUpgradeTXID
	slog.Info("Starting upgrade lane post-upgrade phase", "litestream_sha", post.activeLitestreamSHA, "binary", post.litestreamCommand(), "pre_upgrade_txid", formatTXID(post.upgradePreTXID))
	return run(ctx, post)
}
