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
	Phase          string     `json:"phase"`
	StartedAt      time.Time  `json:"started_at"`
	SwitchedAt     *time.Time `json:"switched_at,omitempty"`
	BaseSHA        string     `json:"base_litestream_sha"`
	CandidateSHA   string     `json:"candidate_litestream_sha"`
	PreUpgradeTXID uint64     `json:"pre_upgrade_txid,omitempty"`
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
	path := upgradeLaneStatePath(dataDir)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(body, '\n'), 0644); err != nil {
		return fmt.Errorf("write upgrade lane state: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit upgrade lane state: %w", err)
	}
	return nil
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
	return runUpgradeLane(ctx, cfg, time.Now, run)
}

func runUpgradeLane(ctx context.Context, cfg Config, clock func() time.Time, run func(context.Context, Config) error) error {
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

	post, err := cfg.ForUpgradePhase(UpgradePhasePost)
	if err != nil {
		return err
	}
	post.upgradePreTXID = state.PreUpgradeTXID
	slog.Info("Starting upgrade lane post-upgrade phase", "litestream_sha", post.activeLitestreamSHA, "binary", post.litestreamCommand(), "pre_upgrade_txid", formatTXID(post.upgradePreTXID))
	return run(ctx, post)
}
