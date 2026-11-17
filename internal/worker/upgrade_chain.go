package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	preservedChainSuffix      = "-pre-upgrade"
	preservedChainCopyTimeout = 10 * time.Minute
	preservedChainRestoreWait = 10 * time.Minute
)

var (
	errPreservedChainRefused = errors.New("pre-upgrade chain preservation refused")
	ltxObjectNamePattern     = regexp.MustCompile(`^[0-9a-f]{16}-([0-9a-f]{16})\.ltx$`)
)

// preservedChain is the record of the replica copy taken at the switch. The
// historical check restores from that copy, not from the live replica: the
// live replica's L0 files age out and compaction merges the pre-upgrade
// boundary into wider files, so a txid the base build restored during the
// warm-up stops being an explicit restore target on any build. The copy is
// never written to again, and the base build restores TXID from it before the
// candidate starts, so a candidate that cannot restore it is a real failure.
type preservedChain struct {
	Prefix      string    `json:"prefix"`
	TXID        string    `json:"txid,omitempty"`
	Objects     int       `json:"objects,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Attempt     int       `json:"attempt,omitempty"`
	Error       string    `json:"error,omitempty"`
	Succeeded   bool      `json:"succeeded"`
}

func (p *preservedChain) succeeded() bool {
	return p != nil && p.Succeeded && p.Error == "" && p.txid() != 0 && p.Prefix != ""
}

func (p *preservedChain) txid() uint64 {
	if p == nil {
		return 0
	}
	txid, err := strconv.ParseUint(p.TXID, 16, 64)
	if err != nil {
		return 0
	}
	return txid
}

// upgradeChainPreserver copies the replica and proves the copy with the base
// build. restore is the base build's explicit-txid restore of the copy
// followed by the integrity check.
type upgradeChainPreserver struct {
	restore func(ctx context.Context, base, preserved Config, txid uint64) error
}

func newUpgradeChainPreserver() *upgradeChainPreserver {
	return &upgradeChainPreserver{restore: restorePreservedChain}
}

// preservedChainSupported reports whether the replica backend can be copied.
func preservedChainSupported(cfg Config) bool {
	return cfg.ReplicaType == "file" || cfg.ReplicaType == "s3"
}

// preservedChainConfig is the lane config pointed at the preserved copy, a
// sibling of the worker's replica prefix that no Litestream process
// replicates to and the L9 purge never lists.
func preservedChainConfig(cfg Config) Config {
	preserved := cfg
	if cfg.ReplicaType == "file" {
		preserved.ReplicaPath = filepath.Clean(cfg.ReplicaPath) + preservedChainSuffix
	} else {
		preserved.S3Path = replicaOwnPrefix(cfg) + preservedChainSuffix
	}
	return preserved
}

func (p *upgradeChainPreserver) preserve(ctx context.Context, cfg, base Config, now func() time.Time) (preservedChain, error) {
	preserved := preservedChainConfig(cfg)
	record := preservedChain{StartedAt: now().UTC(), Prefix: replicaOwnPrefix(preserved)}
	finish := func(err error) (preservedChain, error) {
		record.CompletedAt = now().UTC()
		if err != nil {
			record.Error = err.Error()
		}
		record.Succeeded = err == nil
		return record, err
	}
	source := replicaOwnPrefix(cfg)
	if source == "" || source == "." || source == "/" {
		return finish(fmt.Errorf("%w: empty replica prefix", errPreservedChainRefused))
	}
	copyCtx, cancel := context.WithTimeout(ctx, preservedChainCopyTimeout)
	defer cancel()
	want, err := replicaRelativeKeys(copyCtx, cfg)
	if err != nil {
		return finish(fmt.Errorf("list replica objects: %w", err))
	}
	stale, err := listReplicaObjects(copyCtx, preserved)
	if err != nil {
		return finish(fmt.Errorf("list %s before the copy: %w", record.Prefix, err))
	}
	for _, key := range stale {
		if !replicaKeyWithinPrefix(preserved, key) {
			return finish(fmt.Errorf("%w: %s is outside %s", errPreservedChainRefused, key, record.Prefix))
		}
		if err := removeReplicaObject(copyCtx, preserved, key); err != nil {
			return finish(fmt.Errorf("clear %s before the copy: %w", record.Prefix, err))
		}
	}
	if err := copyReplicaObjects(copyCtx, cfg, preserved); err != nil {
		return finish(fmt.Errorf("copy replica to %s: %w", record.Prefix, err))
	}
	got, err := replicaRelativeKeys(copyCtx, preserved)
	if err != nil {
		return finish(fmt.Errorf("list preserved objects: %w", err))
	}
	record.Objects = len(got)
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(want, got) {
		return finish(fmt.Errorf("preserved copy under %s holds %d object(s), the replica holds %d, and the two listings differ", record.Prefix, len(got), len(want)))
	}
	txid := maxLTXObjectTXID(got)
	if txid == 0 {
		return finish(fmt.Errorf("replica under %s holds no LTX objects to preserve (%d object(s) listed)", source, len(want)))
	}
	record.TXID = formatTXID(txid)
	restoreCtx, cancelRestore := context.WithTimeout(ctx, preservedChainRestoreWait)
	defer cancelRestore()
	if err := p.restore(restoreCtx, base, preserved, txid); err != nil {
		return finish(fmt.Errorf("base build %s could not restore txid %s from the preserved copy: %w", base.activeLitestreamSHA, record.TXID, err))
	}
	return finish(nil)
}

func replicaRelativeKeys(ctx context.Context, cfg Config) ([]string, error) {
	keys, err := listReplicaObjects(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if cfg.ReplicaType == "file" {
		return keys, nil
	}
	prefix := replicaOwnPrefix(cfg) + "/"
	relative := make([]string, 0, len(keys))
	for _, key := range keys {
		if rest, ok := strings.CutPrefix(key, prefix); ok {
			relative = append(relative, rest)
		}
	}
	return relative, nil
}

func maxLTXObjectTXID(keys []string) uint64 {
	var highest uint64
	for _, key := range keys {
		if _, ok := replicaLevelOfKey(key); !ok {
			continue
		}
		match := ltxObjectNamePattern.FindStringSubmatch(path.Base(key))
		if match == nil {
			continue
		}
		if txid, err := strconv.ParseUint(match[1], 16, 64); err == nil && txid > highest {
			highest = txid
		}
	}
	return highest
}

func copyReplicaObjects(ctx context.Context, cfg, preserved Config) error {
	if cfg.ReplicaType == "file" {
		return copyFileReplica(ctx, replicaOwnPrefix(cfg), replicaOwnPrefix(preserved))
	}
	args, err := s3cmdArgs(cfg)
	if err != nil {
		return err
	}
	args = append(args, "cp", "--recursive",
		fmt.Sprintf("s3://%s/%s/", cfg.S3Bucket, replicaOwnPrefix(cfg)),
		fmt.Sprintf("s3://%s/%s/", preserved.S3Bucket, replicaOwnPrefix(preserved)))
	cmd := exec.CommandContext(ctx, "s3cmd", args...)
	cmd.WaitDelay = replicaLevelKillGrace
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = io.Discard, &output
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %s", err, tailString(strings.TrimSpace(output.String()), 512))
	}
	return nil
}

func copyFileReplica(ctx context.Context, source, destination string) error {
	keys, err := listFileReplicaObjects(Config{ReplicaType: "file", ReplicaPath: source})
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		target := filepath.Join(destination, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(source, filepath.FromSlash(key)))
		if err != nil {
			return err
		}
		if err := writeFileDurable(target, body); err != nil {
			return err
		}
	}
	return nil
}

// writePreservedChainConfig renders a restore-only Litestream config whose
// replica is the preserved copy. S3 restores go straight to the object store,
// like the verifier's observe-mode restores.
func writePreservedChainConfig(cfg Config) (string, func(), error) {
	preserved := preservedChainConfig(cfg)
	f, err := os.CreateTemp(filepath.Dir(cfg.ConfigPath), "litestream-preserved-*.yml")
	if err != nil {
		return "", func() {}, fmt.Errorf("create preserved chain config: %w", err)
	}
	name := f.Name()
	cleanup := func() {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			slog.Warn("Failed to remove preserved chain config", "path", name, "error", err)
		}
	}
	manager := newLitestreamManager(&preserved)
	replica := manager.litestreamConfigReplica(cfg.DBPath)
	if cfg.ReplicaType == "s3" {
		replica.S3Endpoint = cfg.directS3Endpoint()
	}
	data := litestreamConfigData{
		SocketPath: cfg.SocketPath,
		Databases: []litestreamConfigDB{{
			Path:             cfg.DBPath,
			SnapshotInterval: cfg.SnapshotInterval.String(),
			Replica:          replica,
		}},
	}
	err = litestreamConfigTmpl.Execute(f, data)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("write preserved chain config: %w", err)
	}
	return name, cleanup, nil
}

func restorePreservedChain(ctx context.Context, base, _ Config, txid uint64) error {
	configPath, cleanup, err := writePreservedChainConfig(base)
	if err != nil {
		return err
	}
	defer cleanup()
	restoredPath := base.DBPath + ".restored.chain"
	if err := removeRestoredArtifacts(restoredPath); err != nil {
		return fmt.Errorf("clean preserved chain restore artifacts: %w", err)
	}
	defer func() {
		if err := removeRestoredArtifacts(restoredPath); err != nil {
			slog.Warn("Failed to remove preserved chain restore artifacts", "error", err)
		}
	}()
	if err := checkRestoreCapacity(restoredPath, base.DBPath); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, base.litestreamCommand(), restoreArgs(configPath, restoreModeTXID, txid, time.Time{}, restoredPath, base.DBPath)...)
	if base.ReplicaType == "s3" {
		cmd.Env = base.s3CommandEnv("")
	}
	cmd.WaitDelay = time.Second
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("restore: %w: %s", err, tailString(strings.TrimSpace(string(output)), 1024))
	}
	if err := checkRestoredIntegrity(ctx, restoredPath); err != nil {
		return fmt.Errorf("integrity check: %w", err)
	}
	return nil
}

// preserveUntilVerified copies and proves the chain up to
// replicaL9PurgeAttempts times, persisting every attempt. Only a durable
// successful record lets the switch continue.
func preserveUntilVerified(ctx context.Context, cfg Config, clock func() time.Time, preserver *upgradeChainPreserver, state *upgradeLaneState) error {
	base, err := cfg.ForUpgradePhase(UpgradePhasePre)
	if err != nil {
		return err
	}
	var attemptErr error
	for attempt := 1; attempt <= replicaL9PurgeAttempts; attempt++ {
		if ctx.Err() != nil {
			return errors.Join(attemptErr, ctx.Err())
		}
		slog.Info("Preserving the pre-upgrade replica chain before the candidate build starts", "prefix", replicaOwnPrefix(preservedChainConfig(cfg)), "attempt", attempt)
		record, err := preserver.preserve(ctx, cfg, base, clock)
		record.Attempt = attempt
		if state.PreservedChain != nil {
			record.Attempt = state.PreservedChain.Attempt + 1
		}
		updated, writeErr := updateUpgradeLaneState(cfg.DataDir, func(stored *upgradeLaneState) bool {
			stored.PreservedChain = &record
			return true
		})
		if writeErr != nil {
			return errors.Join(err, writeErr)
		}
		*state = updated
		if err == nil {
			slog.Info("Pre-upgrade replica chain preserved and restored by the base build", "prefix", record.Prefix, "txid", record.TXID, "objects", record.Objects)
			return nil
		}
		attemptErr = err
		slog.Warn("Pre-upgrade chain preservation attempt failed; the candidate build will not start without it", "attempt", record.Attempt, "error", err)
		if attempt < replicaL9PurgeAttempts {
			select {
			case <-ctx.Done():
				return errors.Join(attemptErr, ctx.Err())
			case <-time.After(replicaL9PurgeRetryDelay):
			}
		}
	}
	return fmt.Errorf("%d attempt(s) failed: %w", replicaL9PurgeAttempts, attemptErr)
}
