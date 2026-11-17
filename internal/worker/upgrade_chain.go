package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/s3util"
)

const (
	preservedChainDrainChecks = 6
	preservedChainCopyWorkers = 8
	// replicaLTXTree is the part of a replica prefix Litestream restores
	// from: the LTX levels, L9 snapshots included. The worker keeps other
	// objects under the same prefix, such as profile evidence, and those are
	// never copied.
	replicaLTXTree = "ltx/"
)

var (
	preservedChainSettle = 5 * time.Second

	errPreservedChainRefused = errors.New("pre-upgrade chain preservation refused")
	// errBaseProofFailed marks a preservation that failed because the base
	// build could not restore the copy or the restored database failed the
	// integrity check. Everything else (listing, copying, capacity, budgets)
	// is a fault of the lane, not of either build.
	errBaseProofFailed   = errors.New("base build failed to restore the preserved pre-upgrade chain")
	ltxObjectNamePattern = regexp.MustCompile(`^[0-9a-f]{16}-([0-9a-f]{16})\.ltx$`)
)

// replicaObject is what a listing knows about one replica object. ETag is the
// object store's ETag for S3 and a SHA-256 of the content for file replicas.
type replicaObject struct {
	Size int64
	ETag string
}

// preservedChain is the record of the replica copy taken at the switch. The
// historical check restores from that copy, not from the live replica: the
// live replica's L0 files age out and compaction merges the pre-upgrade
// boundary into wider files, so a txid the base build restored during the
// warm-up stops being an explicit restore target on any build. The copy is
// never written to again, and the base build restores TXID from it before the
// lane commits the switch, so a candidate that cannot restore it is a real
// failure.
type preservedChain struct {
	Prefix          string    `json:"prefix"`
	TXID            string    `json:"txid,omitempty"`
	Objects         int       `json:"objects,omitempty"`
	Bytes           int64     `json:"bytes,omitempty"`
	StartedAt       time.Time `json:"started_at"`
	CompletedAt     time.Time `json:"completed_at"`
	Attempt         int       `json:"attempt,omitempty"`
	Error           string    `json:"error,omitempty"`
	BaseProofFailed bool      `json:"base_proof_failed,omitempty"`
	Succeeded       bool      `json:"succeeded"`
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
// followed by the integrity check; it wraps errBaseProofFailed when the build
// itself, not the harness, is what failed.
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
		preserved.ReplicaPath = filepath.Clean(cfg.ReplicaPath) + reporting.PreservedChainPrefixSuffix
	} else {
		preserved.S3Path = replicaOwnPrefix(cfg) + reporting.PreservedChainPrefixSuffix
	}
	return preserved
}

// preserve must only run once the base build has exited. It waits for the
// replica's LTX tree to stop changing, brings the copy of that tree up to date (keeping
// objects an earlier attempt already copied), checks the copy object by
// object, confirms the replica did not change underneath the copy, and then
// has the base build restore the newest txid from the copy.
func (p *upgradeChainPreserver) preserve(ctx context.Context, cfg, base Config, now func() time.Time) (preservedChain, error) {
	preserved := preservedChainConfig(cfg)
	record := preservedChain{StartedAt: now().UTC(), Prefix: replicaOwnPrefix(preserved)}
	finish := func(err error) (preservedChain, error) {
		record.CompletedAt = now().UTC()
		if err != nil {
			record.Error = err.Error()
			record.BaseProofFailed = errors.Is(err, errBaseProofFailed)
		}
		record.Succeeded = err == nil
		return record, err
	}
	source := replicaOwnPrefix(cfg)
	if source == "" || source == "." || source == "/" || record.Prefix == source {
		return finish(fmt.Errorf("%w: empty replica prefix", errPreservedChainRefused))
	}
	drained, err := drainedReplicaObjects(ctx, cfg)
	if err != nil {
		return finish(err)
	}
	txid := maxLTXObjectTXID(slices.Collect(maps.Keys(drained)))
	if txid == 0 {
		return finish(fmt.Errorf("replica under %s holds no LTX objects to preserve (%d object(s) listed)", source, len(drained)))
	}
	record.TXID = formatTXID(txid)
	record.Objects = len(drained)
	for _, object := range drained {
		record.Bytes += object.Size
	}
	budget := verificationPhaseBudget(cfg.LogicalTimeout, record.Bytes)
	copyCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if err := syncReplicaObjects(copyCtx, cfg, preserved, drained); err != nil {
		if copyCtx.Err() != nil && ctx.Err() == nil {
			err = fmt.Errorf("timed out after %s (%d bytes); the next attempt resumes from the objects already copied", budget, record.Bytes)
		}
		return finish(fmt.Errorf("copy replica to %s: %w", record.Prefix, err))
	}
	copied, err := replicaObjectStats(ctx, preserved, "")
	if err != nil {
		return finish(fmt.Errorf("list preserved objects: %w", err))
	}
	if err := matchReplicaObjects(drained, copied, cfg.ReplicaType != "file"); err != nil {
		return finish(fmt.Errorf("preserved copy under %s does not match the replica: %w", record.Prefix, err))
	}
	after, err := replicaObjectStats(ctx, cfg, replicaLTXTree)
	if err != nil {
		return finish(fmt.Errorf("list replica objects after the copy: %w", err))
	}
	if err := matchReplicaObjects(drained, after, false); err != nil {
		return finish(fmt.Errorf("replica under %s changed while it was being copied: %w", source, err))
	}
	if err := p.restore(ctx, base, preserved, txid); err != nil {
		return finish(fmt.Errorf("base build %s, txid %s from the preserved copy: %w", base.activeLitestreamSHA, record.TXID, err))
	}
	return finish(nil)
}

// drainedReplicaObjects returns the replica listing once two listings taken
// preservedChainSettle apart agree on every object's size and ETag, so an
// upload the stopped base build had in flight has either landed or not.
func drainedReplicaObjects(ctx context.Context, cfg Config) (map[string]replicaObject, error) {
	previous, err := replicaObjectStats(ctx, cfg, replicaLTXTree)
	if err != nil {
		return nil, fmt.Errorf("list replica objects: %w", err)
	}
	var mismatch error
	for range preservedChainDrainChecks {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(preservedChainSettle):
		}
		current, err := replicaObjectStats(ctx, cfg, replicaLTXTree)
		if err != nil {
			return nil, fmt.Errorf("list replica objects: %w", err)
		}
		if mismatch = matchReplicaObjects(previous, current, false); mismatch == nil {
			return current, nil
		}
		previous = current
	}
	return nil, fmt.Errorf("replica under %s is still changing %s after the base build stopped: %w", replicaOwnPrefix(cfg), time.Duration(preservedChainDrainChecks)*preservedChainSettle, mismatch)
}

// matchReplicaObjects reports the first difference between two listings.
// Sizes always have to agree. ETags have to agree too, except that a
// server-side copy of a multipart upload gets a new ETag, so with
// copiedMultipart those objects are held to their size and to the base
// build's restore of the copy.
func matchReplicaObjects(want, got map[string]replicaObject, copiedMultipart bool) error {
	if len(want) != len(got) {
		return fmt.Errorf("%d object(s), want %d", len(got), len(want))
	}
	for _, key := range slices.Sorted(maps.Keys(want)) {
		object, ok := got[key]
		switch {
		case !ok:
			return fmt.Errorf("%s is missing", key)
		case object.Size != want[key].Size:
			return fmt.Errorf("%s is %d bytes, want %d", key, object.Size, want[key].Size)
		case object.ETag == want[key].ETag:
		case copiedMultipart && (strings.Contains(want[key].ETag, "-") || strings.Contains(object.ETag, "-")):
		default:
			return fmt.Errorf("%s has ETag %s, want %s", key, object.ETag, want[key].ETag)
		}
	}
	return nil
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

// replicaObjectStats lists the objects under the config's replica prefix,
// keyed relative to that prefix. tree limits the listing to one subtree, such
// as replicaLTXTree; an empty tree lists everything.
func replicaObjectStats(ctx context.Context, cfg Config, tree string) (map[string]replicaObject, error) {
	if cfg.ReplicaType == "file" {
		return fileReplicaObjectStats(ctx, replicaOwnPrefix(cfg), tree)
	}
	client, err := replicaS3Client(cfg)
	if err != nil {
		return nil, err
	}
	prefix := replicaOwnPrefix(cfg) + "/"
	listed, err := client.ListObjects(ctx, prefix+tree)
	if err != nil {
		return nil, err
	}
	objects := make(map[string]replicaObject, len(listed))
	for _, object := range listed {
		objects[strings.TrimPrefix(object.Key, prefix)] = replicaObject{Size: object.Size, ETag: object.ETag}
	}
	return objects, nil
}

// replicaS3Client talks to the replica's object store directly with the
// worker's own credentials.
func replicaS3Client(cfg Config) (*s3util.Client, error) {
	return s3util.NewClient(s3util.Config{
		Bucket:       cfg.S3Bucket,
		Endpoint:     cfg.directS3Endpoint(),
		AccessKey:    cfg.S3AccessKey,
		SecretKey:    cfg.S3SecretKey,
		Region:       cfg.S3Region,
		SessionToken: cfg.S3SessionToken,
	})
}

func fileReplicaObjectStats(ctx context.Context, root, tree string) (map[string]replicaObject, error) {
	keys, err := listFileReplicaObjects(Config{ReplicaType: "file", ReplicaPath: root})
	if err != nil {
		return nil, err
	}
	objects := make(map[string]replicaObject, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, tree) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		object, err := fileReplicaObject(filepath.Join(root, filepath.FromSlash(key)))
		if err != nil {
			return nil, err
		}
		objects[key] = object
	}
	return objects, nil
}

func fileReplicaObject(name string) (replicaObject, error) {
	f, err := os.Open(name)
	if err != nil {
		return replicaObject{}, err
	}
	defer func() { _ = f.Close() }()
	sum := sha256.New()
	size, err := io.Copy(sum, f)
	if err != nil {
		return replicaObject{}, err
	}
	return replicaObject{Size: size, ETag: hex.EncodeToString(sum.Sum(nil))}, nil
}

// syncReplicaObjects makes the preserved prefix hold exactly the source
// objects. Objects that already match are left alone, so a retry after a
// timeout continues where the last attempt stopped; anything else under the
// prefix, such as a copy left by an earlier lane, is removed. S3 objects are
// copied server side, one CopyObject each.
func syncReplicaObjects(ctx context.Context, cfg, preserved Config, source map[string]replicaObject) error {
	if cfg.ReplicaType == "file" {
		return syncFileReplica(ctx, replicaOwnPrefix(cfg), replicaOwnPrefix(preserved), source)
	}
	client, err := replicaS3Client(cfg)
	if err != nil {
		return err
	}
	existing, err := replicaObjectStats(ctx, preserved, "")
	if err != nil {
		return fmt.Errorf("list preserved objects: %w", err)
	}
	sourcePrefix, preservedPrefix := replicaOwnPrefix(cfg)+"/", replicaOwnPrefix(preserved)+"/"
	var stale []string
	for key := range existing {
		if _, ok := source[key]; !ok {
			stale = append(stale, preservedPrefix+key)
		}
	}
	slices.Sort(stale)
	if err := client.DeleteObjects(ctx, stale); err != nil {
		return fmt.Errorf("remove %d stale object(s): %w", len(stale), err)
	}
	var pending []string
	for _, key := range slices.Sorted(maps.Keys(source)) {
		if matchReplicaObjects(map[string]replicaObject{key: source[key]}, map[string]replicaObject{key: existing[key]}, true) != nil {
			pending = append(pending, key)
		}
	}
	copyCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	keys := make(chan string)
	var workers sync.WaitGroup
	for range min(preservedChainCopyWorkers, len(pending)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for key := range keys {
				if copyCtx.Err() != nil {
					continue
				}
				if err := client.CopyObject(copyCtx, sourcePrefix+key, preservedPrefix+key); err != nil {
					cancel(err)
				}
			}
		}()
	}
	for _, key := range pending {
		keys <- key
	}
	close(keys)
	workers.Wait()
	if err := context.Cause(copyCtx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

func syncFileReplica(ctx context.Context, sourceRoot, destinationRoot string, source map[string]replicaObject) error {
	existing, err := fileReplicaObjectStats(ctx, destinationRoot, "")
	if err != nil {
		return err
	}
	for key := range existing {
		if _, ok := source[key]; !ok {
			if err := os.Remove(filepath.Join(destinationRoot, filepath.FromSlash(key))); err != nil {
				return err
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(source)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if existing[key] == source[key] {
			continue
		}
		target := filepath.Join(destinationRoot, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(sourceRoot, filepath.FromSlash(key)))
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

// restorePreservedChain runs the base build's explicit-txid restore of the
// copy and the integrity check, each inside the verifier's own size-scaled
// budget for that phase. A restore that exits with an error, or a restored
// database that fails the integrity check, is the base build's failure;
// running out of budget or disk, or a canceled request, is not.
func restorePreservedChain(ctx context.Context, base, _ Config, txid uint64) error {
	configPath, cleanup, err := writePreservedChainConfig(base)
	if err != nil {
		return err
	}
	defer cleanup()
	restoredPath := base.DBPath + ".restored.pre"
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
	sourceSize := fileSize(base.DBPath) + fileSize(base.DBPath+"-wal")
	restoreBudget := base.verificationPhaseBudget("restore", sourceSize)
	restoreCtx, cancelRestore := context.WithTimeout(ctx, restoreBudget)
	defer cancelRestore()
	cmd := exec.CommandContext(restoreCtx, base.litestreamCommand(), restoreArgs(configPath, restoreModeTXID, txid, time.Time{}, restoredPath, base.DBPath)...)
	if base.ReplicaType == "s3" {
		cmd.Env = base.s3CommandEnv("")
	}
	cmd.WaitDelay = time.Second
	output, err := cmd.CombinedOutput()
	if err != nil {
		detail := fmt.Sprintf("restore failed: %s txid %016x: %v: %s", reporting.PreservedChainRestoreSubject, txid, err, tailString(strings.TrimSpace(string(output)), 1024))
		switch {
		case restoreCtx.Err() != nil:
			return fmt.Errorf("restore did not finish inside its %s budget: %s", restoreBudget, detail)
		case reporting.RestoreOutputDiskFull(detail), reporting.RestoreRequestCanceled(detail):
			return errors.New(detail)
		}
		return fmt.Errorf("%w: %s", errBaseProofFailed, detail)
	}
	integrityBudget := base.verificationPhaseBudget("integrity_check", sourceSize)
	integrityCtx, cancelIntegrity := context.WithTimeout(ctx, integrityBudget)
	defer cancelIntegrity()
	if err := checkRestoredIntegrity(integrityCtx, restoredPath); err != nil {
		if integrityCtx.Err() != nil {
			return fmt.Errorf("integrity check did not finish inside its %s budget: %w", integrityBudget, err)
		}
		return fmt.Errorf("%w: restore failed: %s txid %016x: integrity check: %w", errBaseProofFailed, reporting.PreservedChainRestoreSubject, txid, err)
	}
	return nil
}

// preserveUntilVerified copies and proves the chain, persisting every attempt
// before it starts and again when it ends, and returns the last attempt's
// record. The lane gets replicaL9PurgeAttempts attempts in total, counted
// across restarts, so a worker that dies mid-attempt cannot retry forever.
// The lane is still pre-upgrade while this runs: only a successful record
// lets it commit the switch.
func preserveUntilVerified(ctx context.Context, cfg Config, clock func() time.Time, preserver *upgradeChainPreserver) (preservedChain, error) {
	base, err := cfg.ForUpgradePhase(UpgradePhasePre)
	if err != nil {
		return preservedChain{}, err
	}
	stored, err := readUpgradeLaneState(cfg.DataDir)
	if err != nil {
		return preservedChain{}, err
	}
	var record preservedChain
	var attemptErr error
	used := 0
	if stored.PreservedChain != nil {
		record, used = *stored.PreservedChain, stored.PreservedChain.Attempt
		attemptErr = errors.New(firstNonEmpty(record.Error, "an earlier attempt did not finish"))
	}
	for attempt := used + 1; attempt <= replicaL9PurgeAttempts; attempt++ {
		if ctx.Err() != nil {
			return record, errors.Join(attemptErr, ctx.Err())
		}
		slog.Info("Preserving the pre-upgrade replica chain before the switch", "prefix", replicaOwnPrefix(preservedChainConfig(cfg)), "attempt", attempt)
		started := preservedChain{Prefix: replicaOwnPrefix(preservedChainConfig(cfg)), StartedAt: clock().UTC(), Attempt: attempt, Error: "attempt did not finish"}
		if _, writeErr := updateUpgradeLaneState(cfg.DataDir, func(stored *upgradeLaneState) bool {
			stored.PreservedChain = &started
			return true
		}); writeErr != nil {
			return record, errors.Join(attemptErr, writeErr)
		}
		record, attemptErr = preserver.preserve(ctx, cfg, base, clock)
		record.Attempt = attempt
		if _, writeErr := updateUpgradeLaneState(cfg.DataDir, func(stored *upgradeLaneState) bool {
			stored.PreservedChain = &record
			return true
		}); writeErr != nil {
			return record, errors.Join(attemptErr, writeErr)
		}
		if attemptErr == nil {
			slog.Info("Pre-upgrade replica chain preserved and restored by the base build", "prefix", record.Prefix, "txid", record.TXID, "objects", record.Objects, "bytes", record.Bytes)
			return record, nil
		}
		slog.Warn("Pre-upgrade chain preservation attempt failed; the lane will not switch without it", "attempt", attempt, "error", attemptErr)
		if attempt < replicaL9PurgeAttempts {
			select {
			case <-ctx.Done():
				return record, errors.Join(attemptErr, ctx.Err())
			case <-time.After(replicaL9PurgeRetryDelay):
			}
		}
	}
	return record, fmt.Errorf("%d attempt(s) failed: %w", replicaL9PurgeAttempts, attemptErr)
}
