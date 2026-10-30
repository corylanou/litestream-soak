package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const (
	followCheckType                 = "follow"
	followRestoreMode               = "follow"
	followPinnedUnsupportedFallback = "follow_pinned_unsupported"
	followPinnedUnsupportedMessage  = "cannot use follow mode with -txid"
	followInterval                  = time.Second
	followSidecarPoll               = 500 * time.Millisecond
	followPinnedProbeBudget         = 30 * time.Second
	followLevelsBudget              = 2 * time.Minute
	followStepUnsupported           = "unsupported"
)

var (
	errFollowL9Pending = errors.New("follow restore has not crossed a new L9 snapshot yet")
	// errReplicaLevelsUnavailable marks a replica level listing that could not
	// be read completely, which makes an L9 judgement inconclusive.
	errReplicaLevelsUnavailable = errors.New("replica levels unavailable")
	errFollowBehindL9           = errors.New("follow restore verified behind the new L9 snapshot")
)

// followSession is one long-running `litestream restore -f` that outlives the
// verification cycle which started it, so the primary keeps writing (and
// snapshotting) underneath it between cycles.
type followSession struct {
	path      string
	cmd       *exec.Cmd
	cancel    context.CancelFunc
	output    *lockedRestoreOutput
	done      chan struct{}
	exitErr   error
	startedAt time.Time
	startTXID uint64
	l9        reporting.ReplicaLevelSummary
	passes    int
}

type lockedRestoreOutput struct {
	mu  sync.Mutex
	out restoreOutput
}

func (l *lockedRestoreOutput) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.out.Write(p)
}

func (l *lockedRestoreOutput) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.out.text()
}

func (l *lockedRestoreOutput) finalError() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out.flushLine()
	return l.out.finalError
}

func (s *followSession) exited() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func followDBPath(dbPath string) string {
	return dbPath + ".follow"
}

func followSidecarPath(path string) string {
	return path + "-txid"
}

func removeFollowArtifacts(path string) error {
	var errs []error
	for _, name := range []string{path, path + "-wal", path + "-shm", followSidecarPath(path), followSidecarPath(path) + ".tmp"} {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// readFollowTXID reads the txid sidecar `litestream restore -f` maintains
// next to its output database. A missing sidecar reads as zero.
func readFollowTXID(path string) (uint64, error) {
	body, err := os.ReadFile(followSidecarPath(path))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	text := strings.TrimSpace(string(body))
	if text == "" {
		return 0, nil
	}
	txid, err := strconv.ParseUint(text, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("parse follow txid sidecar %q: %w", text, err)
	}
	return txid, nil
}

func followArgs(configPath string, txid uint64, followedPath, sourcePath string) []string {
	args := []string{"restore", "-config", configPath, "-f", "-follow-interval", followInterval.String()}
	if txid != 0 {
		args = append(args, "-txid", formatTXID(txid))
	}
	return append(args, "-o", followedPath, sourcePath)
}

func (v *Verifier) followCommand(ctx context.Context, args []string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, v.cfg.litestreamCommand(), args...)
	if v.cfg.ReplicaType == "s3" {
		proxyEndpoint := v.cfg.S3FaultProxyEndpoint
		if v.observeModeValidateEndpoint() != "" {
			proxyEndpoint = ""
		}
		cmd.Env = v.cfg.s3CommandEnv(proxyEndpoint)
	}
	cmd.Env = setCommandEnv(cmd.Environ(), "LOG_LEVEL", "DEBUG")
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

func (v *Verifier) startFollowProcess(args []string, path string) (*followSession, error) {
	ctx, cancel := context.WithCancel(context.Background())
	output := &lockedRestoreOutput{}
	cmd := v.followCommand(ctx, args)
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}
	session := &followSession{path: path, cmd: cmd, cancel: cancel, output: output, done: make(chan struct{}), startedAt: v.clock().UTC()}
	go func() {
		session.exitErr = cmd.Wait()
		close(session.done)
	}()
	return session, nil
}

func (s *followSession) stop() {
	if s == nil {
		return
	}
	s.cancel()
	<-s.done
	if err := removeFollowArtifacts(s.path); err != nil {
		slog.Warn("Failed to remove follow restore artifacts", "path", s.path, "error", err)
	}
}

func (v *Verifier) stopFollow() {
	if v.follow == nil {
		return
	}
	v.follow.stop()
	v.follow = nil
}

// takeExtraResults returns the follow and pre-upgrade restore results the last
// cycle produced alongside its main verification, and clears them.
func (v *Verifier) takeExtraResults() []VerificationResult {
	results := v.extraResults
	v.extraResults = nil
	return results
}

// waitForFollowTXID polls the follow sidecar until the followed database has
// applied at least txid, failing with the last observed position otherwise.
func waitForFollowTXID(ctx context.Context, session *followSession, txid uint64) (uint64, error) {
	var seen uint64
	ticker := time.NewTicker(followSidecarPoll)
	defer ticker.Stop()
	for {
		current, err := readFollowTXID(session.path)
		if err != nil {
			return seen, err
		}
		seen = current
		if seen >= txid {
			return seen, nil
		}
		if session.exited() {
			return seen, fmt.Errorf("follow restore exited at txid %s before reaching %s: %v: %s", formatTXID(seen), formatTXID(txid), session.exitErr, session.output.finalError())
		}
		select {
		case <-ctx.Done():
			return seen, fmt.Errorf("follow restore reached txid %s, waiting for %s: %w", formatTXID(seen), formatTXID(txid), ctx.Err())
		case <-ticker.C:
		}
	}
}

func l9Advanced(before, after reporting.ReplicaLevelSummary) bool {
	return after.MaxTXIDDecimal > before.MaxTXIDDecimal || (after.MaxTXIDDecimal == before.MaxTXIDDecimal && after.ObjectCount > before.ObjectCount)
}

func (v *Verifier) followStep(result *VerificationResult, name, status, output string, err error) {
	now := v.clock().UTC()
	step := reporting.VerificationStep{Name: name, StartedAt: now, CompletedAt: now, Status: status, OutputTail: tailString(output, 8192)}
	if err != nil {
		step.Error = err.Error()
	}
	result.Steps = append(result.Steps, step)
}

// runFollowCheck is called from a verification cycle while the load is paused
// and the source boundary is known. The first call starts the follow restore
// (and probes a pinned-txid follow); later calls wait for the follow to reach
// the boundary, compare it with the source, and pass only once the primary has
// written past a new L9 snapshot since the follow started or last passed.
func (v *Verifier) runFollowCheck(ctx context.Context, boundary verificationBoundary) VerificationResult {
	start := v.clock().UTC()
	result := VerificationResult{
		StartedAt:    start,
		CheckType:    followCheckType,
		Status:       "running",
		RestoreMode:  followRestoreMode,
		SyncTXID:     boundary.txid,
		BoundaryTXID: boundary.txid,
	}
	defer func() {
		RecordVerificationOutcome(result.Status, time.Since(start).Seconds())
	}()
	v.validationSteps = nil
	path := followDBPath(v.cfg.DBPath)

	if v.follow != nil && v.follow.exited() {
		err := fmt.Errorf("follow restore exited: %v: %s", v.follow.exitErr, v.follow.output.finalError())
		v.followStep(&result, "follow_process", "error", v.follow.output.text(), err)
		v.stopFollow()
		v.failResult(ctx, &result, err.Error())
		return result
	}

	if v.follow == nil {
		summary, failed := v.startFollowCheck(ctx, &result, boundary, path)
		result.Steps = append(result.Steps, v.validationSteps...)
		if errors.Is(failed, errReplicaLevelsUnavailable) {
			// The L9 baseline could not be read completely; the follow check
			// cannot judge anything this cycle and must not blame the build.
			v.stopFollow()
			result.Status = "pending"
			result.Summary = "follow check inconclusive: " + summarizeVerificationMessage(failed.Error())
			result.ErrorMessage = failed.Error()
			v.finalizeResult(&result)
			return result
		}
		if failed != nil {
			v.stopFollow()
			v.failResult(ctx, &result, failed.Error())
			return result
		}
		result.Status = "pending"
		result.Summary = summary
		v.finalizeResult(&result)
		return result
	}

	session := v.follow
	sourceSize := fileSize(v.cfg.DBPath) + fileSize(v.cfg.DBPath+"-wal")
	if err := v.runValidationPhase(ctx, "follow_catch_up", v.cfg.verificationPhaseBudget("restore", sourceSize), func(phaseCtx context.Context) error {
		_, err := waitForFollowTXID(phaseCtx, session, boundary.txid)
		return err
	}); err != nil {
		result.Steps = append(result.Steps, v.validationSteps...)
		v.stopFollow()
		v.failResult(ctx, &result, err.Error())
		return result
	}
	if err := v.runValidationPhase(ctx, "follow_compare", v.cfg.verificationPhaseBudget("compare", sourceSize), func(phaseCtx context.Context) error {
		return v.compareRestoredLogical(phaseCtx, boundary.logicalSnapshot, path)
	}); err != nil {
		result.Steps = append(result.Steps, v.validationSteps...)
		v.stopFollow()
		v.failResult(ctx, &result, fmt.Sprintf("follow restore diverged from source at txid %s: %v", formatTXID(boundary.txid), err))
		return result
	}
	var current reporting.ReplicaLevelSummary
	l9Err := v.runValidationPhase(ctx, "follow_l9", followLevelsBudget, func(context.Context) error {
		report := v.replicaLevels(v.cfg, boundary.txid)
		if !usableReplicaLevelReport(report) {
			return fmt.Errorf("%w: %s", errReplicaLevelsUnavailable, replicaLevelReportProblem(report))
		}
		current = replicaLevelSummary(report, 9)
		if !l9Advanced(session.l9, current) {
			return errFollowL9Pending
		}
		if boundary.txid < current.MaxTXIDDecimal {
			return errFollowBehindL9
		}
		return nil
	})
	v.validationSteps[len(v.validationSteps)-1].OutputTail = fmt.Sprintf("L9 at follow %s: objects=%d max_txid=%s; now objects=%d max_txid=%s; follower verified at txid %s", followMark(session), session.l9.ObjectCount, formatTXID(session.l9.MaxTXIDDecimal), current.ObjectCount, formatTXID(current.MaxTXIDDecimal), formatTXID(boundary.txid))
	result.Steps = append(result.Steps, v.validationSteps...)
	switch {
	case errors.Is(l9Err, errFollowL9Pending):
		result.Steps[len(result.Steps)-1].Status = "pending"
		result.Status = "pending"
		result.Summary = fmt.Sprintf("follow restore matched source at txid %s; no new L9 snapshot since %s (L9 max_txid %s)", formatTXID(boundary.txid), followMark(session), formatTXID(session.l9.MaxTXIDDecimal))
		v.finalizeResult(&result)
	case errors.Is(l9Err, errFollowBehindL9):
		result.Steps[len(result.Steps)-1].Status = "pending"
		result.Status = "pending"
		result.Summary = fmt.Sprintf("follow restore matched source at txid %s, which is behind the new L9 snapshot at %s; crossing not yet verified", formatTXID(boundary.txid), formatTXID(current.MaxTXIDDecimal))
		v.finalizeResult(&result)
	case l9Err != nil:
		result.Status = "pending"
		result.Summary = "follow check inconclusive: " + summarizeVerificationMessage(l9Err.Error())
		result.ErrorMessage = l9Err.Error()
		v.finalizeResult(&result)
	default:
		result.Status = "passed"
		result.Passed = true
		result.Summary = fmt.Sprintf("follow restore matched source at txid %s after crossing the L9 snapshot that advanced from %s to %s", formatTXID(boundary.txid), formatTXID(session.l9.MaxTXIDDecimal), formatTXID(current.MaxTXIDDecimal))
		session.l9 = current
		session.passes++
		v.finalizeResult(&result)
	}
	return result
}

func followMark(session *followSession) string {
	if session.passes == 0 {
		return "follow start"
	}
	return "last pass"
}

// startFollowCheck probes the pinned-txid follow, then starts the latest
// follow, waits for its initial restore to reach the boundary and compares it
// with the source so a divergence is caught before any L9 crossing.
func (v *Verifier) startFollowCheck(ctx context.Context, result *VerificationResult, boundary verificationBoundary, path string) (string, error) {
	configPath, cleanupConfig, err := v.validateConfigPath(v.cfg.DBPath)
	if err != nil {
		return "", err
	}
	defer cleanupConfig()
	sourceSize := fileSize(v.cfg.DBPath) + fileSize(v.cfg.DBPath+"-wal")

	pinnedNote, err := v.followPinned(ctx, result, boundary, configPath, path+".pinned", sourceSize)
	if err != nil {
		return "", err
	}

	var baseline reporting.ReplicaLevelSummary
	if err := v.runValidationPhase(ctx, "follow_start", followLevelsBudget, func(context.Context) error {
		if err := removeFollowArtifacts(path); err != nil {
			return fmt.Errorf("clean follow artifacts: %w", err)
		}
		report := v.replicaLevels(v.cfg, boundary.txid)
		if !usableReplicaLevelReport(report) {
			return fmt.Errorf("%w: %s", errReplicaLevelsUnavailable, replicaLevelReportProblem(report))
		}
		baseline = replicaLevelSummary(report, 9)
		session, err := v.startFollowProcess(followArgs(configPath, 0, path, v.cfg.DBPath), path)
		if err != nil {
			return fmt.Errorf("start follow restore: %w", err)
		}
		session.startTXID = boundary.txid
		session.l9 = baseline
		v.follow = session
		return nil
	}); err != nil {
		return "", err
	}
	v.validationSteps[len(v.validationSteps)-1].Command = append([]string{v.cfg.litestreamCommand()}, followArgs(configPath, 0, path, v.cfg.DBPath)...)
	v.validationSteps[len(v.validationSteps)-1].OutputTail = fmt.Sprintf("L9 at follow start: objects=%d max_txid=%s", baseline.ObjectCount, formatTXID(baseline.MaxTXIDDecimal))

	session := v.follow
	if err := v.runValidationPhase(ctx, "follow_initial", v.cfg.verificationPhaseBudget("restore", sourceSize), func(phaseCtx context.Context) error {
		_, err := waitForFollowTXID(phaseCtx, session, boundary.txid)
		return err
	}); err != nil {
		v.validationSteps[len(v.validationSteps)-1].OutputTail = session.output.text()
		return "", err
	}
	if err := v.runValidationPhase(ctx, "follow_compare", v.cfg.verificationPhaseBudget("compare", sourceSize), func(phaseCtx context.Context) error {
		return v.compareRestoredLogical(phaseCtx, boundary.logicalSnapshot, path)
	}); err != nil {
		return "", fmt.Errorf("follow restore diverged from source at txid %s: %v", formatTXID(boundary.txid), err)
	}
	summary := fmt.Sprintf("follow restore started and matched source at txid %s; awaiting a new L9 snapshot past max_txid %s", formatTXID(boundary.txid), formatTXID(baseline.MaxTXIDDecimal))
	if pinnedNote != "" {
		summary += "; " + pinnedNote
	}
	return summary, nil
}

// followPinned runs the pinned-txid follow variant. Litestream currently
// rejects `-f` together with `-txid`; that rejection is recorded as an
// unsupported step rather than a failure. If a build accepts it, the follow
// must reach exactly the boundary and match the source there.
func (v *Verifier) followPinned(ctx context.Context, result *VerificationResult, boundary verificationBoundary, configPath, path string, sourceSize int64) (string, error) {
	if err := removeFollowArtifacts(path); err != nil {
		return "", fmt.Errorf("clean pinned follow artifacts: %w", err)
	}
	args := followArgs(configPath, boundary.txid, path, v.cfg.DBPath)
	session, err := v.startFollowProcess(args, path)
	if err != nil {
		return "", fmt.Errorf("start pinned follow restore: %w", err)
	}
	defer session.stop()
	command := append([]string{v.cfg.litestreamCommand()}, args...)

	probeCtx, cancel := context.WithTimeout(ctx, followPinnedProbeBudget)
	defer cancel()
	reached, waitErr := waitForFollowTXID(probeCtx, session, boundary.txid)
	output := session.output.text()
	switch {
	case waitErr != nil && session.exited() && strings.Contains(output, followPinnedUnsupportedMessage):
		v.followStep(result, "follow_pinned", followStepUnsupported, output, nil)
		result.Steps[len(result.Steps)-1].Command = command
		result.RestoreModeFallback = followPinnedUnsupportedFallback
		v.logicalEvidence += " follow_pinned=unsupported"
		return "pinned-txid follow unsupported by litestream (" + followPinnedUnsupportedMessage + "); latest follow only", nil
	case waitErr != nil:
		v.followStep(result, "follow_pinned", "error", output, waitErr)
		result.Steps[len(result.Steps)-1].Command = command
		return "", fmt.Errorf("pinned follow restore: %w", waitErr)
	}
	compareCtx, cancelCompare := context.WithTimeout(ctx, v.cfg.verificationPhaseBudget("compare", sourceSize))
	defer cancelCompare()
	if err := v.compareRestoredLogical(compareCtx, boundary.logicalSnapshot, path); err != nil {
		err = fmt.Errorf("pinned follow restore diverged from source at txid %s: %v", formatTXID(boundary.txid), err)
		v.followStep(result, "follow_pinned", "error", output, err)
		result.Steps[len(result.Steps)-1].Command = command
		return "", err
	}
	v.followStep(result, "follow_pinned", "ok", output, nil)
	result.Steps[len(result.Steps)-1].Command = command
	v.logicalEvidence += fmt.Sprintf(" follow_pinned=%s", formatTXID(reached))
	return fmt.Sprintf("pinned-txid follow matched source at txid %s", formatTXID(reached)), nil
}
