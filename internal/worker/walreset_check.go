package worker

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	_ "modernc.org/sqlite"
)

// The wal reset check reproduces the shape reported in
// benbjohnson/litestream#1540: an application writes a sustained burst with
// SQLite's default autocheckpoint (1000 pages) and no journal_size_limit,
// Litestream is stopped with SIGTERM mid-burst so the position it saves sits
// deep in the WAL, the application keeps writing while Litestream is down so
// its autocheckpoints restart the WAL several times, each new generation
// shorter than the saved position while the physical file keeps its size, so
// the stale frame at the saved position survives, and Litestream is started
// again. A build that resumes incrementally from that stale frame replicates
// a database whose latest restore fails a full integrity check; a build that
// proves continuity takes a snapshot instead.
const (
	walResetCheckType   = "walreset"
	walResetRestoreMode = "walreset-restore"
	walResetStep        = "wal_reset"
	walResetTable       = "wal_reset_burst"
	// walResetPayloadBytes keeps one row close to one 4 KiB page so page
	// counts and row counts track each other.
	walResetPayloadBytes = 3500
	walResetBatchRows    = 64
	// walResetCyclePause is the gap between cycles; with a cycle of four to
	// eight minutes that is a cycle roughly every half hour, about 45 a day.
	walResetCyclePause = 25 * time.Minute
	// walResetBurstBytes is how deep the WAL must be past Litestream's last
	// restart before SIGTERM: at least three autocheckpoint generations, so
	// no post-stop generation can grow back to the saved position.
	walResetBurstBytes    = 12 << 20
	walResetBurstBudget   = 3 * time.Minute
	walResetMinResets     = 3
	walResetTargetResets  = 5
	walResetDownBudget    = 4 * time.Minute
	walResetHeaderPoll    = 50 * time.Millisecond
	walResetStopBudget    = 3 * time.Minute
	walResetStartBudget   = 3 * time.Minute
	walResetCatchUpBudget = 10 * time.Minute
	walResetCatchUpPoll   = 2 * time.Second
	walResetGateBudget    = 15 * time.Minute
	walResetBusyTimeout   = 5 * time.Second
	// walResetSnapshotSlack is how much younger than the snapshot interval
	// the previous L9 must be for a new L9 to be attributed to the reset
	// rather than to the schedule.
	walResetSnapshotSlack = 10 * time.Minute
)

var (
	errWALResetShapeNotReached      = errors.New("wal reset shape not reached")
	errWALResetLitestreamRestart    = errors.New("wal reset litestream restart failed")
	errWALResetCatchUpTimeout       = errors.New("wal reset catch-up timeout")
	errWALResetUnavailable          = errors.New("wal reset check unavailable")
	errWALResetSnapshotInterference = errors.New("wal reset snapshot interference")
	errWALResetGateBusy             = errors.New("verification held the check gate")
)

// walResetCount is the number of WAL resets between two valid headers,
// taken from salt-1, which SQLite increments on each reset.
func walResetCount(before, after walHeader) uint32 {
	if !before.valid || !after.valid {
		return 0
	}
	return after.salt1 - before.salt1
}

// walResetL9 is one listing of the replica's snapshot level.
type walResetL9 struct {
	count    int
	newestAt time.Time
	known    bool
}

// walResetObservation is what one cycle measured on the WAL and the replica.
type walResetObservation struct {
	savedHeader walHeader
	// savedPosition is the WAL offset Litestream resumes from: the end of
	// the newest local L0 LTX file when it can be read, otherwise the end of
	// the committed frames at the moment Litestream exited.
	savedPosition       int64
	savedPositionSource string
	savedPositionSalt1  uint32
	// savedAtSignal and savedAtExit bracket the committed WAL end around the
	// SIGTERM, since the final sync lands somewhere between the two.
	savedAtSignal int64
	savedAtExit   int64
	finalWAL      walGeneration
	// maxGenerationEnd is the furthest committed frame end seen in any
	// generation that started after the stop.
	maxGenerationEnd int64
	resets           uint32
	checkpoints      uint32
	generations      int
	burstRows        int
	downRows         int
	downDuration     time.Duration
	l9Before         walResetL9
	l9After          walResetL9
	snapshotInterval time.Duration
	uploadedBytes    uint64
	uploadedKnown    bool
	syncTXID         uint64
	replicatedTXID   uint64
	catchUpDuration  time.Duration
	restarted        bool
}

func (o walResetObservation) summary() string {
	parts := []string{
		fmt.Sprintf("wal_resets=%d", o.resets),
		fmt.Sprintf("checkpoints=%d", o.checkpoints),
		fmt.Sprintf("generations_seen=%d", o.generations),
		fmt.Sprintf("saved_position=%d saved_position_source=%s saved_at_signal=%d saved_at_exit=%d", o.savedPosition, firstNonEmpty(o.savedPositionSource, "unknown"), o.savedAtSignal, o.savedAtExit),
		fmt.Sprintf("max_generation_end=%d final_generation_end=%d final_generation_frames=%d final_wal_bytes=%d", o.maxGenerationEnd, o.finalWAL.generationEnd(), o.finalWAL.committedFrames, o.finalWAL.fileBytes),
		fmt.Sprintf("burst_rows=%d", o.burstRows),
		fmt.Sprintf("down_rows=%d", o.downRows),
		fmt.Sprintf("down_seconds=%.1f", o.downDuration.Seconds()),
	}
	if o.savedHeader.valid {
		parts = append(parts, fmt.Sprintf("salt1_before=%d salt1_after=%d ckpt_seq_before=%d ckpt_seq_after=%d", o.savedHeader.salt1, o.finalWAL.header.salt1, o.savedHeader.checkpointSeq, o.finalWAL.header.checkpointSeq))
	}
	switch {
	case o.l9Before.known && o.l9After.known:
		parts = append(parts, fmt.Sprintf("l9_before=%d l9_after=%d l9_new=%s", o.l9Before.count, o.l9After.count, o.l9Attribution()))
	default:
		parts = append(parts, "l9_before=unknown l9_after=unknown")
	}
	if o.uploadedKnown {
		parts = append(parts, fmt.Sprintf("uploaded_bytes=%d", o.uploadedBytes))
	}
	if o.syncTXID != 0 {
		parts = append(parts, fmt.Sprintf("catchup_txid=%s replicated_txid=%s catchup_seconds=%.1f", formatTXID(o.syncTXID), formatTXID(o.replicatedTXID), o.catchUpDuration.Seconds()))
	}
	return strings.Join(parts, " ")
}

// l9Attribution names what a new L9 during the cycle was: none, the
// reset-triggered snapshot (the previous L9 was too young for the schedule
// to have fired), or scheduled (the previous L9 had reached the interval).
func (o walResetObservation) l9Attribution() string {
	switch {
	case !o.l9Before.known || !o.l9After.known || o.l9After.count <= o.l9Before.count:
		return "none"
	case o.l9Before.count > 0 && !o.l9Before.newestAt.IsZero() && o.snapshotInterval > 0 && time.Since(o.l9Before.newestAt) < o.snapshotInterval-walResetSnapshotSlack:
		return "reset"
	default:
		return "scheduled"
	}
}

// shapeError reports why the cycle did not reproduce the reported shape, or
// nothing when it did: at least walResetMinResets resets while Litestream
// was down, Litestream's saved position still inside the physical file, and
// every post-stop generation ending before that position, so the frame there
// belongs to an older generation.
func (o walResetObservation) shapeError() error {
	switch {
	case !o.savedHeader.valid:
		return fmt.Errorf("%w: wal header unreadable at stop", errWALResetShapeNotReached)
	case !o.finalWAL.header.valid:
		return fmt.Errorf("%w: wal header unreadable after the down window (wal %d bytes)", errWALResetShapeNotReached, o.finalWAL.fileBytes)
	case o.resets < walResetMinResets:
		return fmt.Errorf("%w: %d wal resets while litestream was down, need at least %d", errWALResetShapeNotReached, o.resets, walResetMinResets)
	case o.savedPosition <= walHeaderSize:
		return fmt.Errorf("%w: litestream saved position unknown", errWALResetShapeNotReached)
	case o.savedPosition > o.finalWAL.fileBytes:
		return fmt.Errorf("%w: the wal shrank to %d bytes below litestream's saved position %d, so the stale frame is gone", errWALResetShapeNotReached, o.finalWAL.fileBytes, o.savedPosition)
	case o.maxGenerationEnd >= o.savedPosition:
		return fmt.Errorf("%w: a post-stop wal generation reached %d bytes, at or past litestream's saved position %d, so the frame there was rewritten", errWALResetShapeNotReached, o.maxGenerationEnd, o.savedPosition)
	default:
		return nil
	}
}

// walResetValidation is the outcome of the restore step: the pinned restore
// at the boundary txid and the unpinned latest restore, which validation
// phase failed, and the steps both produced.
type walResetValidation struct {
	phase        string
	boundary     uint64
	pinnedPassed bool
	latestPassed bool
	latestTXID   uint64
	steps        []reporting.VerificationStep
}

func (v walResetValidation) summary() string {
	status := func(passed bool) string {
		if passed {
			return "passed"
		}
		return "failed"
	}
	return fmt.Sprintf("pinned_restore=%s latest_restore=%s restored_txid=%s latest_txid=%s", status(v.pinnedPassed), status(v.latestPassed), formatTXID(v.boundary), formatTXID(v.latestTXID))
}

// walResetCheck runs the cycle on a schedule. Every external interaction is a
// function so the state machine can be driven in tests.
type walResetCheck struct {
	cfg   Config
	clock func() time.Time

	burst         func(ctx context.Context, rows int) (int, error)
	generation    func() (walGeneration, error)
	savedPosition func() (litestreamSavedPosition, error)
	stop          func(ctx context.Context) error
	start         func(ctx context.Context) error
	catchUp       func(ctx context.Context) (syncResponse, error)
	validate      func(ctx context.Context, result *VerificationResult) (walResetValidation, error)
	l9            func() walResetL9
	uploaded      func() (uint64, bool)
	acquireGate   func(ctx context.Context) (func(), error)
	downtime      func(bool)

	pause         time.Duration
	burstBytes    int64
	burstBudget   time.Duration
	downBudget    time.Duration
	headerPoll    time.Duration
	stopBudget    time.Duration
	startBudget   time.Duration
	catchUpBudget time.Duration
	catchUpPoll   time.Duration
	gateBudget    time.Duration
	minResets     uint32
	targetResets  uint32

	mu     sync.Mutex
	stopFn context.CancelFunc
	done   chan struct{}
}

func newWALResetCheck(cfg Config) *walResetCheck {
	check := &walResetCheck{
		cfg:           cfg,
		clock:         time.Now,
		pause:         walResetCyclePause,
		burstBytes:    walResetBurstBytes,
		burstBudget:   walResetBurstBudget,
		downBudget:    walResetDownBudget,
		headerPoll:    walResetHeaderPoll,
		stopBudget:    walResetStopBudget,
		startBudget:   walResetStartBudget,
		catchUpBudget: walResetCatchUpBudget,
		catchUpPoll:   walResetCatchUpPoll,
		gateBudget:    walResetGateBudget,
		minResets:     walResetMinResets,
		targetResets:  walResetTargetResets,
		l9:            func() walResetL9 { return replicaL9(cfg) },
		uploaded:      func() (uint64, bool) { return 0, false },
		acquireGate:   func(context.Context) (func(), error) { return func() {}, nil },
		downtime:      func(bool) {},
	}
	check.burst = func(ctx context.Context, rows int) (int, error) { return writeWALResetBurst(ctx, cfg.DBPath, rows) }
	check.generation = func() (walGeneration, error) { return readWALGeneration(cfg.DBPath + "-wal") }
	check.savedPosition = func() (litestreamSavedPosition, error) { return readLitestreamSavedPosition(cfg.DBPath) }
	check.stop = func(context.Context) error { return errWALResetUnavailable }
	check.start = func(context.Context) error { return errWALResetUnavailable }
	check.catchUp = func(ctx context.Context) (syncResponse, error) {
		return requestLitestreamSync(ctx, newIPCClient(cfg.SocketPath, walResetCatchUpPoll+30*time.Second), cfg.DBPath, walResetCatchUpPoll+30*time.Second)
	}
	return check
}

func (c *walResetCheck) Start(ctx context.Context, report func(VerificationResult)) {
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.stopFn = cancel
	c.done = make(chan struct{})
	c.mu.Unlock()
	go func() {
		defer close(c.done)
		for {
			if err := sleepContext(runCtx, c.pause); err != nil {
				return
			}
			result, skipped := c.cycle(runCtx)
			if runCtx.Err() != nil {
				return
			}
			if !skipped {
				report(result)
			}
		}
	}()
}

func (c *walResetCheck) Stop() {
	c.mu.Lock()
	stop, done := c.stopFn, c.done
	c.mu.Unlock()
	if stop != nil {
		stop()
	}
	if done != nil {
		<-done
	}
}

// cycle runs one reproduction and returns its verification; skipped is true
// when the verifier held the check gate for the whole wait, in which case
// nothing is reported and the next cycle tries again.
func (c *walResetCheck) cycle(ctx context.Context) (VerificationResult, bool) {
	startedAt := c.clock().UTC()
	gateCtx, cancel := context.WithTimeout(ctx, c.gateBudget)
	release, err := c.acquireGate(gateCtx)
	cancel()
	if err != nil {
		slog.Warn("WAL reset cycle skipped: verification held the check gate", "error", err)
		return VerificationResult{}, true
	}
	defer release()
	observation, validation, err := c.run(ctx)
	return walResetResult(observation, validation, err, startedAt, c.clock().UTC()), false
}

func (c *walResetCheck) run(ctx context.Context) (observation walResetObservation, validation walResetValidation, err error) {
	observation.snapshotInterval = c.cfg.SnapshotInterval
	uploadedBefore, uploadedKnown := c.uploaded()

	// (1) Burst with Litestream running until the WAL is deep enough past its
	// last restart that no post-stop generation can reach the saved position.
	burstCtx, cancelBurst := context.WithTimeout(ctx, c.burstBudget)
	defer cancelBurst()
	for {
		wal, walErr := c.generation()
		if walErr != nil {
			return observation, validation, fmt.Errorf("%w: read wal: %v", errWALResetUnavailable, walErr)
		}
		if wal.header.valid && wal.generationEnd() >= c.burstBytes {
			break
		}
		if ctx.Err() != nil {
			return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, ctx.Err())
		}
		if burstCtx.Err() != nil {
			return observation, validation, fmt.Errorf("%w: wal reached %d of %d bytes within %s", errWALResetShapeNotReached, wal.generationEnd(), c.burstBytes, c.burstBudget)
		}
		rows, burstErr := c.burst(burstCtx, walResetBatchRows)
		observation.burstRows += rows
		if burstErr != nil {
			if ctx.Err() != nil {
				return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, ctx.Err())
			}
			return observation, validation, fmt.Errorf("%w: burst write: %v", errWALResetUnavailable, burstErr)
		}
	}
	observation.l9Before = c.l9()
	uploadedAtStop, _ := c.uploaded()

	// (2) SIGTERM Litestream mid-burst. From here on Litestream is started
	// again on every exit path, so the scheduled checks never meet a stopped
	// process.
	c.downtime(true)
	defer c.downtime(false)
	signalWAL, walErr := c.generation()
	if walErr != nil {
		return observation, validation, fmt.Errorf("%w: read wal: %v", errWALResetUnavailable, walErr)
	}
	observation.savedHeader = signalWAL.header
	observation.savedAtSignal = signalWAL.generationEnd()
	stopCtx, cancelStop := context.WithTimeout(ctx, c.stopBudget)
	err = c.stop(stopCtx)
	cancelStop()
	if err != nil {
		return observation, validation, fmt.Errorf("%w: stop: %v", errWALResetLitestreamRestart, err)
	}
	downStarted := c.clock()
	defer func() {
		if observation.restarted {
			return
		}
		if restartErr := c.restart(ctx, &observation); restartErr != nil {
			if err == nil {
				err = restartErr
			} else {
				slog.Error("WAL reset cycle could not restart Litestream after a failure", "error", restartErr, "cycle_error", err)
			}
		}
	}()
	exitWAL, walErr := c.generation()
	if walErr != nil {
		return observation, validation, fmt.Errorf("%w: read wal after stop: %v", errWALResetUnavailable, walErr)
	}
	observation.savedAtExit = exitWAL.generationEnd()
	observation.savedPosition, observation.savedPositionSource, observation.savedPositionSalt1 = c.resolveSavedPosition(exitWAL)

	// (3) Keep writing while Litestream is down until the WAL has restarted
	// the target number of times, polling the header for every generation
	// and recording how far each one got.
	observation.generations = 1
	lastSalt2 := observation.savedHeader.salt2
	downCtx, cancelDown := context.WithTimeout(ctx, c.downBudget)
	defer cancelDown()
	for {
		wal, walErr := c.generation()
		if walErr != nil {
			return observation, validation, fmt.Errorf("%w: read wal while litestream was down: %v", errWALResetUnavailable, walErr)
		}
		if wal.header.valid {
			observation.finalWAL = wal
			if wal.header.salt2 != lastSalt2 {
				observation.generations++
				lastSalt2 = wal.header.salt2
			}
			if walResetCount(observation.savedHeader, wal.header) > 0 {
				observation.maxGenerationEnd = max(observation.maxGenerationEnd, wal.generationEnd())
			}
			if walResetCount(observation.savedHeader, wal.header) >= c.targetResets {
				break
			}
		}
		if ctx.Err() != nil {
			return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, ctx.Err())
		}
		if downCtx.Err() != nil {
			break
		}
		rows, burstErr := c.burst(downCtx, walResetBatchRows)
		observation.downRows += rows
		if burstErr != nil {
			if ctx.Err() != nil {
				return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, ctx.Err())
			}
			if downCtx.Err() != nil {
				break
			}
			return observation, validation, fmt.Errorf("%w: burst write while litestream was down: %v", errWALResetUnavailable, burstErr)
		}
		if err := sleepContext(ctx, c.headerPoll); err != nil {
			return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, err)
		}
	}
	observation.downDuration = c.clock().Sub(downStarted)
	if wal, walErr := c.generation(); walErr == nil && wal.header.valid {
		observation.finalWAL = wal
		if walResetCount(observation.savedHeader, wal.header) > 0 {
			observation.maxGenerationEnd = max(observation.maxGenerationEnd, wal.generationEnd())
		}
	}
	observation.resets = walResetCount(observation.savedHeader, observation.finalWAL.header)
	if observation.finalWAL.header.valid {
		observation.checkpoints = observation.finalWAL.header.checkpointSeq - observation.savedHeader.checkpointSeq
	}

	// (4) Restart Litestream and wait until it has caught up with the primary.
	if err = c.restart(ctx, &observation); err != nil {
		return observation, validation, err
	}
	if shapeErr := observation.shapeError(); shapeErr != nil {
		return observation, validation, shapeErr
	}
	catchUpStarted := c.clock()
	catchUpCtx, cancelCatchUp := context.WithTimeout(ctx, c.catchUpBudget)
	defer cancelCatchUp()
	for {
		response, syncErr := c.catchUp(catchUpCtx)
		if syncErr == nil {
			observation.syncTXID, observation.replicatedTXID = response.TXID, response.ReplicatedTXID
			if response.TXID != 0 && response.ReplicatedTXID >= response.TXID {
				break
			}
		}
		if err := sleepContext(catchUpCtx, c.catchUpPoll); err != nil {
			if ctx.Err() != nil {
				return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, ctx.Err())
			}
			detail := fmt.Sprintf("replicated txid %s behind primary txid %s after %s", formatTXID(observation.replicatedTXID), formatTXID(observation.syncTXID), c.catchUpBudget)
			if syncErr != nil {
				detail = fmt.Sprintf("last sync error after %s: %v", c.catchUpBudget, syncErr)
			}
			return observation, validation, fmt.Errorf("%w: %s", errWALResetCatchUpTimeout, detail)
		}
	}
	observation.catchUpDuration = c.clock().Sub(catchUpStarted)
	observation.l9After = c.l9()
	if uploadedAfter, ok := c.uploaded(); ok && uploadedKnown {
		observation.uploadedBytes, observation.uploadedKnown = walResetUploadedBytes(uploadedBefore, uploadedAtStop, uploadedAfter), true
	}
	// A scheduled snapshot inside the cycle rewrites the replica from the
	// database and hides whatever the restart replicated; only the
	// reset-triggered snapshot the fix takes is part of the shape.
	if observation.l9Attribution() == "scheduled" {
		return observation, validation, fmt.Errorf("%w: a new L9 appeared during the cycle (%d -> %d) and the previous one was old enough for the %s schedule, so the replica may have been repaired by a scheduled snapshot", errWALResetSnapshotInterference, observation.l9Before.count, observation.l9After.count, observation.snapshotInterval)
	}

	// (5) Restore the latest replicated position, pinned and unpinned, and
	// compare both with the primary at that txid.
	result := VerificationResult{CheckType: walResetCheckType}
	validation, err = c.validate(ctx, &result)
	return observation, validation, err
}

// restart starts Litestream again after the down window and marks the cycle
// as restarted; a start that fails is a restart failure.
func (c *walResetCheck) restart(ctx context.Context, observation *walResetObservation) error {
	startCtx, cancelStart := context.WithTimeout(context.WithoutCancel(ctx), c.startBudget)
	defer cancelStart()
	err := c.start(startCtx)
	if err != nil {
		return fmt.Errorf("%w: start: %v", errWALResetLitestreamRestart, err)
	}
	observation.restarted = true
	c.downtime(false)
	return nil
}

// resolveSavedPosition picks the WAL position Litestream resumes from: the
// end of its newest local L0 LTX file when that file was written from the
// generation that was current at the stop, otherwise the committed WAL end
// at Litestream's exit, which its final sync cannot have passed.
func (c *walResetCheck) resolveSavedPosition(exitWAL walGeneration) (int64, string, uint32) {
	if position, err := c.savedPosition(); err == nil && position.found && position.salt1 == exitWAL.header.salt1 && position.salt2 == exitWAL.header.salt2 && position.walOffsetEnd > walHeaderSize {
		return position.walOffsetEnd, "ltx:" + position.file, position.salt1
	} else if err != nil {
		slog.Warn("WAL reset cycle could not read litestream's saved position; using the wal end at exit", "error", err)
	}
	return exitWAL.generationEnd(), "wal_end_at_exit", exitWAL.header.salt1
}

// walResetUploadedBytes sums the bytes uploaded during a cycle from counter
// readings taken at the start, just before the stop, and at the end. A
// counter that survives the restart (the observe proxy) only moves forward;
// Litestream's own counter starts again at zero, which shows as a reading
// after the restart that is below the reading at the stop.
func walResetUploadedBytes(before, atStop, after uint64) uint64 {
	if after >= atStop {
		return after - before
	}
	return (atStop - before) + after
}

func walResetResult(observation walResetObservation, validation walResetValidation, err error, startedAt, completedAt time.Time) VerificationResult {
	summary := observation.summary()
	if validation.boundary != 0 || validation.pinnedPassed || validation.latestPassed {
		summary += " " + validation.summary()
	}
	result := VerificationResult{
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		CheckType:    walResetCheckType,
		RestoreMode:  walResetRestoreMode,
		DurationMS:   int(completedAt.Sub(startedAt).Milliseconds()),
		SyncTXID:     observation.syncTXID,
		BoundaryTXID: validation.boundary,
		Steps:        []reporting.VerificationStep{{Name: walResetStep, StartedAt: startedAt, CompletedAt: completedAt, Status: "ok", OutputTail: tailString(summary, 8192)}},
	}
	result.Steps = append(result.Steps, validation.steps...)
	switch {
	case err == nil:
		result.Status = "passed"
		result.Passed = true
		result.Summary = fmt.Sprintf("wal reset check passed: pinned and latest restores at txid %s are intact and match the primary after %d wal resets while litestream was down (%s)", formatTXID(validation.boundary), observation.resets, summary)
	case walResetInconclusive(err):
		message := "wal reset check skipped: " + err.Error()
		result.Steps[0].Status = "inconclusive"
		result.Steps[0].Error = message
		result.Status = "aborted"
		result.ErrorMessage = message
		result.Summary = "verification inconclusive: " + message + " (" + summary + ")"
		result.InconclusiveClassification = &reporting.FailureClassification{Stage: walResetStep, Signature: walResetInconclusiveSignature(err), RestoreMode: walResetRestoreMode}
	default:
		message := walResetFailureMessage(validation.phase, err)
		result.Steps[0].Status = "error"
		result.Steps[0].Error = message
		result.Status = "failed"
		result.ErrorMessage = message
		result.Summary = fmt.Sprintf("wal reset check failed after %d wal resets while litestream was down (%s)", observation.resets, summary)
	}
	return result
}

func walResetInconclusive(err error) bool {
	return errors.Is(err, errWALResetShapeNotReached) || errors.Is(err, errWALResetLitestreamRestart) ||
		errors.Is(err, errWALResetCatchUpTimeout) || errors.Is(err, errWALResetUnavailable) ||
		errors.Is(err, errWALResetSnapshotInterference) || errors.Is(err, errWALResetGateBusy)
}

func walResetInconclusiveSignature(err error) string {
	switch {
	case errors.Is(err, errWALResetShapeNotReached):
		return reporting.SignatureWALResetShapeNotReached
	case errors.Is(err, errWALResetLitestreamRestart):
		return reporting.SignatureWALResetLitestreamRestartFailed
	case errors.Is(err, errWALResetCatchUpTimeout):
		return reporting.SignatureWALResetCatchUpTimeout
	case errors.Is(err, errWALResetSnapshotInterference):
		return reporting.SignatureWALResetSnapshotInterference
	default:
		return reporting.SignatureWALResetUnavailable
	}
}

// walResetFailureMessage prefixes the validation failure with its class so
// restore, integrity, and logical failures classify to distinct signatures;
// the latest-restore phases carry a "(latest)" marker.
func walResetFailureMessage(phase string, err error) string {
	variant := ""
	if strings.HasPrefix(phase, "latest_") {
		variant = " (latest)"
		phase = strings.TrimPrefix(phase, "latest_")
	}
	switch phase {
	case "integrity_check":
		return "wal reset integrity failed" + variant + ": " + err.Error()
	case "compare":
		return "wal reset logical mismatch" + variant + ": " + err.Error()
	default:
		return "wal reset restore failed" + variant + ": " + err.Error()
	}
}

// ensureWALResetTable creates the burst table during populate, before
// Litestream starts, so every cycle's rows land in an existing table.
func ensureWALResetTable(ctx context.Context, path string) error {
	db, err := openWALResetDB(path, "rwc")
	if err != nil {
		return fmt.Errorf("open wal reset table: %w", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+walResetTable+" (id INTEGER PRIMARY KEY, written_at INTEGER NOT NULL, payload BLOB NOT NULL)"); err != nil {
		return fmt.Errorf("create wal reset table: %w", err)
	}
	return nil
}

func openWALResetDB(path, mode string) (*sql.DB, error) {
	uri := url.URL{Scheme: "file", Path: path, RawQuery: fmt.Sprintf("mode=%s&_pragma=busy_timeout(%d)", mode, walResetBusyTimeout.Milliseconds())}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// writeWALResetBurst commits rows of random payload in one transaction on a
// connection with SQLite's default autocheckpoint and no journal_size_limit,
// which is the application in the report: once Litestream's reader is gone,
// the connection's own autocheckpoint restarts the WAL every 1000 pages and
// leaves the physical file at its full size.
func writeWALResetBurst(ctx context.Context, path string, rows int) (int, error) {
	defer harnessWriters.begin(walResetTable, path)()
	db, err := openWALResetDB(path, "rw")
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	payload := make([]byte, walResetPayloadBytes)
	written := 0
	for i := 0; i < rows; i++ {
		if _, err := rand.Read(payload); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+walResetTable+" (written_at, payload) VALUES (?, ?)", time.Now().UnixNano(), payload); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		written++
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return written, nil
}

// replicaL9 lists the replica's levels with the litestream binary and returns
// the snapshot level's object count and the creation time of its newest
// object.
func replicaL9(cfg Config) walResetL9 {
	command := []string{cfg.litestreamCommand(), "ltx", "-config", cfg.ConfigPath, "-level", "all", cfg.DBPath}
	listing := listReplicaLTX(command)
	if listing.err != "" || !listing.complete {
		return walResetL9{}
	}
	level := walResetL9{known: true}
	for _, candidate := range listing.candidates {
		if candidate.level != ltxSnapshotLevel {
			continue
		}
		level.count++
		if at, err := time.Parse(time.RFC3339, candidate.createdAt); err == nil && at.After(level.newestAt) {
			level.newestAt = at
		}
	}
	return level
}

// walResetValidator restores the latest replicated position with its own
// verifier so the cycle shares nothing with the scheduled verification: it
// pauses the load, syncs to the primary's txid, restores at that txid with
// the full integrity check and the logical comparison, then restores the
// unpinned latest position (the path the report used) and validates it the
// same way against the same boundary, and reports which phase failed.
func walResetValidator(cfg Config, objectStore *objectStoreMeter, pausers []loadPauser) func(context.Context, *VerificationResult) (walResetValidation, error) {
	return func(ctx context.Context, result *VerificationResult) (walResetValidation, error) {
		verifier := NewVerifier(cfg, pausers...)
		if objectStore != nil {
			verifier.objectStore = objectStore
		}
		pinnedPath := cfg.DBPath + ".walreset.restored"
		latestPath := cfg.DBPath + ".walreset.latest.restored"
		for _, path := range []string{pinnedPath, latestPath} {
			if err := removeRestoredArtifacts(path); err != nil {
				return walResetValidation{}, fmt.Errorf("%w: clean restored artifacts: %v", errWALResetUnavailable, err)
			}
		}
		defer func() {
			for _, path := range []string{pinnedPath, latestPath} {
				if err := removeRestoredArtifacts(path); err != nil {
					slog.Warn("Failed to remove wal reset restored artifacts", "path", path, "error", err)
				}
			}
		}()
		if err := verifier.pauseLoad(ctx); err != nil {
			return walResetValidation{}, fmt.Errorf("%w: pause load: %v", errWALResetUnavailable, err)
		}
		defer verifier.resumeLoad()
		if err := sleepContext(ctx, 2*time.Second); err != nil {
			return walResetValidation{}, fmt.Errorf("%w: %v", errWALResetUnavailable, err)
		}
		if err := verifier.waitForSync(ctx, result); err != nil {
			return walResetValidation{}, fmt.Errorf("%w: wait for sync: %v", errWALResetCatchUpTimeout, err)
		}
		result.RestoreMode = restoreModeTXID
		_, err := verifier.validateDBResult(ctx, cfg.DBPath, pinnedPath, result)
		validation := walResetValidation{boundary: result.BoundaryTXID, steps: verifier.validationSteps}
		if len(verifier.validationSteps) > 0 {
			validation.phase = verifier.validationSteps[len(verifier.validationSteps)-1].Name
		}
		if err != nil {
			return validation, walResetValidationError(validation.phase, err)
		}
		validation.pinnedPassed = true
		if verifier.boundary == nil {
			return validation, fmt.Errorf("%w: pinned restore left no boundary for the latest restore", errWALResetUnavailable)
		}
		latestSteps, latestTXID, latestErr := walResetLatestRestore(ctx, verifier, cfg, *verifier.boundary, latestPath)
		validation.steps = append(validation.steps, latestSteps...)
		validation.latestTXID = latestTXID
		if latestErr != nil {
			if len(latestSteps) > 0 {
				validation.phase = "latest_" + latestSteps[len(latestSteps)-1].Name
			}
			return validation, walResetValidationError(validation.phase, latestErr)
		}
		validation.latestPassed = true
		return validation, nil
	}
}

// walResetLatestRestore runs the unpinned latest restore: with the load
// paused and the replica synced to the boundary, latest must equal the
// boundary, so the restored database is checked for integrity and compared
// with the boundary's logical snapshot exactly like the pinned restore.
func walResetLatestRestore(ctx context.Context, verifier *Verifier, cfg Config, boundary verificationBoundary, restoredPath string) ([]reporting.VerificationStep, uint64, error) {
	verifier.validationSteps = nil
	sourceSize := fileSize(cfg.DBPath) + fileSize(cfg.DBPath+"-wal")
	configPath, cleanupConfig, err := verifier.validateConfigPath(cfg.DBPath)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: validate config: %v", errWALResetUnavailable, err)
	}
	defer cleanupConfig()
	var evidence restoreOutput
	restoreErr := verifier.runValidationPhase(ctx, "restore", cfg.verificationPhaseBudget("restore", sourceSize), func(phaseCtx context.Context) error {
		cmd := exec.CommandContext(phaseCtx, cfg.litestreamCommand(), restoreArgs(configPath, restoreModeLatest, 0, time.Time{}, restoredPath, cfg.DBPath)...)
		if cfg.ReplicaType == "s3" {
			proxyEndpoint := cfg.S3FaultProxyEndpoint
			if verifier.observeModeValidateEndpoint() != "" {
				proxyEndpoint = ""
			}
			cmd.Env = cfg.s3CommandEnv(proxyEndpoint)
		}
		cmd.Env = setCommandEnv(cmd.Environ(), "LOG_LEVEL", "DEBUG")
		cmd.Stdout, cmd.Stderr = &evidence, &evidence
		cmd.WaitDelay = time.Second
		err := cmd.Run()
		output := []byte(evidence.text())
		verifier.objectStore.recordRestoreRequests(evidence.requests)
		if err == nil {
			return nil
		}
		metadata := validationStepMetadata(phaseCtx, cmd, output, err)
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			metadata.err = fmt.Errorf("restore failed: validation failed (exit %d): %s", exitErr.ExitCode(), evidence.finalError)
		} else {
			metadata.err = fmt.Errorf("restore failed: run validate: %w: %s", err, evidence.finalError)
		}
		switch {
		case reporting.RestoreRequestCanceled(metadata.err.Error()):
			metadata.err = &restoreEvidenceInconclusiveError{err: metadata.err}
		case reporting.RestoreOutputDiskFull(metadata.err.Error()):
			metadata.err = restoreCapacityExhausted(metadata.err, restoredPath, cfg.DBPath)
		}
		return metadata
	})
	if len(verifier.validationSteps) > 0 {
		step := &verifier.validationSteps[len(verifier.validationSteps)-1]
		step.OutputTail = evidence.text()
		step.RestoreReplanCount = evidence.replans
		step.RestoreOutputTruncated = evidence.truncated
	}
	if restoreErr != nil {
		return verifier.validationSteps, evidence.planTXID, restoreErr
	}
	if err := verifier.runValidationPhase(ctx, "latest_boundary", cfg.verificationPhaseBudget("latest_marker", sourceSize), func(context.Context) error {
		return checkLatestRestoreTXID(evidence.planTXID, boundary.txid)
	}); err != nil {
		return verifier.validationSteps, evidence.planTXID, err
	}
	if err := verifier.runValidationPhase(ctx, "integrity_check", cfg.verificationPhaseBudget("integrity_check", sourceSize), func(phaseCtx context.Context) error {
		return checkRestoredIntegrity(phaseCtx, restoredPath)
	}); err != nil {
		return verifier.validationSteps, evidence.planTXID, err
	}
	if err := verifier.runValidationPhase(ctx, "compare", cfg.verificationPhaseBudget("compare", sourceSize), func(phaseCtx context.Context) error {
		return verifier.compareRestoredLogical(phaseCtx, boundary.logicalSnapshot, restoredPath)
	}); err != nil {
		return verifier.validationSteps, evidence.planTXID, err
	}
	return verifier.validationSteps, evidence.planTXID, nil
}

// walResetValidationError keeps a validation failure as corruption evidence
// only when it is one: harness-side problems, a verifier deadline, a
// cancellation, a lost restore target, exhausted restore capacity, or a
// locked restored comparison are inconclusive.
func walResetValidationError(phase string, err error) error {
	var evidence *restoreEvidenceInconclusiveError
	var capacity *restoreCapacityInconclusiveError
	var locked *restoredComparisonLockedError
	var deadline *verifierDeadlineError
	switch {
	case errors.As(err, &evidence), errors.As(err, &capacity), errors.As(err, &locked):
		return fmt.Errorf("%w: %s: %v", errWALResetUnavailable, phase, err)
	case errors.As(err, &deadline), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return fmt.Errorf("%w: %s ran out of time, no corruption evidence: %v", errWALResetUnavailable, phase, err)
	case strings.TrimPrefix(phase, "latest_") == "source_snapshot", strings.TrimPrefix(phase, "latest_") == "restore_preflight", phase == "latest_boundary":
		return fmt.Errorf("%w: %s: %v", errWALResetUnavailable, phase, err)
	default:
		return err
	}
}

func (r *Runner) walResetCheckEnabled() bool {
	return r.cfg.WALResetCheck && !r.cfg.ManyDBEnabled()
}

// stopLitestreamForCheck stops Litestream with sig and waits for the exit,
// bounded by ctx so a process that ignores the signal fails the cycle
// instead of hanging it.
func (r *Runner) stopLitestreamForCheck(ctx context.Context, sig os.Signal) error {
	done := make(chan struct{})
	go func() {
		r.stopLitestreamWithSignal(sig)
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("litestream did not exit after %v: %w", sig, ctx.Err())
	}
}

// walResetUploadedCounter reads the bytes uploaded so far: the observe
// proxy's PUT body bytes when the proxy runs (they survive a Litestream
// restart), otherwise Litestream's own PUT byte counter from the last
// metrics scrape, which starts again at zero after the restart.
func (r *Runner) walResetUploadedCounter() (uint64, bool) {
	counters := r.currentSnapshot().ObjectStoreCounters
	if counters.ReplicaRequestBytesSent != nil {
		return counters.ReplicaRequestBytesSent["PUT"], true
	}
	if counters.LitestreamObjectOperationBytes != nil {
		return counters.LitestreamObjectOperationBytes["PUT"], true
	}
	return 0, false
}

func (r *Runner) reportWALResetResult(ctx context.Context, result VerificationResult) {
	if result.Status == "failed" {
		r.triggerProfile("verification-failed")
	}
	r.sendVerification(context.WithoutCancel(ctx), result, nil)
	r.recordLocalResult(result)
	switch result.Status {
	case "failed":
		slog.Error("WAL reset check failed", "error", result.ErrorMessage)
	case "aborted":
		slog.Warn("WAL reset check inconclusive", "error", result.ErrorMessage)
	default:
		slog.Info("WAL reset check passed", "summary", result.Summary)
	}
}

// checkGate serializes the scheduled verification with checks that restart
// Litestream: whichever holds it runs alone.
type checkGate struct {
	slot chan struct{}
}

func newCheckGate() *checkGate {
	return &checkGate{slot: make(chan struct{}, 1)}
}

func (g *checkGate) acquire(ctx context.Context) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	select {
	case g.slot <- struct{}{}:
		return func() { <-g.slot }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", errWALResetGateBusy, ctx.Err())
	}
}
