package worker

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"modernc.org/sqlite"
)

// The wal reset check reproduces the shape reported in
// benbjohnson/litestream#1540: an application writes a sustained burst with
// no journal_size_limit, Litestream is stopped with SIGTERM while an
// application read transaction is open, so its shutdown checkpoint cannot
// restart the WAL and the position it saves sits deep in the file, the
// application then checkpoints and writes while Litestream is down so the WAL
// restarts several times, each generation longer than the last but all of
// them shorter than the saved position, while the physical file keeps its
// size, and Litestream is started again. Its startup verification finds the
// frame before its saved position intact and only its own salts between the
// final generation's end and that frame; a build that resumes incrementally
// from there replicates a database whose latest restore misses every
// generation that was checkpointed while it was down, while a build that
// proves continuity takes a snapshot instead. The gate pairs the baseline
// with the candidate on the same cycle shape, so the baseline failing is the
// reproduction evidence, and a cycle whose measurements do not confirm the
// shape is inconclusive.
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
	// restart before SIGTERM, four times the longest post-stop generation,
	// so no post-stop generation can grow back to the saved position.
	walResetBurstBytes = 12 << 20
	// walResetReaderHoldBytes is the depth at which the cycle opens the read
	// transaction it holds across the SIGTERM. A reader whose snapshot is
	// behind the WAL end keeps SQLite from restarting the WAL, so neither
	// Litestream's checkpoints for the rest of the burst nor its shutdown
	// checkpoint can move the saved position back to the start of the file.
	walResetReaderHoldBytes = walResetBurstBytes / 2
	walResetBurstBudget     = 3 * time.Minute
	walResetMinResets       = 3
	walResetTargetResets    = 5
	// Each post-stop generation is a PASSIVE checkpoint followed by one batch
	// of walResetGenerationBaseRows + k*walResetGenerationStepRows rows, so
	// every generation is longer than the one before it and the final one is
	// the longest, and all stay under SQLite's 1000-page autocheckpoint so no
	// connection restarts the WAL at an unplanned moment.
	walResetGenerationBaseRows = 200
	walResetGenerationStepRows = 100
	walResetRestartAttempts    = 40
	walResetReaderOpenBudget   = 30 * time.Second
	// A paused load process is frozen wherever it was, possibly inside a
	// write transaction. The drain takes SQLite's write lock on the check's
	// own connection for up to walResetDrainBudget; while the frozen writer
	// still holds it, the load is resumed for walResetDrainPause to finish
	// and paused again, up to walResetDrainAttempts times.
	walResetDrainBudget   = 15 * time.Second
	walResetDrainPause    = 250 * time.Millisecond
	walResetDrainAttempts = 8
	walResetDownBudget    = 4 * time.Minute
	walResetHeaderPoll    = 50 * time.Millisecond
	walResetStopBudget    = 3 * time.Minute
	walResetStartBudget   = 3 * time.Minute
	walResetCatchUpBudget = 10 * time.Minute
	walResetCatchUpPoll   = 2 * time.Second
	walResetGateBudget    = 15 * time.Minute
	walResetBusyTimeout   = 5 * time.Second

	walResetShapeConfirmed    = "confirmed"
	walResetShapeUnverified   = "unverified"
	walResetShapeContradicted = "contradicted"
)

var (
	errWALResetShapeNotReached      = errors.New("wal reset shape not reached")
	errWALResetLitestreamRestart    = errors.New("wal reset litestream restart failed")
	errWALResetCatchUpTimeout       = errors.New("wal reset catch-up timeout")
	errWALResetUnavailable          = errors.New("wal reset check unavailable")
	errWALResetSnapshotInterference = errors.New("wal reset snapshot interference")
	errWALResetGateBusy             = errors.New("verification held the check gate")
	errWALResetMissingPlan          = errors.New("latest restore reported no plan txid")
	errWALResetLatestAdvanced       = errors.New("latest advanced past the synced boundary")
)

// walResetCount is the number of WAL resets between two valid headers,
// taken from salt-1, which SQLite increments on each reset.
func walResetCount(before, after walHeader) uint32 {
	if !before.valid || !after.valid {
		return 0
	}
	return after.salt1 - before.salt1
}

// walResetL9 is one listing of the replica's snapshot level: the identity of
// every L9 object (txid range and creation time), so a new or replaced
// snapshot inside a cycle is visible even when the count does not change.
type walResetL9 struct {
	objects []string
	known   bool
}

func (l walResetL9) count() int {
	return len(l.objects)
}

// changed reports whether the set of L9 objects differs between two listings.
func (l walResetL9) changed(other walResetL9) bool {
	if len(l.objects) != len(other.objects) {
		return true
	}
	for i := range l.objects {
		if l.objects[i] != other.objects[i] {
			return true
		}
	}
	return false
}

// walResetObservation is what one cycle measured on the WAL and the replica.
type walResetObservation struct {
	// signalHeader is the WAL header read just before SIGTERM; savedHeader is
	// the valid header read after Litestream exited and is the baseline the
	// reset count is taken from.
	signalHeader walHeader
	savedHeader  walHeader
	// savedPosition is Litestream's recorded resume position, read from the
	// header of its newest local L0 LTX file (WALOffset+WALSize) when that
	// file belongs to the generation that was current at the stop. Zero when
	// it could not be read; the WAL ends below are never substituted for it.
	savedPosition      int64
	savedPositionFile  string
	savedPositionSalt1 uint32
	// savedAtSignal and savedAtExit bracket the committed WAL end around the
	// SIGTERM, since the final sync lands somewhere between the two.
	savedAtSignal int64
	savedAtExit   int64
	finalWAL      walGeneration
	// maxGenerationEnd and maxGenerationWritten are the furthest committed
	// and written frame ends seen in any generation started after the stop;
	// maxEarlierGenerationWritten is the same over the post-stop generations
	// before the final one.
	maxGenerationEnd            int64
	maxGenerationWritten        int64
	maxEarlierGenerationWritten int64
	// readerHeldAt is the committed WAL end when the read transaction held
	// across the SIGTERM was opened.
	readerHeldAt int64
	// tail is the frame-salt scan of the WAL file between the final
	// generation's end and the saved position, read before the restart; the
	// sampled generation ends above can miss a generation that background
	// writes extended after it was sampled, the file cannot.
	tail   walResetSaltScan
	resets uint32
	// checkpoints is the number of checkpoints that restarted the WAL while
	// Litestream was down, which is the reset count; the header's checkpoint
	// sequence is not used because SQLite starts it over when the wal-index
	// is rebuilt.
	checkpoints     uint32
	generations     int
	burstRows       int
	downRows        int
	downDuration    time.Duration
	litestreamExit  string
	l9Before        walResetL9
	l9After         walResetL9
	uploadedBytes   uint64
	uploadedKnown   bool
	syncTXID        uint64
	replicatedTXID  uint64
	catchUpDuration time.Duration
	restarted       bool
}

// shapeEvidence grades what the measurements say about the reported stale
// frame shape. It is confirmed only from Litestream's own recorded position:
// that position lies inside the physical file, no post-stop generation wrote
// up to it, and the final generation is the longest of them, so the frames
// between its end and the saved position carry the saved generation's salts.
// Without that position the shape is unverified; with it and a generation
// that reached it, or an earlier generation longer than the final one, it is
// contradicted.
func (o walResetObservation) shapeEvidence() (string, string) {
	switch {
	case o.savedPosition <= walHeaderSize:
		return walResetShapeUnverified, "litestream's saved position was not readable from its newest L0 ltx file"
	case !o.finalWAL.header.valid:
		return walResetShapeUnverified, "wal header unreadable after the down window"
	case o.savedPosition > o.finalWAL.fileBytes:
		return walResetShapeContradicted, fmt.Sprintf("the wal shrank to %d bytes below litestream's saved position %d", o.finalWAL.fileBytes, o.savedPosition)
	case o.maxGenerationWritten >= o.savedPosition:
		return walResetShapeContradicted, fmt.Sprintf("a post-stop wal generation wrote up to %d bytes, at or past litestream's saved position %d", o.maxGenerationWritten, o.savedPosition)
	case o.maxEarlierGenerationWritten > o.finalWAL.writtenEnd():
		return walResetShapeContradicted, fmt.Sprintf("an earlier post-stop wal generation wrote up to %d bytes, past the final generation's %d, so its stale frames sit between the final generation and litestream's saved frames", o.maxEarlierGenerationWritten, o.finalWAL.writtenEnd())
	case o.tail.err != "":
		return walResetShapeUnverified, fmt.Sprintf("the wal frames between the final generation's end %d and litestream's saved position %d could not be read: %s", o.tail.from, o.tail.to, o.tail.err)
	case !o.tail.checked:
		return walResetShapeUnverified, "the wal frames between the final generation's end and litestream's saved position were not read"
	case o.tail.foreignFrames > 0:
		return walResetShapeContradicted, fmt.Sprintf("%d of the %d wal frames between the final generation's end %d and litestream's saved position %d carry other salts (first at %d: %d/%d), so litestream's startup scan meets a generation it does not know", o.tail.foreignFrames, o.tail.frames, o.tail.from, o.tail.to, o.tail.foreignOffset, o.tail.foreignSalt1, o.tail.foreignSalt2)
	default:
		return walResetShapeConfirmed, fmt.Sprintf("litestream's saved position %d is inside the %d-byte wal, every post-stop generation ended below it (max written %d), the final generation is the longest (%d), and the %d frames between its end and the saved position carry the saved generation's salts", o.savedPosition, o.finalWAL.fileBytes, o.maxGenerationWritten, o.finalWAL.writtenEnd(), o.tail.frames)
	}
}

func (o walResetObservation) summary() string {
	shape, detail := o.shapeEvidence()
	parts := []string{
		fmt.Sprintf("wal_resets=%d", o.resets),
		fmt.Sprintf("checkpoints=%d", o.checkpoints),
		fmt.Sprintf("generations_seen=%d", o.generations),
		"shape_evidence=" + shape,
		fmt.Sprintf("saved_position=%d saved_position_ltx=%s saved_at_signal=%d saved_at_exit=%d reader_held_at=%d", o.savedPosition, firstNonEmpty(o.savedPositionFile, "none"), o.savedAtSignal, o.savedAtExit, o.readerHeldAt),
		fmt.Sprintf("max_generation_end=%d max_generation_written=%d max_earlier_generation_written=%d final_generation_end=%d final_generation_written=%d final_generation_frames=%d final_wal_bytes=%d", o.maxGenerationEnd, o.maxGenerationWritten, o.maxEarlierGenerationWritten, o.finalWAL.generationEnd(), o.finalWAL.writtenEnd(), o.finalWAL.committedFrames, o.finalWAL.fileBytes),
		fmt.Sprintf("burst_rows=%d", o.burstRows),
		fmt.Sprintf("down_rows=%d", o.downRows),
		fmt.Sprintf("down_seconds=%.1f", o.downDuration.Seconds()),
	}
	if o.litestreamExit != "" {
		parts = append(parts, "litestream_exit="+o.litestreamExit)
	}
	if o.savedHeader.valid {
		parts = append(parts, fmt.Sprintf("salt1_before=%d salt1_after=%d ckpt_seq_before=%d ckpt_seq_after=%d", o.savedHeader.salt1, o.finalWAL.header.salt1, o.savedHeader.checkpointSeq, o.finalWAL.header.checkpointSeq))
	}
	if o.signalHeader.valid {
		parts = append(parts, fmt.Sprintf("salt1_at_signal=%d", o.signalHeader.salt1))
	}
	switch {
	case o.l9Before.known && o.l9After.known:
		parts = append(parts, fmt.Sprintf("l9_before=%d l9_after=%d l9_changed=%t", o.l9Before.count(), o.l9After.count(), o.l9Before.changed(o.l9After)))
	case o.l9Before.known:
		parts = append(parts, fmt.Sprintf("l9_before=%d l9_after=unknown", o.l9Before.count()))
	default:
		parts = append(parts, "l9_before=unknown l9_after=unknown")
	}
	if o.uploadedKnown {
		parts = append(parts, fmt.Sprintf("uploaded_bytes=%d", o.uploadedBytes))
	}
	if o.syncTXID != 0 {
		parts = append(parts, fmt.Sprintf("catchup_txid=%s replicated_txid=%s catchup_seconds=%.1f", formatTXID(o.syncTXID), formatTXID(o.replicatedTXID), o.catchUpDuration.Seconds()))
	}
	parts = append(parts, "shape_detail="+strings.ReplaceAll(detail, " ", "_"))
	return strings.Join(parts, " ")
}

// snapshotInterference reports whether the replica's snapshot level changed
// between the listing before SIGTERM and the listing after both restores, or
// could not be listed at either point. A scheduled snapshot inside the cycle
// rewrites the replica from the database and hides whatever the restart
// replicated, so such a cycle proves nothing.
func (o walResetObservation) snapshotInterference() error {
	switch {
	case !o.l9Before.known:
		return fmt.Errorf("%w: the replica's L9 objects could not be listed before the stop", errWALResetSnapshotInterference)
	case !o.l9After.known:
		return fmt.Errorf("%w: the replica's L9 objects could not be listed after the restores", errWALResetSnapshotInterference)
	case o.l9Before.changed(o.l9After):
		return fmt.Errorf("%w: the replica's L9 objects changed during the cycle (%d -> %d: %s -> %s)", errWALResetSnapshotInterference, o.l9Before.count(), o.l9After.count(), strings.Join(o.l9Before.objects, ","), strings.Join(o.l9After.objects, ","))
	default:
		return nil
	}
}

// walResetValidation is the outcome of the restore step: the pinned restore
// at the boundary txid and the unpinned latest restore, which validation
// phase failed, the restore classifier's signature for a failed restore, and
// the steps both produced.
type walResetValidation struct {
	phase            string
	boundary         uint64
	pinnedPassed     bool
	latestPassed     bool
	latestTXID       uint64
	restoreSignature string
	steps            []reporting.VerificationStep
}

func (v walResetValidation) summary() string {
	status := func(passed bool) string {
		if passed {
			return "passed"
		}
		return "failed"
	}
	summary := fmt.Sprintf("pinned_restore=%s latest_restore=%s restored_txid=%s latest_txid=%s", status(v.pinnedPassed), status(v.latestPassed), formatTXID(v.boundary), formatTXID(v.latestTXID))
	if v.restoreSignature != "" {
		summary += " restore_signature=" + v.restoreSignature
	}
	return summary
}

// walResetCheck runs the cycle on a schedule. Every external interaction is a
// function so the state machine can be driven in tests.
type walResetCheck struct {
	cfg   Config
	clock func() time.Time

	burst         func(ctx context.Context, rows int) (int, error)
	generation    func() (walGeneration, error)
	savedPosition func() (litestreamSavedPosition, error)
	// holdReader opens a read transaction on the database and returns its
	// release; restartWrite runs a PASSIVE checkpoint and then one row's
	// write on the same connection, which restarts the WAL when nothing
	// pins it; frameSalts reads the salts of the WAL frames in [from, to).
	holdReader   func(ctx context.Context) (func() error, error)
	restartWrite func(ctx context.Context) (int, error)
	frameSalts   func(from, to, frameSize int64, allowed [][2]uint32) (walResetSaltScan, error)
	// pauseLoad stops the workload's writers and returns the resume, so the
	// WAL layout holds still from the final sample through Litestream's
	// startup verification.
	pauseLoad func(ctx context.Context) (func(), error)
	// drainWriters takes and releases SQLite's write lock on the check's own
	// connection, which proves no write transaction is in flight.
	drainWriters func(ctx context.Context) error
	// stop sends SIGTERM and waits for the exit, returning how the process
	// ended; start makes sure exactly one Litestream is running and ready,
	// whether none, a stopping one, or a started but not yet ready one exists.
	stop        func(ctx context.Context) (string, error)
	start       func(ctx context.Context) error
	catchUp     func(ctx context.Context) (syncResponse, error)
	validate    func(ctx context.Context, result *VerificationResult) (walResetValidation, error)
	l9          func() walResetL9
	uploaded    func() (uint64, bool)
	acquireGate func(ctx context.Context) (func(), error)
	downtime    func(bool)

	pause           time.Duration
	burstBytes      int64
	readerHoldBytes int64
	drainBudget     time.Duration
	drainPause      time.Duration
	burstBudget     time.Duration
	downBudget      time.Duration
	headerPoll      time.Duration
	stopBudget      time.Duration
	startBudget     time.Duration
	catchUpBudget   time.Duration
	catchUpPoll     time.Duration
	gateBudget      time.Duration
	minResets       uint32
	targetResets    uint32

	mu     sync.Mutex
	stopFn context.CancelFunc
	done   chan struct{}
}

func newWALResetCheck(cfg Config) *walResetCheck {
	check := &walResetCheck{
		cfg:             cfg,
		clock:           time.Now,
		pause:           walResetCyclePause,
		burstBytes:      walResetBurstBytes,
		readerHoldBytes: walResetReaderHoldBytes,
		burstBudget:     walResetBurstBudget,
		downBudget:      walResetDownBudget,
		headerPoll:      walResetHeaderPoll,
		stopBudget:      walResetStopBudget,
		startBudget:     walResetStartBudget,
		catchUpBudget:   walResetCatchUpBudget,
		catchUpPoll:     walResetCatchUpPoll,
		gateBudget:      walResetGateBudget,
		minResets:       walResetMinResets,
		targetResets:    walResetTargetResets,
		l9:              func() walResetL9 { return replicaL9(cfg) },
		uploaded:        func() (uint64, bool) { return 0, false },
		acquireGate:     func(context.Context) (func(), error) { return func() {}, nil },
		downtime:        func(bool) {},
		pauseLoad:       func(context.Context) (func(), error) { return func() {}, nil },
		drainBudget:     walResetDrainBudget,
		drainPause:      walResetDrainPause,
	}
	check.burst = func(ctx context.Context, rows int) (int, error) { return writeWALResetBurst(ctx, cfg.DBPath, rows) }
	check.drainWriters = func(ctx context.Context) error { return drainWALResetWriters(ctx, cfg.DBPath) }
	check.holdReader = func(ctx context.Context) (func() error, error) { return holdWALResetReader(ctx, cfg.DBPath) }
	check.restartWrite = func(ctx context.Context) (int, error) { return checkpointAndWriteWALReset(ctx, cfg.DBPath) }
	check.frameSalts = func(from, to, frameSize int64, allowed [][2]uint32) (walResetSaltScan, error) {
		return scanWALFrameSalts(cfg.DBPath+"-wal", from, to, frameSize, allowed)
	}
	check.generation = func() (walGeneration, error) { return readWALGeneration(cfg.DBPath + "-wal") }
	check.savedPosition = func() (litestreamSavedPosition, error) { return readLitestreamSavedPosition(cfg.DBPath) }
	check.stop = func(context.Context) (string, error) { return "", errWALResetUnavailable }
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
	uploadedBefore, uploadedKnown := c.uploaded()

	// (1) Burst with Litestream running until the WAL is deep enough past its
	// last restart that no post-stop generation can grow back to the saved
	// position. Halfway there a read transaction is opened and held until
	// Litestream has exited: its snapshot sits behind the WAL end, so SQLite
	// cannot restart the WAL for anyone, and Litestream's shutdown checkpoint
	// leaves the saved position deep in the file instead of at its start.
	releaseReader := func() error { return nil }
	defer func() {
		if releaseErr := releaseReader(); releaseErr != nil {
			slog.Warn("WAL reset cycle could not release its read transaction", "error", releaseErr)
		}
	}()
	burstCtx, cancelBurst := context.WithTimeout(ctx, c.burstBudget)
	defer cancelBurst()
	for {
		wal, walErr := c.generation()
		if walErr != nil {
			return observation, validation, fmt.Errorf("%w: read wal: %v", errWALResetUnavailable, walErr)
		}
		if wal.header.valid && observation.readerHeldAt == 0 && wal.generationEnd() >= c.readerHoldBytes {
			release, holdErr := c.holdReader(ctx)
			if holdErr != nil {
				return observation, validation, fmt.Errorf("%w: hold read transaction: %v", errWALResetUnavailable, holdErr)
			}
			releaseReader = releaseOnce(release)
			observation.readerHeldAt = wal.generationEnd()
		}
		if wal.header.valid && observation.readerHeldAt != 0 && wal.generationEnd() >= c.burstBytes {
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

	// (2) SIGTERM Litestream mid-burst. The recovery is installed before the
	// stop, so every path from here on, including a stop that times out and
	// completes late, ends with exactly one running Litestream: start is the
	// manager's ensure step, which never starts a second process.
	c.downtime(true)
	defer c.downtime(false)
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
	signalWAL, walErr := c.generation()
	if walErr != nil {
		return observation, validation, fmt.Errorf("%w: read wal: %v", errWALResetUnavailable, walErr)
	}
	observation.signalHeader = signalWAL.header
	observation.savedAtSignal = signalWAL.generationEnd()
	stopCtx, cancelStop := context.WithTimeout(ctx, c.stopBudget)
	observation.litestreamExit, err = c.stop(stopCtx)
	cancelStop()
	if err != nil {
		return observation, validation, fmt.Errorf("%w: stop: %v", errWALResetLitestreamRestart, err)
	}
	downStarted := c.clock()
	// The reset baseline is the header after the exit: Litestream's own
	// shutdown checkpoint may restart the WAL once more on its way out, and
	// only resets after it has gone are the application's.
	exitWAL, walErr := c.generation()
	if walErr != nil {
		return observation, validation, fmt.Errorf("%w: read wal after stop: %v", errWALResetUnavailable, walErr)
	}
	if !exitWAL.header.valid {
		return observation, validation, fmt.Errorf("%w: wal header unreadable after litestream exited (wal %d bytes)", errWALResetUnavailable, exitWAL.fileBytes)
	}
	observation.savedHeader = exitWAL.header
	observation.savedAtExit = exitWAL.generationEnd()
	c.recordSavedPosition(exitWAL, &observation)
	if releaseErr := releaseReader(); releaseErr != nil {
		return observation, validation, fmt.Errorf("%w: release read transaction: %v", errWALResetUnavailable, releaseErr)
	}

	// (3) Restart the WAL the target number of times while Litestream is
	// down: each generation is a PASSIVE checkpoint, a one-row write that
	// restarts the WAL, and one batch longer than the previous generation's,
	// recording how far every generation got.
	observation.generations = 1
	lastSalt2 := observation.savedHeader.salt2
	downCtx, cancelDown := context.WithTimeout(ctx, c.downBudget)
	defer cancelDown()
	for k := uint32(1); k <= c.targetResets; k++ {
		if restartErr := c.restartWAL(downCtx, &observation, &lastSalt2); restartErr != nil {
			if ctx.Err() != nil {
				return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, ctx.Err())
			}
			if downCtx.Err() != nil || errors.Is(restartErr, errWALResetShapeNotReached) {
				slog.Warn("WAL reset cycle could not restart the wal again", "error", restartErr, "resets", k-1)
				break
			}
			return observation, validation, restartErr
		}
		rows, burstErr := c.burst(downCtx, c.generationRows(k))
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
		wal, walErr := c.generation()
		if walErr != nil {
			return observation, validation, fmt.Errorf("%w: read wal while litestream was down: %v", errWALResetUnavailable, walErr)
		}
		if wal.header.valid {
			observation.recordGeneration(wal, &lastSalt2)
		}
	}
	observation.downDuration = c.clock().Sub(downStarted)

	// The writers are paused and drained from here until Litestream has read
	// its position and caught up, so the layout the final sample and the
	// file scan measure is the layout Litestream's startup verification sees.
	resumeLoad, pauseErr := c.pauseAndDrain(ctx)
	if pauseErr != nil {
		return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, pauseErr)
	}
	defer resumeLoad()
	if wal, walErr := c.generation(); walErr == nil && wal.header.valid {
		observation.recordGeneration(wal, &lastSalt2)
	}
	observation.resets = walResetCount(observation.savedHeader, observation.finalWAL.header)
	observation.checkpoints = observation.resets
	observation.tail = c.scanTail(observation)

	// (4) Restart Litestream and wait until it has caught up with the primary.
	// The preconditions for a result are that it was really stopped and
	// restarted, that the WAL reset at least minResets times while it was
	// down, that the measurements confirm the stale frame shape, and that the
	// replica reached the primary's txid.
	if err = c.restart(ctx, &observation); err != nil {
		return observation, validation, err
	}
	if observation.resets < c.minResets {
		return observation, validation, fmt.Errorf("%w: %d wal resets while litestream was down, need at least %d", errWALResetShapeNotReached, observation.resets, c.minResets)
	}
	if shape, detail := observation.shapeEvidence(); shape != walResetShapeConfirmed {
		return observation, validation, fmt.Errorf("%w: shape %s: %s", errWALResetShapeNotReached, shape, detail)
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
	resumeLoad()

	// (5) Restore the latest replicated position, pinned and unpinned, and
	// compare both with the primary at that txid. The snapshot level is
	// listed again afterwards: a snapshot anywhere in the window makes the
	// cycle inconclusive whatever the restores said.
	result := VerificationResult{CheckType: walResetCheckType}
	validation, err = c.validate(ctx, &result)
	observation.l9After = c.l9()
	if uploadedAfter, ok := c.uploaded(); ok && uploadedKnown {
		observation.uploadedBytes, observation.uploadedKnown = walResetUploadedBytes(uploadedBefore, uploadedAtStop, uploadedAfter), true
	}
	if interference := observation.snapshotInterference(); interference != nil {
		return observation, validation, interference
	}
	return observation, validation, err
}

// recordGeneration folds one WAL reading into the observation: the final
// generation, the count of distinct generations seen, how far any generation
// started after the stop committed and wrote, and how far the post-stop
// generations before the final one wrote.
func (o *walResetObservation) recordGeneration(wal walGeneration, lastSalt2 *uint32) {
	if wal.header.salt2 != *lastSalt2 {
		if o.finalWAL.header.valid && walResetCount(o.savedHeader, o.finalWAL.header) > 0 {
			o.maxEarlierGenerationWritten = max(o.maxEarlierGenerationWritten, o.finalWAL.writtenEnd())
		}
		o.generations++
		*lastSalt2 = wal.header.salt2
	}
	o.finalWAL = wal
	if walResetCount(o.savedHeader, wal.header) > 0 {
		o.maxGenerationEnd = max(o.maxGenerationEnd, wal.generationEnd())
		o.maxGenerationWritten = max(o.maxGenerationWritten, wal.writtenEnd())
	}
}

// scanTail reads the WAL file's frame salts from the final generation's end
// up to Litestream's saved position, the frames Litestream's startup scan
// walks before it meets its own saved salts, the frame ending at the saved
// position included. Every one of them must carry the saved generation's
// salts: any other salt is a generation Litestream does not know, which
// makes it snapshot, and the final generation's own salts there mean it
// grew past the sample, so the sampled end is not the end the shape rests
// on. The writers are paused while this runs, so the file does not move
// between the sample, the scan and Litestream's restart.
func (c *walResetCheck) scanTail(o walResetObservation) walResetSaltScan {
	if o.savedPosition <= walHeaderSize || !o.finalWAL.header.valid || o.finalWAL.frameSize <= 0 {
		return walResetSaltScan{}
	}
	allowed := [][2]uint32{{o.savedHeader.salt1, o.savedHeader.salt2}}
	scan, err := c.frameSalts(o.finalWAL.writtenEnd(), o.savedPosition, o.finalWAL.frameSize, allowed)
	if err != nil {
		scan.err = err.Error()
		scan.checked = false
	}
	return scan
}

// generationRows is the batch size of post-stop generation k: each one is
// longer than the one before it, so the final generation is the longest.
func (c *walResetCheck) generationRows(k uint32) int {
	return walResetGenerationBaseRows + int(k)*walResetGenerationStepRows
}

// restartWAL makes SQLite restart the WAL once: a PASSIVE checkpoint
// backfills every frame, and the next write then starts a new generation at
// the first frame, which the header's salt-1 shows. A checkpoint that another
// connection's read transaction kept from completing is retried.
func (c *walResetCheck) restartWAL(ctx context.Context, observation *walResetObservation, lastSalt2 *uint32) error {
	before := observation.savedHeader
	if observation.finalWAL.header.valid {
		before = observation.finalWAL.header
	}
	for attempt := 0; attempt < walResetRestartAttempts; attempt++ {
		rows, err := c.restartWrite(ctx)
		observation.downRows += rows
		if err != nil {
			return fmt.Errorf("%w: checkpoint and write while litestream was down: %v", errWALResetUnavailable, err)
		}
		wal, err := c.generation()
		if err != nil {
			return fmt.Errorf("%w: read wal while litestream was down: %v", errWALResetUnavailable, err)
		}
		if wal.header.valid {
			observation.recordGeneration(wal, lastSalt2)
			if walResetCount(before, wal.header) > 0 {
				return nil
			}
		}
		if err := sleepContext(ctx, c.headerPoll); err != nil {
			return fmt.Errorf("%w: %v", errWALResetUnavailable, err)
		}
	}
	return fmt.Errorf("%w: the wal did not restart after %d passive checkpoints while litestream was down", errWALResetShapeNotReached, walResetRestartAttempts)
}

func releaseOnce(release func() error) func() error {
	var once sync.Once
	var err error
	return func() error {
		once.Do(func() { err = release() })
		return err
	}
}

func releaseOnceFunc(release func()) func() {
	var once sync.Once
	return func() { once.Do(release) }
}

// walResetLoadPauser pauses the workload's writers through the verifier's
// pausers and returns the resume.
// pauseAndDrain pauses the workload's writers and waits for their in-flight
// write transaction to finish. The pause freezes the load process wherever it
// is, possibly inside a transaction that holds SQLite's write lock with
// frames not yet committed; that would stall Litestream's restart and put
// uncommitted frames in the scan. Taking and releasing the write lock on the
// check's own connection proves no transaction is in flight; while the
// frozen writer still holds it, the load is resumed to finish and paused
// again. The returned resume is safe to call more than once.
func (c *walResetCheck) pauseAndDrain(ctx context.Context) (func(), error) {
	var lastErr error
	for attempt := 0; attempt < walResetDrainAttempts; attempt++ {
		resume, err := c.pauseLoad(ctx)
		if err != nil {
			return nil, fmt.Errorf("pause load before the restart: %w", err)
		}
		drainCtx, cancel := context.WithTimeout(ctx, c.drainBudget)
		lastErr = c.drainWriters(drainCtx)
		cancel()
		if lastErr == nil {
			return releaseOnceFunc(resume), nil
		}
		resume()
		slog.Warn("WAL reset cycle paused the load inside a write transaction; letting it finish", "attempt", attempt+1, "error", lastErr)
		if err := sleepContext(ctx, c.drainPause); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("drain writers before the restart after %d pauses: %w", walResetDrainAttempts, lastErr)
}

// drainWALResetWriters takes SQLite's write lock with BEGIN IMMEDIATE on a
// connection of its own, the way the verifier reserves the writer, and
// releases it at once: holding it would block Litestream's own writes after
// the restart, and the paused load starts no new transaction.
func drainWALResetWriters(ctx context.Context, path string) error {
	db, conn, err := openWALResetConn(ctx, path, "rw")
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(); _ = db.Close() }()
	if err := acquireVerificationReservation(ctx, conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "ROLLBACK")
	return err
}

func walResetLoadPauser(cfg Config, pausers []loadPauser) func(context.Context) (func(), error) {
	return func(ctx context.Context) (func(), error) {
		verifier := NewVerifier(cfg, pausers...)
		if err := verifier.pauseLoad(ctx); err != nil {
			return nil, err
		}
		return verifier.resumeLoad, nil
	}
}

// restart makes sure Litestream is running and ready again after the down
// window and marks the cycle as restarted; a start that fails is a restart
// failure.
func (c *walResetCheck) restart(ctx context.Context, observation *walResetObservation) error {
	startCtx, cancelStart := context.WithTimeout(context.WithoutCancel(ctx), c.startBudget)
	defer cancelStart()
	if err := c.start(startCtx); err != nil {
		return fmt.Errorf("%w: start: %v", errWALResetLitestreamRestart, err)
	}
	observation.restarted = true
	c.downtime(false)
	return nil
}

// recordSavedPosition reads Litestream's recorded resume position from its
// newest local L0 LTX file and keeps it only when that file was written from
// the generation current at the stop; otherwise the position stays unknown.
func (c *walResetCheck) recordSavedPosition(exitWAL walGeneration, observation *walResetObservation) {
	position, err := c.savedPosition()
	switch {
	case err != nil:
		slog.Warn("WAL reset cycle could not read litestream's saved position", "error", err)
	case position.found && position.salt1 == exitWAL.header.salt1 && position.salt2 == exitWAL.header.salt2 && position.walOffsetEnd > walHeaderSize:
		observation.savedPosition = position.walOffsetEnd
		observation.savedPositionFile = position.file
		observation.savedPositionSalt1 = position.salt1
	case position.found:
		slog.Warn("WAL reset cycle found an L0 ltx file from another wal generation", "file", position.file, "ltx_salt1", position.salt1, "wal_salt1", exitWAL.header.salt1)
	}
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

// openWALResetConn opens one connection to the database with PERSIST_WAL
// set, so closing it, even as the last connection, keeps the WAL file and
// its salts in place; the cycle's measurements live in that file, and
// without this the driver deletes it on close whenever no other connection
// holds a lock at that moment.
func openWALResetConn(ctx context.Context, path, mode string) (*sql.DB, *sql.Conn, error) {
	db, err := openWALResetDB(path, mode)
	if err != nil {
		return nil, nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := conn.Raw(func(driverConn any) error {
		control, ok := driverConn.(sqlite.FileControl)
		if !ok {
			return errors.New("sqlite driver does not expose file control")
		}
		_, err := control.FileControlPersistWAL("main", 1)
		return err
	}); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, nil, fmt.Errorf("persist wal: %w", err)
	}
	return db, conn, nil
}

// writeWALResetBurst commits rows of random payload in one transaction on a
// connection with SQLite's default autocheckpoint and no journal_size_limit,
// which is the application in the report: once Litestream's reader is gone,
// the connection's own autocheckpoint restarts the WAL every 1000 pages and
// leaves the physical file at its full size.
func writeWALResetBurst(ctx context.Context, path string, rows int) (int, error) {
	defer harnessWriters.begin(walResetTable, path)()
	db, conn, err := openWALResetConn(ctx, path, "rw")
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(); _ = db.Close() }()
	tx, err := conn.BeginTx(ctx, nil)
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

// holdWALResetReader opens a read transaction whose snapshot is the WAL end
// at this moment and returns its release. While it is held, SQLite cannot
// restart the WAL on any connection, because a restart needs no reader on a
// read mark behind the end.
// The transaction lives on a context that no caller deadline cancels, because
// database/sql rolls a transaction back when its context ends and that would
// drop the pin while Litestream is still shutting down; only the release ends
// it. Opening the connection and taking the snapshot are bounded on their own.
func holdWALResetReader(ctx context.Context, path string) (func() error, error) {
	holdCtx, cancelHold := context.WithCancel(context.WithoutCancel(ctx))
	openCtx, cancelOpen := context.WithTimeout(holdCtx, walResetReaderOpenBudget)
	defer cancelOpen()
	db, conn, err := openWALResetConn(openCtx, path, "rw")
	if err != nil {
		cancelHold()
		return nil, err
	}
	tx, err := conn.BeginTx(holdCtx, nil)
	if err != nil {
		_ = conn.Close()
		_ = db.Close()
		cancelHold()
		return nil, err
	}
	var newest sql.NullInt64
	if err := tx.QueryRowContext(openCtx, "SELECT max(id) FROM "+walResetTable).Scan(&newest); err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		_ = db.Close()
		cancelHold()
		return nil, err
	}
	return func() error {
		err := errors.Join(tx.Rollback(), conn.Close(), db.Close())
		cancelHold()
		return err
	}, nil
}

// walResetSaltScan is what reading the WAL file's frame salts between the
// final generation's end and Litestream's saved position found: the frames
// Litestream's startup scan meets before it reaches its own saved salts.
type walResetSaltScan struct {
	checked       bool
	from, to      int64
	frames        int64
	foreignFrames int64
	foreignOffset int64
	foreignSalt1  uint32
	foreignSalt2  uint32
	err           string
}

// scanWALFrameSalts reads the salts of every frame in [from, to) of the WAL
// file and counts the frames whose salts are not among allowed, keeping the
// first such frame's offset and salts. A frame that cannot be read in full is
// an error, so a file shorter than the range never passes as clean.
func scanWALFrameSalts(path string, from, to, frameSize int64, allowed [][2]uint32) (walResetSaltScan, error) {
	scan := walResetSaltScan{from: from, to: to}
	if frameSize <= 0 {
		return scan, fmt.Errorf("frame size %d", frameSize)
	}
	file, err := os.Open(path)
	if err != nil {
		return scan, err
	}
	defer func() { _ = file.Close() }()
	header := make([]byte, 16)
	for offset := from; offset+frameSize <= to; offset += frameSize {
		if _, err := file.ReadAt(header, offset); err != nil {
			return scan, fmt.Errorf("read frame at %d: %w", offset, err)
		}
		salt1, salt2 := binary.BigEndian.Uint32(header[8:12]), binary.BigEndian.Uint32(header[12:16])
		scan.frames++
		known := false
		for _, salts := range allowed {
			if salts[0] == salt1 && salts[1] == salt2 {
				known = true
				break
			}
		}
		if known {
			continue
		}
		if scan.foreignFrames == 0 {
			scan.foreignOffset, scan.foreignSalt1, scan.foreignSalt2 = offset, salt1, salt2
		}
		scan.foreignFrames++
	}
	scan.checked = true
	return scan, nil
}

// checkpointAndWriteWALReset runs a PASSIVE checkpoint and then writes one
// row on the same connection. SQLite restarts the WAL for a writer whose
// read mark is zero, which it is only when the checkpoint's backfill is
// still recorded in the wal-index that writer reads; a connection that
// closes between the two drops that record when it was the last one, so the
// checkpoint and the write share the connection. The checkpoint result is
// not inspected: whether the WAL restarted shows in its header.
func checkpointAndWriteWALReset(ctx context.Context, path string) (int, error) {
	defer harnessWriters.begin(walResetTable, path)()
	db, conn, err := openWALResetConn(ctx, path, "rw")
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(); _ = db.Close() }()
	var busy, frames, checkpointed int
	if err := conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &frames, &checkpointed); err != nil {
		return 0, err
	}
	payload := make([]byte, walResetPayloadBytes)
	if _, err := rand.Read(payload); err != nil {
		return 0, err
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO "+walResetTable+" (written_at, payload) VALUES (?, ?)", time.Now().UnixNano(), payload); err != nil {
		return 0, err
	}
	return 1, nil
}

// replicaL9 lists the replica's levels with the litestream binary and returns
// the identities of the snapshot level's objects.
func replicaL9(cfg Config) walResetL9 {
	command := []string{cfg.litestreamCommand(), "ltx", "-config", cfg.ConfigPath, "-level", "all", cfg.DBPath}
	listing := listReplicaLTX(command)
	if listing.err != "" || !listing.complete {
		return walResetL9{}
	}
	return l9Identities(listing.candidates)
}

func l9Identities(candidates []restorePlanCandidate) walResetL9 {
	level := walResetL9{known: true, objects: []string{}}
	for _, candidate := range candidates {
		if candidate.level != ltxSnapshotLevel {
			continue
		}
		level.objects = append(level.objects, fmt.Sprintf("%016x-%016x@%s", candidate.minTXID, candidate.maxTXID, candidate.createdAt))
	}
	sort.Strings(level.objects)
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
			validation.restoreSignature, err = walResetValidationError(validation.phase, err)
			return validation, err
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
			validation.restoreSignature, latestErr = walResetValidationError(validation.phase, latestErr)
			return validation, latestErr
		}
		validation.latestPassed = true
		return validation, nil
	}
}

// walResetLatestRestore runs the unpinned latest restore: with the load
// paused and the replica synced to the boundary, latest must land exactly on
// the boundary, so the restored database is checked for integrity and
// compared with the boundary's logical snapshot exactly like the pinned
// restore.
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
	if err := verifier.runValidationPhase(ctx, "boundary", cfg.verificationPhaseBudget("latest_marker", sourceSize), func(context.Context) error {
		return checkWALResetLatestTXID(evidence.planTXID, boundary.txid)
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

// checkWALResetLatestTXID requires the unpinned restore to have landed exactly
// on the synced boundary. Behind it the replica lost committed data, which is
// the failure the check exists for. Ahead of it proves nothing: Litestream's
// own checkpoint (its _litestream_seq write) can add an LTX file while the
// load is paused, so the latest point is simply newer than the boundary the
// comparison was taken at. A restore log without a plan txid proves nothing
// either.
func checkWALResetLatestTXID(restored, boundary uint64) error {
	switch {
	case restored == 0:
		return errWALResetMissingPlan
	case restored < boundary:
		return fmt.Errorf("latest restore landed on txid %s, behind boundary %s", formatTXID(restored), formatTXID(boundary))
	case restored > boundary:
		return fmt.Errorf("%w: latest restore landed on txid %s, boundary %s", errWALResetLatestAdvanced, formatTXID(restored), formatTXID(boundary))
	default:
		return nil
	}
}

// walResetValidationError keeps a validation failure as corruption evidence
// only when it is one: harness-side problems, a verifier deadline, a
// cancellation, exhausted restore capacity, a locked restored comparison, or
// a restore log without a plan txid are inconclusive, and a failed restore is
// evidence only when the restore classifier names an unambiguous data defect
// and the verifier did not already mark the restore inconclusive. It returns
// the classifier's signature for a failed restore.
func walResetValidationError(phase string, err error) (string, error) {
	var evidence *restoreEvidenceInconclusiveError
	var capacity *restoreCapacityInconclusiveError
	var locked *restoredComparisonLockedError
	var deadline *verifierDeadlineError
	switch {
	case errors.As(err, &deadline), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "", fmt.Errorf("%w: %s ran out of time, no corruption evidence: %v", errWALResetUnavailable, phase, err)
	case errors.As(err, &capacity), errors.As(err, &locked), errors.Is(err, errWALResetMissingPlan), errors.Is(err, errWALResetLatestAdvanced):
		return "", fmt.Errorf("%w: %s: %v", errWALResetUnavailable, phase, err)
	case strings.TrimPrefix(phase, "latest_") == "source_snapshot", strings.TrimPrefix(phase, "latest_") == "restore_preflight":
		return "", fmt.Errorf("%w: %s: %v", errWALResetUnavailable, phase, err)
	case strings.TrimPrefix(phase, "latest_") == "restore":
		signature, defect := walResetRestoreSignature(err)
		if !defect || errors.As(err, &evidence) {
			return signature, fmt.Errorf("%w: %s: %s: %v", errWALResetUnavailable, phase, signature, err)
		}
		return signature, fmt.Errorf("%s: %w", signature, err)
	default:
		return "", err
	}
}

// walResetRestoreSignature runs a failed restore through the verifier's
// restore classifier and reports whether the failure is an unambiguous defect
// in the replicated data: a decode, page header, checksum or unexpected EOF
// error, an LTX file the plan needs but cannot find, or a corrupt index. A
// lost restore target is not, because a pinned target can vanish to
// compaction or retention between the boundary capture and the restore. A
// plan failure is not either unless the message carries one of those data
// markers, because a DNS or connection error without object store markers
// can hide behind the plan prefix. Any object store error and a failure the
// classifier cannot name are environmental.
func walResetRestoreSignature(err error) (string, bool) {
	classification := reporting.ClassifyVerificationFailure("restore", err.Error())
	if classification.ObjectStore != nil {
		return classification.Signature, false
	}
	switch classification.Signature {
	case "restore_decode_error", "restore_missing_ltx", "sqlite_index_mismatch":
		return classification.Signature, true
	case "restore_plan_failed":
		return classification.Signature, walResetDataDefectMarker(err.Error())
	default:
		return classification.Signature, false
	}
}

func walResetDataDefectMarker(message string) bool {
	text := strings.ToLower(message)
	for _, marker := range []string{"decode", "read page header", "checksum mismatch", "unexpected eof", "missing ltx", "open ltx file: file does not exist", "wrong # of entries in index"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func (r *Runner) walResetCheckEnabled() bool {
	return r.cfg.WALResetCheck && !r.cfg.ManyDBEnabled()
}

// stopLitestreamForCheck stops the current Litestream process with sig and
// waits for the exit, bounded by ctx so a process that ignores the signal
// fails the cycle instead of hanging it. The process is captured and marked
// as stopping synchronously, before the signal goes out and before ctx can
// expire, so the recovery that follows a timeout always sees a stopping
// process and replaces it; its kill supersedes this stop, and a signal still
// pending then is never sent. It returns how the process ended; a signal that
// could not be delivered, a process that had already exited, or an exit from
// another signal is an error, because the cycle then cannot claim the stop as
// its own.
func (r *Runner) stopLitestreamForCheck(ctx context.Context, sig os.Signal) (string, error) {
	proc := r.currentProcess()
	stop, err := proc.beginStop(sig)
	if err != nil {
		return describeProcessExit(proc.exitSnapshotOrNil()), err
	}
	done := make(chan error, 1)
	go func() { done <- proc.finishStop(stop) }()
	select {
	case err := <-done:
		if err != nil {
			return describeProcessExit(proc.exitSnapshotOrNil()), err
		}
		return describeProcessExit(proc.exitSnapshot()), nil
	case <-ctx.Done():
		return "", fmt.Errorf("litestream did not exit after %v: %w", sig, ctx.Err())
	}
}

func (p *litestreamProcess) exitSnapshotOrNil() *reporting.ProcessExitSnapshot {
	if p == nil {
		return nil
	}
	return p.exitSnapshot()
}

func describeProcessExit(exit *reporting.ProcessExitSnapshot) string {
	switch {
	case exit == nil:
		return "unknown"
	case exit.Signal != "":
		return "signal:" + exit.Signal
	case exit.ExitCode != nil:
		return fmt.Sprintf("exit:%d", *exit.ExitCode)
	default:
		return firstNonEmpty(exit.Error, "unknown")
	}
}

// ensureLitestreamForCheck makes exactly one Litestream run and become ready:
// a process the harness asked to stop but that is still alive is killed first,
// a process that is alive and was not asked to stop is given the socket wait
// and killed only if that fails, and a fresh process is started only when
// none is alive. A second process is never started next to a live one.
func (r *Runner) ensureLitestreamForCheck(ctx context.Context, start func() error) error {
	if r.litestreamRunning() {
		if !r.litestreamExitExpected() {
			if err := r.waitForFirstSync(ctx); err == nil {
				return nil
			}
			slog.Warn("Litestream started by the wal reset check never became ready; replacing it")
		}
		// The kill supersedes a stop whose signal is still pending, and waits
		// for the captured process, so no earlier stop can touch the
		// replacement. A process that exited first, or on the earlier stop's
		// signal in the same instant, is gone either way and gets replaced.
		if err := r.stopLitestreamWithSignal(syscall.SIGKILL); err != nil && !errors.Is(err, errLitestreamExitedBeforeStop) && !errors.Is(err, errLitestreamNotRunning) && !errors.Is(err, errLitestreamExitNotAttributed) {
			return fmt.Errorf("replace litestream: %w", err)
		}
	}
	if err := start(); err != nil {
		return err
	}
	return r.waitForFirstSync(ctx)
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
