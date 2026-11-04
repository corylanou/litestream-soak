package worker

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	_ "modernc.org/sqlite"
)

// The wal reset check reproduces the shape reported in
// benbjohnson/litestream#1540: an application writes a sustained burst with
// SQLite's default autocheckpoint (1000 pages) and no journal_size_limit,
// Litestream is stopped with SIGTERM mid-burst so its saved position sits
// deep in the WAL, the application keeps writing while Litestream is down so
// its autocheckpoints restart the WAL several times (each generation shorter
// than the saved position, so the stale frame at that position survives),
// and Litestream is started again. A build that resumes incrementally from
// that stale frame replicates a database whose latest restore fails a full
// integrity check; a build that proves continuity takes a snapshot instead.
const (
	walResetCheckType   = "walreset"
	walResetRestoreMode = "walreset-restore"
	walResetStep        = "wal_reset"
	walResetTable       = "wal_reset_burst"
	// walResetPayloadBytes keeps one row close to one 4 KiB page so page
	// counts and row counts track each other.
	walResetPayloadBytes = 3500
	walResetBatchRows    = 64
	// walResetCyclePause is the gap between cycles; with a cycle of three to
	// six minutes that is a cycle roughly every half hour, about 45 a day.
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
	walResetStartBudget   = 3 * time.Minute
	walResetCatchUpBudget = 10 * time.Minute
	walResetCatchUpPoll   = 2 * time.Second
	walResetGateBudget    = 15 * time.Minute
	walResetBusyTimeout   = 5 * time.Second
)

var (
	errWALResetShapeNotReached   = errors.New("wal reset shape not reached")
	errWALResetLitestreamRestart = errors.New("wal reset litestream restart failed")
	errWALResetCatchUpTimeout    = errors.New("wal reset catch-up timeout")
	errWALResetUnavailable       = errors.New("wal reset check unavailable")
	errWALResetGateBusy          = errors.New("verification held the check gate")
)

// walHeader is the first 32 bytes of a SQLite WAL file. Salt-1 is incremented
// by one on every WAL reset and salt-2 is re-randomized, so the salt-1
// difference between two headers is the number of resets between them.
type walHeader struct {
	magic         uint32
	version       uint32
	pageSize      uint32
	checkpointSeq uint32
	salt1         uint32
	salt2         uint32
	valid         bool
}

func parseWALHeader(raw []byte) walHeader {
	if len(raw) < 32 {
		return walHeader{}
	}
	header := walHeader{
		magic:         binary.BigEndian.Uint32(raw[0:4]),
		version:       binary.BigEndian.Uint32(raw[4:8]),
		pageSize:      binary.BigEndian.Uint32(raw[8:12]),
		checkpointSeq: binary.BigEndian.Uint32(raw[12:16]),
		salt1:         binary.BigEndian.Uint32(raw[16:20]),
		salt2:         binary.BigEndian.Uint32(raw[20:24]),
	}
	header.valid = (header.magic == 0x377f0682 || header.magic == 0x377f0683) && header.version == 3007000
	return header
}

func readWALHeader(path string) (walHeader, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return walHeader{}, 0, nil
		}
		return walHeader{}, 0, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return walHeader{}, 0, err
	}
	raw := make([]byte, 32)
	n, err := io.ReadFull(file, raw)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return walHeader{}, info.Size(), err
	}
	return parseWALHeader(raw[:n]), info.Size(), nil
}

// walResetCount is the number of WAL resets between two valid headers,
// taken from salt-1, which SQLite increments on each reset.
func walResetCount(before, after walHeader) uint32 {
	if !before.valid || !after.valid {
		return 0
	}
	return after.salt1 - before.salt1
}

// walResetObservation is what one cycle measured on the WAL and the replica.
type walResetObservation struct {
	savedHeader        walHeader
	savedWALBytes      int64
	finalHeader        walHeader
	finalWALBytes      int64
	resets             uint32
	checkpoints        uint32
	generations        int
	maxGenerationBytes int64
	burstRows          int
	downRows           int
	downDuration       time.Duration
	l9Before, l9After  int
	l9Known            bool
	uploadedBytes      uint64
	uploadedKnown      bool
	syncTXID           uint64
	replicatedTXID     uint64
	restoredTXID       uint64
	catchUpDuration    time.Duration
}

func (o walResetObservation) summary() string {
	parts := []string{
		fmt.Sprintf("wal_resets=%d", o.resets),
		fmt.Sprintf("checkpoints=%d", o.checkpoints),
		fmt.Sprintf("generations_seen=%d", o.generations),
		fmt.Sprintf("saved_wal_bytes=%d", o.savedWALBytes),
		fmt.Sprintf("max_generation_bytes=%d", o.maxGenerationBytes),
		fmt.Sprintf("final_wal_bytes=%d", o.finalWALBytes),
		fmt.Sprintf("burst_rows=%d", o.burstRows),
		fmt.Sprintf("down_rows=%d", o.downRows),
		fmt.Sprintf("down_seconds=%.1f", o.downDuration.Seconds()),
	}
	if o.savedHeader.valid {
		parts = append(parts, fmt.Sprintf("salt1_before=%d salt1_after=%d ckpt_seq_before=%d ckpt_seq_after=%d", o.savedHeader.salt1, o.finalHeader.salt1, o.savedHeader.checkpointSeq, o.finalHeader.checkpointSeq))
	}
	if o.l9Known {
		parts = append(parts, fmt.Sprintf("l9_before=%d l9_after=%d", o.l9Before, o.l9After))
	} else {
		parts = append(parts, "l9_before=unknown l9_after=unknown")
	}
	if o.uploadedKnown {
		parts = append(parts, fmt.Sprintf("uploaded_bytes=%d", o.uploadedBytes))
	}
	if o.syncTXID != 0 {
		parts = append(parts, fmt.Sprintf("catchup_txid=%s replicated_txid=%s catchup_seconds=%.1f", formatTXID(o.syncTXID), formatTXID(o.replicatedTXID), o.catchUpDuration.Seconds()))
	}
	if o.restoredTXID != 0 {
		parts = append(parts, "restored_txid="+formatTXID(o.restoredTXID))
	}
	return strings.Join(parts, " ")
}

// shapeError reports why the cycle did not reproduce the reported shape, or
// nothing when it did: at least walResetMinResets resets while Litestream
// was down, and no generation that grew back to the saved position.
func (o walResetObservation) shapeError() error {
	switch {
	case !o.savedHeader.valid:
		return fmt.Errorf("%w: wal header unreadable at stop (wal %d bytes)", errWALResetShapeNotReached, o.savedWALBytes)
	case !o.finalHeader.valid:
		return fmt.Errorf("%w: wal header unreadable after the down window (wal %d bytes)", errWALResetShapeNotReached, o.finalWALBytes)
	case o.resets < walResetMinResets:
		return fmt.Errorf("%w: %d wal resets while litestream was down, need at least %d", errWALResetShapeNotReached, o.resets, walResetMinResets)
	case o.maxGenerationBytes >= o.savedWALBytes:
		return fmt.Errorf("%w: a wal generation grew to %d bytes, past the %d bytes saved at stop, so the stale frame was overwritten", errWALResetShapeNotReached, o.maxGenerationBytes, o.savedWALBytes)
	default:
		return nil
	}
}

// walResetValidation is the outcome of the restore step: which validation
// phase failed, if any, so the failure carries its own signature.
type walResetValidation struct {
	phase    string
	boundary uint64
	steps    []reporting.VerificationStep
}

// walResetCheck runs the cycle on a schedule. Every external interaction is a
// function so the state machine can be driven in tests.
type walResetCheck struct {
	cfg   Config
	clock func() time.Time

	burst       func(ctx context.Context, rows int) (int, error)
	header      func() (walHeader, int64, error)
	stop        func(ctx context.Context) error
	start       func(ctx context.Context) error
	catchUp     func(ctx context.Context) (syncResponse, error)
	validate    func(ctx context.Context, result *VerificationResult) (walResetValidation, error)
	l9Count     func() (int, bool)
	uploaded    func() (uint64, bool)
	acquireGate func(ctx context.Context) (func(), error)
	downtime    func(bool)

	pause         time.Duration
	burstBytes    int64
	burstBudget   time.Duration
	downBudget    time.Duration
	headerPoll    time.Duration
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
		startBudget:   walResetStartBudget,
		catchUpBudget: walResetCatchUpBudget,
		catchUpPoll:   walResetCatchUpPoll,
		gateBudget:    walResetGateBudget,
		minResets:     walResetMinResets,
		targetResets:  walResetTargetResets,
		l9Count:       func() (int, bool) { return replicaL9Count(cfg) },
		uploaded:      func() (uint64, bool) { return 0, false },
		acquireGate:   func(context.Context) (func(), error) { return func() {}, nil },
		downtime:      func(bool) {},
	}
	check.burst = func(ctx context.Context, rows int) (int, error) { return writeWALResetBurst(ctx, cfg.DBPath, rows) }
	check.header = func() (walHeader, int64, error) { return readWALHeader(cfg.DBPath + "-wal") }
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
	if l9, ok := c.l9Count(); ok {
		observation.l9Before, observation.l9Known = l9, true
	}
	uploadedBefore, uploadedKnown := c.uploaded()

	// (1) Burst with Litestream running until the WAL is deep enough past its
	// last restart that no post-stop generation can reach the saved position.
	burstCtx, cancelBurst := context.WithTimeout(ctx, c.burstBudget)
	defer cancelBurst()
	for {
		header, size, headerErr := c.header()
		if headerErr != nil {
			return observation, validation, fmt.Errorf("%w: read wal header: %v", errWALResetUnavailable, headerErr)
		}
		if header.valid && size >= c.burstBytes {
			break
		}
		if ctx.Err() != nil {
			return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, ctx.Err())
		}
		if burstCtx.Err() != nil {
			return observation, validation, fmt.Errorf("%w: wal reached %d of %d bytes within %s", errWALResetShapeNotReached, size, c.burstBytes, c.burstBudget)
		}
		rows, burstErr := c.burst(burstCtx, walResetBatchRows)
		observation.burstRows += rows
		if burstErr != nil {
			if ctx.Err() != nil {
				return observation, validation, fmt.Errorf("%w: %v", errWALResetUnavailable, ctx.Err())
			}
			if burstCtx.Err() != nil {
				return observation, validation, fmt.Errorf("%w: wal reached %d of %d bytes within %s", errWALResetShapeNotReached, size, c.burstBytes, c.burstBudget)
			}
			return observation, validation, fmt.Errorf("%w: burst write: %v", errWALResetUnavailable, burstErr)
		}
	}
	uploadedAtStop, _ := c.uploaded()

	// (2) SIGTERM Litestream mid-burst and record the position it saved.
	c.downtime(true)
	defer c.downtime(false)
	if observation.savedHeader, observation.savedWALBytes, err = c.header(); err != nil {
		return observation, validation, fmt.Errorf("%w: read wal header: %v", errWALResetUnavailable, err)
	}
	if err = c.stop(ctx); err != nil {
		return observation, validation, fmt.Errorf("%w: stop: %v", errWALResetLitestreamRestart, err)
	}
	downStarted := c.clock()

	// (3) Keep writing while Litestream is down until the WAL has restarted
	// the target number of times, polling the header for every generation.
	observation.generations = 1
	lastSalt2 := observation.savedHeader.salt2
	downCtx, cancelDown := context.WithTimeout(ctx, c.downBudget)
	defer cancelDown()
	for {
		header, size, headerErr := c.header()
		if headerErr != nil {
			return observation, validation, fmt.Errorf("%w: read wal header: %v", errWALResetUnavailable, headerErr)
		}
		if header.valid {
			observation.finalHeader = header
			observation.finalWALBytes = size
			if header.salt2 != lastSalt2 {
				observation.generations++
				lastSalt2 = header.salt2
			}
			if resets := walResetCount(observation.savedHeader, header); resets > 0 {
				observation.maxGenerationBytes = max(observation.maxGenerationBytes, size)
			}
			if walResetCount(observation.savedHeader, header) >= c.targetResets {
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
	if header, size, headerErr := c.header(); headerErr == nil && header.valid {
		observation.finalHeader, observation.finalWALBytes = header, size
	}
	observation.resets = walResetCount(observation.savedHeader, observation.finalHeader)
	observation.checkpoints = observation.finalHeader.checkpointSeq - observation.savedHeader.checkpointSeq
	if !observation.finalHeader.valid {
		observation.checkpoints = 0
	}

	// (4) Restart Litestream and wait until it has caught up with the primary.
	startCtx, cancelStart := context.WithTimeout(ctx, c.startBudget)
	err = c.start(startCtx)
	cancelStart()
	if err != nil {
		return observation, validation, fmt.Errorf("%w: start: %v", errWALResetLitestreamRestart, err)
	}
	c.downtime(false)
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
	if l9, ok := c.l9Count(); ok && observation.l9Known {
		observation.l9After = l9
	} else {
		observation.l9Known = false
	}
	if uploadedAfter, ok := c.uploaded(); ok && uploadedKnown {
		observation.uploadedBytes, observation.uploadedKnown = walResetUploadedBytes(uploadedBefore, uploadedAtStop, uploadedAfter), true
	}

	// (5) Restore the latest replicated position and compare it with the
	// primary at that txid.
	result := VerificationResult{CheckType: walResetCheckType}
	validation, err = c.validate(ctx, &result)
	observation.restoredTXID = validation.boundary
	return observation, validation, err
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
	result := VerificationResult{
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		CheckType:    walResetCheckType,
		RestoreMode:  walResetRestoreMode,
		DurationMS:   int(completedAt.Sub(startedAt).Milliseconds()),
		SyncTXID:     observation.syncTXID,
		BoundaryTXID: validation.boundary,
		Steps:        []reporting.VerificationStep{{Name: walResetStep, StartedAt: startedAt, CompletedAt: completedAt, Status: "ok", OutputTail: tailString(observation.summary(), 8192)}},
	}
	result.Steps = append(result.Steps, validation.steps...)
	switch {
	case err == nil:
		result.Status = "passed"
		result.Passed = true
		result.Summary = fmt.Sprintf("wal reset check passed: latest restore at txid %s is intact and matches the primary after %d wal resets while litestream was down (%s)", formatTXID(validation.boundary), observation.resets, observation.summary())
	case walResetInconclusive(err):
		message := "wal reset check skipped: " + err.Error()
		result.Steps[0].Status = "inconclusive"
		result.Steps[0].Error = message
		result.Status = "aborted"
		result.ErrorMessage = message
		result.Summary = "verification inconclusive: " + message + " (" + observation.summary() + ")"
		result.InconclusiveClassification = &reporting.FailureClassification{Stage: walResetStep, Signature: walResetInconclusiveSignature(err), RestoreMode: walResetRestoreMode}
	default:
		message := walResetFailureMessage(validation.phase, err)
		result.Steps[0].Status = "error"
		result.Steps[0].Error = message
		result.Status = "failed"
		result.ErrorMessage = message
		result.Summary = "wal reset check failed after " + fmt.Sprintf("%d", observation.resets) + " wal resets while litestream was down (" + observation.summary() + ")"
	}
	return result
}

func walResetInconclusive(err error) bool {
	return errors.Is(err, errWALResetShapeNotReached) || errors.Is(err, errWALResetLitestreamRestart) ||
		errors.Is(err, errWALResetCatchUpTimeout) || errors.Is(err, errWALResetUnavailable) || errors.Is(err, errWALResetGateBusy)
}

func walResetInconclusiveSignature(err error) string {
	switch {
	case errors.Is(err, errWALResetShapeNotReached):
		return reporting.SignatureWALResetShapeNotReached
	case errors.Is(err, errWALResetLitestreamRestart):
		return reporting.SignatureWALResetLitestreamRestartFailed
	case errors.Is(err, errWALResetCatchUpTimeout):
		return reporting.SignatureWALResetCatchUpTimeout
	default:
		return reporting.SignatureWALResetUnavailable
	}
}

// walResetFailureMessage prefixes the validation failure with its class so
// restore, integrity, and logical failures classify to distinct signatures.
func walResetFailureMessage(phase string, err error) string {
	switch phase {
	case "integrity_check":
		return "wal reset integrity failed: " + err.Error()
	case "compare":
		return "wal reset logical mismatch: " + err.Error()
	default:
		return "wal reset restore failed: " + err.Error()
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
// the connection's own autocheckpoint restarts the WAL every 1000 pages.
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

// replicaL9Count lists the replica's levels with the litestream binary and
// returns the snapshot level's object count.
func replicaL9Count(cfg Config) (int, bool) {
	report := collectReplicaLevelReport(cfg, 0)
	if report == nil || report.Error != "" || !report.Complete {
		return 0, false
	}
	for _, level := range report.Levels {
		if level.Level == ltxSnapshotLevel {
			return level.ObjectCount, true
		}
	}
	return 0, true
}

// walResetValidator restores the latest replicated position with its own
// verifier so the cycle shares nothing with the scheduled verification: it
// pauses the load, syncs to the primary's txid, restores at that txid, runs
// the full integrity check and the logical comparison, and reports which
// phase failed.
func walResetValidator(cfg Config, objectStore *objectStoreMeter, pausers []loadPauser) func(context.Context, *VerificationResult) (walResetValidation, error) {
	return func(ctx context.Context, result *VerificationResult) (walResetValidation, error) {
		verifier := NewVerifier(cfg, pausers...)
		if objectStore != nil {
			verifier.objectStore = objectStore
		}
		restoredPath := cfg.DBPath + ".walreset.restored"
		if err := removeRestoredArtifacts(restoredPath); err != nil {
			return walResetValidation{}, fmt.Errorf("%w: clean restored artifacts: %v", errWALResetUnavailable, err)
		}
		defer func() {
			if err := removeRestoredArtifacts(restoredPath); err != nil {
				slog.Warn("Failed to remove wal reset restored artifacts", "error", err)
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
		_, err := verifier.validateDBResult(ctx, cfg.DBPath, restoredPath, result)
		validation := walResetValidation{boundary: result.BoundaryTXID, steps: verifier.validationSteps}
		if len(verifier.validationSteps) > 0 {
			validation.phase = verifier.validationSteps[len(verifier.validationSteps)-1].Name
		}
		if err != nil {
			var evidence *restoreEvidenceInconclusiveError
			var capacity *restoreCapacityInconclusiveError
			var locked *restoredComparisonLockedError
			if errors.As(err, &evidence) || errors.As(err, &capacity) || errors.As(err, &locked) {
				return validation, fmt.Errorf("%w: %v", errWALResetUnavailable, err)
			}
			if validation.phase == "source_snapshot" || validation.phase == "restore_preflight" {
				return validation, fmt.Errorf("%w: %s: %v", errWALResetUnavailable, validation.phase, err)
			}
		}
		return validation, err
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
