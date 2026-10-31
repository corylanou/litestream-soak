package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const (
	vfsCheckType           = "vfs"
	vfsSnapshotRestoreMode = "vfs-snapshot"
	vfsReadRestoreMode     = "vfs-read"
	vfsSnapshotStep        = "vfs_snapshot"
	vfsReadStep            = "vfs_read"
	vfsSnapshotSession     = "snapshot"
	vfsReadSession         = "read"

	vfsSnapshotRepeats        = 16
	vfsSnapshotRepeatInterval = 15 * time.Second
	vfsSnapshotPause          = 2 * time.Minute
	vfsHydrationBudget        = 10 * time.Minute
	vfsReadHydrationBudget    = time.Minute
	vfsHydrationPoll          = 5 * time.Second
	vfsCatchUpBudget          = 10 * time.Minute
	vfsCatchUpPoll            = time.Second
	vfsProbeWriteBudget       = 10 * time.Minute
	vfsProbeWritePoll         = time.Second
	vfsSyncBudget             = 2 * time.Minute
	// vfsLogSettle is how long a check waits after its last read before the
	// final log scan: the VFS logs a failed page apply only after the poll
	// that hit it releases the file mutex, so a scan taken right after a
	// statement can run ahead of the line. One poll interval (1s) plus margin.
	vfsLogSettle = 3 * time.Second
	// vfsStatementTimeout bounds one shell statement. The VFS applies every
	// poll's page updates to the hydrated file under its file mutex with one
	// remote fetch per page, and a compaction that moves pages to a new level
	// makes it fetch every page in the compacted file again, so a statement can
	// wait for the whole database's worth of page fetches before it runs.
	vfsStatementTimeout = 10 * time.Minute
	vfsReadTableLimit   = 64
)

var (
	errVFSReaderUnavailable   = errors.New("vfs reader unavailable")
	errVFSHydrationIncomplete = errors.New("vfs hydration incomplete")
	// errVFSHydrationFailed marks a session whose VFS gave up on hydration and
	// serves pages from the replica; the session is dropped so the next cycle
	// hydrates again.
	errVFSHydrationFailed   = errors.New("vfs hydration failed")
	errVFSReaderNotCaughtUp = errors.New("vfs reader not caught up")
	// errVFSStatementTimeout marks a statement whose output never arrived: the
	// shell's late rows and sentinel are still in the stream, so the session
	// cannot be reused.
	errVFSStatementTimeout = errors.New("vfs reader statement timed out")
	// errVFSProbeSyncFailed marks a Litestream sync that could not vouch for
	// the cycle's probe update; the reader session itself is still clean.
	errVFSProbeSyncFailed = errors.New("vfs probe sync failed")
	// vfsReaderPragmas size the reader the way issue 1608 does: a 1 MiB page
	// cache that a scan of the probe table overflows, and no mmap.
	vfsReaderPragmas = []string{"PRAGMA cache_size=-1024", "PRAGMA mmap_size=0"}
)

// vfsProbe is one read of the probe aggregate together with the replica
// position the VFS reported right after it.
type vfsProbe struct {
	raw   string
	count int64
	min   int64
	max   int64
	sum   int64
	txid  uint64
}

func parseVFSProbe(raw string) (vfsProbe, error) {
	fields := strings.Split(strings.TrimSpace(raw), "|")
	if len(fields) != 4 {
		return vfsProbe{}, fmt.Errorf("probe row %q has %d columns, want 4", raw, len(fields))
	}
	probe := vfsProbe{raw: strings.TrimSpace(raw)}
	for i, target := range []*int64{&probe.count, &probe.min, &probe.max, &probe.sum} {
		value, err := strconv.ParseInt(strings.TrimSpace(fields[i]), 10, 64)
		if err != nil {
			return vfsProbe{}, fmt.Errorf("probe row %q column %d: %w", raw, i+1, err)
		}
		*target = value
	}
	return probe, nil
}

func (p vfsProbe) String() string {
	if p.txid == 0 {
		return p.raw
	}
	return p.raw + "@" + formatTXID(p.txid)
}

// vfsSnapshotOutcome is one held read transaction. The cycle takes the first
// probe inside the transaction, then commits one probe update on the primary
// and syncs Litestream, so syncTXID is a position that holds the update and
// that the first read could not have seen. It waits inside the transaction
// for the reader to reach syncTXID, re-reads the same query, commits and
// reads once more. Repeatable reads hold when every repeat equals the first;
// the post-COMMIT read must equal the primary's aggregate after the update
// (expected) or the cycle proved nothing about the reader.
type vfsSnapshotOutcome struct {
	hydration string
	first     vfsProbe
	expected  string
	syncTXID  uint64
	reached   uint64
	repeats   []vfsProbe
	after     vfsProbe
	committed bool
	err       error
	logTail   string
}

func (o vfsSnapshotOutcome) drifted() []int {
	var drifted []int
	for i, repeat := range o.repeats {
		if repeat.raw != o.first.raw {
			drifted = append(drifted, i+1)
		}
	}
	return drifted
}

// confirmed reports whether the post-COMMIT read shows the probe update the
// cycle committed, which proves the reader applied the txid it waited for.
func (o vfsSnapshotOutcome) confirmed() bool {
	return o.committed && o.expected != "" && o.expected != o.first.raw && o.after.raw == o.expected
}

func (o vfsSnapshotOutcome) noEvidence() string {
	if o.expected == o.first.raw {
		return fmt.Sprintf("probe update left the aggregate at %s", o.expected)
	}
	return fmt.Sprintf("probe after COMMIT %s did not match the primary %s after the update synced at txid %s", o.after, o.expected, formatTXID(o.syncTXID))
}

func (o vfsSnapshotOutcome) driftError() string {
	drifted := o.drifted()
	if len(drifted) == 0 {
		return ""
	}
	repeat := o.repeats[drifted[0]-1]
	return fmt.Sprintf("vfs snapshot drift: repeat %d of %d inside one read transaction returned %s, first read returned %s (%d of %d repeats differ)", drifted[0], len(o.repeats), repeat, o.first, len(drifted), len(o.repeats))
}

func (o vfsSnapshotOutcome) summary() string {
	parts := []string{"hydration=" + firstNonEmpty(o.hydration, "pending")}
	if o.first.raw != "" {
		parts = append(parts, "first="+o.first.String())
	}
	if o.expected != "" {
		parts = append(parts, "primary_after_update="+o.expected, "sync_txid="+formatTXID(o.syncTXID), "reader_reached="+formatTXID(o.reached))
	}
	if len(o.repeats) > 0 {
		parts = append(parts, fmt.Sprintf("repeats=%d drifted=%d last_repeat=%s", len(o.repeats), len(o.drifted()), o.repeats[len(o.repeats)-1]))
	}
	if o.committed {
		parts = append(parts, "after_commit="+o.after.String(), fmt.Sprintf("confirmed=%t", o.confirmed()))
	}
	if o.err != nil {
		parts = append(parts, "error="+o.err.Error())
	}
	return strings.Join(parts, " ")
}

func vfsSnapshotResult(outcome vfsSnapshotOutcome, startedAt, completedAt time.Time) VerificationResult {
	result := newVFSResult(vfsSnapshotStep, vfsSnapshotRestoreMode, outcome.summary(), outcome.logTail, startedAt, completedAt)
	switch {
	case outcome.err != nil:
		vfsInconclusive(&result, vfsSnapshotStep, "vfs snapshot check skipped: "+outcome.err.Error(), vfsInconclusiveSignature(outcome.err))
	case len(outcome.drifted()) > 0:
		vfsFailed(&result, outcome.driftError(), "vfs snapshot check failed")
	case !outcome.confirmed():
		vfsInconclusive(&result, vfsSnapshotStep, fmt.Sprintf("vfs snapshot check skipped: %s: %s", errVFSReaderNotCaughtUp, outcome.noEvidence()), reporting.SignatureVFSReaderNotCaughtUp)
	default:
		result.Status = "passed"
		result.Passed = true
		result.Summary = fmt.Sprintf("vfs snapshot check passed: %d repeats matched the first read after the reader applied the probe update synced at txid %s", len(outcome.repeats), formatTXID(outcome.syncTXID))
	}
	return result
}

// vfsReadQuery is one statement run on both the primary and the VFS reader
// once the reader has reached the verification boundary.
type vfsReadQuery struct {
	name      string
	statement string
	primary   string
	vfs       string
}

type vfsReadOutcome struct {
	boundary  uint64
	txid      uint64
	hydration string
	queries   []vfsReadQuery
	err       error
	logTail   string
}

func (o vfsReadOutcome) mismatches() []vfsReadQuery {
	var mismatched []vfsReadQuery
	for _, query := range o.queries {
		if query.primary != query.vfs {
			mismatched = append(mismatched, query)
		}
	}
	return mismatched
}

func (o vfsReadOutcome) mismatchError() string {
	mismatched := o.mismatches()
	if len(mismatched) == 0 {
		return ""
	}
	first := mismatched[0]
	return fmt.Sprintf("vfs read mismatch: %s returned vfs=%q primary=%q at txid %s (%d of %d queries differ)", first.name, first.vfs, first.primary, formatTXID(o.boundary), len(mismatched), len(o.queries))
}

func (o vfsReadOutcome) summary() string {
	parts := []string{fmt.Sprintf("boundary=%s reader=%s hydration=%s", formatTXID(o.boundary), formatTXID(o.txid), firstNonEmpty(o.hydration, "pending"))}
	if len(o.queries) > 0 {
		parts = append(parts, fmt.Sprintf("queries=%d mismatched=%d", len(o.queries), len(o.mismatches())))
	}
	for _, query := range o.mismatches()[:min(len(o.mismatches()), 5)] {
		parts = append(parts, fmt.Sprintf("%s vfs=%q primary=%q", query.name, query.vfs, query.primary))
	}
	if o.err != nil {
		parts = append(parts, "error="+o.err.Error())
	}
	return strings.Join(parts, " ")
}

func vfsReadResult(outcome vfsReadOutcome, startedAt, completedAt time.Time) VerificationResult {
	result := newVFSResult(vfsReadStep, vfsReadRestoreMode, outcome.summary(), outcome.logTail, startedAt, completedAt)
	result.SyncTXID = outcome.boundary
	result.BoundaryTXID = outcome.boundary
	switch {
	case outcome.err != nil:
		vfsInconclusive(&result, vfsReadStep, "vfs read check skipped: "+outcome.err.Error(), vfsInconclusiveSignature(outcome.err))
	case len(outcome.queries) == 0:
		vfsInconclusive(&result, vfsReadStep, "vfs read check skipped: no queries compared", reporting.SignatureVFSReaderUnavailable)
	case len(outcome.mismatches()) > 0:
		vfsFailed(&result, outcome.mismatchError(), "vfs read check failed")
	default:
		result.Status = "passed"
		result.Passed = true
		result.Summary = fmt.Sprintf("vfs read check passed: %d queries matched the primary at txid %s", len(outcome.queries), formatTXID(outcome.boundary))
	}
	return result
}

func newVFSResult(step, mode, summary, logTail string, startedAt, completedAt time.Time) VerificationResult {
	output := summary
	if logTail != "" {
		output += "\n" + logTail
	}
	return VerificationResult{
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		CheckType:   vfsCheckType,
		RestoreMode: mode,
		DurationMS:  int(completedAt.Sub(startedAt).Milliseconds()),
		Steps:       []reporting.VerificationStep{{Name: step, StartedAt: startedAt, CompletedAt: completedAt, Status: "ok", OutputTail: tailString(output, 8192)}},
	}
}

func vfsInconclusive(result *VerificationResult, step, message, signature string) {
	result.Steps[0].Status = "inconclusive"
	result.Steps[0].Error = message
	result.Status = "aborted"
	result.ErrorMessage = message
	result.Summary = "verification inconclusive: " + message
	result.InconclusiveClassification = &reporting.FailureClassification{Stage: step, Signature: signature, RestoreMode: result.RestoreMode}
}

func vfsFailed(result *VerificationResult, message, summary string) {
	result.Steps[0].Status = "error"
	result.Steps[0].Error = message
	result.Status = "failed"
	result.ErrorMessage = message
	result.Summary = summary
}

func vfsInconclusiveSignature(err error) string {
	switch {
	case errors.Is(err, errVFSHydrationIncomplete), errors.Is(err, errVFSHydrationFailed):
		return reporting.SignatureVFSHydrationIncomplete
	case errors.Is(err, errVFSReaderNotCaughtUp):
		return reporting.SignatureVFSReaderNotCaughtUp
	default:
		return reporting.SignatureVFSReaderUnavailable
	}
}

// vfsReader is what the checks need from a reader session; vfsSession is the
// sqlite3 shell implementation.
type vfsReader interface {
	exec(context.Context, string) ([]string, error)
	txid(context.Context) (uint64, error)
	hydration() (vfsHydration, error)
	logOversized() bool
	alive() bool
	logTail() string
	close()
}

// vfsReaderCheck owns the two VFS reader sessions: one that holds long read
// transactions while the primary writes, and one the verifier queries at each
// boundary. A session that fails a statement is dropped and started again on
// the next cycle, resuming from its persisted hydration file.
type vfsReaderCheck struct {
	cfg              Config
	start            func(context.Context, string) (vfsReader, error)
	primaryQueries   func(context.Context) ([]vfsReadQuery, error)
	holdProbe        func()
	releaseProbe     func()
	updateProbe      func(context.Context) (string, error)
	sync             func(context.Context) (uint64, error)
	clock            func() time.Time
	repeats          int
	repeatInterval   time.Duration
	pause            time.Duration
	hydrationBudget  time.Duration
	readHydration    time.Duration
	hydrationPoll    time.Duration
	catchUpBudget    time.Duration
	catchUpPoll      time.Duration
	writeBudget      time.Duration
	writePoll        time.Duration
	logSettle        time.Duration
	statementTimeout time.Duration

	mu       sync.Mutex
	sessions map[string]vfsReader
	stop     context.CancelFunc
	done     chan struct{}
}

func newVFSReaderCheck(cfg Config) *vfsReaderCheck {
	check := &vfsReaderCheck{
		cfg:              cfg,
		clock:            time.Now,
		repeats:          vfsSnapshotRepeats,
		repeatInterval:   vfsSnapshotRepeatInterval,
		pause:            vfsSnapshotPause,
		hydrationBudget:  vfsHydrationBudget,
		readHydration:    vfsReadHydrationBudget,
		hydrationPoll:    vfsHydrationPoll,
		catchUpBudget:    vfsCatchUpBudget,
		catchUpPoll:      vfsCatchUpPoll,
		writeBudget:      vfsProbeWriteBudget,
		writePoll:        vfsProbeWritePoll,
		logSettle:        vfsLogSettle,
		statementTimeout: vfsStatementTimeout,
		sessions:         map[string]vfsReader{},
	}
	check.start = func(ctx context.Context, name string) (vfsReader, error) {
		spec := cfg.vfsSessionSpec(name)
		spec.pragmas = vfsReaderPragmas
		startCtx, cancel := context.WithTimeout(ctx, check.hydrationBudget)
		defer cancel()
		return startVFSSession(startCtx, name, spec)
	}
	check.primaryQueries = func(ctx context.Context) ([]vfsReadQuery, error) { return primaryReadQueries(ctx, cfg.DBPath) }
	check.holdProbe = func() {}
	check.releaseProbe = func() {}
	check.updateProbe = func(context.Context) (string, error) { return "", errCacheProbePaused }
	check.sync = func(ctx context.Context) (uint64, error) {
		response, err := requestLitestreamSync(ctx, newIPCClient(cfg.SocketPath, vfsSyncBudget+30*time.Second), cfg.DBPath, vfsSyncBudget)
		if err != nil {
			return 0, err
		}
		if response.TXID == 0 {
			return 0, errors.New("sync returned no txid")
		}
		return response.TXID, nil
	}
	return check
}

// useProbeWriter lets snapshot cycles own the probe updates: the background
// ticker is held while a cycle runs and each cycle commits its own update.
func (c *vfsReaderCheck) useProbeWriter(writer *cacheProbeWriter) {
	c.holdProbe = writer.Hold
	c.releaseProbe = writer.Release
	c.updateProbe = writer.Update
}

func (c *vfsReaderCheck) session(ctx context.Context, name string) (vfsReader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if session := c.sessions[name]; session != nil && session.alive() && !session.logOversized() {
		return session, nil
	}
	if session := c.sessions[name]; session != nil {
		if session.alive() {
			slog.Info("VFS reader log reached its cap; starting a fresh session", "session", name)
		}
		session.close()
		delete(c.sessions, name)
	}
	session, err := c.start(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errVFSReaderUnavailable, err)
	}
	c.sessions[name] = session
	return session, nil
}

func (c *vfsReaderCheck) drop(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if session := c.sessions[name]; session != nil {
		session.close()
		delete(c.sessions, name)
	}
}

func (c *vfsReaderCheck) Stop() {
	c.mu.Lock()
	stop, done := c.stop, c.done
	c.mu.Unlock()
	if stop != nil {
		stop()
	}
	if done != nil {
		<-done
	}
	for _, name := range []string{vfsSnapshotSession, vfsReadSession} {
		c.drop(name)
	}
}

// StartSnapshotLoop holds one read transaction after another on the snapshot
// session and reports each as a verification.
func (c *vfsReaderCheck) StartSnapshotLoop(ctx context.Context, report func(VerificationResult)) {
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.stop = cancel
	c.done = make(chan struct{})
	c.mu.Unlock()
	go func() {
		defer close(c.done)
		for {
			result := c.snapshotCycle(runCtx)
			if runCtx.Err() != nil {
				return
			}
			report(result)
			if err := sleepContext(runCtx, c.pause); err != nil {
				return
			}
		}
	}()
}

func (c *vfsReaderCheck) snapshotCycle(ctx context.Context) VerificationResult {
	startedAt := c.clock().UTC()
	outcome := c.runSnapshot(ctx)
	return vfsSnapshotResult(outcome, startedAt, c.clock().UTC())
}

func (c *vfsReaderCheck) runSnapshot(ctx context.Context) (outcome vfsSnapshotOutcome) {
	session, err := c.session(ctx, vfsSnapshotSession)
	if err != nil {
		outcome.err = err
		return outcome
	}
	inTransaction := false
	defer func() {
		if inTransaction && vfsSessionReusable(outcome.err) {
			if _, err := c.exec(ctx, session, "ROLLBACK"); err != nil {
				outcome.err = err
			}
		}
		outcome.logTail = session.logTail()
		if !vfsSessionReusable(outcome.err) {
			c.drop(vfsSnapshotSession)
		}
	}()
	if outcome.hydration, outcome.err = c.waitHydrated(ctx, session, c.hydrationBudget); outcome.err != nil {
		return outcome
	}
	c.holdProbe()
	defer c.releaseProbe()
	if _, outcome.err = c.exec(ctx, session, "BEGIN"); outcome.err != nil {
		return outcome
	}
	inTransaction = true
	if outcome.first, outcome.err = c.probe(ctx, session); outcome.err != nil {
		return outcome
	}
	if outcome.expected, outcome.err = c.writeProbe(ctx); outcome.err != nil {
		return outcome
	}
	if outcome.syncTXID, outcome.err = c.sync(ctx); outcome.err != nil {
		outcome.err = fmt.Errorf("%w: sync after probe update: %v", errVFSProbeSyncFailed, outcome.err)
		return outcome
	}
	if outcome.reached, outcome.err = c.waitTXID(ctx, session, outcome.syncTXID); outcome.err != nil {
		return outcome
	}
	if outcome.err = c.hydrationActive(session); outcome.err != nil {
		return outcome
	}
	for i := 0; i < c.repeats; i++ {
		if outcome.err = sleepContext(ctx, c.repeatInterval); outcome.err != nil {
			return outcome
		}
		repeat, err := c.probe(ctx, session)
		if err != nil {
			outcome.err = err
			return outcome
		}
		outcome.repeats = append(outcome.repeats, repeat)
	}
	if _, outcome.err = c.exec(ctx, session, "COMMIT"); outcome.err != nil {
		return outcome
	}
	inTransaction = false
	outcome.committed = true
	if outcome.after, outcome.err = c.probe(ctx, session); outcome.err != nil {
		return outcome
	}
	outcome.err = c.settledHydrationActive(ctx, session)
	return outcome
}

// settledHydrationActive waits out one poll interval before the final log
// scan, so a page-apply failure the VFS logs only after its poll releases the
// file mutex is seen before the cycle is judged.
func (c *vfsReaderCheck) settledHydrationActive(ctx context.Context, session vfsReader) error {
	if err := sleepContext(ctx, c.logSettle); err != nil {
		return fmt.Errorf("%w: %v", errVFSReaderUnavailable, err)
	}
	return c.hydrationActive(session)
}

// hydrationActive folds the log written since the reader was judged ready and
// fails the cycle when the VFS has since turned hydrated reads off: a cycle
// that ran on replica reads proves nothing about the hydrated reader.
func (c *vfsReaderCheck) hydrationActive(session vfsReader) error {
	state, err := session.hydration()
	if err != nil {
		return fmt.Errorf("%w: read vfs log: %v", errVFSReaderUnavailable, err)
	}
	if state.failure != "" {
		return fmt.Errorf("%w: %s", errVFSHydrationFailed, state.failure)
	}
	return nil
}

// writeProbe commits one probe update on the primary once the verifier is not
// holding the load paused, and returns the primary's aggregate after it.
func (c *vfsReaderCheck) writeProbe(ctx context.Context) (string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, c.writeBudget)
	defer cancel()
	for {
		expected, err := c.updateProbe(ctx)
		if err == nil {
			return expected, nil
		}
		if !errors.Is(err, errCacheProbePaused) {
			return "", fmt.Errorf("%w: probe update: %v", errVFSReaderUnavailable, err)
		}
		if err := sleepContext(waitCtx, c.writePoll); err != nil {
			if ctx.Err() != nil {
				return "", fmt.Errorf("%w: %v", errVFSReaderUnavailable, ctx.Err())
			}
			return "", fmt.Errorf("%w: load stayed paused for verification for %s, no probe update committed", errVFSReaderNotCaughtUp, c.writeBudget)
		}
	}
}

// ReadCheck compares the VFS reader with the primary at a verification
// boundary: the load is paused and the replica holds every txid up to the
// boundary, so once the reader's position reaches it the two must agree.
func (c *vfsReaderCheck) ReadCheck(ctx context.Context, boundary uint64) VerificationResult {
	startedAt := c.clock().UTC()
	outcome := c.runRead(ctx, boundary)
	return vfsReadResult(outcome, startedAt, c.clock().UTC())
}

func (c *vfsReaderCheck) runRead(ctx context.Context, boundary uint64) (outcome vfsReadOutcome) {
	outcome.boundary = boundary
	if boundary == 0 {
		outcome.err = fmt.Errorf("%w: boundary txid unavailable", errVFSReaderNotCaughtUp)
		return outcome
	}
	session, err := c.session(ctx, vfsReadSession)
	if err != nil {
		outcome.err = err
		return outcome
	}
	defer func() {
		outcome.logTail = session.logTail()
		if !vfsSessionReusable(outcome.err) {
			c.drop(vfsReadSession)
		}
	}()
	if outcome.hydration, outcome.err = c.waitHydrated(ctx, session, c.readHydration); outcome.err != nil {
		return outcome
	}
	if outcome.txid, outcome.err = c.waitTXID(ctx, session, boundary); outcome.err != nil {
		return outcome
	}
	if outcome.err = c.hydrationActive(session); outcome.err != nil {
		return outcome
	}
	queries, err := c.primaryQueries(ctx)
	if err != nil {
		outcome.err = fmt.Errorf("%w: read primary: %v", errVFSReaderUnavailable, err)
		return outcome
	}
	for i := range queries {
		rows, err := c.exec(ctx, session, queries[i].statement)
		if err != nil {
			outcome.err = err
			return outcome
		}
		queries[i].vfs = strings.Join(rows, "\n")
	}
	outcome.queries = queries
	outcome.err = c.settledHydrationActive(ctx, session)
	return outcome
}

// vfsSessionReusable reports whether the session that produced err still has
// a clean stream: only a budget that ran out between statements keeps it.
func vfsSessionReusable(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, errVFSStatementTimeout) {
		return false
	}
	return errors.Is(err, errVFSHydrationIncomplete) || errors.Is(err, errVFSReaderNotCaughtUp) || errors.Is(err, errVFSProbeSyncFailed)
}

func (c *vfsReaderCheck) exec(ctx context.Context, session vfsReader, statement string) ([]string, error) {
	execCtx, cancel := context.WithTimeout(ctx, c.statementTimeout)
	defer cancel()
	rows, err := session.exec(execCtx, statement)
	if err != nil {
		return rows, fmt.Errorf("%w: %v", errVFSReaderUnavailable, err)
	}
	return rows, nil
}

func (c *vfsReaderCheck) probe(ctx context.Context, session vfsReader) (vfsProbe, error) {
	rows, err := c.exec(ctx, session, cacheProbeQuery)
	if err != nil {
		return vfsProbe{}, err
	}
	if len(rows) != 1 {
		return vfsProbe{}, fmt.Errorf("%w: probe returned %d rows", errVFSReaderUnavailable, len(rows))
	}
	probe, err := parseVFSProbe(rows[0])
	if err != nil {
		return vfsProbe{}, fmt.Errorf("%w: %v", errVFSReaderUnavailable, err)
	}
	execCtx, cancel := context.WithTimeout(ctx, c.statementTimeout)
	defer cancel()
	if probe.txid, err = session.txid(execCtx); err != nil {
		return vfsProbe{}, fmt.Errorf("%w: %v", errVFSReaderUnavailable, err)
	}
	return probe, nil
}

// waitHydrated waits for the VFS to log that hydration is complete, which is
// when the reader serves pages from its local copy; before that every page is
// one request to the replica and a probe scan takes minutes. It reads the log
// rather than running a statement, so it never blocks behind the VFS mutex.
// The snapshot loop can afford the full budget; the read check runs while the
// load is paused, so it gives up sooner and reports the cycle inconclusive
// while hydration continues in the background.
func (c *vfsReaderCheck) waitHydrated(ctx context.Context, session vfsReader, budget time.Duration) (string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	for {
		state, err := session.hydration()
		if err != nil {
			return "", fmt.Errorf("%w: read vfs log: %v", errVFSReaderUnavailable, err)
		}
		if state.failure != "" {
			return "failed", fmt.Errorf("%w: %s", errVFSHydrationFailed, state.failure)
		}
		if state.complete {
			return "complete", nil
		}
		if err := sleepContext(waitCtx, c.hydrationPoll); err != nil {
			if ctx.Err() != nil {
				return "", fmt.Errorf("%w: %v", errVFSReaderUnavailable, ctx.Err())
			}
			return "pending", fmt.Errorf("%w: not complete after %s", errVFSHydrationIncomplete, budget)
		}
	}
}

func (c *vfsReaderCheck) waitTXID(ctx context.Context, session vfsReader, boundary uint64) (uint64, error) {
	waitCtx, cancel := context.WithTimeout(ctx, c.catchUpBudget)
	defer cancel()
	var seen uint64
	for {
		var err error
		if seen, err = session.txid(waitCtx); err != nil {
			if ctx.Err() == nil && waitCtx.Err() != nil {
				return seen, fmt.Errorf("%w: %w: reader at txid %s, waiting for %s: %v", errVFSReaderNotCaughtUp, errVFSStatementTimeout, vfsTXIDText(seen), formatTXID(boundary), err)
			}
			return seen, fmt.Errorf("%w: %v", errVFSReaderUnavailable, err)
		}
		if seen >= boundary {
			return seen, nil
		}
		if err := sleepContext(waitCtx, c.catchUpPoll); err != nil {
			if ctx.Err() != nil {
				return seen, fmt.Errorf("%w: %v", errVFSReaderUnavailable, ctx.Err())
			}
			return seen, fmt.Errorf("%w: reader at txid %s after %s, boundary %s", errVFSReaderNotCaughtUp, vfsTXIDText(seen), c.catchUpBudget, formatTXID(boundary))
		}
	}
}

// vfsTXIDText names a reader position in messages; zero means the position was
// never read, which formatTXID would print as nothing.
func vfsTXIDText(txid uint64) string {
	if txid == 0 {
		return "unknown"
	}
	return formatTXID(txid)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// primaryReadQueries reads the probe aggregate and every user table's row
// count from the quiesced primary, formatted the way the sqlite3 shell prints
// them in list mode so the VFS rows compare byte for byte.
func primaryReadQueries(ctx context.Context, path string) ([]vfsReadQuery, error) {
	uri := url.URL{Scheme: "file", Path: path, RawQuery: fmt.Sprintf("mode=ro&_pragma=busy_timeout(%d)", logicalReadBusyTimeout.Milliseconds())}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	tables, err := db.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT ?", vfsReadTableLimit)
	if err != nil {
		return nil, err
	}
	queries := []vfsReadQuery{{name: cacheProbeTable + " aggregate", statement: cacheProbeQuery}}
	for tables.Next() {
		var name string
		if err := tables.Scan(&name); err != nil {
			_ = tables.Close()
			return nil, err
		}
		queries = append(queries, vfsReadQuery{name: name + " count", statement: `SELECT count(*) FROM "` + strings.ReplaceAll(name, `"`, `""`) + `"`})
	}
	if err := tables.Err(); err != nil {
		return nil, err
	}
	for i := range queries {
		if queries[i].primary, err = queryListRows(ctx, db, queries[i].statement); err != nil {
			return nil, fmt.Errorf("%s: %w", queries[i].name, err)
		}
	}
	return queries, nil
}

func queryListRows(ctx context.Context, db *sql.DB, statement string) (string, error) {
	rows, err := db.QueryContext(ctx, statement)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	var lines []string
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return "", err
		}
		fields := make([]string, len(values))
		for i, value := range values {
			fields[i] = value.String
		}
		lines = append(lines, strings.Join(fields, "|"))
	}
	return strings.Join(lines, "\n"), rows.Err()
}

func (r *Runner) vfsReaderCheckEnabled() bool {
	return r.cfg.VFSReaderCheck && !r.cfg.ManyDBEnabled()
}

func (r *Runner) reportVFSResult(ctx context.Context, result VerificationResult) {
	if result.Status == "failed" {
		r.triggerProfile("verification-failed")
	}
	r.sendVerification(context.WithoutCancel(ctx), result, nil)
	r.recordLocalResult(result)
	switch result.Status {
	case "failed":
		slog.Error("VFS reader check failed", "mode", result.RestoreMode, "error", result.ErrorMessage)
	case "aborted":
		slog.Warn("VFS reader check inconclusive", "mode", result.RestoreMode, "error", result.ErrorMessage)
	default:
		slog.Info("VFS reader check passed", "mode", result.RestoreMode, "summary", result.Summary)
	}
}
