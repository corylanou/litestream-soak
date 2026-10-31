package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

// fakeVFSReader scripts a reader session: successive probe rows, replica
// txids and hydration percentages are handed out in order and the last value
// repeats once a list runs out. Statements in rows answer read-check queries,
// and the statement named by failAt closes the session the way -bail does.
type fakeVFSReader struct {
	mu         sync.Mutex
	probes     []string
	txids      []uint64
	hydrations []vfsHydration
	rows       map[string][]string
	failAt     string
	hangTXID   bool
	statements []string
	oversized  bool
	closed     bool
}

func (f *fakeVFSReader) exec(_ context.Context, statement string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statements = append(f.statements, statement)
	if f.closed {
		return nil, errVFSSessionClosed
	}
	if statement == f.failAt {
		f.closed = true
		return nil, fmt.Errorf("%w after %q: Runtime error: no such table", errVFSSessionClosed, statement)
	}
	if statement == cacheProbeQuery {
		return []string{next(&f.probes)}, nil
	}
	if rows, ok := f.rows[statement]; ok {
		return rows, nil
	}
	return nil, nil
}

func (f *fakeVFSReader) txid(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, errVFSSessionClosed
	}
	if f.hangTXID {
		<-ctx.Done()
		return 0, fmt.Errorf("vfs reader statement %q: %w", "PRAGMA litestream_txid", ctx.Err())
	}
	return next(&f.txids), nil
}

func (f *fakeVFSReader) hydration() (vfsHydration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.hydrations) == 0 {
		return vfsHydration{complete: true}, nil
	}
	return next(&f.hydrations), nil
}

func (f *fakeVFSReader) logOversized() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.oversized
}

func (f *fakeVFSReader) alive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.closed
}

func (f *fakeVFSReader) logTail() string { return "level=DEBUG msg=\"hydration complete\"" }

func (f *fakeVFSReader) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func next[T any](values *[]T) T {
	if len(*values) == 0 {
		var zero T
		return zero
	}
	value := (*values)[0]
	if len(*values) > 1 {
		*values = (*values)[1:]
	}
	return value
}

func (f *fakeVFSReader) count(statement string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.statements {
		if s == statement {
			n++
		}
	}
	return n
}

// fakeProbeWriter stands in for the cache probe writer: Update hands out the
// scripted primary aggregates (or the paused error) and counts holds.
type fakeProbeWriter struct {
	mu        sync.Mutex
	updates   []string
	updateErr error
	holds     int
	releases  int
	calls     int
}

func (f *fakeProbeWriter) hold() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.holds++
}

func (f *fakeProbeWriter) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
}

func (f *fakeProbeWriter) update(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.updateErr != nil {
		return "", f.updateErr
	}
	f.calls++
	return next(&f.updates), nil
}

func newTestVFSReaderCheck(t *testing.T, reader *fakeVFSReader, startErr error) (*vfsReaderCheck, *fakeProbeWriter) {
	t.Helper()
	check := newVFSReaderCheck(Config{DBPath: t.TempDir() + "/test.db"})
	writer := &fakeProbeWriter{updates: []string{"2048|0|1|128"}}
	check.holdProbe = writer.hold
	check.releaseProbe = writer.release
	check.updateProbe = writer.update
	check.sync = func(context.Context) (uint64, error) { return 12, nil }
	check.writeBudget = 50 * time.Millisecond
	check.writePoll = time.Millisecond
	check.logSettle = 0
	check.repeats = 2
	check.repeatInterval = 0
	check.pause = 0
	check.hydrationBudget = 50 * time.Millisecond
	check.readHydration = 50 * time.Millisecond
	check.hydrationPoll = time.Millisecond
	check.catchUpBudget = 50 * time.Millisecond
	check.catchUpPoll = time.Millisecond
	check.statementTimeout = time.Second
	check.start = func(context.Context, string) (vfsReader, error) {
		if startErr != nil {
			return nil, startErr
		}
		return reader, nil
	}
	t.Cleanup(check.Stop)
	return check, writer
}

func TestParseVFSProbe(t *testing.T) {
	probe, err := parseVFSProbe("2048|3|4|7000")
	if err != nil || probe != (vfsProbe{raw: "2048|3|4|7000", count: 2048, min: 3, max: 4, sum: 7000}) {
		t.Fatalf("probe = %+v, err %v", probe, err)
	}
	for _, raw := range []string{"", "2048|3|4", "2048|3|4|x", "a|b|c|d"} {
		if _, err := parseVFSProbe(raw); err == nil {
			t.Fatalf("parseVFSProbe(%q) accepted a bad row", raw)
		}
	}
}

func TestVFSSnapshotCycleJudgesRepeatableReads(t *testing.T) {
	const (
		first   = "2048|0|0|0"
		updated = "2048|0|1|128"
	)
	tests := []struct {
		name          string
		reader        *fakeVFSReader
		writer        *fakeProbeWriter
		sync          func(context.Context) (uint64, error)
		startErr      error
		wantStatus    string
		wantSignature string
		wantMessage   string
		wantDropped   bool
		wantUpdates   int
	}{
		{
			name:        "repeats match after the reader applied the cycle's probe update and the post-commit read shows it",
			reader:      &fakeVFSReader{probes: []string{first, first, first, updated}, txids: []uint64{10, 11, 12, 12, 12, 13}},
			wantStatus:  "passed",
			wantUpdates: 1,
		},
		{
			name:          "a repeat that sees the probe update is snapshot drift",
			reader:        &fakeVFSReader{probes: []string{first, first, updated, updated}, txids: []uint64{10, 12, 12, 12, 12}},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureVFSSnapshotDrift,
			wantMessage:   "vfs snapshot drift: repeat 2 of 2 inside one read transaction returned 2048|0|1|128@000000000000000c, first read returned 2048|0|0|0@000000000000000a (1 of 2 repeats differ)",
			wantUpdates:   1,
		},
		{
			name:          "a reader that never reaches the synced txid is not judged",
			reader:        &fakeVFSReader{probes: []string{first}, txids: []uint64{10, 11}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "reader at txid 000000000000000b after",
			wantUpdates:   1,
		},
		{
			name:          "a failed sync after the probe update is inconclusive",
			reader:        &fakeVFSReader{probes: []string{first}, txids: []uint64{10}},
			sync:          func(context.Context) (uint64, error) { return 0, errors.New("sync returned 503: busy") },
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderUnavailable,
			wantMessage:   "sync after probe update: sync returned 503: busy",
			wantUpdates:   1,
		},
		{
			name:          "a post-commit read that does not show the probe update is inconclusive",
			reader:        &fakeVFSReader{probes: []string{first, first, first, first}, txids: []uint64{10, 12, 12, 12, 12}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "probe after COMMIT 2048|0|0|0@000000000000000c did not match the primary 2048|0|1|128 after the update synced at txid 000000000000000c",
			wantUpdates:   1,
		},
		{
			name:          "a verifier pause that outlasts the write budget commits no probe update",
			reader:        &fakeVFSReader{probes: []string{first}, txids: []uint64{10}},
			writer:        &fakeProbeWriter{updateErr: errCacheProbePaused},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "load stayed paused for verification",
		},
		{
			name:          "hydration still running keeps the session for the next cycle",
			reader:        &fakeVFSReader{probes: []string{first}, hydrations: []vfsHydration{{}}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSHydrationIncomplete,
			wantMessage:   "not complete after",
		},
		{
			name:          "a hydration failure logged mid-cycle is inconclusive, never a pass",
			reader:        &fakeVFSReader{probes: []string{first, first, first, updated}, txids: []uint64{10, 11, 12, 12, 12, 13}, hydrations: []vfsHydration{{complete: true}, {complete: true}, {complete: true, failure: `time=2026-10-31T18:15:44.327Z level=ERROR msg="cannot fetch new ltx files" vfs=true error="apply hydration updates: fetch updated page 8904: open ltx file: file does not exist"`}}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSHydrationIncomplete,
			wantMessage:   `vfs hydration failed: time=2026-10-31T18:15:44.327Z level=ERROR msg="cannot fetch new ltx files"`,
			wantDropped:   true,
			wantUpdates:   1,
		},
		{
			name:          "a hydration failure logged while the reader caught up is inconclusive",
			reader:        &fakeVFSReader{probes: []string{first}, txids: []uint64{10, 12}, hydrations: []vfsHydration{{complete: true}, {complete: true, failure: `level=ERROR msg="cannot fetch new ltx files" error="apply hydration updates: fetch updated page 7: context deadline exceeded"`}}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSHydrationIncomplete,
			wantMessage:   "apply hydration updates",
			wantDropped:   true,
			wantUpdates:   1,
		},
		{
			name:          "a hydration the vfs gave up on drops the session",
			reader:        &fakeVFSReader{probes: []string{first}, hydrations: []vfsHydration{{failure: `time=2026-10-31T17:06:00Z level=ERROR msg="hydration failed" error="open ltx file: no such key"`}}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSHydrationIncomplete,
			wantMessage:   `vfs hydration failed: time=2026-10-31T17:06:00Z level=ERROR msg="hydration failed"`,
			wantDropped:   true,
		},
		{
			name:          "a statement failure mid-transaction drops the session",
			reader:        &fakeVFSReader{probes: []string{first}, txids: []uint64{10, 12}, failAt: "COMMIT"},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderUnavailable,
			wantDropped:   true,
			wantUpdates:   1,
		},
		{
			name:          "a reader that cannot start is unavailable",
			reader:        &fakeVFSReader{},
			startErr:      errors.New("vfs extension unavailable: stat /usr/local/lib/litestream-vfs.so: no such file or directory"),
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderUnavailable,
			wantMessage:   "vfs extension unavailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check, writer := newTestVFSReaderCheck(t, tt.reader, tt.startErr)
			if tt.writer != nil {
				writer = tt.writer
				check.holdProbe, check.releaseProbe, check.updateProbe = writer.hold, writer.release, writer.update
			}
			if tt.sync != nil {
				check.sync = tt.sync
			}
			result := check.snapshotCycle(context.Background())
			if result.Status != tt.wantStatus || result.Passed != (tt.wantStatus == "passed") || result.CheckType != vfsCheckType || result.RestoreMode != vfsSnapshotRestoreMode {
				t.Fatalf("result = %+v", result)
			}
			if len(result.Steps) != 1 || result.Steps[0].Name != vfsSnapshotStep {
				t.Fatalf("steps = %+v", result.Steps)
			}
			if tt.wantMessage != "" && !strings.Contains(result.ErrorMessage, tt.wantMessage) {
				t.Fatalf("error = %q, want %q", result.ErrorMessage, tt.wantMessage)
			}
			switch tt.wantStatus {
			case "failed":
				if got := reporting.ClassifyVerificationFailure(result.CheckType, result.ErrorMessage); got.Signature != tt.wantSignature || got.Stage != vfsCheckType {
					t.Fatalf("classification = %+v, want %s", got, tt.wantSignature)
				}
				if result.InconclusiveClassification != nil || result.Steps[0].Status != "error" {
					t.Fatalf("failed result = %+v", result)
				}
			case "aborted":
				if c := result.InconclusiveClassification; c == nil || c.Signature != tt.wantSignature || c.Stage != vfsSnapshotStep || c.RestoreMode != vfsSnapshotRestoreMode {
					t.Fatalf("inconclusive classification = %+v, want %s", result.InconclusiveClassification, tt.wantSignature)
				}
				if !strings.HasPrefix(result.Summary, "verification inconclusive:") || result.Steps[0].Status != "inconclusive" {
					t.Fatalf("inconclusive result = %+v", result)
				}
			default:
				if result.InconclusiveClassification != nil || !strings.Contains(result.Summary, "2 repeats matched the first read after the reader applied the probe update synced at txid 000000000000000c") {
					t.Fatalf("passed result = %+v", result)
				}
			}
			if tt.startErr == nil && !tt.wantDropped && tt.reader.count("BEGIN") != tt.reader.count("COMMIT")+tt.reader.count("ROLLBACK") {
				t.Fatalf("transactions unbalanced: %v", tt.reader.statements)
			}
			if updates := min(writer.calls, 1); updates != tt.wantUpdates {
				t.Fatalf("probe updates = %d, want %d", updates, tt.wantUpdates)
			}
			if writer.holds != writer.releases {
				t.Fatalf("probe writer holds %d != releases %d", writer.holds, writer.releases)
			}
			if tt.startErr == nil {
				probeIndex, updateCalled := -1, writer.calls > 0
				for i, statement := range tt.reader.statements {
					if statement == cacheProbeQuery {
						probeIndex = i
						break
					}
				}
				if updateCalled && probeIndex < 0 {
					t.Fatalf("probe update committed before the first read: %v", tt.reader.statements)
				}
			}
			check.mu.Lock()
			_, kept := check.sessions[vfsSnapshotSession]
			check.mu.Unlock()
			if tt.startErr == nil && kept == tt.wantDropped {
				t.Fatalf("session kept = %v, want dropped %v", kept, tt.wantDropped)
			}
			if !strings.Contains(result.Steps[0].OutputTail, "hydration=") {
				t.Fatalf("step output = %q", result.Steps[0].OutputTail)
			}
		})
	}
}

func TestVFSReaderCheckSettlesPastOnePollBeforeTheFinalLogScan(t *testing.T) {
	check := newVFSReaderCheck(Config{DBPath: t.TempDir() + "/test.db"})
	if check.logSettle != vfsLogSettle || vfsLogSettle < 2*time.Second {
		t.Fatalf("log settle = %s, want at least one 1s poll interval plus margin", check.logSettle)
	}
	check.logSettle = 40 * time.Millisecond
	reader := &fakeVFSReader{probes: []string{"2048|0|0|0", "2048|0|0|0", "2048|0|0|0", "2048|0|1|128"}, txids: []uint64{10, 11, 12, 12, 12, 13}, hydrations: []vfsHydration{{complete: true}, {complete: true}, {complete: true, failure: `level=ERROR msg="cannot fetch new ltx files" error="apply hydration updates: fetch updated page 5: unexpected EOF"`}}}
	check.repeats, check.repeatInterval, check.pause, check.hydrationPoll, check.catchUpPoll, check.writePoll = 2, 0, 0, time.Millisecond, time.Millisecond, time.Millisecond
	check.statementTimeout, check.hydrationBudget, check.catchUpBudget, check.writeBudget = time.Second, 50*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond
	writer := &fakeProbeWriter{updates: []string{"2048|0|1|128"}}
	check.holdProbe, check.releaseProbe, check.updateProbe = writer.hold, writer.release, writer.update
	check.sync = func(context.Context) (uint64, error) { return 12, nil }
	check.start = func(context.Context, string) (vfsReader, error) { return reader, nil }
	t.Cleanup(check.Stop)
	started := time.Now()
	result := check.snapshotCycle(context.Background())
	if result.Passed || result.Status != "aborted" || !strings.Contains(result.ErrorMessage, "apply hydration updates") {
		t.Fatalf("failure logged during the settle still passed: %+v", result)
	}
	if time.Since(started) < 40*time.Millisecond {
		t.Fatal("final log scan ran before the settle elapsed")
	}
}

func TestVFSReaderCheckRetiresASessionWhoseLogPassedTheCap(t *testing.T) {
	first := &fakeVFSReader{probes: []string{"2048|0|0|0", "2048|0|0|0", "2048|0|0|0", "2048|0|1|128"}, txids: []uint64{10, 11, 12, 12, 12, 13}, oversized: true}
	second := &fakeVFSReader{probes: []string{"2048|0|0|0", "2048|0|0|0", "2048|0|0|0", "2048|0|1|128"}, txids: []uint64{10, 11, 12, 12, 12, 13}}
	check, writer := newTestVFSReaderCheck(t, first, nil)
	writer.updates = []string{"2048|0|1|128", "2048|0|2|256"}
	starts := 0
	check.start = func(context.Context, string) (vfsReader, error) {
		starts++
		if starts == 1 {
			return first, nil
		}
		return second, nil
	}
	if result := check.snapshotCycle(context.Background()); !result.Passed {
		t.Fatalf("first cycle = %+v", result)
	}
	if starts != 1 || first.closed {
		t.Fatalf("first cycle starts=%d closed=%v", starts, first.closed)
	}
	second.probes = []string{"2048|0|1|128", "2048|0|1|128", "2048|0|1|128", "2048|0|2|256"}
	second.txids = []uint64{13, 14, 15, 15, 15, 16}
	if result := check.snapshotCycle(context.Background()); !result.Passed {
		t.Fatalf("second cycle = %+v", result)
	}
	if starts != 2 || !first.closed || second.closed {
		t.Fatalf("oversized log did not retire the session: starts=%d first closed=%v second closed=%v", starts, first.closed, second.closed)
	}
	if len(first.statements) == 0 || first.statements[len(first.statements)-1] == "BEGIN" {
		t.Fatalf("first session left mid-transaction: %v", first.statements)
	}
}

func TestVFSSnapshotCycleUpdatesProbeOnlyAfterTheFirstRead(t *testing.T) {
	reader := &fakeVFSReader{probes: []string{"2048|0|0|0", "2048|0|0|0", "2048|0|0|0", "2048|0|1|128"}, txids: []uint64{10, 12, 12, 12, 12}}
	check, writer := newTestVFSReaderCheck(t, reader, nil)
	var order []string
	check.updateProbe = func(ctx context.Context) (string, error) {
		order = append(order, "update")
		return writer.update(ctx)
	}
	check.sync = func(context.Context) (uint64, error) {
		order = append(order, "sync")
		return 12, nil
	}
	if result := check.snapshotCycle(context.Background()); !result.Passed {
		t.Fatalf("result = %+v", result)
	}
	if strings.Join(order, ",") != "update,sync" {
		t.Fatalf("order = %v", order)
	}
	var firstRead, update int
	for i, statement := range reader.statements {
		if statement == cacheProbeQuery && firstRead == 0 {
			firstRead = i + 1
		}
	}
	update = firstRead + 1
	if firstRead == 0 || reader.statements[0] != "BEGIN" || update <= firstRead {
		t.Fatalf("first read must precede the probe update: %v", reader.statements)
	}
}

func TestVFSReadCheckComparesReaderWithPrimaryAtBoundary(t *testing.T) {
	queries := func(count string) []vfsReadQuery {
		return []vfsReadQuery{
			{name: cacheProbeTable + " aggregate", statement: cacheProbeQuery, primary: "2048|3|4|7000"},
			{name: "load_test count", statement: `SELECT count(*) FROM "load_test"`, primary: count},
		}
	}
	tests := []struct {
		name          string
		boundary      uint64
		reader        *fakeVFSReader
		primary       []vfsReadQuery
		primaryErr    error
		wantStatus    string
		wantSignature string
		wantMessage   string
		wantDropped   bool
	}{
		{
			name:       "reader at the boundary matches the primary",
			boundary:   20,
			reader:     &fakeVFSReader{probes: []string{"2048|3|4|7000"}, txids: []uint64{18, 20}, rows: map[string][]string{`SELECT count(*) FROM "load_test"`: {"500"}}},
			primary:    queries("500"),
			wantStatus: "passed",
		},
		{
			name:          "a differing row count is a read mismatch",
			boundary:      20,
			reader:        &fakeVFSReader{probes: []string{"2048|3|4|7000"}, txids: []uint64{20}, rows: map[string][]string{`SELECT count(*) FROM "load_test"`: {"499"}}},
			primary:       queries("500"),
			wantStatus:    "failed",
			wantSignature: reporting.SignatureVFSReadMismatch,
			wantMessage:   `vfs read mismatch: load_test count returned vfs="499" primary="500" at txid 0000000000000014 (1 of 2 queries differ)`,
		},
		{
			name:          "a hydration failure logged during the read check is inconclusive",
			boundary:      20,
			reader:        &fakeVFSReader{probes: []string{"2048|3|4|7000"}, txids: []uint64{20}, rows: map[string][]string{`SELECT count(*) FROM "load_test"`: {"500"}}, hydrations: []vfsHydration{{complete: true}, {complete: true}, {complete: true, failure: `level=ERROR msg="cannot fetch new ltx files" error="apply hydration updates: fetch updated page 9: no such key"`}}},
			primary:       queries("500"),
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSHydrationIncomplete,
			wantMessage:   "apply hydration updates",
			wantDropped:   true,
		},
		{
			name:          "a reader behind the boundary is not judged and keeps its session",
			boundary:      20,
			reader:        &fakeVFSReader{probes: []string{"2048|3|4|7000"}, txids: []uint64{15}},
			primary:       queries("500"),
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "reader at txid 000000000000000f after",
		},
		{
			name:          "a txid statement that times out drops the desynchronized session",
			boundary:      20,
			reader:        &fakeVFSReader{probes: []string{"2048|3|4|7000"}, hangTXID: true},
			primary:       queries("500"),
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "vfs reader statement timed out",
			wantDropped:   true,
		},
		{
			name:          "an unknown boundary is not judged",
			reader:        &fakeVFSReader{},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
		},
		{
			name:          "an unreadable primary is unavailable",
			boundary:      20,
			reader:        &fakeVFSReader{txids: []uint64{20}},
			primaryErr:    errors.New("database is locked"),
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderUnavailable,
			wantDropped:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check, _ := newTestVFSReaderCheck(t, tt.reader, nil)
			check.primaryQueries = func(context.Context) ([]vfsReadQuery, error) { return tt.primary, tt.primaryErr }
			result := check.ReadCheck(context.Background(), tt.boundary)
			if result.Status != tt.wantStatus || result.Passed != (tt.wantStatus == "passed") || result.CheckType != vfsCheckType || result.RestoreMode != vfsReadRestoreMode || result.BoundaryTXID != tt.boundary {
				t.Fatalf("result = %+v", result)
			}
			if tt.wantMessage != "" && !strings.Contains(result.ErrorMessage, tt.wantMessage) {
				t.Fatalf("error = %q, want %q", result.ErrorMessage, tt.wantMessage)
			}
			switch tt.wantStatus {
			case "failed":
				if got := reporting.ClassifyVerificationFailure(result.CheckType, result.ErrorMessage); got.Signature != tt.wantSignature || got.Stage != vfsCheckType {
					t.Fatalf("classification = %+v, want %s", got, tt.wantSignature)
				}
			case "aborted":
				if c := result.InconclusiveClassification; c == nil || c.Signature != tt.wantSignature || c.Stage != vfsReadStep {
					t.Fatalf("inconclusive classification = %+v, want %s", result.InconclusiveClassification, tt.wantSignature)
				}
			default:
				if !strings.Contains(result.Summary, "2 queries matched the primary at txid 0000000000000014") {
					t.Fatalf("summary = %q", result.Summary)
				}
			}
			check.mu.Lock()
			_, kept := check.sessions[vfsReadSession]
			check.mu.Unlock()
			if tt.boundary != 0 && kept == tt.wantDropped {
				t.Fatalf("read session kept = %v, want dropped %v", kept, tt.wantDropped)
			}
		})
	}
}

func TestVFSReaderCheckConfig(t *testing.T) {
	env := map[string]string{
		"VFS_READER_CHECK":         "true",
		"LITESTREAM_VFS_EXTENSION": "/opt/vfs/litestream-vfs.so",
		"DATA_DIR":                 "/data",
		"REPLICA_TYPE":             "s3",
		"S3_BUCKET":                "soak",
		"S3_PATH":                  "rc-aaaaaaaa/worker-gate-vfs",
		"S3_ENDPOINT":              "https://fly.storage.tigris.dev",
		"AWS_ACCESS_KEY_ID":        "key",
		"AWS_SECRET_ACCESS_KEY":    "secret",
	}
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.VFSReaderCheck || cfg.VFSExtensionPath != "/opt/vfs/litestream-vfs.so" || !cfg.WorkloadConfig().VFSReaderCheck {
		t.Fatalf("config = vfs_reader_check %v extension %q workload %v", cfg.VFSReaderCheck, cfg.VFSExtensionPath, cfg.WorkloadConfig().VFSReaderCheck)
	}
	spec := cfg.vfsSessionSpec(vfsSnapshotSession)
	wantEnv := map[string]string{
		"LITESTREAM_REPLICA_URL":       "s3://soak/rc-aaaaaaaa/worker-gate-vfs",
		"LITESTREAM_S3_ENDPOINT":       "https://fly.storage.tigris.dev",
		"LITESTREAM_HYDRATION_ENABLED": "true",
		"LITESTREAM_HYDRATION_PATH":    "/data/vfs/snapshot/hydrated.db",
		"LITESTREAM_WRITE_ENABLED":     "false",
		"LITESTREAM_LOG_LEVEL":         "DEBUG",
		"LITESTREAM_LOG_FILE":          "/data/vfs/snapshot/vfs.log",
		"AWS_ACCESS_KEY_ID":            "key",
		"AWS_SECRET_ACCESS_KEY":        "secret",
	}
	got := map[string]string{}
	for _, entry := range spec.env {
		key, value, _ := strings.Cut(entry, "=")
		got[key] = value
	}
	for key, want := range wantEnv {
		if got[key] != want {
			t.Fatalf("env %s = %q, want %q", key, got[key], want)
		}
	}
	for _, proxy := range []string{"HTTP_PROXY", "HTTPS_PROXY"} {
		if _, set := got[proxy]; set {
			t.Fatalf("reader must not route through the fault proxy: %s set", proxy)
		}
	}
	args := strings.Join(spec.args(), " ")
	if args != `-batch -bail -list -noheader -cmd .load /opt/vfs/litestream-vfs.so -cmd .open "file:/data/vfs/snapshot/replica.db?vfs=litestream&mode=ro"` {
		t.Fatalf("args = %q", args)
	}
	if strings.Contains(args, "secret") {
		t.Fatal("credentials leaked into the shell arguments")
	}
	plain, err := configFromLookup(func(string) string { return "" })
	if err != nil || plain.VFSReaderCheck || plain.vfsExtension() != vfsExtensionDefaultPath {
		t.Fatalf("defaults = vfs_reader_check %v extension %q err %v", plain.VFSReaderCheck, plain.vfsExtension(), err)
	}
}
