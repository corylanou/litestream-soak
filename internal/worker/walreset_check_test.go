package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const testWALPageSize = 4096

// walBuilder writes a synthetic WAL file the way SQLite lays it out: a header
// with its checksum, then frames whose checksum chain continues from the
// header, with the page size and salts the test asks for. A reset starts a
// new generation at frame 0 without shrinking the file.
type walBuilder struct {
	buf           []byte
	bigEndian     bool
	salt1, salt2  uint32
	checkpointSeq uint32
	s0, s1        uint32
	frames        int64
}

func newWALBuilder(bigEndian bool, salt1, salt2 uint32) *walBuilder {
	b := &walBuilder{bigEndian: bigEndian, salt1: salt1, salt2: salt2, checkpointSeq: 40}
	b.writeHeader()
	return b
}

func (b *walBuilder) writeHeader() {
	magic := uint32(walMagicLittle)
	if b.bigEndian {
		magic = walMagicBigEndian
	}
	header := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(header[0:], magic)
	binary.BigEndian.PutUint32(header[4:], 3007000)
	binary.BigEndian.PutUint32(header[8:], testWALPageSize)
	binary.BigEndian.PutUint32(header[12:], b.checkpointSeq)
	binary.BigEndian.PutUint32(header[16:], b.salt1)
	binary.BigEndian.PutUint32(header[20:], b.salt2)
	b.s0, b.s1 = walChecksum(b.bigEndian, 0, 0, header[:24])
	binary.BigEndian.PutUint32(header[24:], b.s0)
	binary.BigEndian.PutUint32(header[28:], b.s1)
	if len(b.buf) < walHeaderSize {
		b.buf = append(b.buf, make([]byte, walHeaderSize-len(b.buf))...)
	}
	copy(b.buf, header)
	b.frames = 0
}

// appendFrame writes one frame at the next slot of the current generation,
// overwriting whatever older generation left there; commit marks it as a
// commit frame.
func (b *walBuilder) appendFrame(page uint32, commit bool, corruptChecksum bool) {
	frame := make([]byte, walFrameHeaderSize+testWALPageSize)
	binary.BigEndian.PutUint32(frame[0:], page)
	if commit {
		binary.BigEndian.PutUint32(frame[4:], page)
	}
	binary.BigEndian.PutUint32(frame[8:], b.salt1)
	binary.BigEndian.PutUint32(frame[12:], b.salt2)
	for i := walFrameHeaderSize; i < len(frame); i++ {
		frame[i] = byte(page + uint32(i))
	}
	b.s0, b.s1 = walChecksum(b.bigEndian, b.s0, b.s1, frame[:8])
	b.s0, b.s1 = walChecksum(b.bigEndian, b.s0, b.s1, frame[walFrameHeaderSize:])
	binary.BigEndian.PutUint32(frame[16:], b.s0)
	binary.BigEndian.PutUint32(frame[20:], b.s1)
	if corruptChecksum {
		frame[16] ^= 0xff
	}
	offset := walHeaderSize + b.frames*int64(len(frame))
	if end := offset + int64(len(frame)); int64(len(b.buf)) < end {
		b.buf = append(b.buf, make([]byte, end-int64(len(b.buf)))...)
	}
	copy(b.buf[offset:], frame)
	b.frames++
}

func (b *walBuilder) reset() {
	b.salt1++
	b.salt2 += 7919
	b.checkpointSeq++
	b.writeHeader()
}

func TestWalkWALGenerationCountsOnlyTheCurrentGenerationsCommittedFrames(t *testing.T) {
	for _, bigEndian := range []bool{false, true} {
		wal := newWALBuilder(bigEndian, 1000, 77)
		for page := uint32(1); page <= 2000; page++ {
			wal.appendFrame(page, page%10 == 0, false)
		}
		before, err := walkWALGeneration(bytes.NewReader(wal.buf), int64(len(wal.buf)))
		if err != nil {
			t.Fatal(err)
		}
		if !before.header.valid || before.committedFrames != 2000 || before.frameSlots != 2000 || before.frameSize != testWALPageSize+walFrameHeaderSize {
			t.Fatalf("big-endian %v before reset = %+v", bigEndian, before)
		}

		wal.reset()
		for page := uint32(1); page <= 300; page++ {
			wal.appendFrame(page, page%10 == 0, false)
		}
		wal.appendFrame(301, false, false)
		after, err := walkWALGeneration(bytes.NewReader(wal.buf), int64(len(wal.buf)))
		if err != nil {
			t.Fatal(err)
		}
		if after.committedFrames != 300 || after.frameSlots != 2000 || after.fileBytes != int64(len(wal.buf)) || after.header.salt1 != 1001 {
			t.Fatalf("big-endian %v after reset = %+v, want 300 committed frames in a 2000-slot file", bigEndian, after)
		}
		if walResetCount(before.header, after.header) != 1 || after.generationEnd() != walHeaderSize+300*after.frameSize {
			t.Fatalf("reset count %d generation end %d", walResetCount(before.header, after.header), after.generationEnd())
		}

		wal.appendFrame(302, true, true)
		wal.appendFrame(303, true, false)
		corrupt, err := walkWALGeneration(bytes.NewReader(wal.buf), int64(len(wal.buf)))
		if err != nil {
			t.Fatal(err)
		}
		if corrupt.committedFrames != 300 {
			t.Fatalf("frames after a bad checksum counted: %+v", corrupt)
		}
	}
	if empty, err := walkWALGeneration(bytes.NewReader(nil), 0); err != nil || empty.header.valid || empty.committedFrames != 0 {
		t.Fatalf("empty wal = %+v, %v", empty, err)
	}
}

func TestParseLTXWALPosition(t *testing.T) {
	raw := make([]byte, ltxHeaderSize)
	copy(raw, "LTX1")
	binary.BigEndian.PutUint64(raw[48:], 32)
	binary.BigEndian.PutUint64(raw[56:], 16_515_072)
	binary.BigEndian.PutUint32(raw[64:], 1000)
	binary.BigEndian.PutUint32(raw[68:], 77)
	position, err := parseLTXWALPosition(raw)
	if err != nil || !position.found || position.walOffsetEnd != 16_515_104 || position.salt1 != 1000 || position.salt2 != 77 {
		t.Fatalf("position = %+v, %v", position, err)
	}
	if _, err := parseLTXWALPosition(append([]byte("JUNK"), raw[4:]...)); err == nil {
		t.Fatal("wrong magic accepted")
	}
	if _, err := parseLTXWALPosition(raw[:50]); err == nil {
		t.Fatal("short header accepted")
	}
}

func TestWALResetCountUsesSaltOneAndWraps(t *testing.T) {
	for name, tc := range map[string]struct {
		before, after walHeader
		want          uint32
	}{
		"five resets":    {before: walHeader{valid: true, salt1: 10}, after: walHeader{valid: true, salt1: 15}, want: 5},
		"no reset":       {before: walHeader{valid: true, salt1: 10}, after: walHeader{valid: true, salt1: 10}, want: 0},
		"wraps at 2^32":  {before: walHeader{valid: true, salt1: 0xfffffffe}, after: walHeader{valid: true, salt1: 2}, want: 4},
		"invalid before": {before: walHeader{}, after: walHeader{valid: true, salt1: 3}, want: 0},
		"invalid after":  {before: walHeader{valid: true, salt1: 3}, after: walHeader{}, want: 0},
	} {
		if got := walResetCount(tc.before, tc.after); got != tc.want {
			t.Fatalf("%s: walResetCount() = %d, want %d", name, got, tc.want)
		}
	}
}

func TestWALResetUploadedBytes(t *testing.T) {
	if got := walResetUploadedBytes(1000, 1500, 2200); got != 1200 {
		t.Fatalf("cumulative counter = %d, want 1200", got)
	}
	if got := walResetUploadedBytes(1000, 1500, 300); got != 800 {
		t.Fatalf("restarted counter = %d, want 500 before the stop plus 300 after", got)
	}
}

// fakeWAL stands in for the primary's WAL: bursts add one committed frame per
// row to the current generation, and while Litestream is down every
// autocheckpoint worth of frames restarts the generation at frame 0 with new
// salts while the physical file keeps its size, as SQLite does without a
// journal_size_limit. Litestream's saved position is the generation end at
// the stop, exposed as its newest L0 LTX header.
type fakeWAL struct {
	mu            sync.Mutex
	header        walHeader
	frames        int64
	slots         int64
	down          bool
	resetEvery    int64
	maxResets     uint32
	resets        uint32
	overrun       bool
	shrink        bool
	saved         litestreamSavedPosition
	savedFound    bool
	readErr       error
	downReadErr   error
	downBurstErr  error
	stops, starts int
}

const fakeFrameSize = testWALPageSize + walFrameHeaderSize

func newFakeWAL() *fakeWAL {
	return &fakeWAL{header: walHeader{valid: true, pageSize: testWALPageSize, checkpointSeq: 40, salt1: 1000, salt2: 77}, resetEvery: 1000, maxResets: 1 << 30, savedFound: true}
}

func (w *fakeWAL) generation() (walGeneration, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.readErr != nil {
		return walGeneration{}, w.readErr
	}
	if w.down && w.downReadErr != nil {
		return walGeneration{}, w.downReadErr
	}
	return walGeneration{header: w.header, fileBytes: walHeaderSize + w.slots*fakeFrameSize, frameSize: fakeFrameSize, frameSlots: w.slots, committedFrames: w.frames}, nil
}

func (w *fakeWAL) burst(_ context.Context, rows int) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.down && w.downBurstErr != nil {
		return 0, w.downBurstErr
	}
	w.frames += int64(rows)
	w.slots = max(w.slots, w.frames)
	if w.down && w.frames >= w.resetEvery && w.resets < w.maxResets {
		w.resets++
		w.header.checkpointSeq++
		w.header.salt1++
		w.header.salt2 += 7919
		w.frames = int64(rows)
		if w.overrun {
			w.frames = w.saved.walOffsetEnd/fakeFrameSize + 1
			w.slots = max(w.slots, w.frames)
		}
		if w.shrink {
			w.slots = w.frames
		}
	}
	return rows, nil
}

func (w *fakeWAL) stop(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stops++
	w.down = true
	w.saved = litestreamSavedPosition{walOffsetEnd: walHeaderSize + w.frames*fakeFrameSize, salt1: w.header.salt1, salt2: w.header.salt2, found: true, file: "0000000000000001-0000000000000400.ltx"}
	return nil
}

func (w *fakeWAL) start(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.starts++
	w.down = false
	return nil
}

func (w *fakeWAL) savedPosition() (litestreamSavedPosition, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.savedFound {
		return litestreamSavedPosition{}, nil
	}
	return w.saved, nil
}

type walResetScript struct {
	wal      *fakeWAL
	stopErr  error
	startErr error
	lagging  bool
	phase    string
	validErr error
	l9       []walResetL9
	uploaded []uint64
	gateErr  error
	downtime []bool
	interval time.Duration
}

func newTestWALResetCheck(script *walResetScript) *walResetCheck {
	check := newWALResetCheck(Config{DBPath: "/data/test.db", SnapshotInterval: script.interval})
	check.pause = time.Hour
	check.burstBytes = walHeaderSize + 3*1000*fakeFrameSize
	check.burstBudget = 5 * time.Second
	check.downBudget = 2 * time.Second
	check.headerPoll = 0
	check.stopBudget = time.Second
	check.startBudget = time.Second
	check.catchUpBudget = 300 * time.Millisecond
	check.catchUpPoll = 20 * time.Millisecond
	check.gateBudget = 50 * time.Millisecond
	check.generation = script.wal.generation
	check.savedPosition = script.wal.savedPosition
	check.burst = script.wal.burst
	check.stop = func(ctx context.Context) error {
		if script.stopErr != nil {
			return script.stopErr
		}
		return script.wal.stop(ctx)
	}
	check.start = func(ctx context.Context) error {
		if script.startErr != nil {
			return script.startErr
		}
		return script.wal.start(ctx)
	}
	check.catchUp = func(context.Context) (syncResponse, error) {
		if script.lagging {
			return syncResponse{TXID: 500, ReplicatedTXID: 420}, nil
		}
		return syncResponse{TXID: 500, ReplicatedTXID: 500}, nil
	}
	check.validate = func(_ context.Context, result *VerificationResult) (walResetValidation, error) {
		result.BoundaryTXID = 500
		validation := walResetValidation{phase: script.phase, boundary: 500, steps: []reporting.VerificationStep{{Name: "restore", Status: "ok"}}}
		if script.validErr == nil {
			validation.pinnedPassed, validation.latestPassed, validation.latestTXID = true, true, 500
			return validation, nil
		}
		if strings.HasPrefix(script.phase, "latest_") {
			validation.pinnedPassed = true
		}
		return validation, walResetValidationError(script.phase, script.validErr)
	}
	l9Calls, uploadCalls := 0, 0
	check.l9 = func() walResetL9 {
		if l9Calls >= len(script.l9) {
			return walResetL9{}
		}
		l9Calls++
		return script.l9[l9Calls-1]
	}
	check.uploaded = func() (uint64, bool) {
		if uploadCalls >= len(script.uploaded) {
			return 0, false
		}
		uploadCalls++
		return script.uploaded[uploadCalls-1], true
	}
	check.acquireGate = func(ctx context.Context) (func(), error) {
		if script.gateErr != nil {
			return nil, script.gateErr
		}
		return func() {}, nil
	}
	check.downtime = func(on bool) { script.downtime = append(script.downtime, on) }
	return check
}

func TestWALResetCycleStateMachine(t *testing.T) {
	recent := time.Now().Add(-2 * time.Hour)
	old := time.Now().Add(-49 * time.Hour)
	tests := []struct {
		name          string
		script        func() *walResetScript
		wantStatus    string
		wantSignature string
		wantMessage   string
		wantSummary   []string
		wantSkipped   bool
		wantStarts    int
	}{
		{
			name: "five resets below litestream's saved position then intact pinned and latest restores pass",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), interval: 48 * time.Hour, l9: []walResetL9{{count: 1, newestAt: recent, known: true}, {count: 1, newestAt: recent, known: true}}, uploaded: []uint64{1000, 5000, 9000}}
			},
			wantStatus:  "passed",
			wantSummary: []string{"wal_resets=5", "checkpoints=5", "generations_seen=6", "saved_position_source=ltx:", "l9_before=1 l9_after=1 l9_new=none", "uploaded_bytes=8000", "pinned_restore=passed latest_restore=passed restored_txid=00000000000001f4", "catchup_txid=00000000000001f4"},
			wantStarts:  1,
		},
		{
			name: "a reset-triggered snapshot is recorded and the cycle still counts",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), interval: 48 * time.Hour, l9: []walResetL9{{count: 1, newestAt: recent, known: true}, {count: 2, newestAt: time.Now(), known: true}}}
			},
			wantStatus:  "passed",
			wantSummary: []string{"l9_before=1 l9_after=2 l9_new=reset"},
			wantStarts:  1,
		},
		{
			name: "a scheduled snapshot during the cycle is interference",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), interval: 48 * time.Hour, l9: []walResetL9{{count: 1, newestAt: old, known: true}, {count: 2, newestAt: time.Now(), known: true}}}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetSnapshotInterference,
			wantMessage:   "a new L9 appeared during the cycle (1 -> 2)",
			wantSummary:   []string{"l9_new=scheduled"},
			wantStarts:    1,
		},
		{
			name: "litestream's position falls back to the wal end at exit when no ltx matches",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.savedFound = false
				return &walResetScript{wal: wal}
			},
			wantStatus:  "passed",
			wantSummary: []string{"saved_position_source=wal_end_at_exit"},
			wantStarts:  1,
		},
		{
			name: "fewer than three resets while litestream is down is not the reported shape",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.maxResets = 2
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetShapeNotReached,
			wantMessage:   "2 wal resets while litestream was down, need at least 3",
			wantSummary:   []string{"wal_resets=2"},
			wantStarts:    1,
		},
		{
			name: "a generation that reaches the saved position rewrites the stale frame",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.overrun = true
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetShapeNotReached,
			wantMessage:   "so the frame there was rewritten",
			wantStarts:    1,
		},
		{
			name: "a wal that shrinks below the saved position has lost the stale frame",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.shrink = true
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetShapeNotReached,
			wantMessage:   "so the stale frame is gone",
			wantStarts:    1,
		},
		{
			name: "a stop that does not exit is a litestream restart failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), stopErr: errors.New("litestream did not exit after terminated")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetLitestreamRestartFailed,
			wantMessage:   "stop: litestream did not exit",
			wantStarts:    0,
		},
		{
			name: "a start that fails is a litestream restart failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), startErr: errors.New("timed out waiting for first sync")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetLitestreamRestartFailed,
			wantMessage:   "start: timed out waiting for first sync",
			wantStarts:    0,
		},
		{
			name: "a wal read error after the stop still restarts litestream",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.downReadErr = errors.New("open test.db-wal: permission denied")
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "read wal after stop: open test.db-wal: permission denied",
			wantStarts:    1,
		},
		{
			name: "a burst error while litestream is down still restarts litestream",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.downBurstErr = errors.New("database is locked")
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "burst write while litestream was down: database is locked",
			wantStarts:    1,
		},
		{
			name:          "a replica that never catches up is a catch-up timeout",
			script:        func() *walResetScript { return &walResetScript{wal: newFakeWAL(), lagging: true} },
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetCatchUpTimeout,
			wantMessage:   "replicated txid 00000000000001a4 behind primary txid 00000000000001f4",
			wantStarts:    1,
		},
		{
			name: "a failed pinned restore is a restore failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: errors.New("restore failed: validation failed (exit 1): page 50485: never used")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetRestoreFailed,
			wantMessage:   "wal reset restore failed: restore failed: validation failed (exit 1): page 50485: never used",
			wantSummary:   []string{"pinned_restore=failed latest_restore=failed"},
			wantStarts:    1,
		},
		{
			name: "a failed integrity check is an integrity failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "integrity_check", validErr: errors.New("restored integrity: Page 50485: never used")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetIntegrityFailed,
			wantMessage:   "wal reset integrity failed: restored integrity: Page 50485: never used",
			wantStarts:    1,
		},
		{
			name: "a logical difference is a logical mismatch",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "compare", validErr: errors.New("table wal_reset_burst: 80 rows restored, 201 in source")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetLogicalMismatch,
			wantMessage:   "wal reset logical mismatch: table wal_reset_burst: 80 rows restored, 201 in source",
			wantStarts:    1,
		},
		{
			name: "a latest restore that fails integrity fails the cycle with the latest marker",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "latest_integrity_check", validErr: errors.New("restored integrity: Page 50485: never used")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetIntegrityFailed,
			wantMessage:   "wal reset integrity failed (latest): restored integrity: Page 50485: never used",
			wantSummary:   []string{"pinned_restore=passed latest_restore=failed"},
			wantStarts:    1,
		},
		{
			name: "a latest restore that differs logically is a logical mismatch",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "latest_compare", validErr: errors.New("table wal_reset_burst: 80 rows restored, 201 in source")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetLogicalMismatch,
			wantMessage:   "wal reset logical mismatch (latest): table wal_reset_burst",
			wantStarts:    1,
		},
		{
			name: "a verifier deadline during the restore is inconclusive, not corruption",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "integrity_check", validErr: &verifierDeadlineError{phase: "integrity_check", elapsed: 10 * time.Minute}}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "integrity_check ran out of time, no corruption evidence",
			wantStarts:    1,
		},
		{
			name: "a cancelled compare is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "compare", validErr: fmt.Errorf("compare: %w", context.Canceled)}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "compare ran out of time, no corruption evidence",
			wantStarts:    1,
		},
		{
			name: "a latest restore whose target is not the boundary is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "latest_boundary", validErr: errors.New("latest restore behind boundary: restored 00000000000001f3 boundary 00000000000001f4")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "latest_boundary: latest restore behind boundary",
			wantStarts:    1,
		},
		{
			name: "a harness-side validation problem is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "source_snapshot", validErr: errors.New("database is locked")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "source_snapshot: database is locked",
			wantStarts:    1,
		},
		{
			name: "an unreadable wal before the stop is inconclusive without a restart",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.readErr = errors.New("open test.db-wal: permission denied")
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "read wal: open test.db-wal: permission denied",
			wantStarts:    0,
		},
		{
			name:        "a verification holding the check gate skips the cycle",
			script:      func() *walResetScript { return &walResetScript{wal: newFakeWAL(), gateErr: errWALResetGateBusy} },
			wantSkipped: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := tt.script()
			check := newTestWALResetCheck(script)
			result, skipped := check.cycle(context.Background())
			if skipped != tt.wantSkipped {
				t.Fatalf("skipped = %v, want %v (result %+v)", skipped, tt.wantSkipped, result)
			}
			if skipped {
				return
			}
			if result.Status != tt.wantStatus || result.CheckType != walResetCheckType || result.RestoreMode != walResetRestoreMode {
				t.Fatalf("result = %s %s %s, want %s\n%s", result.Status, result.CheckType, result.RestoreMode, tt.wantStatus, result.ErrorMessage)
			}
			if tt.wantMessage != "" && !strings.Contains(result.ErrorMessage, tt.wantMessage) {
				t.Fatalf("error = %q, want %q", result.ErrorMessage, tt.wantMessage)
			}
			switch result.Status {
			case "aborted":
				if result.InconclusiveClassification == nil || result.InconclusiveClassification.Signature != tt.wantSignature {
					t.Fatalf("inconclusive classification = %+v, want %s", result.InconclusiveClassification, tt.wantSignature)
				}
			case "failed":
				classification := reporting.ClassifyVerificationFailure(result.CheckType, result.ErrorMessage)
				if classification.Signature != tt.wantSignature || classification.Stage != walResetCheckType {
					t.Fatalf("classification = %+v, want %s in stage %s", classification, tt.wantSignature, walResetCheckType)
				}
			case "passed":
				if !result.Passed || result.BoundaryTXID != 500 {
					t.Fatalf("passed result = %+v", result)
				}
			}
			for _, want := range tt.wantSummary {
				if !strings.Contains(result.Steps[0].OutputTail, want) {
					t.Fatalf("step output %q lacks %q", result.Steps[0].OutputTail, want)
				}
			}
			if len(script.downtime) > 0 && script.downtime[len(script.downtime)-1] {
				t.Fatalf("planned downtime left on: %v", script.downtime)
			}
			if script.wal.starts != tt.wantStarts {
				t.Fatalf("litestream starts = %d, want %d", script.wal.starts, tt.wantStarts)
			}
			if script.wal.stops > 0 && script.wal.starts == 0 && tt.wantStarts > 0 {
				t.Fatal("litestream left stopped")
			}
		})
	}
}

func TestWALResetCycleStopsLitestreamOnlyAfterTheBurstIsDeepEnough(t *testing.T) {
	wal := newFakeWAL()
	script := &walResetScript{wal: wal}
	check := newTestWALResetCheck(script)
	observation, _, err := check.run(context.Background())
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if observation.savedAtSignal < check.burstBytes || observation.savedPosition < observation.savedAtSignal || observation.savedPosition > observation.savedAtExit {
		t.Fatalf("saved position %d not bracketed by signal %d and exit %d, burst floor %d", observation.savedPosition, observation.savedAtSignal, observation.savedAtExit, check.burstBytes)
	}
	if observation.maxGenerationEnd >= observation.savedPosition || observation.finalWAL.fileBytes < observation.savedPosition {
		t.Fatalf("generation end %d against saved position %d in a %d-byte file", observation.maxGenerationEnd, observation.savedPosition, observation.finalWAL.fileBytes)
	}
	if observation.resets != 5 || observation.savedHeader.salt1 != 1000 || observation.finalWAL.header.salt1 != 1005 || observation.savedPositionSalt1 != 1000 {
		t.Fatalf("resets = %d salt1 %d -> %d (saved from %d), want 5 from 1000 to 1005", observation.resets, observation.savedHeader.salt1, observation.finalWAL.header.salt1, observation.savedPositionSalt1)
	}
	if !strings.Contains(observation.summary(), "salt1_before=1000 salt1_after=1005 ckpt_seq_before=40 ckpt_seq_after=45") {
		t.Fatalf("summary = %s", observation.summary())
	}
	if wal.stops != 1 || wal.starts != 1 {
		t.Fatalf("stops %d starts %d, want one each", wal.stops, wal.starts)
	}
}

func TestWALResetCheckConfig(t *testing.T) {
	env := map[string]string{
		"WAL_RESET_CHECK": "true",
		"DATA_DIR":        "/data",
		"REPLICA_TYPE":    "s3",
		"S3_BUCKET":       "soak",
		"S3_PATH":         "rc-aaaaaaaa/worker-gate-walreset",
	}
	cfg, err := configFromLookup(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.WALResetCheck || !cfg.WorkloadConfig().WALResetCheck {
		t.Fatalf("config = wal_reset_check %v workload %v", cfg.WALResetCheck, cfg.WorkloadConfig().WALResetCheck)
	}
	parsed, err := WorkloadFromEnvironment(map[string]string{"WAL_RESET_CHECK": "true"})
	if err != nil || !parsed.WALResetCheck {
		t.Fatalf("WorkloadFromEnvironment() = %+v, %v", parsed, err)
	}
	runner := NewRunner(cfg)
	if !runner.walResetCheckEnabled() {
		t.Fatal("wal reset check disabled on a single-database worker")
	}
	cfg.NumDatabases = 10
	if NewRunner(cfg).walResetCheckEnabled() {
		t.Fatal("wal reset check enabled on a many-database worker")
	}
}

func TestCheckGateSerializesHolders(t *testing.T) {
	gate := newCheckGate()
	release, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := gate.acquire(ctx); !errors.Is(err, errWALResetGateBusy) {
		t.Fatalf("second acquire error = %v, want %v", err, errWALResetGateBusy)
	}
	release()
	release2, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after release error = %v", err)
	}
	release2()
}

func TestMonitorProcessIgnoresAnExpectedExitEvenAfterARestart(t *testing.T) {
	manager := newLitestreamManager(&Config{})
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	first := &litestreamProcess{done: make(chan struct{})}
	manager.litestreamProc = first
	manager.monitorProcess(ctx, cancel, first)
	first.markExpectedExit()

	second := &litestreamProcess{done: make(chan struct{})}
	manager.litestreamProc = second
	manager.monitorProcess(ctx, cancel, second)
	first.finish(errors.New("signal: terminated"), nil)

	select {
	case <-ctx.Done():
		t.Fatalf("expected exit of the stopped process cancelled the run: %v", context.Cause(ctx))
	case <-time.After(50 * time.Millisecond):
	}
	if manager.litestreamExitError() != nil {
		t.Fatalf("exit error of the new process = %v, want none", manager.litestreamExitError())
	}

	second.finish(errors.New("exit status 2"), nil)
	select {
	case <-ctx.Done():
		if cause := context.Cause(ctx); cause == nil || !strings.Contains(cause.Error(), "exit status 2") {
			t.Fatalf("cause = %v", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected exit of the running process did not cancel the run")
	}
}
