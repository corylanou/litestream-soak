package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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

func TestWalkWALGenerationCountsOnlyTheCurrentGenerationsFrames(t *testing.T) {
	for _, bigEndian := range []bool{false, true} {
		wal := newWALBuilder(bigEndian, 1000, 77)
		for page := uint32(1); page <= 2000; page++ {
			wal.appendFrame(page, page%10 == 0, false)
		}
		before, err := walkWALGeneration(bytes.NewReader(wal.buf), int64(len(wal.buf)))
		if err != nil {
			t.Fatal(err)
		}
		if !before.header.valid || before.committedFrames != 2000 || before.writtenFrames != 2000 || before.frameSlots != 2000 || before.frameSize != testWALPageSize+walFrameHeaderSize {
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
		if after.committedFrames != 300 || after.writtenFrames != 301 || after.frameSlots != 2000 || after.fileBytes != int64(len(wal.buf)) || after.header.salt1 != 1001 {
			t.Fatalf("big-endian %v after reset = %+v, want 300 committed and 301 written frames in a 2000-slot file", bigEndian, after)
		}
		if walResetCount(before.header, after.header) != 1 || after.generationEnd() != walHeaderSize+300*after.frameSize || after.writtenEnd() != walHeaderSize+301*after.frameSize {
			t.Fatalf("reset count %d generation end %d written end %d", walResetCount(before.header, after.header), after.generationEnd(), after.writtenEnd())
		}

		wal.appendFrame(302, true, true)
		wal.appendFrame(303, true, false)
		corrupt, err := walkWALGeneration(bytes.NewReader(wal.buf), int64(len(wal.buf)))
		if err != nil {
			t.Fatal(err)
		}
		if corrupt.committedFrames != 300 || corrupt.writtenFrames != 301 {
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

func TestCheckWALResetLatestTXIDRequiresTheBoundary(t *testing.T) {
	if err := checkWALResetLatestTXID(500, 500); err != nil {
		t.Fatalf("exact boundary rejected: %v", err)
	}
	if err := checkWALResetLatestTXID(499, 500); err == nil || !strings.Contains(err.Error(), "landed on txid 00000000000001f3, behind boundary 00000000000001f4") || errors.Is(err, errWALResetMissingPlan) || errors.Is(err, errWALResetLatestAdvanced) {
		t.Fatalf("behind the boundary = %v, want a restore failure", err)
	}
	if err := checkWALResetLatestTXID(501, 500); !errors.Is(err, errWALResetLatestAdvanced) {
		t.Fatalf("ahead of the boundary = %v, want %v", err, errWALResetLatestAdvanced)
	}
	if err := checkWALResetLatestTXID(0, 500); !errors.Is(err, errWALResetMissingPlan) {
		t.Fatalf("missing plan = %v, want %v", err, errWALResetMissingPlan)
	}
}

func TestL9IdentitiesSeeReplacedSnapshots(t *testing.T) {
	before := l9Identities([]restorePlanCandidate{{level: 9, minTXID: 1, maxTXID: 0x400, createdAt: "2026-11-03T09:00:00Z"}, {level: 0, minTXID: 0x401, maxTXID: 0x401}})
	same := l9Identities([]restorePlanCandidate{{level: 9, minTXID: 1, maxTXID: 0x400, createdAt: "2026-11-03T09:00:00Z"}, {level: 1, minTXID: 1, maxTXID: 0x420}})
	replaced := l9Identities([]restorePlanCandidate{{level: 9, minTXID: 1, maxTXID: 0x7ff, createdAt: "2026-11-03T09:40:00Z"}})
	added := l9Identities([]restorePlanCandidate{{level: 9, minTXID: 1, maxTXID: 0x400, createdAt: "2026-11-03T09:00:00Z"}, {level: 9, minTXID: 1, maxTXID: 0x7ff, createdAt: "2026-11-03T09:40:00Z"}})
	if !before.known || before.count() != 1 || before.changed(same) {
		t.Fatalf("identical listings differ: %+v vs %+v", before, same)
	}
	if !before.changed(replaced) || !before.changed(added) || replaced.count() != 1 || added.count() != 2 {
		t.Fatalf("replaced %+v or added %+v snapshot not seen against %+v", replaced, added, before)
	}
	if none := l9Identities(nil); !none.known || none.count() != 0 || none.changed(l9Identities([]restorePlanCandidate{})) {
		t.Fatalf("empty listing = %+v", none)
	}
}

// fakeWAL stands in for the primary's WAL and the Litestream process. Bursts
// add one committed frame per row to the current generation, and while
// Litestream is down every autocheckpoint worth of frames restarts the
// generation at frame 0 with new salts while the physical file keeps its
// size, as SQLite does without a journal_size_limit. Litestream's saved
// position is the generation end at the stop, exposed as its newest L0 LTX
// header. The process model tracks live processes so a recovery that starts
// a second one is visible.
type fakeWAL struct {
	mu           sync.Mutex
	header       walHeader
	frames       int64
	slots        int64
	down         bool
	resetEvery   int64
	maxResets    uint32
	resets       uint32
	overrun      bool
	shrink       bool
	saved        litestreamSavedPosition
	savedFound   bool
	savedStale   bool
	readErr      error
	downReadErr  error
	downBurstErr error

	live              int
	stopPending       bool
	startFailOnce     bool
	shutdownReset     bool
	exitHeaderInvalid bool
	stops, starts     int
}

const fakeFrameSize = testWALPageSize + walFrameHeaderSize

func newFakeWAL() *fakeWAL {
	return &fakeWAL{header: walHeader{valid: true, pageSize: testWALPageSize, checkpointSeq: 40, salt1: 1000, salt2: 77}, resetEvery: 1000, maxResets: 1 << 30, savedFound: true, live: 1}
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
	header := w.header
	if w.down && w.exitHeaderInvalid {
		header = walHeader{}
	}
	return walGeneration{header: header, fileBytes: walHeaderSize + w.slots*fakeFrameSize, frameSize: fakeFrameSize, frameSlots: w.slots, committedFrames: w.frames, writtenFrames: w.frames}, nil
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

// stop is the SIGTERM: the process exits and the WAL is down. With
// stopPending the signal is delivered but the exit comes late, so the call
// times out while the process is still alive.
func (w *fakeWAL) stop(context.Context) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stops++
	w.down = true
	w.saved = litestreamSavedPosition{walOffsetEnd: walHeaderSize + w.frames*fakeFrameSize, salt1: w.header.salt1, salt2: w.header.salt2, found: true, file: "0000000000000001-0000000000000400.ltx"}
	if w.savedStale {
		w.saved.salt1--
	}
	if w.stopPending {
		return "", errors.New("litestream did not exit after terminated: context deadline exceeded")
	}
	if w.shutdownReset {
		w.header.checkpointSeq++
		w.header.salt1++
		w.header.salt2 += 7919
		w.frames = 8
	}
	w.live--
	return "exit:0", nil
}

// start is the manager's ensure step: a stopping process is replaced, a live
// one is kept, and a fresh one is started only when none is alive. With
// startFailOnce the first start brings a process up that never becomes ready.
func (w *fakeWAL) start(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopPending {
		w.live--
		w.stopPending = false
	}
	if w.live == 0 {
		w.live++
		w.starts++
		if w.startFailOnce {
			w.startFailOnce = false
			return errors.New("timed out waiting for first sync")
		}
	}
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
}

func l9Of(objects ...string) walResetL9 {
	return walResetL9{objects: objects, known: true}
}

func newTestWALResetCheck(script *walResetScript) *walResetCheck {
	check := newWALResetCheck(Config{DBPath: "/data/test.db"})
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
	check.stop = func(ctx context.Context) (string, error) {
		if script.stopErr != nil {
			return "", script.stopErr
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
		signature, err := walResetValidationError(script.phase, script.validErr)
		validation.restoreSignature = signature
		return validation, err
	}
	if script.l9 == nil {
		script.l9 = []walResetL9{l9Of("0000000000000001-0000000000000400@2026-11-03T09:00:00Z"), l9Of("0000000000000001-0000000000000400@2026-11-03T09:00:00Z")}
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
			name: "five resets below litestream's saved position then intact pinned and latest restores pass with the shape confirmed",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), uploaded: []uint64{1000, 5000, 9000}}
			},
			wantStatus:  "passed",
			wantSummary: []string{"wal_resets=5", "checkpoints=5", "generations_seen=6", "shape_evidence=confirmed", "saved_position_ltx=0000000000000001-0000000000000400.ltx", "litestream_exit=exit:0", "l9_before=1 l9_after=1 l9_changed=false", "uploaded_bytes=8000", "pinned_restore=passed latest_restore=passed restored_txid=00000000000001f4", "catchup_txid=00000000000001f4"},
			wantStarts:  1,
		},
		{
			name: "no readable ltx position leaves the shape unverified but the cycle still counts",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.savedFound = false
				return &walResetScript{wal: wal}
			},
			wantStatus:  "passed",
			wantSummary: []string{"shape_evidence=unverified", "saved_position=0 saved_position_ltx=none"},
			wantStarts:  1,
		},
		{
			name: "an ltx file from another generation is not used as the saved position",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.savedStale = true
				return &walResetScript{wal: wal}
			},
			wantStatus:  "passed",
			wantSummary: []string{"shape_evidence=unverified", "saved_position=0"},
			wantStarts:  1,
		},
		{
			name: "a generation that reaches the saved position contradicts the shape but the cycle still counts",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.overrun = true
				return &walResetScript{wal: wal}
			},
			wantStatus:  "passed",
			wantSummary: []string{"shape_evidence=contradicted", "at_or_past_litestream's_saved_position"},
			wantStarts:  1,
		},
		{
			name: "a wal that shrinks below the saved position contradicts the shape",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.shrink = true
				return &walResetScript{wal: wal}
			},
			wantStatus:  "passed",
			wantSummary: []string{"shape_evidence=contradicted", "the_wal_shrank"},
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
			name: "a new L9 inside the cycle is snapshot interference even when the restores pass",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), l9: []walResetL9{l9Of("0000000000000001-0000000000000400@2026-11-03T09:00:00Z"), l9Of("0000000000000001-0000000000000400@2026-11-03T09:00:00Z", "0000000000000001-00000000000007ff@2026-11-03T09:40:00Z")}}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetSnapshotInterference,
			wantMessage:   "L9 objects changed during the cycle (1 -> 2",
			wantSummary:   []string{"l9_before=1 l9_after=2 l9_changed=true"},
			wantStarts:    1,
		},
		{
			name: "a replaced L9 with the same count is snapshot interference",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), l9: []walResetL9{l9Of("0000000000000001-0000000000000400@2026-11-03T09:00:00Z"), l9Of("0000000000000001-00000000000007ff@2026-11-03T09:40:00Z")}}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetSnapshotInterference,
			wantMessage:   "L9 objects changed during the cycle (1 -> 1",
			wantStarts:    1,
		},
		{
			name: "snapshot interference outranks a corrupt restore",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "integrity_check", validErr: errors.New("restored integrity: Page 50485: never used"), l9: []walResetL9{l9Of("a@1"), l9Of("b@2")}}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetSnapshotInterference,
			wantStarts:    1,
		},
		{
			name:          "an unreadable L9 listing before the stop is snapshot interference",
			script:        func() *walResetScript { return &walResetScript{wal: newFakeWAL(), l9: []walResetL9{{}, l9Of("a@1")}} },
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetSnapshotInterference,
			wantMessage:   "could not be listed before the stop",
			wantStarts:    1,
		},
		{
			name:          "an unreadable L9 listing after the restores is snapshot interference",
			script:        func() *walResetScript { return &walResetScript{wal: newFakeWAL(), l9: []walResetL9{l9Of("a@1"), {}}} },
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetSnapshotInterference,
			wantMessage:   "could not be listed after the restores",
			wantSummary:   []string{"l9_before=1 l9_after=unknown"},
			wantStarts:    1,
		},
		{
			name: "a stop that does not exit is a litestream restart failure and recovery restarts once",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), stopErr: errors.New("litestream did not exit after terminated")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetLitestreamRestartFailed,
			wantMessage:   "stop: litestream did not exit",
			wantStarts:    0,
		},
		{
			name: "a stop that completes late is replaced by exactly one running litestream",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.stopPending = true
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetLitestreamRestartFailed,
			wantMessage:   "stop: litestream did not exit after terminated",
			wantStarts:    1,
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
			name: "a start whose socket wait times out is recovered without a second process",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.startFailOnce = true
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetLitestreamRestartFailed,
			wantMessage:   "start: timed out waiting for first sync",
			wantStarts:    1,
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
			name: "a pinned restore that cannot decode the replica is a restore failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: errors.New("restore failed: validation failed (exit 1): apply ltx file 0000000000000001-0000000000000400.ltx: decode ltx header: unexpected EOF")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetRestoreFailed,
			wantMessage:   "wal reset restore failed: restore_decode_error: restore failed: validation failed (exit 1): apply ltx file 0000000000000001-0000000000000400.ltx: decode ltx header: unexpected EOF",
			wantSummary:   []string{"pinned_restore=failed latest_restore=failed", "restore_signature=restore_decode_error"},
			wantStarts:    1,
		},
		{
			name: "a checksum mismatch in the replica is a restore failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: errors.New("restore failed: validation failed (exit 1): apply ltx file: ltx: checksum mismatch")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetRestoreFailed,
			wantMessage:   "wal reset restore failed: restore_decode_error: restore failed: validation failed (exit 1): apply ltx file: ltx: checksum mismatch",
			wantStarts:    1,
		},
		{
			name: "a synced boundary the replica cannot serve is a restore failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: &restoreEvidenceInconclusiveError{err: errors.New("restore failed: validation failed (exit 1): restore target txid 00000000000001f4 is no longer available: no matching backup files available")}}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetRestoreFailed,
			wantMessage:   "wal reset restore failed: restore_target_unavailable: restore failed: validation failed (exit 1): restore target txid 00000000000001f4 is no longer available: no matching backup files available",
			wantSummary:   []string{"restore_signature=restore_target_unavailable"},
			wantStarts:    1,
		},
		{
			name: "an object store 403 during the restore is inconclusive, not a reproduction",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: errors.New("restore failed: validation failed (exit 1): operation error S3: GetObject, https response error StatusCode: 403, RequestID: 7F3A9C, HostID: abc=, api error AccessDenied: Access Denied")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "wal reset check unavailable: restore: restore_s3_access_denied: restore failed: validation failed (exit 1): operation error S3: GetObject",
			wantSummary:   []string{"pinned_restore=failed latest_restore=failed", "restore_signature=restore_s3_access_denied"},
			wantStarts:    1,
		},
		{
			name: "an object store 500 during the restore is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: errors.New("restore failed: validation failed (exit 1): operation error S3: GetObject, https response error StatusCode: 500, RequestID: 7F3A9C, HostID: abc=, api error InternalError: We encountered an internal error. Please try again.")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "wal reset check unavailable: restore: restore_s3_get_failed: restore failed",
			wantSummary:   []string{"restore_signature=restore_s3_get_failed"},
			wantStarts:    1,
		},
		{
			name: "a dns failure during the restore is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: errors.New(`restore failed: validation failed (exit 1): operation error S3: ListObjectsV2, exceeded maximum number of attempts, 3, https response error StatusCode: 0, RequestID: , HostID: , request send failed, Get "https://soak.s3.us-east-1.amazonaws.com/?list-type=2": dial tcp: lookup soak.s3.us-east-1.amazonaws.com: no such host`)}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "wal reset check unavailable: restore: restore_s3_list_failed: restore failed",
			wantSummary:   []string{"restore_signature=restore_s3_list_failed"},
			wantStarts:    1,
		},
		{
			name: "a cancelled object store request during the latest restore is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "latest_restore", validErr: &restoreEvidenceInconclusiveError{err: errors.New("restore failed: validation failed (exit 1): operation error S3: GetObject, https response error StatusCode: 408, RequestID: 7F3A9C, HostID: abc=, api error RequestCanceled: request canceled")}}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "wal reset check unavailable: latest_restore: restore_s3_get_request_canceled: restore failed",
			wantSummary:   []string{"pinned_restore=passed latest_restore=failed", "restore_signature=restore_s3_get_request_canceled"},
			wantStarts:    1,
		},
		{
			name: "a restore failure the classifier cannot name is inconclusive and keeps its signature",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: errors.New("restore failed: validation failed (exit 1): page 50485: never used")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "wal reset check unavailable: restore: validation_failed: restore failed: validation failed (exit 1): page 50485: never used",
			wantSummary:   []string{"restore_signature=validation_failed"},
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
			name: "a latest restore behind the synced boundary is a restore failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "latest_boundary", validErr: checkWALResetLatestTXID(499, 500)}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetRestoreFailed,
			wantMessage:   "wal reset restore failed (latest): latest restore landed on txid 00000000000001f3, behind boundary 00000000000001f4",
			wantStarts:    1,
		},
		{
			name: "a latest restore ahead of the synced boundary is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "latest_boundary", validErr: checkWALResetLatestTXID(501, 500)}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "latest_boundary: latest advanced past the synced boundary: latest restore landed on txid 00000000000001f5, boundary 00000000000001f4",
			wantStarts:    1,
		},
		{
			name: "litestream's own shutdown reset is not counted and the baseline is the header after exit",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.shutdownReset = true
				return &walResetScript{wal: wal}
			},
			wantStatus:  "passed",
			wantSummary: []string{"wal_resets=5", "salt1_before=1001 salt1_after=1006", "salt1_at_signal=1000"},
			wantStarts:  1,
		},
		{
			name: "an unreadable wal header after exit is inconclusive and restarts litestream",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.exitHeaderInvalid = true
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "wal header unreadable after litestream exited",
			wantStarts:    1,
		},
		{
			name: "a latest restore without a plan txid is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "latest_boundary", validErr: checkWALResetLatestTXID(0, 500)}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "latest_boundary: latest restore reported no plan txid",
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
			if script.wal.live != 1 && (script.stopErr == nil && script.startErr == nil) {
				t.Fatalf("live litestream processes = %d, want exactly one", script.wal.live)
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
	if shape, _ := observation.shapeEvidence(); shape != walResetShapeConfirmed || observation.maxGenerationWritten >= observation.savedPosition || observation.finalWAL.fileBytes < observation.savedPosition {
		t.Fatalf("shape %s: written end %d against saved position %d in a %d-byte file", shape, observation.maxGenerationWritten, observation.savedPosition, observation.finalWAL.fileBytes)
	}
	if observation.resets != 5 || observation.savedHeader.salt1 != 1000 || observation.finalWAL.header.salt1 != 1005 || observation.savedPositionSalt1 != 1000 {
		t.Fatalf("resets = %d salt1 %d -> %d (saved from %d), want 5 from 1000 to 1005", observation.resets, observation.savedHeader.salt1, observation.finalWAL.header.salt1, observation.savedPositionSalt1)
	}
	if !strings.Contains(observation.summary(), "salt1_before=1000 salt1_after=1005 ckpt_seq_before=40 ckpt_seq_after=45") {
		t.Fatalf("summary = %s", observation.summary())
	}
	if wal.stops != 1 || wal.starts != 1 || wal.live != 1 {
		t.Fatalf("stops %d starts %d live %d, want one each", wal.stops, wal.starts, wal.live)
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
	if !manager.litestreamRunning() || manager.litestreamExitExpected() {
		t.Fatalf("running=%v expected=%v before the stop", manager.litestreamRunning(), manager.litestreamExitExpected())
	}
	first.markExpectedExit()
	if !manager.litestreamRunning() || !manager.litestreamExitExpected() {
		t.Fatalf("running=%v expected=%v after the stop was requested", manager.litestreamRunning(), manager.litestreamExitExpected())
	}

	second := &litestreamProcess{done: make(chan struct{})}
	manager.litestreamProc = second
	manager.monitorProcess(ctx, cancel, second)
	first.finish(errors.New("signal: terminated"), nil)

	select {
	case <-ctx.Done():
		t.Fatalf("expected exit of the stopped process cancelled the run: %v", context.Cause(ctx))
	case <-time.After(50 * time.Millisecond):
	}
	if manager.litestreamExitError() != nil || !manager.litestreamRunning() || manager.litestreamExitExpected() {
		t.Fatalf("new process: err=%v running=%v expected=%v", manager.litestreamExitError(), manager.litestreamRunning(), manager.litestreamExitExpected())
	}

	second.finish(errors.New("exit status 2"), nil)
	if manager.litestreamRunning() {
		t.Fatal("exited process still reported running")
	}
	select {
	case <-ctx.Done():
		if cause := context.Cause(ctx); cause == nil || !strings.Contains(cause.Error(), "exit status 2") {
			t.Fatalf("cause = %v", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected exit of the running process did not cancel the run")
	}
}

func startSleepProcess(t *testing.T) *litestreamProcess {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Skipf("sleep unavailable: %v", err)
	}
	proc := &litestreamProcess{done: make(chan struct{}), cmd: cmd}
	go func() {
		err := cmd.Wait()
		proc.finish(err, processExitSnapshot("litestream", time.Now().UTC(), err))
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-proc.done
	})
	return proc
}

func processAlive(cmd *exec.Cmd) bool {
	return cmd.Process.Signal(syscall.Signal(0)) == nil
}

func TestStopLitestreamProcessSignalsOnlyTheCapturedProcess(t *testing.T) {
	manager := newLitestreamManager(&Config{})
	first := startSleepProcess(t)
	manager.litestreamCmd, manager.litestreamProc = first.cmd, first

	captured := manager.currentProcess()
	second := startSleepProcess(t)
	manager.litestreamCmd, manager.litestreamProc = second.cmd, second

	if err := stopLitestreamProcess(captured, syscall.SIGTERM); err != nil {
		t.Fatalf("delayed stop of the captured process = %v", err)
	}
	if exit := captured.exitSnapshot(); exit == nil || exit.Signal != syscall.SIGTERM.String() {
		t.Fatalf("captured process exit = %+v, want terminated", exit)
	}
	if !processAlive(second.cmd) || !manager.litestreamRunning() || manager.litestreamExitExpected() {
		t.Fatalf("replacement touched by the delayed stop: alive=%v running=%v expected=%v", processAlive(second.cmd), manager.litestreamRunning(), manager.litestreamExitExpected())
	}

	if err := stopLitestreamProcess(captured, syscall.SIGTERM); !errors.Is(err, errLitestreamExitedBeforeStop) {
		t.Fatalf("stop of an exited process = %v, want %v", err, errLitestreamExitedBeforeStop)
	}
	if err := stopLitestreamProcess(nil, syscall.SIGTERM); !errors.Is(err, errLitestreamNotRunning) {
		t.Fatalf("stop without a process = %v, want %v", err, errLitestreamNotRunning)
	}

	if err := manager.stopLitestreamWithSignal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill of the replacement = %v", err)
	}
	if processAlive(second.cmd) || manager.litestreamRunning() {
		t.Fatal("replacement still alive after the kill")
	}
}

func TestStopLitestreamProcessRejectsAnExitFromAnotherSignal(t *testing.T) {
	proc := startSleepProcess(t)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = proc.cmd.Process.Kill()
	}()
	err := stopLitestreamProcess(proc, syscall.SIGSTOP)
	if !errors.Is(err, errLitestreamExitNotAttributed) || !strings.Contains(err.Error(), "exited on killed after") {
		t.Fatalf("stop racing a kill = %v, want %v", err, errLitestreamExitNotAttributed)
	}
	if !signalMatches("terminated", syscall.SIGTERM) || signalMatches("killed", syscall.SIGTERM) {
		t.Fatal("signalMatches does not compare signal names")
	}
}

func TestBeginStopMarksTheProcessBeforeAnySignalAndALaterStopSupersedesIt(t *testing.T) {
	manager := newLitestreamManager(&Config{})
	proc := startSleepProcess(t)
	manager.litestreamCmd, manager.litestreamProc = proc.cmd, proc

	term, err := proc.beginStop(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("beginStop = %v", err)
	}
	if !manager.litestreamRunning() || !manager.litestreamExitExpected() || !processAlive(proc.cmd) {
		t.Fatalf("after the request: running=%v expected=%v alive=%v, want a live process already marked as stopping", manager.litestreamRunning(), manager.litestreamExitExpected(), processAlive(proc.cmd))
	}

	kill, err := proc.beginStop(syscall.SIGKILL)
	if err != nil {
		t.Fatalf("superseding beginStop = %v", err)
	}
	if err := proc.finishStop(term); !errors.Is(err, errLitestreamStopSuperseded) {
		t.Fatalf("superseded stop = %v, want %v", err, errLitestreamStopSuperseded)
	}
	if !processAlive(proc.cmd) {
		t.Fatal("superseded stop signalled the process")
	}
	if err := proc.finishStop(kill); err != nil {
		t.Fatalf("superseding stop = %v", err)
	}
	if exit := proc.exitSnapshot(); exit == nil || exit.Signal != syscall.SIGKILL.String() {
		t.Fatalf("exit = %+v, want killed", exit)
	}
	if err := proc.finishStop(term); !errors.Is(err, errLitestreamStopSuperseded) {
		t.Fatalf("superseded stop after the exit = %v, want %v", err, errLitestreamStopSuperseded)
	}
}

func TestStopLitestreamForCheckTimeoutLeavesAStoppingProcessForRecoveryToReplace(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "litestream.sock")
	if err := os.WriteFile(socket, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r := &Runner{cfg: Config{SocketPath: socket}}
	r.litestreamManager = newLitestreamManager(&r.cfg)
	first := startSleepProcess(t)
	r.litestreamCmd, r.litestreamProc = first.cmd, first

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	exit, err := r.stopLitestreamForCheck(expired, syscall.SIGSTOP)
	if err == nil || !errors.Is(err, context.Canceled) || exit != "" {
		t.Fatalf("stop with an expired deadline = %q, %v", exit, err)
	}
	if !r.litestreamRunning() || !r.litestreamExitExpected() {
		t.Fatalf("running=%v expected=%v, want the captured process marked as stopping before the deadline", r.litestreamRunning(), r.litestreamExitExpected())
	}

	var second *litestreamProcess
	starts := 0
	ctx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelWait()
	if err := r.ensureLitestreamForCheck(ctx, func() error {
		starts++
		second = startSleepProcess(t)
		r.litestreamCmd, r.litestreamProc = second.cmd, second
		return nil
	}); err != nil {
		t.Fatalf("ensure after a timed-out stop = %v", err)
	}
	if starts != 1 || second == nil || !processAlive(second.cmd) || !r.litestreamRunning() || r.litestreamExitExpected() {
		t.Fatalf("starts=%d running=%v expected=%v, want exactly one fresh process", starts, r.litestreamRunning(), r.litestreamExitExpected())
	}
	select {
	case <-first.done:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not replace the stopping process")
	}
	if exit := first.exitSnapshot(); exit == nil || exit.Signal != syscall.SIGKILL.String() {
		t.Fatalf("captured process exit = %+v, want killed by the recovery", exit)
	}
	if !processAlive(second.cmd) {
		t.Fatal("the pending stop reached the replacement")
	}
}

func TestDescribeProcessExit(t *testing.T) {
	code := 0
	if got := describeProcessExit(&reporting.ProcessExitSnapshot{ExitCode: &code}); got != "exit:0" {
		t.Fatalf("clean exit = %q", got)
	}
	if got := describeProcessExit(&reporting.ProcessExitSnapshot{Signal: "terminated", Error: "signal: terminated"}); got != "signal:terminated" {
		t.Fatalf("signalled exit = %q", got)
	}
	if got := describeProcessExit(nil); got != "unknown" {
		t.Fatalf("missing snapshot = %q", got)
	}
}
