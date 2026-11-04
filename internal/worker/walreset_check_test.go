package worker

import (
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

func walHeaderBytes(magic, checkpointSeq, salt1, salt2 uint32) []byte {
	raw := make([]byte, 32)
	binary.BigEndian.PutUint32(raw[0:4], magic)
	binary.BigEndian.PutUint32(raw[4:8], 3007000)
	binary.BigEndian.PutUint32(raw[8:12], 4096)
	binary.BigEndian.PutUint32(raw[12:16], checkpointSeq)
	binary.BigEndian.PutUint32(raw[16:20], salt1)
	binary.BigEndian.PutUint32(raw[20:24], salt2)
	return raw
}

func TestParseWALHeader(t *testing.T) {
	header := parseWALHeader(walHeaderBytes(0x377f0683, 7, 0xfffffffe, 0xdeadbeef))
	if !header.valid || header.pageSize != 4096 || header.checkpointSeq != 7 || header.salt1 != 0xfffffffe || header.salt2 != 0xdeadbeef {
		t.Fatalf("header = %+v", header)
	}
	if little := parseWALHeader(walHeaderBytes(0x377f0682, 1, 2, 3)); !little.valid {
		t.Fatalf("little-endian magic rejected: %+v", little)
	}
	for name, raw := range map[string][]byte{
		"empty":         nil,
		"short":         walHeaderBytes(0x377f0683, 1, 2, 3)[:31],
		"wrong magic":   walHeaderBytes(0x12345678, 1, 2, 3),
		"wrong version": append(walHeaderBytes(0x377f0683, 1, 2, 3)[:4], append([]byte{0, 0, 0, 1}, walHeaderBytes(0x377f0683, 1, 2, 3)[8:]...)...),
	} {
		if parseWALHeader(raw).valid {
			t.Fatalf("%s header parsed as valid", name)
		}
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

// fakeWAL stands in for the primary's WAL file: bursts grow it by one page
// per row, and while Litestream is down every autocheckpoint worth of pages
// restarts it, incrementing salt-1 and re-randomizing salt-2, the way SQLite
// does once no reader holds the WAL.
type fakeWAL struct {
	mu           sync.Mutex
	header       walHeader
	size         int64
	sinceReset   int64
	down         bool
	resetEvery   int64
	maxResets    uint32
	resets       uint32
	overrunSaved bool
	saved        int64
	headerErr    error
}

func newFakeWAL() *fakeWAL {
	return &fakeWAL{header: walHeader{valid: true, checkpointSeq: 40, salt1: 1000, salt2: 77}, resetEvery: 1000 * 4096, maxResets: 1 << 30}
}

func (w *fakeWAL) read() (walHeader, int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.headerErr != nil {
		return walHeader{}, 0, w.headerErr
	}
	return w.header, w.size, nil
}

func (w *fakeWAL) burst(_ context.Context, rows int) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	grow := int64(rows) * 4096
	w.size += grow
	w.sinceReset += grow
	if w.down && w.sinceReset >= w.resetEvery && w.resets < w.maxResets {
		w.resets++
		w.header.checkpointSeq++
		w.header.salt1++
		w.header.salt2 += 7919
		w.size = grow
		w.sinceReset = grow
		if w.overrunSaved {
			w.size = w.saved + 4096
		}
	}
	return rows, nil
}

func (w *fakeWAL) stop(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.down = true
	w.saved = w.size
	w.sinceReset = w.size
	return nil
}

func (w *fakeWAL) start(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.down = false
	return nil
}

type walResetScript struct {
	wal      *fakeWAL
	stopErr  error
	startErr error
	lagging  bool
	phase    string
	validErr error
	l9       []int
	uploaded []uint64
	gateErr  error
	downtime []bool
}

func newTestWALResetCheck(script *walResetScript) *walResetCheck {
	check := newWALResetCheck(Config{DBPath: "/data/test.db"})
	check.pause = time.Hour
	check.burstBytes = 3 * 1000 * 4096
	check.burstBudget = 5 * time.Second
	check.downBudget = 2 * time.Second
	check.headerPoll = 0
	check.startBudget = time.Second
	check.catchUpBudget = 300 * time.Millisecond
	check.catchUpPoll = 20 * time.Millisecond
	check.gateBudget = 50 * time.Millisecond
	check.header = script.wal.read
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
		return walResetValidation{phase: script.phase, boundary: 500, steps: []reporting.VerificationStep{{Name: "restore", Status: "ok"}}}, script.validErr
	}
	l9Calls, uploadCalls := 0, 0
	check.l9Count = func() (int, bool) {
		if l9Calls >= len(script.l9) {
			return 0, false
		}
		l9Calls++
		return script.l9[l9Calls-1], true
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
	}{
		{
			name: "five resets below the saved position then an intact latest restore passes",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), l9: []int{1, 2}, uploaded: []uint64{1000, 5000, 9000}}
			},
			wantStatus:  "passed",
			wantSummary: []string{"wal_resets=5", "checkpoints=5", "generations_seen=6", "l9_before=1 l9_after=2", "uploaded_bytes=8000", "restored_txid=00000000000001f4", "catchup_txid=00000000000001f4"},
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
		},
		{
			name: "a generation that grows back to the saved position overwrites the stale frame",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.overrunSaved = true
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetShapeNotReached,
			wantMessage:   "so the stale frame was overwritten",
		},
		{
			name: "a stop that does not exit is a litestream restart failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), stopErr: errors.New("litestream did not exit after terminated")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetLitestreamRestartFailed,
			wantMessage:   "stop: litestream did not exit",
		},
		{
			name: "a start that fails is a litestream restart failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), startErr: errors.New("timed out waiting for first sync")}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetLitestreamRestartFailed,
			wantMessage:   "start: timed out waiting for first sync",
		},
		{
			name:          "a replica that never catches up is a catch-up timeout",
			script:        func() *walResetScript { return &walResetScript{wal: newFakeWAL(), lagging: true} },
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetCatchUpTimeout,
			wantMessage:   "replicated txid 00000000000001a4 behind primary txid 00000000000001f4",
		},
		{
			name: "a failed restore is a restore failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "restore", validErr: errors.New("restore failed: validation failed (exit 1): page 50485: never used")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetRestoreFailed,
			wantMessage:   "wal reset restore failed: restore failed: validation failed (exit 1): page 50485: never used",
		},
		{
			name: "a failed integrity check is an integrity failure",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "integrity_check", validErr: errors.New("restored integrity: Page 50485: never used")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetIntegrityFailed,
			wantMessage:   "wal reset integrity failed: restored integrity: Page 50485: never used",
		},
		{
			name: "a logical difference is a logical mismatch",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "compare", validErr: errors.New("table wal_reset_burst: 80 rows restored, 201 in source")}
			},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureWALResetLogicalMismatch,
			wantMessage:   "wal reset logical mismatch: table wal_reset_burst: 80 rows restored, 201 in source",
		},
		{
			name: "a harness-side validation problem is inconclusive",
			script: func() *walResetScript {
				return &walResetScript{wal: newFakeWAL(), phase: "source_snapshot", validErr: fmt.Errorf("%w: source_snapshot: database is locked", errWALResetUnavailable)}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "source_snapshot: database is locked",
		},
		{
			name: "an unreadable wal header is inconclusive",
			script: func() *walResetScript {
				wal := newFakeWAL()
				wal.headerErr = errors.New("open test.db-wal: permission denied")
				return &walResetScript{wal: wal}
			},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureWALResetUnavailable,
			wantMessage:   "read wal header: open test.db-wal: permission denied",
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
			if script.wal.down && tt.wantStatus != "aborted" {
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
	if observation.savedWALBytes < check.burstBytes {
		t.Fatalf("stopped litestream at %d wal bytes, want at least %d", observation.savedWALBytes, check.burstBytes)
	}
	if observation.maxGenerationBytes >= observation.savedWALBytes {
		t.Fatalf("a post-stop generation reached %d bytes against %d saved", observation.maxGenerationBytes, observation.savedWALBytes)
	}
	if observation.resets != 5 || observation.savedHeader.salt1 != 1000 || observation.finalHeader.salt1 != 1005 {
		t.Fatalf("resets = %d salt1 %d -> %d, want 5 from 1000 to 1005", observation.resets, observation.savedHeader.salt1, observation.finalHeader.salt1)
	}
	if !strings.Contains(observation.summary(), "salt1_before=1000 salt1_after=1005 ckpt_seq_before=40 ckpt_seq_after=45") {
		t.Fatalf("summary = %s", observation.summary())
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
