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
	progress   []float64
	rows       map[string][]string
	failAt     string
	hangTXID   bool
	hangPct    bool
	statements []string
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

func (f *fakeVFSReader) hydrationProgress(ctx context.Context) (float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, errVFSSessionClosed
	}
	if f.hangPct {
		<-ctx.Done()
		return 0, fmt.Errorf("vfs reader statement %q: %w", "PRAGMA litestream_hydration_progress", ctx.Err())
	}
	return next(&f.progress), nil
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

func newTestVFSReaderCheck(t *testing.T, reader *fakeVFSReader, startErr error) *vfsReaderCheck {
	t.Helper()
	check := newVFSReaderCheck(Config{DBPath: t.TempDir() + "/test.db"})
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
	check.probeCommits = func() []cacheProbeCommit { return []cacheProbeCommit{{txid: 9}, {txid: 11}} }
	t.Cleanup(check.Stop)
	return check
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
	const first = "2048|0|0|0"
	tests := []struct {
		name          string
		reader        *fakeVFSReader
		commits       []cacheProbeCommit
		startErr      error
		wantStatus    string
		wantSignature string
		wantMessage   string
		wantDropped   bool
	}{
		{
			name:       "repeats match after a probe update landed mid-transaction and the reader advances after commit",
			reader:     &fakeVFSReader{probes: []string{first, first, first, "2048|0|1|128"}, txids: []uint64{10, 11, 11, 12}, progress: []float64{100}},
			wantStatus: "passed",
		},
		{
			name:          "a probe update that lands only after the last repeat is no evidence",
			reader:        &fakeVFSReader{probes: []string{first, first, first, "2048|0|1|128"}, txids: []uint64{10, 10, 11, 12}, progress: []float64{100}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "probe update landing by txid 000000000000000b was not observed before the last repeat",
		},
		{
			name:          "synthetic writes replicating mid-transaction are no evidence without a probe update",
			reader:        &fakeVFSReader{probes: []string{first, first, first, "2048|0|1|128"}, txids: []uint64{10, 11, 12, 13}, progress: []float64{100}},
			commits:       []cacheProbeCommit{{txid: 9}, {txid: 13}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "probe update landing by txid 000000000000000d was not observed before the last repeat",
		},
		{
			name:          "a replica that never moves during the transaction is no evidence",
			reader:        &fakeVFSReader{probes: []string{first, first, first, "2048|0|1|128"}, txids: []uint64{10, 10, 10, 12}, progress: []float64{100}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "probe update landing by txid 000000000000000b was not observed before the last repeat",
		},
		{
			name:          "no probe commit past the first read is no evidence even when the reader advances",
			reader:        &fakeVFSReader{probes: []string{first, first, first, "2048|0|1|128"}, txids: []uint64{10, 11, 12, 13}, progress: []float64{100}},
			commits:       []cacheProbeCommit{{txid: 9}, {txid: 0}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "no probe update landed past the first read at txid 000000000000000a",
		},
		{
			name:          "a repeat that sees replicated updates is snapshot drift",
			reader:        &fakeVFSReader{probes: []string{first, first, "2048|0|1|128", "2048|0|1|256"}, txids: []uint64{10, 10, 11, 12}, progress: []float64{100}},
			wantStatus:    "failed",
			wantSignature: reporting.SignatureVFSSnapshotDrift,
			wantMessage:   "vfs snapshot drift: repeat 2 of 2 inside one read transaction returned 2048|0|1|128@000000000000000b, first read returned 2048|0|0|0@000000000000000a (1 of 2 repeats differ)",
		},
		{
			name:          "no advance after commit gives no evidence",
			reader:        &fakeVFSReader{probes: []string{first}, txids: []uint64{10, 11}, progress: []float64{100}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "probe after COMMIT 2048|0|0|0@000000000000000b did not advance past the first read",
		},
		{
			name:          "a hydration statement that times out drops the desynchronized session",
			reader:        &fakeVFSReader{probes: []string{first}, hangPct: true},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSHydrationIncomplete,
			wantMessage:   "vfs reader statement timed out",
			wantDropped:   true,
		},
		{
			name:          "hydration still running keeps the session for the next cycle",
			reader:        &fakeVFSReader{probes: []string{first}, progress: []float64{40}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSHydrationIncomplete,
			wantMessage:   "40.0% after",
		},
		{
			name:          "a statement failure mid-transaction drops the session",
			reader:        &fakeVFSReader{probes: []string{first}, progress: []float64{100}, failAt: "COMMIT"},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderUnavailable,
			wantDropped:   true,
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
			check := newTestVFSReaderCheck(t, tt.reader, tt.startErr)
			if tt.commits != nil {
				check.probeCommits = func() []cacheProbeCommit { return tt.commits }
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
				if result.InconclusiveClassification != nil || !strings.Contains(result.Summary, "2 repeats matched the first read across 2 replica txids") {
					t.Fatalf("passed result = %+v", result)
				}
			}
			if tt.startErr == nil && tt.reader.count("BEGIN") != tt.reader.count("COMMIT") {
				t.Fatalf("transactions unbalanced: %v", tt.reader.statements)
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
			reader:     &fakeVFSReader{probes: []string{"2048|3|4|7000"}, txids: []uint64{18, 20}, progress: []float64{100}, rows: map[string][]string{`SELECT count(*) FROM "load_test"`: {"500"}}},
			primary:    queries("500"),
			wantStatus: "passed",
		},
		{
			name:          "a differing row count is a read mismatch",
			boundary:      20,
			reader:        &fakeVFSReader{probes: []string{"2048|3|4|7000"}, txids: []uint64{20}, progress: []float64{100}, rows: map[string][]string{`SELECT count(*) FROM "load_test"`: {"499"}}},
			primary:       queries("500"),
			wantStatus:    "failed",
			wantSignature: reporting.SignatureVFSReadMismatch,
			wantMessage:   `vfs read mismatch: load_test count returned vfs="499" primary="500" at txid 0000000000000014 (1 of 2 queries differ)`,
		},
		{
			name:          "a reader behind the boundary is not judged and keeps its session",
			boundary:      20,
			reader:        &fakeVFSReader{probes: []string{"2048|3|4|7000"}, txids: []uint64{15}, progress: []float64{100}},
			primary:       queries("500"),
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "reader at txid 000000000000000f after",
		},
		{
			name:          "a txid statement that times out drops the desynchronized session",
			boundary:      20,
			reader:        &fakeVFSReader{probes: []string{"2048|3|4|7000"}, hangTXID: true, progress: []float64{100}},
			primary:       queries("500"),
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
			wantMessage:   "vfs reader statement timed out",
			wantDropped:   true,
		},
		{
			name:          "an unknown boundary is not judged",
			reader:        &fakeVFSReader{progress: []float64{100}},
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderNotCaughtUp,
		},
		{
			name:          "an unreadable primary is unavailable",
			boundary:      20,
			reader:        &fakeVFSReader{txids: []uint64{20}, progress: []float64{100}},
			primaryErr:    errors.New("database is locked"),
			wantStatus:    "aborted",
			wantSignature: reporting.SignatureVFSReaderUnavailable,
			wantDropped:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := newTestVFSReaderCheck(t, tt.reader, nil)
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
	if args != "-batch -bail -list -noheader -cmd .load /opt/vfs/litestream-vfs.so file:/data/vfs/snapshot/replica.db?vfs=litestream&mode=ro" {
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
