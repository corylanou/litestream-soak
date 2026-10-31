package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/corylanou/litestream-soak/internal/reporting"
	_ "modernc.org/sqlite"
)

func TestManyDBPathsAndActiveSubset(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 20

	paths := cfg.ManyDBPaths()
	if len(paths) != 10 {
		t.Fatalf("ManyDBPaths() len = %d, want 10", len(paths))
	}
	if paths[0] != "/data/dbs/db-00001.db" {
		t.Fatalf("first path = %q, want /data/dbs/db-00001.db", paths[0])
	}
	if paths[9] != "/data/dbs/db-00010.db" {
		t.Fatalf("last path = %q, want /data/dbs/db-00010.db", paths[9])
	}

	active := cfg.ManyDBActivePathsAt(time.Unix(0, 0))
	if len(active) != 2 {
		t.Fatalf("ManyDBActivePaths() len = %d, want 2", len(active))
	}
	for _, activePath := range active {
		if !slices.Contains(paths, activePath) {
			t.Fatalf("active path %q is not in all paths %v", activePath, paths)
		}
	}
}

func TestManyDBActivePathsRotateDeterministically(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 30
	cfg.ActiveRotateInterval = time.Minute
	cfg.ActiveSetSeed = 42

	base := time.Unix(1700000000, 0)
	first := cfg.ManyDBActivePathsAt(base)
	sameInterval := cfg.ManyDBActivePathsAt(base.Add(30 * time.Second))
	nextInterval := cfg.ManyDBActivePathsAt(base.Add(time.Minute))
	repeated := cfg.ManyDBActivePathsAt(base.Add(time.Minute))

	if len(first) != 3 {
		t.Fatalf("first active len = %d, want 3: %v", len(first), first)
	}
	if !slices.Equal(first, sameInterval) {
		t.Fatalf("active paths changed inside one interval: %v vs %v", first, sameInterval)
	}
	if slices.Equal(first, nextInterval) {
		t.Fatalf("active paths did not rotate across intervals: %v", first)
	}
	if !slices.Equal(nextInterval, repeated) {
		t.Fatalf("active paths are not deterministic for same seed and interval: %v vs %v", nextInterval, repeated)
	}
	assertUniqueSubset(t, cfg.ManyDBPaths(), first)
	assertUniqueSubset(t, cfg.ManyDBPaths(), nextInterval)
}

func TestManyDBLoadCurrentActivePathsRotates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 20
	cfg.ActiveRotateInterval = time.Minute
	cfg.ActiveSetSeed = 7

	now := time.Unix(1700000000, 0)
	load := newManyDBLoad(&cfg)
	load.now = func() time.Time { return now }

	first := load.currentActivePaths()
	now = now.Add(time.Minute)
	next := load.currentActivePaths()

	if !slices.Equal(first, cfg.ManyDBActivePathsAt(time.Unix(1700000000, 0))) {
		t.Fatalf("currentActivePaths() = %v, want config snapshot", first)
	}
	if !slices.Equal(next, cfg.ManyDBActivePathsAt(time.Unix(1700000060, 0))) {
		t.Fatalf("currentActivePaths() after rotation = %v, want config snapshot", next)
	}
	if slices.Equal(first, next) {
		t.Fatalf("currentActivePaths() did not rotate: %v", first)
	}
}

func TestManyDBLoadDispatchesCyclicSlotsPerDatabase(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.NumDatabases = 2
	cfg.MaxRowsPerDatabase = 2
	cfg.ActivePercent = 100
	cfg.WriteRate = 1000

	load := newManyDBLoad(&cfg)
	load.now = func() time.Time { return time.Unix(0, 0) }
	load.jobs = make(chan manyDBWrite)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	load.wg.Add(1)
	go load.dispatch(ctx)

	paths := cfg.ManyDBPaths()
	want := []manyDBWrite{
		{dbPath: paths[0], rowSlot: 1},
		{dbPath: paths[1], rowSlot: 1},
		{dbPath: paths[0], rowSlot: 2},
		{dbPath: paths[1], rowSlot: 2},
		{dbPath: paths[0], rowSlot: 1},
		{dbPath: paths[1], rowSlot: 1},
	}
	got := make([]manyDBWrite, 0, len(want))
	for range want {
		select {
		case write := <-load.jobs:
			got = append(got, write)
		case <-ctx.Done():
			t.Fatalf("dispatch timed out after jobs %v", got)
		}
	}
	cancel()
	load.wg.Wait()

	if !slices.Equal(got, want) {
		t.Fatalf("dispatched writes = %v, want %v", got, want)
	}
}

func TestManyDBLoadChangedPathsRetainedUntilSuccess(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 5
	load := newManyDBLoad(&cfg)
	paths := cfg.ManyDBPaths()
	first := load.pendingManyDBChanges()
	if len(first) != 5 {
		t.Fatalf("untouched pending = %d, want 5", len(first))
	}
	load.markChanged(paths[0])
	load.acknowledgeManyDBChange(first[0])
	if got := len(load.pendingManyDBChanges()); got != 5 {
		t.Fatalf("new generation lost: %d", got)
	}
	for _, change := range load.pendingManyDBChanges() {
		load.acknowledgeManyDBChange(change)
	}
	if got := len(load.pendingManyDBChanges()); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	load.markChanged(paths[0])
	load.acknowledgeManyDBChange(first[0])
	if got := len(load.pendingManyDBChanges()); got != 1 {
		t.Fatalf("stale acknowledgment lost requeued work: %d", got)
	}
}

func TestManyDBPendingFairness(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 5
	cfg.VerifyChangedLimit = 2
	load := newManyDBLoad(&cfg)
	paths := cfg.ManyDBPaths()
	for i := range 15 {
		changes := load.pendingManyDBChanges()
		if changes[0].path != paths[i%5] {
			t.Fatalf("attempt %d: first=%s", i, changes[0].path)
		}
		load.attemptManyDBChange(changes[0])
	}
	load.markChanged(paths[3])
	for i, want := range []int{0, 1, 2, 4, 0, 1} {
		changes := load.pendingManyDBChanges()
		if changes[0].path != paths[3] || !changes[0].written {
			t.Fatalf("attempt %d: written database not first: %+v", i, changes[0])
		}
		if changes[1].path != paths[want] {
			t.Fatalf("attempt %d: backlog round robin = %s, want %s", i, changes[1].path, paths[want])
		}
		load.attemptManyDBChange(changes[1])
	}
	if got := len(load.pendingManyDBChanges()); got != 5 {
		t.Fatalf("failed work lost: %d", got)
	}
}

func TestManyDBPendingOrdersWrittenAheadOfSeededBacklog(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 6
	cfg.VerifyChangedLimit = 3
	load := newManyDBLoad(&cfg)
	paths := cfg.ManyDBPaths()
	if written := load.manyDBPendingCoverage().written; written != 0 {
		t.Fatalf("seeded set counted as written: %d", written)
	}
	load.markChanged(paths[4])
	load.markChanged(paths[1])
	got := load.pendingManyDBChanges()
	var order []string
	for _, change := range got {
		order = append(order, filepath.Base(change.path))
	}
	want := []string{filepath.Base(paths[1]), filepath.Base(paths[4]), filepath.Base(paths[0]), filepath.Base(paths[2]), filepath.Base(paths[3]), filepath.Base(paths[5])}
	if !slices.Equal(order, want) {
		t.Fatalf("order = %v, want written first then backlog %v", order, want)
	}
	if !got[0].written || !got[1].written || got[2].written {
		t.Fatalf("written flags = %v %v %v", got[0].written, got[1].written, got[2].written)
	}
	if written := load.manyDBPendingCoverage().written; written != 2 {
		t.Fatalf("written pending = %d, want 2", written)
	}

	var changed []string
	for _, change := range got {
		changed = append(changed, change.path)
	}
	targets, total := selectManyDBVerificationTargets(cfg, changed)
	if total != 6 || !slices.Equal(targets, []string{paths[1], paths[4], paths[0]}) {
		t.Fatalf("targets = %v (total %d), want both written databases then the first backlog entry", targets, total)
	}

	load.attemptManyDBChange(got[2])
	order = order[:0]
	for _, change := range load.pendingManyDBChanges() {
		order = append(order, filepath.Base(change.path))
	}
	want = []string{filepath.Base(paths[1]), filepath.Base(paths[4]), filepath.Base(paths[2]), filepath.Base(paths[3]), filepath.Base(paths[5]), filepath.Base(paths[0])}
	if !slices.Equal(order, want) {
		t.Fatalf("round robin after an attempt = %v, want %v", order, want)
	}
	load.acknowledgeManyDBChange(got[0])
	load.acknowledgeManyDBChange(got[1])
	if coverage := load.manyDBPendingCoverage(); coverage.written != 0 || coverage.pending != 4 {
		t.Fatalf("coverage after acknowledging the written databases = %+v, want 4 pending none written", coverage)
	}
	load.markChanged(paths[0])
	if load.manyDBPendingCoverage().written != 1 || load.pendingManyDBChanges()[0].path != paths[0] {
		t.Fatalf("a write to a backlog database must move it to the front: %+v", load.pendingManyDBChanges()[0])
	}
}

func manyDBWrittenStateDir(t *testing.T, cfg *Config) {
	t.Helper()
	cfg.DataDir = t.TempDir()
	if err := os.MkdirAll(cfg.ManyDBDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, path := range cfg.ManyDBPaths() {
		if err := os.WriteFile(path, []byte("db"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeManyDBWrittenState(*cfg, nil); err != nil {
		t.Fatal(err)
	}
}

func TestManyDBWrittenSetSurvivesRestart(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 5
	cfg.VerifyChangedLimit = 2
	manyDBWrittenStateDir(t, &cfg)
	paths := cfg.ManyDBPaths()

	before := newManyDBLoad(&cfg)
	if coverage := before.manyDBPendingCoverage(); coverage.written != 0 || coverage.pending != 5 {
		t.Fatalf("fresh volume with an empty written set = %+v", coverage)
	}
	before.markChanged(paths[2])
	before.markChanged(paths[2])
	before.markChanged(paths[4])
	written, err := readManyDBWrittenState(&cfg)
	if err != nil || !slices.Equal(written, []string{paths[2], paths[4]}) {
		t.Fatalf("persisted written set = %v, %v", written, err)
	}
	for _, change := range before.pendingManyDBChanges() {
		if change.path == paths[4] {
			before.acknowledgeManyDBChange(change)
		}
	}
	if written, err = readManyDBWrittenState(&cfg); err != nil || !slices.Equal(written, []string{paths[2]}) {
		t.Fatalf("acknowledged database still persisted: %v, %v", written, err)
	}

	after := newManyDBLoad(&cfg)
	coverage := after.manyDBPendingCoverage()
	if coverage.written != 1 || coverage.pending != 5 {
		t.Fatalf("restarted tracker = %+v, want the one outstanding write restored", coverage)
	}
	pending := after.pendingManyDBChanges()
	if pending[0].path != paths[2] || !pending[0].written {
		t.Fatalf("restored write not first: %+v", pending[0])
	}
	var changed []string
	for _, change := range pending {
		changed = append(changed, change.path)
	}
	targets, _ := selectManyDBVerificationTargets(cfg, changed)
	if targets[0] != paths[2] {
		t.Fatalf("truncated batch left out the restored write: %v", targets)
	}
	for _, change := range pending[1:] {
		after.acknowledgeManyDBChange(change)
	}
	if status, summary := (manyDBBatch{targets: 2, coverage: after.manyDBPendingCoverage()}).outcome(); status != "pending" {
		t.Fatalf("batch excluding the restored write passed: %s %s", status, summary)
	}
	after.acknowledgeManyDBChange(pending[0])
	if status, _ := (manyDBBatch{targets: 1, coverage: after.manyDBPendingCoverage()}).outcome(); status != "passed" {
		t.Fatalf("status after verifying the restored write = %s", status)
	}
}

func TestManyDBWrittenSetFallsBackToEveryDatabase(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 4
	manyDBWrittenStateDir(t, &cfg)
	statePath := manyDBWrittenStatePath(&cfg)

	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	missing := newManyDBLoad(&cfg)
	if coverage := missing.manyDBPendingCoverage(); coverage.written != 4 || coverage.pending != 4 {
		t.Fatalf("missing state with databases on the volume = %+v, want every database written", coverage)
	}

	if err := os.WriteFile(statePath, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	corrupt := newManyDBLoad(&cfg)
	if coverage := corrupt.manyDBPendingCoverage(); coverage.written != 4 {
		t.Fatalf("corrupt state = %+v, want every database written", coverage)
	}

	fresh := DefaultConfig()
	fresh.NumDatabases = 4
	fresh.DataDir = t.TempDir()
	empty := newManyDBLoad(&fresh)
	if coverage := empty.manyDBPendingCoverage(); coverage.written != 0 || coverage.pending != 4 {
		t.Fatalf("fresh volume = %+v, want seeded only", coverage)
	}
	empty.markChanged(fresh.ManyDBPaths()[0])
	if _, err := os.Stat(manyDBWrittenStatePath(&fresh)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("written set persisted before the database directory exists: %v", err)
	}
}

func TestPopulateManyDBsRecordsEmptyWrittenSetOnAFreshVolume(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.NumDatabases = 2
	if err := populateManyDBs(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	written, err := readManyDBWrittenState(&cfg)
	if err != nil || len(written) != 0 {
		t.Fatalf("written set after a fresh populate = %v, %v", written, err)
	}
	load := newManyDBLoad(&cfg)
	load.markChanged(cfg.ManyDBPaths()[1])
	if err := populateManyDBs(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if written, err = readManyDBWrittenState(&cfg); err != nil || !slices.Equal(written, []string{cfg.ManyDBPaths()[1]}) {
		t.Fatalf("re-populate on a restart overwrote the written set: %v, %v", written, err)
	}
}

func TestManyDBWrittenSetMatchesMemoryAfterConcurrentChanges(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 16
	manyDBWrittenStateDir(t, &cfg)
	paths := cfg.ManyDBPaths()
	load := newManyDBLoad(&cfg)
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				path := paths[(i*7+worker)%len(paths)]
				load.markChanged(path)
				if i%3 == 0 {
					load.changedMu.Lock()
					change, ok := load.changed[paths[(i+worker)%len(paths)]]
					load.changedMu.Unlock()
					if ok {
						load.acknowledgeManyDBChange(change)
					}
				}
			}
		}()
	}
	wg.Wait()
	var inMemory []string
	for _, change := range load.pendingManyDBChanges() {
		if change.written {
			inMemory = append(inMemory, change.path)
		}
	}
	sort.Strings(inMemory)
	onDisk, err := readManyDBWrittenState(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(onDisk, inMemory) {
		t.Fatalf("persisted written set %v differs from memory %v", onDisk, inMemory)
	}
	var restored []string
	for _, change := range newManyDBLoad(&cfg).pendingManyDBChanges() {
		if change.written {
			restored = append(restored, change.path)
		}
	}
	sort.Strings(restored)
	if !slices.Equal(restored, inMemory) {
		t.Fatalf("restart restored %v, memory had %v", restored, inMemory)
	}
}

func TestManyDBWritePersistsIntentBeforeTheRowCommits(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 3
	manyDBWrittenStateDir(t, &cfg)
	paths := cfg.ManyDBPaths()
	load := newManyDBLoad(&cfg)
	var order []string
	load.writeRow = func(_ context.Context, dbPath string, _ int, _ int64) error {
		written, err := readManyDBWrittenState(&cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(written, dbPath) {
			t.Fatalf("row written before the written set recorded %s: file has %v", dbPath, written)
		}
		order = append(order, "write "+filepath.Base(dbPath))
		return nil
	}
	load.performWrite(context.Background(), manyDBWrite{dbPath: paths[1], rowSlot: 1})
	if !slices.Equal(order, []string{"write " + filepath.Base(paths[1])}) {
		t.Fatalf("order = %v", order)
	}
	crashed := newManyDBLoad(&cfg)
	if coverage := crashed.manyDBPendingCoverage(); coverage.written != 1 || crashed.pendingManyDBChanges()[0].path != paths[1] {
		t.Fatalf("a crash right after the commit lost the write: %+v %+v", coverage, crashed.pendingManyDBChanges()[0])
	}
	load.writeRow = func(context.Context, string, int, int64) error { return errors.New("database is locked") }
	load.performWrite(context.Background(), manyDBWrite{dbPath: paths[2], rowSlot: 1})
	if written, err := readManyDBWrittenState(&cfg); err != nil || !slices.Equal(written, []string{paths[1], paths[2]}) {
		t.Fatalf("a failed write must stay recorded as written: %v, %v", written, err)
	}
}

func TestManyDBWrittenSetFailureGoesDirtyAndFallsBackOnRestart(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 4
	manyDBWrittenStateDir(t, &cfg)
	paths := cfg.ManyDBPaths()
	load := newManyDBLoad(&cfg)
	var events []string
	load.onStateEvent = func(eventType, message string) { events = append(events, eventType+": "+message) }
	load.markChanged(paths[0])
	saves := 0
	load.saveState = func([]string) error { saves++; return errors.New("read-only file system") }
	load.markChanged(paths[1])
	load.markChanged(paths[1])
	load.markChanged(paths[1])
	if !load.stateIsDirty() || saves != 3 {
		t.Fatalf("dirty=%v saves=%d, want dirty with a retry on every later mark", load.stateIsDirty(), saves)
	}
	if _, err := os.Stat(manyDBWrittenStatePath(&cfg)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale written set left on disk: %v", err)
	}
	if len(events) != 1 || !strings.HasPrefix(events[0], manyDBStateDirtyEvent+": ") {
		t.Fatalf("events = %v, want one dirty event", events)
	}
	restarted := newManyDBLoad(&cfg)
	if coverage := restarted.manyDBPendingCoverage(); coverage.written != 4 {
		t.Fatalf("restart while dirty = %+v, want every database written", coverage)
	}
	load.saveState = func(written []string) error { return writeManyDBWrittenState(cfg, written) }
	for _, change := range load.pendingManyDBChanges() {
		if change.path == paths[3] {
			load.acknowledgeManyDBChange(change)
		}
	}
	if load.stateIsDirty() {
		t.Fatal("a successful save did not clear the dirty flag")
	}
	if written, err := readManyDBWrittenState(&cfg); err != nil || !slices.Equal(written, []string{paths[0], paths[1]}) {
		t.Fatalf("recovered written set = %v, %v", written, err)
	}
	if len(events) != 2 || !strings.HasPrefix(events[1], manyDBStateRecoveredEvent+": ") {
		t.Fatalf("events = %v, want dirty then recovered", events)
	}
	load.markChanged(paths[2])
	if len(events) != 2 {
		t.Fatalf("events after recovery = %v, want no repeats", events)
	}
}

func TestManyDBPendingCoverageIsReadUnderOneLock(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumDatabases = 8
	load := newManyDBLoad(&cfg)
	paths := cfg.ManyDBPaths()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			load.markChanged(paths[i%len(paths)])
			if change, ok := load.changed[paths[(i+3)%len(paths)]]; ok {
				load.acknowledgeManyDBChange(change)
			}
		}
	}()
	for i := 0; i < 2000; i++ {
		coverage := load.manyDBPendingCoverage()
		if coverage.written > coverage.pending {
			t.Fatalf("written %d exceeds pending %d", coverage.written, coverage.pending)
		}
		if coverage.pending == 0 && (coverage.written != 0 || coverage.oldestAge != 0) {
			t.Fatalf("empty set reported %+v", coverage)
		}
	}
	<-done
}

func TestManyDBBatchOutcome(t *testing.T) {
	stats := manyDBStepStats{sync: manyDBDurationStats{count: 2, total: 3 * time.Second, max: 2 * time.Second}, restore: manyDBDurationStats{count: 2, total: 20 * time.Second, max: 12 * time.Second}}
	tests := []struct {
		name        string
		batch       manyDBBatch
		wantStatus  string
		wantSummary []string
	}{
		{
			name:        "written databases beyond the cap keep coverage pending",
			batch:       manyDBBatch{targets: 100, truncated: 40, coverage: manyDBCoverage{pending: 440, written: 40, oldestAge: 90.5}, stats: stats},
			wantStatus:  "pending",
			wantSummary: []string{"verified batch of 100 databases", "coverage pending for 40 written databases", "440 pending in all", "oldest 90.500s", "sync_mean_seconds=1.5 sync_max_seconds=2.0", "restore_mean_seconds=10.0 restore_max_seconds=12.0"},
		},
		{
			name:        "seeded backlog alone passes and is reported",
			batch:       manyDBBatch{targets: 100, truncated: 300, coverage: manyDBCoverage{pending: 300, oldestAge: 1800}, stats: stats},
			wantStatus:  "passed",
			wantSummary: []string{"verification passed (100 databases verified", "0 written databases still pending", "seeded backlog 300", "oldest 1800.000s", "restore_max_seconds=12.0"},
		},
		{
			name:        "drained set passes with no backlog",
			batch:       manyDBBatch{targets: 12},
			wantStatus:  "passed",
			wantSummary: []string{"verification passed (12 databases verified", "seeded backlog 0", "sync_mean_seconds=0.0"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, summary := tt.batch.outcome()
			if status != tt.wantStatus {
				t.Fatalf("status = %s, want %s (%s)", status, tt.wantStatus, summary)
			}
			for _, want := range tt.wantSummary {
				if !strings.Contains(summary, want) {
					t.Fatalf("summary %q is missing %q", summary, want)
				}
			}
			step := tt.batch.String()
			for _, want := range []string{fmt.Sprintf("targets=%d truncated=%d", tt.batch.targets, tt.batch.truncated), fmt.Sprintf("pending_databases=%d written_pending=%d", tt.batch.coverage.pending, tt.batch.coverage.written)} {
				if !strings.Contains(step, want) {
					t.Fatalf("step output %q is missing %q", step, want)
				}
			}
		})
	}
}

func TestManyDBBatchStatsAggregatePerDatabaseSteps(t *testing.T) {
	steps := []reporting.VerificationStep{
		{Name: "pause_load", DurationMS: 5000},
		{Name: "sync db-0001.db", DurationMS: 1000},
		{Name: "restore_validate db-0001.db", DurationMS: 8000},
		{Name: "sync db-0002.db", DurationMS: 3000},
		{Name: "restore_validate db-0002.db", DurationMS: 12000},
		{Name: "checkpoint db-0002.db", DurationMS: 9000},
	}
	stats := manyDBBatchStats(steps)
	if stats.sync.count != 2 || stats.sync.mean() != 2*time.Second || stats.sync.max != 3*time.Second {
		t.Fatalf("sync stats = %+v", stats.sync)
	}
	if stats.restore.count != 2 || stats.restore.mean() != 10*time.Second || stats.restore.max != 12*time.Second {
		t.Fatalf("restore stats = %+v", stats.restore)
	}
	if got := stats.String(); got != "sync_mean_seconds=2.0 sync_max_seconds=3.0 restore_mean_seconds=10.0 restore_max_seconds=12.0" {
		t.Fatalf("stats text = %q", got)
	}
	if got := manyDBTruncatedMessage(440, 100, 100); got != "many database changed set truncated: 440 pending databases, 100 selected this cycle (limit 100)" {
		t.Fatalf("truncation message = %q", got)
	}
}

func TestPopulateManyDBCreatesWALDatabases(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.NumDatabases = 3
	cfg.ActivePercent = 0

	runner := NewRunner(cfg)
	if err := runner.populate(context.Background()); err != nil {
		t.Fatalf("populate() error = %v", err)
	}

	for _, dbPath := range cfg.ManyDBPaths() {
		if _, err := os.Stat(dbPath); err != nil {
			t.Fatalf("stat seeded database %s: %v", dbPath, err)
		}
		db, err := sql.Open("sqlite", walDSN(dbPath))
		if err != nil {
			t.Fatalf("open seeded database %s: %v", dbPath, err)
		}
		var journalMode string
		if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
			_ = db.Close()
			t.Fatalf("read journal mode for %s: %v", dbPath, err)
		}
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM soak_payloads").Scan(&count); err != nil {
			_ = db.Close()
			t.Fatalf("read seeded row count for %s: %v", dbPath, err)
		}
		_ = db.Close()
		if journalMode != "wal" {
			t.Fatalf("journal_mode for %s = %q, want wal", dbPath, journalMode)
		}
		if count == 0 {
			t.Fatalf("seeded row count for %s = 0, want > 0", dbPath)
		}
	}
}

func TestPopulateManyDBPrunesRowsAboveConfiguredLimit(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.NumDatabases = 1
	cfg.MaxRowsPerDatabase = 3

	dbPath := cfg.ManyDBPaths()[0]
	if err := seedManyDB(ctx, dbPath); err != nil {
		t.Fatalf("seedManyDB() error = %v", err)
	}
	db, err := sql.Open("sqlite", manyDBDSN(dbPath))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	for row := 0; row < 5; row++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO soak_writes (body) VALUES (randomblob(128))"); err != nil {
			_ = db.Close()
			t.Fatalf("insert legacy row %d: %v", row+1, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	if err := populateManyDBs(ctx, cfg); err != nil {
		t.Fatalf("populateManyDBs() error = %v", err)
	}

	db, err = sql.Open("sqlite", manyDBDSN(dbPath))
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	var rowCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM soak_writes").Scan(&rowCount); err != nil {
		t.Fatalf("read retained row count: %v", err)
	}
	if rowCount != cfg.MaxRowsPerDatabase {
		t.Fatalf("retained row count = %d, want %d", rowCount, cfg.MaxRowsPerDatabase)
	}
}

func TestWriteManyDBRowRotatesRetainedRowsAndPreservesIntegrity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "source.db")
	const maxRows = 3
	const payloadSize = 128

	if err := seedManyDB(ctx, dbPath); err != nil {
		t.Fatalf("seedManyDB() error = %v", err)
	}
	for writeIndex := 0; writeIndex < maxRows; writeIndex++ {
		rowSlot := int64(writeIndex%maxRows + 1)
		if err := writeManyDBRow(ctx, dbPath, payloadSize, rowSlot); err != nil {
			t.Fatalf("writeManyDBRow() write %d error = %v", writeIndex+1, err)
		}
	}

	db, err := sql.Open("sqlite", manyDBDSN(dbPath))
	if err != nil {
		t.Fatalf("open source database: %v", err)
	}
	var payloadBeforeRotation []byte
	if err := db.QueryRowContext(ctx, "SELECT body FROM soak_writes WHERE id = 1").Scan(&payloadBeforeRotation); err != nil {
		_ = db.Close()
		t.Fatalf("read payload before rotation: %v", err)
	}
	for writeIndex := maxRows; writeIndex < maxRows+2; writeIndex++ {
		rowSlot := int64(writeIndex%maxRows + 1)
		if err := writeManyDBRow(ctx, dbPath, payloadSize, rowSlot); err != nil {
			_ = db.Close()
			t.Fatalf("writeManyDBRow() write %d error = %v", writeIndex+1, err)
		}
	}
	var payloadAfterRotation []byte
	if err := db.QueryRowContext(ctx, "SELECT body FROM soak_writes WHERE id = 1").Scan(&payloadAfterRotation); err != nil {
		_ = db.Close()
		t.Fatalf("read payload after rotation: %v", err)
	}
	if slices.Equal(payloadBeforeRotation, payloadAfterRotation) {
		_ = db.Close()
		t.Fatal("rotated payload did not change")
	}
	var rowCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM soak_writes").Scan(&rowCount); err != nil {
		_ = db.Close()
		t.Fatalf("read source row count: %v", err)
	}
	if rowCount != maxRows {
		_ = db.Close()
		t.Fatalf("source row count = %d, want %d", rowCount, maxRows)
	}
	var retainedPayloads int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM soak_writes WHERE length(body) = ?", payloadSize).Scan(&retainedPayloads); err != nil {
		_ = db.Close()
		t.Fatalf("read retained payload count: %v", err)
	}
	if retainedPayloads != maxRows {
		_ = db.Close()
		t.Fatalf("retained payload count = %d, want %d", retainedPayloads, maxRows)
	}
	var busy, logFrames, checkpointed int
	if err := db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint source database: %v", err)
	}
	if busy != 0 {
		_ = db.Close()
		t.Fatalf("checkpoint busy = %d, want 0", busy)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source database: %v", err)
	}

	body, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read source database: %v", err)
	}
	copyPath := filepath.Join(dir, "copy.db")
	if err := os.WriteFile(copyPath, body, 0o600); err != nil {
		t.Fatalf("copy source database: %v", err)
	}

	copyDB, err := sql.Open("sqlite", manyDBDSN(copyPath))
	if err != nil {
		t.Fatalf("open copied database: %v", err)
	}
	t.Cleanup(func() {
		_ = copyDB.Close()
	})
	if err := copyDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM soak_writes").Scan(&rowCount); err != nil {
		t.Fatalf("read copied row count: %v", err)
	}
	if rowCount != maxRows {
		t.Fatalf("copied row count = %d, want %d", rowCount, maxRows)
	}
	var integrity string
	if err := copyDB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("integrity check copied database: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("copied database integrity = %q, want ok", integrity)
	}
}

func TestWriteLitestreamConfigManyDBListUsesIsolatedReplicaTargets(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.NumDatabases = 3
	cfg.ConfigMode = "list"
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-many"

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}

	body, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	config := string(body)
	for _, dbName := range []string{"db-00001.db", "db-00002.db", "db-00003.db"} {
		if !strings.Contains(config, "path: "+filepath.Join(dir, "dbs", dbName)) {
			t.Fatalf("config missing db path %s:\n%s", dbName, config)
		}
		if !strings.Contains(config, "url: s3://bucket/soak/worker-many/"+dbName) {
			t.Fatalf("config missing isolated replica for %s:\n%s", dbName, config)
		}
	}
}

func TestWriteLitestreamConfigManyDBDirUsesWatcher(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.NumDatabases = 3
	cfg.ConfigMode = "dir"
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/worker-many"

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}

	body, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	config := string(body)
	for _, want := range []string{
		"dir: " + filepath.Join(dir, "dbs"),
		`pattern: "*.db"`,
		"watch: true",
		"url: s3://bucket/soak/worker-many",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("config missing %q:\n%s", want, config)
		}
	}
}

func TestPollDBStatsAggregatesManyDBList(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WorkerID = "test-worker-many-poll"
	cfg.ProfileName = "many-dbs-100-dir"
	cfg.Source = "test"
	cfg.DataDir = dir
	cfg.NumDatabases = 3
	cfg.ActivePercent = 33
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("litestream-soak-many-%d.sock", time.Now().UnixNano()))
	cfg.ReplicationLagThreshold = 1

	for i, dbPath := range cfg.ManyDBPaths() {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dbPath, []byte(strings.Repeat("d", 10+i)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dbPath+"-wal", []byte(strings.Repeat("w", 2+i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	SetWorkerInfo(cfg)
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/info":
			_, _ = w.Write([]byte(`{"uptime_seconds":99}`))
		case "/list":
			now := time.Now().UTC()
			body := fmt.Sprintf(`{"databases":[
{"path":%q,"status":"replicating","txid":10,"replicated_txid":10,"last_sync_at":%q},
{"path":%q,"status":"replicating","txid":10,"replicated_txid":8,"last_sync_at":%q},
{"path":%q,"status":"replicating","txid":20,"replicated_txid":15,"last_sync_at":%q}
]}`,
				cfg.ManyDBPaths()[0], now.Add(-1*time.Second).Format(time.RFC3339Nano),
				cfg.ManyDBPaths()[1], now.Add(-10*time.Second).Format(time.RFC3339Nano),
				cfg.ManyDBPaths()[2], now.Add(-30*time.Second).Format(time.RFC3339Nano),
			)
			_, _ = w.Write([]byte(body))
		default:
			http.NotFound(w, r)
		}
	}))

	runner := NewRunner(cfg)
	runner.pollDBStats()
	snapshot := runner.currentSnapshot()

	if snapshot.DBCount != 3 {
		t.Fatalf("DBCount = %d, want 3", snapshot.DBCount)
	}
	if snapshot.DBTotalSizeBytes != 33 {
		t.Fatalf("DBTotalSizeBytes = %d, want 33", snapshot.DBTotalSizeBytes)
	}
	if snapshot.WALTotalSizeBytes != 9 {
		t.Fatalf("WALTotalSizeBytes = %d, want 9", snapshot.WALTotalSizeBytes)
	}
	if snapshot.ReplicationLagMax != 5 {
		t.Fatalf("ReplicationLagMax = %d, want 5", snapshot.ReplicationLagMax)
	}
	if snapshot.ReplicationLagP95 != 5 {
		t.Fatalf("ReplicationLagP95 = %d, want 5", snapshot.ReplicationLagP95)
	}
	if snapshot.ReplicationLagOverThreshold != 2 {
		t.Fatalf("ReplicationLagOverThreshold = %d, want 2", snapshot.ReplicationLagOverThreshold)
	}
	if snapshot.LastSyncAgeMaxSeconds < 20 {
		t.Fatalf("LastSyncAgeMaxSeconds = %v, want >= 20", snapshot.LastSyncAgeMaxSeconds)
	}
	if !snapshot.LitestreamSnapshotHealthy {
		t.Fatalf("expected healthy snapshot, got error %q", snapshot.LitestreamSnapshotError)
	}
}

func TestManyDBVerificationTargetsUseChangedPaths(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 20
	cfg.VerifySampleSize = 5

	paths := cfg.ManyDBPaths()
	changed := []string{paths[7], paths[2], paths[7], paths[4]}
	targets, totalChanged := selectManyDBVerificationTargets(cfg, changed)
	want := []string{paths[7], paths[2], paths[4]}

	if totalChanged != len(want) {
		t.Fatalf("totalChanged = %d, want %d", totalChanged, len(want))
	}
	if !slices.Equal(targets, want) {
		t.Fatalf("targets = %v, want changed paths %v", targets, want)
	}
}

func TestManyDBVerificationTargetsTruncateChangedPaths(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data"
	cfg.NumDatabases = 10
	cfg.ActivePercent = 60
	cfg.VerifySampleSize = 2
	cfg.VerifyChangedLimit = 3

	paths := cfg.ManyDBPaths()
	changed := []string{paths[6], paths[1], paths[4], paths[8], paths[2]}
	targets, totalChanged := selectManyDBVerificationTargets(cfg, changed)
	want := []string{paths[6], paths[1], paths[4]}

	if totalChanged != 5 {
		t.Fatalf("totalChanged = %d, want 5", totalChanged)
	}
	if !slices.Equal(targets, want) {
		t.Fatalf("targets = %v, want capped changed paths %v", targets, want)
	}
}

func assertUniqueSubset(t *testing.T, all []string, subset []string) {
	t.Helper()

	seen := map[string]bool{}
	for _, path := range subset {
		if seen[path] {
			t.Fatalf("duplicate path %q in %v", path, subset)
		}
		if !slices.Contains(all, path) {
			t.Fatalf("path %q is not in all paths %v", path, all)
		}
		seen[path] = true
	}
}

func TestManyDBPendingMetricsAgeWithoutActivity(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.NumDatabases = 3
	load := newManyDBLoad(&cfg)
	now := time.Now().Add(time.Hour)
	load.now = func() time.Time { return now }
	registry := prometheus.NewRegistry()
	registry.MustRegister(load)
	for _, elapsed := range []float64{0, 60} {
		now = now.Add(time.Duration(elapsed) * time.Second)
		metrics, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, metric := range metrics {
			value := metric.Metric[0].GetGauge().GetValue()
			switch metric.GetName() {
			case "soak_many_db_pending_count":
				if value != 3 {
					t.Fatalf("pending=%f", value)
				}
			case "soak_many_db_oldest_pending_age_seconds":
				if value < 3600+elapsed {
					t.Fatalf("oldest=%f", value)
				}
			case "soak_many_db_written_pending_count":
				if value != 0 {
					t.Fatalf("written pending=%f, want 0 for a seeded-only set", value)
				}
			default:
				t.Fatalf("unexpected metric %s", metric.GetName())
			}
		}
	}
	for _, change := range load.pendingManyDBChanges() {
		load.acknowledgeManyDBChange(change)
	}
	if coverage := load.manyDBPendingCoverage(); coverage.pending != 0 || coverage.written != 0 || coverage.oldestAge != 0 {
		t.Fatalf("drained=%+v", coverage)
	}
}
