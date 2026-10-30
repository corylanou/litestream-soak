package worker

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func ltxFile(level int, minTXID, maxTXID uint64) restorePlanCandidate {
	return restorePlanCandidate{level: level, minTXID: minTXID, maxTXID: maxTXID}
}

func TestComputeLTXCoverage(t *testing.T) {
	tests := []struct {
		name       string
		files      []restorePlanCandidate
		wantFiles  int
		wantWindow txidRange
		wantHoles  []txidRange
	}{
		{
			name:       "contiguous L0 files",
			files:      []restorePlanCandidate{ltxFile(0, 1, 10), ltxFile(0, 11, 20), ltxFile(0, 21, 30)},
			wantFiles:  3,
			wantWindow: txidRange{1, 30},
		},
		{
			name:       "overlapping levels",
			files:      []restorePlanCandidate{ltxFile(1, 1, 100), ltxFile(0, 50, 60), ltxFile(0, 90, 120), ltxFile(2, 1, 80)},
			wantFiles:  4,
			wantWindow: txidRange{1, 120},
		},
		{
			name:       "single gap",
			files:      []restorePlanCandidate{ltxFile(1, 1, 100), ltxFile(1, 106, 200)},
			wantFiles:  2,
			wantWindow: txidRange{1, 200},
			wantHoles:  []txidRange{{101, 105}},
		},
		{
			name:       "multiple gaps in unsorted listing",
			files:      []restorePlanCandidate{ltxFile(0, 300, 400), ltxFile(1, 1, 100), ltxFile(1, 150, 200), ltxFile(0, 202, 250)},
			wantFiles:  4,
			wantWindow: txidRange{1, 400},
			wantHoles:  []txidRange{{101, 149}, {201, 201}, {251, 299}},
		},
		{
			name:       "gap filled by a higher level",
			files:      []restorePlanCandidate{ltxFile(1, 1, 100), ltxFile(1, 106, 200), ltxFile(3, 50, 150)},
			wantFiles:  3,
			wantWindow: txidRange{1, 200},
		},
		{
			name:       "L9 does not fill a gap or extend the window",
			files:      []restorePlanCandidate{ltxFile(1, 1, 100), ltxFile(9, 1, 105), ltxFile(1, 106, 200), ltxFile(9, 1, 500)},
			wantFiles:  2,
			wantWindow: txidRange{1, 200},
			wantHoles:  []txidRange{{101, 105}},
		},
		{
			name:  "only L9",
			files: []restorePlanCandidate{ltxFile(9, 1, 500)},
		},
		{
			name:       "nested file inside a wider one",
			files:      []restorePlanCandidate{ltxFile(2, 1, 500), ltxFile(0, 10, 20), ltxFile(0, 600, 700)},
			wantFiles:  3,
			wantWindow: txidRange{1, 700},
			wantHoles:  []txidRange{{501, 599}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeLTXCoverage(tt.files)
			if got.files != tt.wantFiles || got.window != tt.wantWindow || !reflect.DeepEqual(got.holes, tt.wantHoles) {
				t.Fatalf("coverage = %+v, want files=%d window=%v holes=%v", got, tt.wantFiles, tt.wantWindow, tt.wantHoles)
			}
		})
	}
}

func TestIntersectTXIDRanges(t *testing.T) {
	tests := []struct {
		name string
		a, b []txidRange
		want []txidRange
	}{
		{name: "identical", a: []txidRange{{10, 20}}, b: []txidRange{{10, 20}}, want: []txidRange{{10, 20}}},
		{name: "disjoint", a: []txidRange{{10, 20}}, b: []txidRange{{21, 30}}},
		{name: "partial overlap", a: []txidRange{{10, 20}, {40, 50}}, b: []txidRange{{15, 45}}, want: []txidRange{{15, 20}, {40, 45}}},
		{name: "one side empty", a: []txidRange{{10, 20}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := intersectTXIDRanges(tt.a, tt.b); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("intersect = %v, want %v", got, tt.want)
			}
		})
	}
}

func scriptedLister(t *testing.T, listings ...replicaLTXListing) (func() replicaLTXListing, *int) {
	t.Helper()
	calls := 0
	return func() replicaLTXListing {
		if calls >= len(listings) {
			t.Fatalf("unexpected listing call %d", calls+1)
		}
		calls++
		return listings[calls-1]
	}, &calls
}

func completeListing(files ...restorePlanCandidate) replicaLTXListing {
	return replicaLTXListing{candidates: files, complete: true}
}

func TestRunLTXCoverageCheckConfirmsHolesOnRelist(t *testing.T) {
	gapped := completeListing(ltxFile(0, 1, 100), ltxFile(1, 106, 200))
	healed := completeListing(ltxFile(0, 1, 100), ltxFile(1, 101, 200))
	shifted := completeListing(ltxFile(0, 1, 102), ltxFile(1, 106, 300), ltxFile(1, 310, 320))
	tests := []struct {
		name          string
		listings      []replicaLTXListing
		wantCalls     int
		wantConfirmed []txidRange
		wantErr       string
	}{
		{name: "no hole skips the relist", listings: []replicaLTXListing{healed}, wantCalls: 1},
		{name: "hole seen on both passes is confirmed", listings: []replicaLTXListing{gapped, gapped}, wantCalls: 2, wantConfirmed: []txidRange{{101, 105}}},
		{name: "hole closed by in-flight compaction is not reported", listings: []replicaLTXListing{gapped, healed}, wantCalls: 2},
		{name: "only the overlap of both passes is reported", listings: []replicaLTXListing{gapped, shifted}, wantCalls: 2, wantConfirmed: []txidRange{{103, 105}}},
		{name: "first listing error is inconclusive", listings: []replicaLTXListing{{err: "command timed out"}}, wantCalls: 1, wantErr: "first listing: command timed out"},
		{name: "first listing incomplete is inconclusive", listings: []replicaLTXListing{{candidates: gapped.candidates}}, wantCalls: 1, wantErr: "first listing: listing incomplete"},
		{name: "empty listing is inconclusive", listings: []replicaLTXListing{{complete: true}}, wantCalls: 1, wantErr: "first listing: no ltx entries parsed"},
		{name: "relist failure is inconclusive", listings: []replicaLTXListing{gapped, {err: "exit status 1"}}, wantCalls: 2, wantErr: "relisting: exit status 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list, calls := scriptedLister(t, tt.listings...)
			waits := 0
			outcome := runLTXCoverageCheck(list, func() { waits++ })
			if *calls != tt.wantCalls || waits != tt.wantCalls-1 {
				t.Fatalf("calls = %d waits = %d, want %d listings", *calls, waits, tt.wantCalls)
			}
			if outcome.err != tt.wantErr || !reflect.DeepEqual(outcome.confirmed, tt.wantConfirmed) {
				t.Fatalf("outcome = %+v, want err %q confirmed %v", outcome, tt.wantErr, tt.wantConfirmed)
			}
		})
	}
}

func TestLTXCoverageResult(t *testing.T) {
	started := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	completed := started.Add(2 * time.Second)

	passed := ltxCoverageResult(ltxCoverageOutcome{first: computeLTXCoverage([]restorePlanCandidate{ltxFile(0, 1, 10)})}, started, completed)
	if !passed.Passed || passed.Status != "passed" || passed.CheckType != ltxCoverageCheckType || passed.RestoreMode != ltxCoverageRestoreMode || passed.DurationMS != 2000 {
		t.Fatalf("passed = %+v", passed)
	}
	if len(passed.Steps) != 1 || passed.Steps[0].Name != ltxCoverageStep || passed.Steps[0].Status != "ok" || !strings.Contains(passed.Steps[0].OutputTail, "window=0000000000000001-000000000000000a holes=0") {
		t.Fatalf("passed steps = %+v", passed.Steps)
	}

	inconclusive := ltxCoverageResult(ltxCoverageOutcome{err: "first listing: listing incomplete"}, started, completed)
	if inconclusive.Passed || inconclusive.Status != "aborted" || !strings.HasPrefix(inconclusive.Summary, "verification inconclusive:") {
		t.Fatalf("inconclusive = %+v", inconclusive)
	}
	if c := inconclusive.InconclusiveClassification; c == nil || c.Signature != reporting.SignatureReplicaLevelReportIncomplete || c.Stage != ltxCoverageStep {
		t.Fatalf("inconclusive classification = %+v", inconclusive.InconclusiveClassification)
	}
	if strings.Contains(inconclusive.ErrorMessage, "hole") || inconclusive.Steps[0].Status != "inconclusive" {
		t.Fatalf("inconclusive result judged a hole: %+v", inconclusive)
	}

	unconfirmed := ltxCoverageOutcome{first: computeLTXCoverage([]restorePlanCandidate{ltxFile(0, 1, 10), ltxFile(0, 20, 30)}), relisted: true, second: computeLTXCoverage([]restorePlanCandidate{ltxFile(0, 1, 30)})}
	if result := ltxCoverageResult(unconfirmed, started, completed); !result.Passed || !strings.Contains(result.Steps[0].OutputTail, "unconfirmed holes: 000000000000000b-0000000000000013") {
		t.Fatalf("unconfirmed = %+v", result)
	}

	relistFailed := ltxCoverageOutcome{first: unconfirmed.first, err: "relisting: exit status 1"}
	if result := ltxCoverageResult(relistFailed, started, completed); result.Passed || result.Status != "aborted" || result.InconclusiveClassification == nil {
		t.Fatalf("relist failure = %+v", result)
	}
}

func TestLTXCoverageCatchesHoleBeforeL9Snapshot(t *testing.T) {
	listing := strings.Join([]string{
		"level  min_txid          max_txid          size     created",
		"0      0000000000007175  0000000000007175  4096     2026-10-30T11:58:00Z",
		"0      0000000000007180  0000000000007180  4096     2026-10-30T11:59:30Z",
		"0      0000000000007181  0000000000007181  4096     2026-10-30T11:59:31Z",
		"1      0000000000007101  0000000000007174  1048576  2026-10-30T11:58:00Z",
		"1      0000000000007180  0000000000007200  1048576  2026-10-30T12:00:30Z",
		"2      0000000000007001  0000000000007100  4194304  2026-10-30T11:55:00Z",
		"9      0000000000000001  000000000000717f  9437184  2026-10-30T11:59:00Z",
	}, "\n")
	files := parseRestorePlanCandidates(listing)
	coverage := computeLTXCoverage(files)
	want := []txidRange{{0x7176, 0x717f}}
	if coverage.window != (txidRange{0x7001, 0x7200}) || !reflect.DeepEqual(coverage.holes, want) {
		t.Fatalf("coverage = %+v, want window 7001-7200 holes %v", coverage, want)
	}

	list, _ := scriptedLister(t, completeListing(files...), completeListing(files...))
	result := ltxCoverageResult(runLTXCoverageCheck(list, func() {}), time.Now().UTC(), time.Now().UTC())
	wantMessage := "ltx coverage hole: L0-L8 union missing txids 0000000000007176-000000000000717f (1 holes)"
	if result.Passed || result.Status != "failed" || result.ErrorMessage != wantMessage || result.Summary != "ltx coverage check failed" {
		t.Fatalf("result = %+v, want failure %q", result, wantMessage)
	}
	classification := reporting.ClassifyVerificationFailure(result.CheckType, result.ErrorMessage)
	if classification.Signature != reporting.SignatureLTXCoverageHole || classification.Stage != ltxCoverageCheckType {
		t.Fatalf("classification = %+v", classification)
	}

	mainFiles := append(append([]restorePlanCandidate(nil), files...), ltxFile(0, 0x7176, 0x717f))
	if coverage := computeLTXCoverage(mainFiles); len(coverage.holes) != 0 {
		t.Fatalf("main-shaped listing holes = %v, want none", coverage.holes)
	}
}

func TestLTXCoverageCheckOnlyOnSingleDBWorkers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LTXCoverageCheck = true
	if !(&Runner{cfg: cfg}).ltxCoverageCheckEnabled() {
		t.Fatal("coverage check disabled on a single-db worker")
	}
	cfg.NumDatabases = 500
	if (&Runner{cfg: cfg}).ltxCoverageCheckEnabled() {
		t.Fatal("coverage check enabled on a many-db worker")
	}
	cfg = DefaultConfig()
	if (&Runner{cfg: cfg}).ltxCoverageCheckEnabled() {
		t.Fatal("coverage check enabled without LTX_COVERAGE_CHECK")
	}
}
