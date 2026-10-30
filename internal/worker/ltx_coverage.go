package worker

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const (
	ltxCoverageCheckType   = "coverage"
	ltxCoverageRestoreMode = "coverage"
	ltxCoverageStep        = "ltx_coverage"
	ltxCoverageMaxLevel    = 8
	ltxSnapshotLevel       = 9
	ltxCoverageHoleSample  = 5
)

var ltxCoverageRelistDelay = 5 * time.Second

type txidRange struct {
	min uint64
	max uint64
}

func (r txidRange) String() string {
	return formatTXID(r.min) + "-" + formatTXID(r.max)
}

// ltxCoverage is the union of the L0-L8 file txid ranges in one listing: the
// window from the smallest min_txid to the largest max_txid, and every txid
// range inside it that no file covers. L9 snapshots never fill a hole because a
// restore into it cannot be served from L0-L8 even when a snapshot exists.
//
// L9 bounds which holes are judged. Snapshot retention deletes L1-L8 files
// whose max_txid is below a cutoff that is never above the oldest retained
// snapshot's max_txid, keeping the last file at each level, and L0 retention
// only drops files that L1 covered. Every hole healthy retention can leave
// therefore lies below the oldest L9 max_txid: holes holds only the txids at or
// above that floor, and retained holds the rest, which are never judged.
// Without any L9 file the floor is unknown and floorKnown is false.
type ltxCoverage struct {
	files      int
	window     txidRange
	floor      uint64
	floorKnown bool
	holes      []txidRange
	retained   []txidRange
}

func computeLTXCoverage(candidates []restorePlanCandidate) ltxCoverage {
	ranges := make([]txidRange, 0, len(candidates))
	var coverage ltxCoverage
	for _, candidate := range candidates {
		if candidate.level == ltxSnapshotLevel && (!coverage.floorKnown || candidate.maxTXID < coverage.floor) {
			coverage.floor, coverage.floorKnown = candidate.maxTXID, true
		}
		if candidate.level < 0 || candidate.level > ltxCoverageMaxLevel || candidate.minTXID > candidate.maxTXID {
			continue
		}
		ranges = append(ranges, txidRange{min: candidate.minTXID, max: candidate.maxTXID})
	}
	coverage.files = len(ranges)
	if len(ranges) == 0 {
		return coverage
	}
	slices.SortFunc(ranges, func(a, b txidRange) int {
		switch {
		case a.min < b.min:
			return -1
		case a.min > b.min:
			return 1
		default:
			return 0
		}
	})
	coverage.window = ranges[0]
	coveredTo := ranges[0].max
	for _, next := range ranges[1:] {
		if coveredTo == math.MaxUint64 {
			break
		}
		if next.min > coveredTo+1 {
			coverage.addHole(txidRange{min: coveredTo + 1, max: next.min - 1})
		}
		coveredTo = max(coveredTo, next.max)
	}
	coverage.window.max = coveredTo
	return coverage
}

func (c *ltxCoverage) addHole(hole txidRange) {
	switch {
	case !c.floorKnown || hole.min >= c.floor:
		c.holes = append(c.holes, hole)
	case hole.max < c.floor:
		c.retained = append(c.retained, hole)
	default:
		c.retained = append(c.retained, txidRange{min: hole.min, max: c.floor - 1})
		c.holes = append(c.holes, txidRange{min: c.floor, max: hole.max})
	}
}

// intersectTXIDRanges returns the txids present in both sorted, disjoint range
// lists.
func intersectTXIDRanges(a, b []txidRange) []txidRange {
	var out []txidRange
	for i, j := 0, 0; i < len(a) && j < len(b); {
		lo, hi := max(a[i].min, b[j].min), min(a[i].max, b[j].max)
		if lo <= hi {
			out = append(out, txidRange{min: lo, max: hi})
		}
		if a[i].max < b[j].max {
			i++
		} else {
			j++
		}
	}
	return out
}

// ltxCoverageOutcome is one coverage check. A hole is reported only when a
// second listing taken after ltxCoverageRelistDelay shows it too: confirmed
// holds the txids missing from both listings, so a gap that an in-flight
// compaction or retention pass opened and closed between the two never fails
// the check.
type ltxCoverageOutcome struct {
	first     ltxCoverage
	second    ltxCoverage
	relisted  bool
	confirmed []txidRange
	err       string
}

func runLTXCoverageCheck(list func() replicaLTXListing, wait func()) ltxCoverageOutcome {
	first := list()
	if problem := ltxListingProblem(first); problem != "" {
		return ltxCoverageOutcome{err: "first listing: " + problem}
	}
	outcome := ltxCoverageOutcome{first: computeLTXCoverage(first.candidates)}
	if len(outcome.first.holes) == 0 {
		return outcome
	}
	wait()
	second := list()
	if problem := ltxListingProblem(second); problem != "" {
		outcome.err = "relisting: " + problem
		return outcome
	}
	outcome.relisted = true
	outcome.second = computeLTXCoverage(second.candidates)
	outcome.confirmed = intersectTXIDRanges(outcome.first.holes, outcome.second.holes)
	return outcome
}

// floorUnknown reports confirmed holes that cannot be judged because a
// listing had no L9 file to bound what retention may have trimmed.
func (o ltxCoverageOutcome) floorUnknown() bool {
	return len(o.confirmed) > 0 && (!o.first.floorKnown || !o.second.floorKnown)
}

func ltxListingProblem(listing replicaLTXListing) string {
	switch {
	case listing.err != "":
		return listing.err
	case !listing.complete:
		return "listing incomplete"
	case len(listing.candidates) == 0:
		return "no ltx entries parsed"
	default:
		return ""
	}
}

func (o ltxCoverageOutcome) holeError() string {
	if len(o.confirmed) == 0 {
		return ""
	}
	first := o.confirmed[0]
	return fmt.Sprintf("ltx coverage hole: L0-L8 union missing txids %s-%s (%d holes)", formatTXID(first.min), formatTXID(first.max), len(o.confirmed))
}

func (o ltxCoverageOutcome) summary() string {
	if o.first.files == 0 {
		return "L0-L8 coverage unavailable"
	}
	parts := []string{fmt.Sprintf("L0-L8 files=%d window=%s %s holes=%d below_floor=%d", o.first.files, o.first.window, o.first.floorText(), len(o.first.holes), len(o.first.retained))}
	if o.relisted {
		parts = append(parts, fmt.Sprintf("relist files=%d window=%s %s holes=%d below_floor=%d confirmed=%d", o.second.files, o.second.window, o.second.floorText(), len(o.second.holes), len(o.second.retained), len(o.confirmed)))
	}
	if sample := sampleTXIDRanges(o.confirmed); sample != "" {
		parts = append(parts, "confirmed holes: "+sample)
	} else if sample := sampleTXIDRanges(o.first.holes); sample != "" {
		parts = append(parts, "unconfirmed holes: "+sample)
	}
	if sample := sampleTXIDRanges(o.first.retained); sample != "" {
		parts = append(parts, "below retention floor, not judged: "+sample)
	}
	return strings.Join(parts, "; ")
}

func (c ltxCoverage) floorText() string {
	if !c.floorKnown {
		return "retention_floor=unknown"
	}
	return "retention_floor=" + formatTXID(c.floor)
}

func sampleTXIDRanges(ranges []txidRange) string {
	if len(ranges) == 0 {
		return ""
	}
	names := make([]string, 0, ltxCoverageHoleSample)
	for _, r := range ranges[:min(len(ranges), ltxCoverageHoleSample)] {
		names = append(names, r.String())
	}
	if len(ranges) > ltxCoverageHoleSample {
		names = append(names, fmt.Sprintf("+%d more", len(ranges)-ltxCoverageHoleSample))
	}
	return strings.Join(names, ", ")
}

func ltxCoverageResult(outcome ltxCoverageOutcome, startedAt, completedAt time.Time) VerificationResult {
	step := reporting.VerificationStep{Name: ltxCoverageStep, StartedAt: startedAt, CompletedAt: completedAt, Status: "ok", OutputTail: outcome.summary()}
	result := VerificationResult{
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		CheckType:   ltxCoverageCheckType,
		RestoreMode: ltxCoverageRestoreMode,
		DurationMS:  int(completedAt.Sub(startedAt).Milliseconds()),
		Steps:       []reporting.VerificationStep{step},
	}
	switch {
	case outcome.floorUnknown():
		message := "ltx coverage check skipped: no L9 snapshot bounds retention: " + outcome.holeError()
		result.Steps[0].Status = "inconclusive"
		result.Steps[0].Error = message
		result.Status = "aborted"
		result.ErrorMessage = message
		result.Summary = "verification inconclusive: " + message
		result.InconclusiveClassification = &reporting.FailureClassification{Stage: ltxCoverageStep, Signature: reporting.SignatureLTXCoverageFloorUnknown}
	case outcome.err != "":
		message := "ltx coverage check skipped: replica level report incomplete: " + outcome.err
		result.Steps[0].Status = "inconclusive"
		result.Steps[0].Error = message
		result.Status = "aborted"
		result.ErrorMessage = message
		result.Summary = "verification inconclusive: " + message
		result.InconclusiveClassification = &reporting.FailureClassification{Stage: ltxCoverageStep, Signature: reporting.SignatureReplicaLevelReportIncomplete}
	case len(outcome.confirmed) > 0:
		message := outcome.holeError()
		result.Steps[0].Status = "error"
		result.Steps[0].Error = message
		result.Status = "failed"
		result.ErrorMessage = message
		result.Summary = "ltx coverage check failed"
	default:
		result.Status = "passed"
		result.Passed = true
		result.Summary = "ltx coverage check passed"
	}
	return result
}

func (r *Runner) ltxCoverageCheckEnabled() bool {
	return r.cfg.LTXCoverageCheck && !r.cfg.ManyDBEnabled()
}

func (r *Runner) checkLTXCoverage() VerificationResult {
	command := []string{r.cfg.litestreamCommand(), "ltx", "-config", r.cfg.ConfigPath, "-level", "all", r.cfg.DBPath}
	startedAt := time.Now().UTC()
	outcome := runLTXCoverageCheck(func() replicaLTXListing { return listReplicaLTX(command) }, func() { time.Sleep(ltxCoverageRelistDelay) })
	return ltxCoverageResult(outcome, startedAt, time.Now().UTC())
}
