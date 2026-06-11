package orchestrator

import (
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func chartStat(startedAt time.Time, passed bool, status string, durationMS int) model.VerificationStat {
	return model.VerificationStat{
		StartedAt:  startedAt,
		Status:     status,
		Passed:     passed,
		DurationMS: durationMS,
	}
}

func TestBuildChartSeriesBucketsHourly(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 6, 10, 6, 0, 0, 0, time.UTC)
	stats := []model.VerificationStat{
		chartStat(from.Add(-10*time.Minute), false, "failed", 9999),
		chartStat(from.Add(10*time.Minute), true, "completed", 100),
		chartStat(from.Add(20*time.Minute), true, "completed", 300),
		chartStat(from.Add(30*time.Minute), false, "failed", 900),
		chartStat(from.Add(40*time.Minute), false, "aborted", 50),
		chartStat(from.Add(2*time.Hour+5*time.Minute), true, "completed", 200),
	}

	series := buildChartSeries(stats, from, 3)

	if len(series.Labels) != 3 {
		t.Fatalf("len(Labels) = %d, want 3", len(series.Labels))
	}
	if series.Labels[0] != from.Format(time.RFC3339) {
		t.Fatalf("Labels[0] = %q, want %q", series.Labels[0], from.Format(time.RFC3339))
	}

	if series.PassRate[0] == nil {
		t.Fatal("PassRate[0] = nil, want value (aborted excluded from denominator)")
	}
	if got, want := *series.PassRate[0], 100.0*2.0/3.0; !floatNear(got, want) {
		t.Fatalf("PassRate[0] = %v, want %v", got, want)
	}
	if series.PassRate[1] != nil {
		t.Fatalf("PassRate[1] = %v, want nil (no checks in empty hour)", *series.PassRate[1])
	}
	if series.PassRate[2] == nil || *series.PassRate[2] != 100.0 {
		t.Fatalf("PassRate[2] = %v, want 100", series.PassRate[2])
	}

	if series.Failures[0] != 1 || series.Failures[1] != 0 || series.Failures[2] != 0 {
		t.Fatalf("Failures = %v, want [1 0 0] (stat before from must not land in bucket 0)", series.Failures)
	}

	if series.P50[0] == nil || *series.P50[0] != 300 {
		t.Fatalf("P50[0] = %v, want 300", series.P50[0])
	}
	if series.P95[0] == nil || *series.P95[0] != 900 {
		t.Fatalf("P95[0] = %v, want 900", series.P95[0])
	}
	if series.P50[1] != nil {
		t.Fatal("P50[1] should be nil for empty bucket")
	}
}

func TestPassRateSummaryExcludesAborted(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 10, 6, 0, 0, 0, time.UTC)
	stats := []model.VerificationStat{
		chartStat(now, true, "completed", 100),
		chartStat(now, true, "completed", 100),
		chartStat(now, false, "failed", 100),
		chartStat(now, false, "aborted", 100),
	}

	rate, total := passRateSummary(stats)
	if total != 3 {
		t.Fatalf("total = %d, want 3", total)
	}
	if want := 100.0 * 2.0 / 3.0; !floatNear(rate, want) {
		t.Fatalf("rate = %v, want %v", rate, want)
	}

	emptyRate, emptyTotal := passRateSummary(nil)
	if emptyTotal != 0 || emptyRate != 0 {
		t.Fatalf("passRateSummary(nil) = (%v, %d), want (0, 0)", emptyRate, emptyTotal)
	}
}

func TestPercentileEmptyInputReturnsZero(t *testing.T) {
	t.Parallel()

	if got := percentile(nil, 0.95); got != 0 {
		t.Fatalf("percentile(nil) = %d, want 0", got)
	}
	if got := percentile([]int{}, 0.5); got != 0 {
		t.Fatalf("percentile(empty) = %d, want 0", got)
	}
}

func floatNear(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 0.0001
}
