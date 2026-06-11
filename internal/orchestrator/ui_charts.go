package orchestrator

import (
	"sort"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

type chartSeries struct {
	Labels   []string   `json:"labels"`
	PassRate []*float64 `json:"pass_rate"`
	P50      []*int     `json:"p50"`
	P95      []*int     `json:"p95"`
	Failures []int      `json:"failures"`
}

func buildChartSeries(stats []model.VerificationStat, from time.Time, hours int) chartSeries {
	series := chartSeries{
		Labels:   make([]string, hours),
		PassRate: make([]*float64, hours),
		P50:      make([]*int, hours),
		P95:      make([]*int, hours),
		Failures: make([]int, hours),
	}

	type bucket struct {
		passed    int
		failed    int
		durations []int
	}
	buckets := make([]bucket, hours)

	for i := range hours {
		series.Labels[i] = from.Add(time.Duration(i) * time.Hour).Format(time.RFC3339)
	}

	for _, stat := range stats {
		index := int(stat.StartedAt.Sub(from) / time.Hour)
		if index < 0 || index >= hours {
			continue
		}
		verification := model.Verification{Status: stat.Status, Passed: stat.Passed}
		if verification.Aborted() {
			continue
		}
		if verification.Failed() {
			buckets[index].failed++
		} else {
			buckets[index].passed++
		}
		buckets[index].durations = append(buckets[index].durations, stat.DurationMS)
	}

	for i := range buckets {
		total := buckets[i].passed + buckets[i].failed
		series.Failures[i] = buckets[i].failed
		if total == 0 {
			continue
		}
		rate := 100.0 * float64(buckets[i].passed) / float64(total)
		series.PassRate[i] = &rate
		p50 := percentile(buckets[i].durations, 0.50)
		p95 := percentile(buckets[i].durations, 0.95)
		series.P50[i] = &p50
		series.P95[i] = &p95
	}

	return series
}

func percentile(values []int, fraction float64) int {
	sorted := make([]int, len(values))
	copy(sorted, values)
	sort.Ints(sorted)
	index := max(int(float64(len(sorted))*fraction+0.999999)-1, 0)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func passRateSummary(stats []model.VerificationStat) (float64, int) {
	passed := 0
	total := 0
	for _, stat := range stats {
		verification := model.Verification{Status: stat.Status, Passed: stat.Passed}
		if verification.Aborted() {
			continue
		}
		total++
		if !verification.Failed() {
			passed++
		}
	}
	if total == 0 {
		return 0, 0
	}
	return 100.0 * float64(passed) / float64(total), total
}
