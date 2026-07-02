package worker

import (
	"io"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const (
	litestreamHeapInuseMetricName  = "go_memstats_heap_inuse_bytes"
	litestreamStackInuseMetricName = "go_memstats_stack_inuse_bytes"
	litestreamAllocTotalMetricName = "go_memstats_alloc_bytes_total"
)

type litestreamMetricsSnapshot struct {
	DiskFull        bool
	DiskFullPresent bool
	HeapInuseBytes  float64
	StackInuseBytes float64
	AllocBytesTotal float64
	MemStatsPresent bool
}

func parseLitestreamMetrics(r io.Reader, dbPath string) (litestreamMetricsSnapshot, error) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return litestreamMetricsSnapshot{}, err
	}

	snapshot := litestreamMetricsSnapshot{}
	snapshot.DiskFull, snapshot.DiskFullPresent = extractDiskFullMetric(families[litestreamDiskFullMetricName], dbPath)

	heap, heapPresent := extractSingleMetricValue(families[litestreamHeapInuseMetricName])
	stack, stackPresent := extractSingleMetricValue(families[litestreamStackInuseMetricName])
	alloc, allocPresent := extractSingleMetricValue(families[litestreamAllocTotalMetricName])
	if heapPresent && stackPresent && allocPresent {
		snapshot.HeapInuseBytes = heap
		snapshot.StackInuseBytes = stack
		snapshot.AllocBytesTotal = alloc
		snapshot.MemStatsPresent = true
	}
	return snapshot, nil
}

func extractDiskFullMetric(family *dto.MetricFamily, dbPath string) (bool, bool) {
	if family == nil {
		return false, false
	}

	present := false
	for _, metric := range family.GetMetric() {
		if !metricMatchesDBPath(metric, dbPath) {
			continue
		}
		gauge := metric.GetGauge()
		if gauge == nil {
			continue
		}
		present = true
		if gauge.GetValue() > 0 {
			return true, true
		}
	}
	return false, present
}

func extractSingleMetricValue(family *dto.MetricFamily) (float64, bool) {
	if family == nil {
		return 0, false
	}
	for _, metric := range family.GetMetric() {
		if gauge := metric.GetGauge(); gauge != nil {
			return gauge.GetValue(), true
		}
		if counter := metric.GetCounter(); counter != nil {
			return counter.GetValue(), true
		}
		if untyped := metric.GetUntyped(); untyped != nil {
			return untyped.GetValue(), true
		}
	}
	return 0, false
}
