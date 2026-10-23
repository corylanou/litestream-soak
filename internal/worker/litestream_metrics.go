package worker

import (
	"io"
	"math"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const (
	litestreamHeapInuseMetricName    = "go_memstats_heap_inuse_bytes"
	litestreamStackInuseMetricName   = "go_memstats_stack_inuse_bytes"
	litestreamAllocTotalMetricName   = "go_memstats_alloc_bytes_total"
	litestreamReplicaOpsMetricName   = "litestream_replica_operation_total"
	litestreamReplicaBytesMetricName = "litestream_replica_operation_bytes"
)

type litestreamMetricsSnapshot struct {
	DiskFull        bool
	DiskFullPresent bool
	HeapInuseBytes  float64
	StackInuseBytes float64
	AllocBytesTotal float64
	MemStatsPresent bool

	ReplicaOperationTotals map[string]uint64
	ReplicaOperationBytes  map[string]uint64
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
	snapshot.ReplicaOperationTotals = sumCountersByLabel(families[litestreamReplicaOpsMetricName], "operation")
	snapshot.ReplicaOperationBytes = sumCountersByLabel(families[litestreamReplicaBytesMetricName], "operation")
	return snapshot, nil
}

func sumCountersByLabel(family *dto.MetricFamily, label string) map[string]uint64 {
	if family == nil {
		return nil
	}
	var totals map[string]uint64
	for _, metric := range family.GetMetric() {
		var value float64
		switch {
		case metric.GetCounter() != nil:
			value = metric.GetCounter().GetValue()
		case metric.GetUntyped() != nil:
			value = metric.GetUntyped().GetValue()
		default:
			continue
		}
		key := ""
		for _, pair := range metric.GetLabel() {
			if pair.GetName() == label {
				key = pair.GetValue()
			}
		}
		if key == "" || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		if totals == nil {
			totals = make(map[string]uint64)
		}
		totals[key] += uint64(value)
	}
	return totals
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
