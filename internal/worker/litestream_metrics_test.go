package worker

import (
	"reflect"
	"strings"
	"testing"
)

const litestreamMetricsExpositionWithMemStats = `
# HELP litestream_disk_full Whether replication is paused because the local disk is full
# TYPE litestream_disk_full gauge
litestream_disk_full{db="/data/test.db"} 1
# HELP go_memstats_heap_inuse_bytes Number of heap bytes that are in use.
# TYPE go_memstats_heap_inuse_bytes gauge
go_memstats_heap_inuse_bytes 5.0331648e+07
# HELP go_memstats_stack_inuse_bytes Number of bytes in use by the stack allocator.
# TYPE go_memstats_stack_inuse_bytes gauge
go_memstats_stack_inuse_bytes 2.097152e+06
# HELP go_memstats_alloc_bytes_total Total number of bytes allocated, even if freed.
# TYPE go_memstats_alloc_bytes_total counter
go_memstats_alloc_bytes_total 1.23456789e+08
`

const litestreamMetricsExpositionWithoutMemStats = `
# HELP litestream_disk_full Whether replication is paused because the local disk is full
# TYPE litestream_disk_full gauge
litestream_disk_full{db="/data/test.db"} 0
# HELP litestream_sync_count Total sync count
# TYPE litestream_sync_count counter
litestream_sync_count{db="/data/test.db"} 4
`

func TestParseLitestreamMetricsExtractsDiskFullAndMemStats(t *testing.T) {
	snapshot, err := parseLitestreamMetrics(strings.NewReader(litestreamMetricsExpositionWithMemStats), "/data/test.db")
	if err != nil {
		t.Fatalf("parseLitestreamMetrics() error = %v", err)
	}

	if !snapshot.DiskFullPresent {
		t.Fatal("DiskFullPresent = false, want true")
	}
	if !snapshot.DiskFull {
		t.Fatal("DiskFull = false, want true")
	}
	if !snapshot.MemStatsPresent {
		t.Fatal("MemStatsPresent = false, want true")
	}
	if snapshot.HeapInuseBytes != 50331648 {
		t.Fatalf("HeapInuseBytes = %f, want 50331648", snapshot.HeapInuseBytes)
	}
	if snapshot.StackInuseBytes != 2097152 {
		t.Fatalf("StackInuseBytes = %f, want 2097152", snapshot.StackInuseBytes)
	}
	if snapshot.AllocBytesTotal != 123456789 {
		t.Fatalf("AllocBytesTotal = %f, want 123456789", snapshot.AllocBytesTotal)
	}
}

func TestParseLitestreamMetricsWithoutMemStatsFamilies(t *testing.T) {
	snapshot, err := parseLitestreamMetrics(strings.NewReader(litestreamMetricsExpositionWithoutMemStats), "/data/test.db")
	if err != nil {
		t.Fatalf("parseLitestreamMetrics() error = %v", err)
	}

	if !snapshot.DiskFullPresent {
		t.Fatal("DiskFullPresent = false, want true")
	}
	if snapshot.DiskFull {
		t.Fatal("DiskFull = true, want false")
	}
	if snapshot.MemStatsPresent {
		t.Fatal("MemStatsPresent = true, want false")
	}
	if snapshot.HeapInuseBytes != 0 || snapshot.StackInuseBytes != 0 || snapshot.AllocBytesTotal != 0 {
		t.Fatalf("memstats values = %f/%f/%f, want all zero", snapshot.HeapInuseBytes, snapshot.StackInuseBytes, snapshot.AllocBytesTotal)
	}
}

const litestreamMetricsExpositionWithReplicaOperations = `
# HELP litestream_disk_full Whether replication is paused because the local disk is full
# TYPE litestream_disk_full gauge
litestream_disk_full{db="/data/test.db"} 0
# HELP litestream_replica_operation_bytes The number of bytes used by replica operations
# TYPE litestream_replica_operation_bytes counter
litestream_replica_operation_bytes{operation="GET",replica_type="s3"} 4.194304e+06
litestream_replica_operation_bytes{operation="PUT",replica_type="s3"} 1.2582912e+07
litestream_replica_operation_bytes{operation="PUT",replica_type="file"} 1024
# HELP litestream_replica_operation_duration_seconds Duration of replica operations by type and operation
# TYPE litestream_replica_operation_duration_seconds histogram
litestream_replica_operation_duration_seconds_bucket{operation="PUT",replica_type="s3",le="0.01"} 3
litestream_replica_operation_duration_seconds_bucket{operation="PUT",replica_type="s3",le="+Inf"} 1532
litestream_replica_operation_duration_seconds_sum{operation="PUT",replica_type="s3"} 91.2
litestream_replica_operation_duration_seconds_count{operation="PUT",replica_type="s3"} 1532
# HELP litestream_replica_operation_errors_total Number of replica operation errors by type, operation, and error code
# TYPE litestream_replica_operation_errors_total counter
litestream_replica_operation_errors_total{code="SlowDown",operation="PUT",replica_type="s3"} 2
# HELP litestream_replica_operation_total The number of replica operations performed
# TYPE litestream_replica_operation_total counter
litestream_replica_operation_total{operation="DELETE",replica_type="s3"} 210
litestream_replica_operation_total{operation="GET",replica_type="s3"} 18
litestream_replica_operation_total{operation="LIST",replica_type="s3"} 977
litestream_replica_operation_total{operation="PUT",replica_type="s3"} 1532
litestream_replica_operation_total{operation="PUT",replica_type="file"} 4
# HELP process_start_time_seconds Start time of the process since unix epoch in seconds.
# TYPE process_start_time_seconds gauge
process_start_time_seconds 1.76118002456e+09
`

func TestParseLitestreamMetricsExtractsLogicalObjectOperations(t *testing.T) {
	snapshot, err := parseLitestreamMetrics(strings.NewReader(litestreamMetricsExpositionWithReplicaOperations), "/data/test.db")
	if err != nil {
		t.Fatalf("parseLitestreamMetrics() error = %v", err)
	}
	wantTotals := map[string]uint64{"DELETE": 210, "GET": 18, "LIST": 977, "PUT": 1536}
	if !reflect.DeepEqual(snapshot.LitestreamObjectOperationTotals, wantTotals) {
		t.Fatalf("ReplicaOperationTotals = %v, want %v", snapshot.LitestreamObjectOperationTotals, wantTotals)
	}
	wantBytes := map[string]uint64{"GET": 4194304, "PUT": 12583936}
	if !reflect.DeepEqual(snapshot.LitestreamObjectOperationBytes, wantBytes) {
		t.Fatalf("ReplicaOperationBytes = %v, want %v", snapshot.LitestreamObjectOperationBytes, wantBytes)
	}
	if snapshot.LitestreamCounterEpoch != "start:1761180024.56" {
		t.Fatalf("LitestreamCounterEpoch = %q", snapshot.LitestreamCounterEpoch)
	}
}

func TestParseLitestreamMetricsWithoutReplicaOperations(t *testing.T) {
	snapshot, err := parseLitestreamMetrics(strings.NewReader(litestreamMetricsExpositionWithoutMemStats), "/data/test.db")
	if err != nil {
		t.Fatalf("parseLitestreamMetrics() error = %v", err)
	}
	if snapshot.LitestreamObjectOperationTotals != nil || snapshot.LitestreamObjectOperationBytes != nil || snapshot.LitestreamCounterEpoch != "" {
		t.Fatalf("replica operations = %v / %v, want nil", snapshot.LitestreamObjectOperationTotals, snapshot.LitestreamObjectOperationBytes)
	}
}
