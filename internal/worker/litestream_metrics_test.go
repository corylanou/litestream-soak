package worker

import (
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
