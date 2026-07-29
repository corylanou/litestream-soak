package reporting

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRuntimePayloadNormalizePreservesHealthySnapshot(t *testing.T) {
	observedAt := time.Date(2026, 4, 11, 22, 0, 0, 0, time.UTC)
	payload := RuntimePayload{
		DBTXID:                    42,
		DBStatus:                  "replicating",
		LastSyncAgeSeconds:        1,
		LitestreamUptimeSeconds:   99,
		SnapshotCollectedAt:       observedAt,
		LitestreamSnapshotHealthy: true,
	}

	normalized := payload.Normalize(observedAt.Add(time.Minute))
	if !normalized.LitestreamSnapshotHealthy {
		t.Fatal("expected healthy snapshot to stay healthy")
	}
	if normalized.LitestreamSnapshotError != "" {
		t.Fatalf("expected no snapshot error, got %q", normalized.LitestreamSnapshotError)
	}
	if !normalized.SnapshotCollectedAt.Equal(observedAt) {
		t.Fatalf("snapshot_collected_at=%s want %s", normalized.SnapshotCollectedAt, observedAt)
	}
}

func TestRuntimePayloadNormalizeMarksLegacyTelemetry(t *testing.T) {
	observedAt := time.Date(2026, 4, 11, 22, 5, 0, 0, time.UTC)
	payload := RuntimePayload{
		DBTXID:                  42,
		DBStatus:                "replicating",
		LastSyncAgeSeconds:      0,
		LitestreamUptimeSeconds: 99,
	}

	normalized := payload.Normalize(observedAt)
	if normalized.LitestreamSnapshotHealthy {
		t.Fatal("expected legacy snapshot to remain unhealthy")
	}
	if normalized.LitestreamSnapshotError != LegacyRuntimeTelemetryError {
		t.Fatalf("snapshot_error=%q want %q", normalized.LitestreamSnapshotError, LegacyRuntimeTelemetryError)
	}
	if !normalized.SnapshotCollectedAt.Equal(observedAt) {
		t.Fatalf("snapshot_collected_at=%s want %s", normalized.SnapshotCollectedAt, observedAt)
	}
}

func TestRuntimePayloadJSONRoundTripTelemetryExtensions(t *testing.T) {
	payload := RuntimePayload{
		S3ListRequestsTotal:            9210,
		LitestreamHeapInuseBytes:       48 * 1024 * 1024,
		LitestreamStackInuseBytes:      2 * 1024 * 1024,
		LitestreamAllocBytesTotal:      123456789.5,
		LitestreamAllocRateBytesPerSec: 4321.25,
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var decoded RuntimePayload
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if decoded.S3ListRequestsTotal != payload.S3ListRequestsTotal {
		t.Fatalf("s3_list_requests_total=%d want %d", decoded.S3ListRequestsTotal, payload.S3ListRequestsTotal)
	}
	if decoded.LitestreamHeapInuseBytes != payload.LitestreamHeapInuseBytes {
		t.Fatalf("litestream_heap_inuse_bytes=%d want %d", decoded.LitestreamHeapInuseBytes, payload.LitestreamHeapInuseBytes)
	}
	if decoded.LitestreamStackInuseBytes != payload.LitestreamStackInuseBytes {
		t.Fatalf("litestream_stack_inuse_bytes=%d want %d", decoded.LitestreamStackInuseBytes, payload.LitestreamStackInuseBytes)
	}
	if decoded.LitestreamAllocBytesTotal != payload.LitestreamAllocBytesTotal {
		t.Fatalf("litestream_alloc_bytes_total=%f want %f", decoded.LitestreamAllocBytesTotal, payload.LitestreamAllocBytesTotal)
	}
	if decoded.LitestreamAllocRateBytesPerSec != payload.LitestreamAllocRateBytesPerSec {
		t.Fatalf("litestream_alloc_rate_bytes_per_sec=%f want %f", decoded.LitestreamAllocRateBytesPerSec, payload.LitestreamAllocRateBytesPerSec)
	}
}

func TestRuntimePayloadDecodesLegacyJSONWithoutTelemetryExtensions(t *testing.T) {
	legacy := `{"db_txid":42,"db_status":"replicating","litestream_snapshot_healthy":true}`

	var decoded RuntimePayload
	if err := json.Unmarshal([]byte(legacy), &decoded); err != nil {
		t.Fatalf("unmarshal legacy payload: %v", err)
	}

	if decoded.S3ListRequestsTotal != 0 {
		t.Fatalf("s3_list_requests_total=%d want 0", decoded.S3ListRequestsTotal)
	}
	if decoded.LitestreamHeapInuseBytes != 0 {
		t.Fatalf("litestream_heap_inuse_bytes=%d want 0", decoded.LitestreamHeapInuseBytes)
	}
	if decoded.LitestreamStackInuseBytes != 0 {
		t.Fatalf("litestream_stack_inuse_bytes=%d want 0", decoded.LitestreamStackInuseBytes)
	}
	if decoded.LitestreamAllocBytesTotal != 0 {
		t.Fatalf("litestream_alloc_bytes_total=%f want 0", decoded.LitestreamAllocBytesTotal)
	}
	if decoded.LitestreamAllocRateBytesPerSec != 0 {
		t.Fatalf("litestream_alloc_rate_bytes_per_sec=%f want 0", decoded.LitestreamAllocRateBytesPerSec)
	}
	if decoded.DBTXID != 42 {
		t.Fatalf("db_txid=%d want 42", decoded.DBTXID)
	}
}

func TestSnapshotStatus(t *testing.T) {
	tests := []struct {
		name    string
		payload *RuntimePayload
		want    string
	}{
		{
			name: "missing",
			want: RuntimeSnapshotStatusMissing,
		},
		{
			name: "healthy",
			payload: &RuntimePayload{
				LitestreamSnapshotHealthy: true,
			},
			want: RuntimeSnapshotStatusHealthy,
		},
		{
			name: "legacy",
			payload: &RuntimePayload{
				LitestreamSnapshotError: LegacyRuntimeTelemetryError,
			},
			want: RuntimeSnapshotStatusLegacy,
		},
		{
			name: "unhealthy",
			payload: &RuntimePayload{
				LitestreamSnapshotError: "dial unix /data/litestream.sock: connect: connection refused",
			},
			want: RuntimeSnapshotStatusUnhealthy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SnapshotStatus(tt.payload); got != tt.want {
				t.Fatalf("SnapshotStatus()=%q want %q", got, tt.want)
			}
		})
	}
}

func TestLitestreamMetricsStatusDistinguishesScrapeAndMetricStates(t *testing.T) {
	tests := []struct {
		name    string
		payload *RuntimePayload
		want    string
	}{
		{name: "unknown", want: LitestreamMetricsStatusUnknown},
		{
			name: "successful scrape with genuine zero",
			payload: &RuntimePayload{
				LitestreamMetricsScrapeStatus:    LitestreamMetricsScrapeStatusHealthy,
				LitestreamDiskFullMetricPresent:  true,
				LitestreamDiskFull:               false,
				LitestreamMemStatsMetricsPresent: true,
			},
			want: LitestreamMetricsStatusHealthy,
		},
		{
			name: "successful scrape missing disk full",
			payload: &RuntimePayload{
				LitestreamMetricsScrapeStatus:    LitestreamMetricsScrapeStatusHealthy,
				LitestreamMemStatsMetricsPresent: true,
			},
			want: LitestreamMetricsStatusMetricMissing,
		},
		{
			name: "successful scrape missing memstats",
			payload: &RuntimePayload{
				LitestreamMetricsScrapeStatus:   LitestreamMetricsScrapeStatusHealthy,
				LitestreamDiskFullMetricPresent: true,
			},
			want: LitestreamMetricsStatusMetricMissing,
		},
		{
			name: "failed scrape has no metric verdict",
			payload: &RuntimePayload{
				LitestreamMetricsScrapeStatus:   LitestreamMetricsScrapeStatusFailed,
				LitestreamDiskFullMetricPresent: true,
				LitestreamDiskFull:              true,
			},
			want: LitestreamMetricsStatusScrapeFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LitestreamMetricsStatus(tt.payload); got != tt.want {
				t.Fatalf("LitestreamMetricsStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRuntimePayloadJSONPreservesFalseMetricsVerdict(t *testing.T) {
	payload := RuntimePayload{
		LitestreamMetricsScrapeStatus:    LitestreamMetricsScrapeStatusHealthy,
		LitestreamDiskFullMetricPresent:  true,
		LitestreamDiskFull:               false,
		LitestreamMemStatsMetricsPresent: true,
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	body := string(encoded)
	for _, want := range []string{
		`"litestream_metrics_scrape_status":"healthy"`,
		`"litestream_disk_full_metric_present":true`,
		`"litestream_disk_full":false`,
		`"litestream_memstats_metrics_present":true`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("encoded payload missing %s: %s", want, body)
		}
	}
}
