package reporting

import (
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
