package litestreamsoak

import (
	"os"
	"strings"
	"testing"
)

func TestOverviewDashboardIncludesDiskFullNoProgressSignal(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile("grafana/soak-overview-dashboard.json")
	if err != nil {
		t.Fatalf("read overview dashboard: %v", err)
	}
	dashboard := string(body)

	for _, want := range []string{
		"Disk-Full No-Progress Guard",
		"platform_disk_full_no_progress",
		"soak_control_worker_latest_platform_event_info",
	} {
		if !strings.Contains(dashboard, want) {
			t.Fatalf("overview dashboard missing %q", want)
		}
	}
}
