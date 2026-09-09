package litestreamsoak

import (
	"os"
	"strings"
	"testing"
)

func TestOverviewDashboardIncludesDiskFullSignalRecoveryGuard(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile("grafana/soak-overview-dashboard.json")
	if err != nil {
		t.Fatalf("read overview dashboard: %v", err)
	}
	dashboard := string(body)

	for _, want := range []string{
		"Disk-Full Signal/Recovery Guard",
		"platform_disk_full_no_progress",
		"platform_disk_full_recovered",
		"soak_control_worker_latest_platform_event_info",
	} {
		if !strings.Contains(dashboard, want) {
			t.Fatalf("overview dashboard missing %q", want)
		}
	}
}

func TestResourceDashboardsExposeObservationValidity(t *testing.T) {
	for _, path := range []string{"grafana/soak-drilldown-dashboard.json", "grafana/soak-source-compare-dashboard.json"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "Resource Observation Status") || !strings.Contains(string(body), "Resource Observation Age") {
			t.Fatalf("%s lacks resource validity panels", path)
		}
	}
}
