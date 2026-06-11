package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestBuildAttentionItemsFlagsStaleHeartbeats(t *testing.T) {
	t.Parallel()

	staleAt := time.Now().UTC().Add(-30 * time.Minute)
	freshAt := time.Now().UTC().Add(-10 * time.Second)
	workers := []homeWorker{
		{Worker: model.Worker{ID: "w-stale", Name: "w-stale", Status: model.WorkerRunning, LastHeartbeatAt: &staleAt}},
		{Worker: model.Worker{ID: "w-fresh", Name: "w-fresh", Status: model.WorkerRunning, LastHeartbeatAt: &freshAt}},
	}

	items := buildAttentionItems("main", diagnosisSnapshot{}, homeSummary{TotalWorkers: 2, HealthyWorkers: 2}, workers, nil, nil, "", "")

	var staleItem *attentionItem
	for i := range items {
		if strings.Contains(items[i].Title, "stale heartbeat") {
			staleItem = &items[i]
		}
	}
	if staleItem == nil {
		t.Fatalf("no stale-heartbeat attention item in %+v", items)
	}
	if staleItem.Severity != "warn" {
		t.Fatalf("stale heartbeat severity = %q, want warn", staleItem.Severity)
	}
	if !strings.Contains(staleItem.Detail, "w-stale") {
		t.Fatalf("stale heartbeat detail %q should name w-stale", staleItem.Detail)
	}
	if strings.Contains(staleItem.Detail, "w-fresh") {
		t.Fatalf("stale heartbeat detail %q must not name w-fresh", staleItem.Detail)
	}
}
