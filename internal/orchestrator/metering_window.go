package orchestrator

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func meteringWindowSources() []string {
	var sources []string
	for _, part := range strings.Split(os.Getenv("SOAK_METERING_WINDOW_SOURCES"), ",") {
		source := strings.TrimSpace(part)
		if source == "" || slices.Contains(sources, source) {
			continue
		}
		sources = append(sources, source)
	}
	return sources
}

func SyncMeteringWindows(db *model.DB, now time.Time) error {
	return db.SyncMeteringWindows(meteringWindowSources(), now)
}

func withMeteringWindowObserveProxy(workers []DesiredWorker) []DesiredWorker {
	sources := meteringWindowSources()
	if len(sources) == 0 {
		return workers
	}
	for i := range workers {
		if !slices.Contains(sources, workers[i].Source) {
			continue
		}
		workers[i].Workload.MeteringWindow = true
		if workers[i].Workload.S3FaultProxyEnabled {
			continue
		}
		workers[i].Workload.S3FaultProxyEnabled = true
		workers[i].Workload.S3FaultProxyMode = "observe"
	}
	return workers
}

func runInMeteringWindow(run reporting.WorkerIdentity) bool {
	if !strings.Contains(run.ProfileConfig, `"metering_window"`) {
		return false
	}
	var marker struct {
		MeteringWindow json.RawMessage `json:"metering_window"`
	}
	if err := json.Unmarshal([]byte(run.ProfileConfig), &marker); err != nil {
		return false
	}
	return string(marker.MeteringWindow) == "true"
}

func excludeMeteringRunVerifications(verifications []model.Verification) ([]model.Verification, int) {
	if !slices.ContainsFunc(verifications, func(verification model.Verification) bool { return runInMeteringWindow(verification.Run) }) {
		return verifications, 0
	}
	kept := make([]model.Verification, 0, len(verifications))
	for _, verification := range verifications {
		if !runInMeteringWindow(verification.Run) {
			kept = append(kept, verification)
		}
	}
	return kept, len(verifications) - len(kept)
}

func activeMeteringWindows(db *model.DB, sources ...string) ([]model.MeteringWindow, error) {
	windows, err := db.ListMeteringWindows("")
	if err != nil {
		return nil, err
	}
	var active []model.MeteringWindow
	for _, window := range windows {
		if window.Active && slices.Contains(sources, window.Source) && !slices.ContainsFunc(active, func(existing model.MeteringWindow) bool { return existing.Source == window.Source }) {
			active = append(active, window)
		}
	}
	return active, nil
}
