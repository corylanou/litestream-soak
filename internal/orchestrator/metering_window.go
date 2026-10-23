package orchestrator

import (
	"os"
	"slices"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
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
		if !slices.Contains(sources, workers[i].Source) || workers[i].Workload.S3FaultProxyEnabled {
			continue
		}
		workers[i].Workload.S3FaultProxyEnabled = true
		workers[i].Workload.S3FaultProxyMode = "observe"
	}
	return workers
}

func meteringWindowsContain(windows []model.MeteringWindow, at time.Time) bool {
	for _, window := range windows {
		if window.Contains(at) {
			return true
		}
	}
	return false
}

func excludeMeteringWindowVerifications(verifications []model.Verification, windows []model.MeteringWindow) ([]model.Verification, int) {
	if len(windows) == 0 {
		return verifications, 0
	}
	kept := make([]model.Verification, 0, len(verifications))
	for _, verification := range verifications {
		if at, ok := verificationObservedAt(verification); ok && meteringWindowsContain(windows, at) {
			continue
		}
		kept = append(kept, verification)
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
