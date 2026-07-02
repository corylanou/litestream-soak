package orchestrator

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestFleetRowStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		worker homeWorker
		want   string
	}{
		{
			name:   "completed success",
			worker: homeWorker{CompletedSuccess: true, Worker: model.Worker{Status: model.WorkerStopped}},
			want:   "passed",
		},
		{
			name:   "failed worker",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerFailed}},
			want:   "failing",
		},
		{
			name:   "stopped worker",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerStopped}},
			want:   "stopped",
		},
		{
			name: "running worker with unhealthy runtime",
			worker: homeWorker{
				Worker:                model.Worker{Status: model.WorkerRunning},
				RuntimeSnapshotStatus: reporting.RuntimeSnapshotStatusUnhealthy,
			},
			want: "failing",
		},
		{
			name:   "probing worker",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerProbing}},
			want:   "probing",
		},
		{
			name:   "starting worker",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerStarting}},
			want:   "probing",
		},
		{
			name:   "building worker",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerBuilding}},
			want:   "probing",
		},
		{
			name:   "pending worker",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerPending}},
			want:   "probing",
		},
		{
			name:   "degraded worker",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerDegraded}},
			want:   "degraded",
		},
		{
			name:   "dormant worker",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerDormant}},
			want:   "degraded",
		},
		{
			name: "running worker with legacy runtime",
			worker: homeWorker{
				Worker:                model.Worker{Status: model.WorkerRunning},
				RuntimeSnapshotStatus: reporting.RuntimeSnapshotStatusLegacy,
			},
			want: "degraded",
		},
		{
			name: "healthy running worker",
			worker: homeWorker{
				Worker:                model.Worker{Status: model.WorkerRunning},
				RuntimeSnapshotStatus: reporting.RuntimeSnapshotStatusHealthy,
			},
			want: "healthy",
		},
		{
			name:   "running worker without runtime snapshot",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerRunning}},
			want:   "healthy",
		},
		{
			name:   "unrecognized status",
			worker: homeWorker{Worker: model.Worker{Status: model.WorkerStatus("mystery")}},
			want:   "unknown",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := fleetRowStatus(tc.worker); got != tc.want {
				t.Fatalf("fleetRowStatus() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHomeWorkerOutcomeRank(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		worker homeWorker
		want   int
	}{
		{
			name: "latest verification failed",
			worker: homeWorker{
				LatestVerification: &model.Verification{Status: "failed", Passed: false},
			},
			want: 0,
		},
		{
			name: "active verification running",
			worker: homeWorker{
				ActiveVerification: &reporting.ActiveVerification{StartedAt: time.Now().UTC()},
			},
			want: 1,
		},
		{
			name: "active verification stale",
			worker: homeWorker{
				ActiveVerification: &reporting.ActiveVerification{StartedAt: time.Now().UTC(), Stale: true},
			},
			want: 1,
		},
		{
			name:   "no verifications",
			worker: homeWorker{},
			want:   2,
		},
		{
			name: "latest verification aborted",
			worker: homeWorker{
				LatestVerification: &model.Verification{Status: "aborted", Passed: false},
			},
			want: 2,
		},
		{
			name: "latest verification passed",
			worker: homeWorker{
				LatestVerification: &model.Verification{Status: "passed", Passed: true},
			},
			want: 3,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := homeWorkerOutcomeRank(tc.worker); got != tc.want {
				t.Fatalf("homeWorkerOutcomeRank() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestFleetTableRendersSortAndFilterMarkup(t *testing.T) {
	t.Parallel()

	heartbeatAt := time.Now().UTC().Add(-30 * time.Second)
	data := homePageData{
		Workers: []homeWorker{
			{
				Worker: model.Worker{
					ID:              "worker-main-failing",
					Name:            "worker-main-failing",
					Status:          model.WorkerFailed,
					LastHeartbeatAt: &heartbeatAt,
					ProfileName:     "high-volume",
				},
				LatestVerification:      &model.Verification{Status: "failed", Passed: false, StartedAt: heartbeatAt},
				CurrentFailureSignature: "replica sync: unexpected EOF",
				CurrentFailureSeverity:  "bad",
			},
		},
	}

	var body bytes.Buffer
	if err := uiTemplates.ExecuteTemplate(&body, "home_body", data); err != nil {
		t.Fatalf("ExecuteTemplate(home_body) error = %v", err)
	}
	rendered := body.String()

	for _, want := range []string{
		`data-status="failing"`,
		`data-worker="worker-main-failing"`,
		`data-outcome="0"`,
		`data-signal="replica sync: unexpected EOF"`,
		`data-sort="worker"`,
		`data-sort="heartbeat"`,
		`data-sort="outcome"`,
		`data-sort="signal"`,
		`data-status-filter="all"`,
		`data-status-filter="failing"`,
		`data-status-filter="degraded"`,
		`data-status-filter="probing"`,
		`data-status-filter="healthy"`,
		`class="panel table-panel"`,
		`aria-sort="none"`,
		`worker-link`,
		`row-chevron`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered home body missing %q", want)
		}
	}

	if strings.Contains(rendered, "overflow-x:auto") {
		t.Fatalf("rendered home body still uses the inline overflow-x style")
	}
	if !strings.Contains(rendered, `data-heartbeat="`+strconv.FormatInt(heartbeatAt.Unix(), 10)+`"`) {
		t.Fatalf("rendered home body missing heartbeat epoch attribute")
	}
	if !strings.Contains(rendered, "row-bad") {
		t.Fatalf("rendered home body missing row-bad class for failing worker")
	}
}
