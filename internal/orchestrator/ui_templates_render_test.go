package orchestrator

import (
	"bytes"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestUITemplatesRenderSmoke(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data any
	}{
		{"home", homePageData{}},
		{"home_body", homePageData{}},
		{"worker", workerPageData{Incident: &IncidentBundle{}}},
		{"help", helpPageData{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			if err := uiTemplates.ExecuteTemplate(&buf, tc.name, tc.data); err != nil {
				t.Fatalf("ExecuteTemplate(%q) error = %v", tc.name, err)
			}
			if buf.Len() == 0 {
				t.Fatalf("ExecuteTemplate(%q) produced empty output", tc.name)
			}
		})
	}
}

func TestWorkerTemplateRendersLitestreamMetricsStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status    string
		wantBadge string
	}{
		{
			status:    reporting.LitestreamMetricsStatusHealthy,
			wantBadge: `badge badge-good">metrics ok</span>`,
		},
		{
			status:    reporting.LitestreamMetricsStatusMetricMissing,
			wantBadge: `badge badge-warn">required metrics missing</span>`,
		},
		{
			status:    reporting.LitestreamMetricsStatusScrapeFailed,
			wantBadge: `badge badge-warn">metrics scrape failed</span>`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.status, func(t *testing.T) {
			t.Parallel()
			var body bytes.Buffer
			data := workerPageData{Incident: &IncidentBundle{
				RuntimeSnapshotStatus:   reporting.RuntimeSnapshotStatusHealthy,
				LitestreamMetricsStatus: test.status,
			}}
			if err := uiTemplates.ExecuteTemplate(&body, "worker", data); err != nil {
				t.Fatalf("ExecuteTemplate(worker) error = %v", err)
			}
			if !strings.Contains(body.String(), test.wantBadge) {
				t.Fatalf("worker template missing %q: %s", test.wantBadge, body.String())
			}
		})
	}
}
