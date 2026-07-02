package orchestrator

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestFleetTableMobileCardMarkup(t *testing.T) {
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
		`data-label="Checks"`,
		`data-label="Heartbeat"`,
		`data-label="Last check"`,
		`data-label="Signal"`,
		`class="cell-dot"`,
		`<span class="visually-hidden">failing</span>`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered home body missing %q", want)
		}
	}
}

func TestDashboardCSSMobileA11y(t *testing.T) {
	t.Parallel()

	body, err := assetsFS.ReadFile("assets/dashboard.css")
	if err != nil {
		t.Fatalf("ReadFile(assets/dashboard.css) error = %v", err)
	}
	css := string(body)

	if strings.Contains(css, "nth-child(n+5)") {
		t.Fatalf("dashboard.css still hides fleet columns via nth-child(n+5)")
	}

	for _, want := range []string{
		".visually-hidden",
		"--faint-fill",
		"--green-quiet-fill",
		".worker-table td[data-label]::before",
		".worker-table tr[hidden] { display: none; }",
	} {
		if !strings.Contains(css, want) {
			t.Fatalf("dashboard.css missing %q", want)
		}
	}
}
