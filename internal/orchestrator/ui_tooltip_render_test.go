package orchestrator

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func renderTooltipHomeBody(t *testing.T, data homePageData) string {
	t.Helper()

	var body bytes.Buffer
	if err := uiTemplates.ExecuteTemplate(&body, "home_body", data); err != nil {
		t.Fatalf("ExecuteTemplate(home_body) error = %v", err)
	}
	return body.String()
}

func tooltipHomeData() homePageData {
	heartbeatAt := time.Now().UTC().Add(-30 * time.Second)
	return homePageData{
		Workers: []homeWorker{
			{
				Worker: model.Worker{
					ID:              "worker-main-high",
					Name:            "worker-main-high",
					Status:          model.WorkerRunning,
					LastHeartbeatAt: &heartbeatAt,
				},
				CurrentFailureSignature: "litestream_sync_socket_refused dial tcp connection refused during replica sync",
				CurrentFailureSeverity:  "bad",
				Ticks: []model.VerificationTick{
					{WorkerID: "worker-main-high", StartedAt: time.Now().UTC().Add(-time.Hour), Status: "passed", Passed: true, DurationMS: 1200},
					{WorkerID: "worker-main-high", StartedAt: time.Now().UTC().Add(-30 * time.Minute), Status: "failed", DurationMS: 900},
				},
			},
		},
	}
}

func ticksMarkup(t *testing.T, rendered string) string {
	t.Helper()

	start := strings.Index(rendered, `<span class="ticks">`)
	if start < 0 {
		t.Fatal("rendered home body missing ticks markup")
	}
	rest := rendered[start:]
	end := strings.Index(rest, "\n")
	if end < 0 {
		end = len(rest)
	}
	return rest[:end]
}

func TestTooltipTickSpansUseDataTip(t *testing.T) {
	t.Parallel()

	rendered := renderTooltipHomeBody(t, tooltipHomeData())
	ticks := ticksMarkup(t, rendered)
	if !strings.Contains(ticks, `data-tip="`) {
		t.Fatalf("tick spans missing data-tip attribute: %s", ticks)
	}
	if strings.Contains(ticks, `title="`) {
		t.Fatalf("tick spans still use native title attribute: %s", ticks)
	}
}

func TestTooltipSignalBadgeCarriesFullSignature(t *testing.T) {
	t.Parallel()

	data := tooltipHomeData()
	signature := data.Workers[0].CurrentFailureSignature
	rendered := renderTooltipHomeBody(t, data)
	if !strings.Contains(rendered, `data-tip="`+signature+`"`) {
		t.Fatalf("signal badge missing full signature in data-tip: %s", rendered[:min(2000, len(rendered))])
	}
	if strings.Contains(rendered, `title="`+signature+`"`) {
		t.Fatalf("signal badge still carries signature in native title: %s", rendered[:min(2000, len(rendered))])
	}
}

func TestTooltipNoLegacyTipClassesOrNoteRole(t *testing.T) {
	t.Parallel()

	rendered := renderTooltipHomeBody(t, tooltipHomeData())
	for _, unwanted := range []string{"tip-end", "tip-start", `role="note"`} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("rendered home body still contains %q", unwanted)
		}
	}
}
