package orchestrator

import (
	"bytes"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestWorkerPageRendersCharterPanel(t *testing.T) {
	data := workerPageData{Incident: &IncidentBundle{
		Worker: model.Worker{Name: "worker-main-high-vol", ProfileName: "high-volume"},
	}}
	var buf bytes.Buffer
	if err := uiTemplates.ExecuteTemplate(&buf, "worker", data); err != nil {
		t.Fatalf("ExecuteTemplate(worker) error = %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Why This Test Exists") {
		t.Error("worker page missing charter panel heading")
	}
	if want := profileCharter("high-volume").GuardsAgainst; !strings.Contains(out, want) {
		t.Errorf("worker page missing charter content %q", want)
	}
}

func TestFleetRowRendersProfileSynopsis(t *testing.T) {
	data := homePageData{Workers: []homeWorker{{
		Worker:   model.Worker{Name: "worker-main-burst-vol", ProfileName: "burst-volume"},
		Workload: workload.Config{LoadMode: "synthetic"},
	}}}
	var buf bytes.Buffer
	if err := uiTemplates.ExecuteTemplate(&buf, "home_body", data); err != nil {
		t.Fatalf("ExecuteTemplate(home_body) error = %v", err)
	}
	if want := profileSynopsis("burst-volume"); !strings.Contains(buf.String(), want) {
		t.Errorf("fleet row missing profile synopsis %q", want)
	}
}

// allFleetProfiles returns every profile name the fleet can deploy, including
// the many-db lanes that are gated behind env flags at runtime.
func allFleetProfiles() map[string]struct{} {
	profiles := map[string]struct{}{}
	for _, w := range DefaultMainFleet().Workers {
		profiles[w.ProfileName] = struct{}{}
	}
	for _, w := range manyDB100FleetWorkers() {
		profiles[w.ProfileName] = struct{}{}
	}
	for _, w := range manyDB500FleetWorkers() {
		profiles[w.ProfileName] = struct{}{}
	}
	profiles[manyDB1000FleetWorker().ProfileName] = struct{}{}
	return profiles
}

func TestEveryFleetProfileHasCompleteCharter(t *testing.T) {
	for profile := range allFleetProfiles() {
		charter := profileCharter(profile)
		if !charter.Known {
			t.Errorf("profile %q has no charter; add one to profileCharters in profile_charter.go", profile)
			continue
		}
		fields := map[string]string{
			"Synopsis":      charter.Synopsis,
			"Stresses":      charter.Stresses,
			"GuardsAgainst": charter.GuardsAgainst,
			"WhyItMatters":  charter.WhyItMatters,
		}
		for name, value := range fields {
			if value == "" {
				t.Errorf("profile %q charter has empty %s", profile, name)
			}
		}
	}
}

func TestProfileCharterUnknownIsNotKnown(t *testing.T) {
	if c := profileCharter("does-not-exist"); c.Known {
		t.Fatalf("unknown profile reported Known=true: %+v", c)
	}
	if s := profileSynopsis("does-not-exist"); s != "" {
		t.Fatalf("unknown profile synopsis = %q, want empty", s)
	}
}

func TestProfileCharterTrimsWhitespace(t *testing.T) {
	if c := profileCharter("  high-volume  "); !c.Known {
		t.Fatal("profileCharter did not trim surrounding whitespace")
	}
}
