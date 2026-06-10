package orchestrator

import (
	"bytes"
	"testing"
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
