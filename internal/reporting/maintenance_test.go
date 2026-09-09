package reporting

import "testing"

func TestMaintenanceEvidenceObservedOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name, line                                 string
		snapshots, compactions, retentions, errors uint64
		complete                                   bool
	}{
		{name: "snapshot", line: `level=INFO msg="snapshot complete" size=4096`, snapshots: 1, complete: true},
		{name: "compaction JSON", line: `{"level":"INFO","msg":"compaction complete","level":1,"size":4096}`, compactions: 1, complete: true},
		{name: "positive retention", line: `level=INFO msg="l0 retention enforced" deleted_count=2`, retentions: 1, complete: true},
		{name: "zero retention", line: `level=INFO msg="l0 retention enforced" deleted_count=0`, complete: true},
		{name: "scheduled", line: `level=INFO msg="starting compaction monitor"`, complete: true},
		{name: "warning", line: `level=WARN msg="provider slow"`, errors: 1, complete: true},
		{name: "failed snapshot", line: `level=ERROR msg="snapshot complete" size=4096`, errors: 1, complete: true},
		{name: "INFO retry", line: `level=INFO msg="retrying upload"`, errors: 1, complete: true},
		{name: "INFO recovery", line: `level=INFO msg="recovered after timeout"`, errors: 1, complete: true},
		{name: "routine DEBUG", line: `level=DEBUG msg="polling database"`, complete: true},
		{name: "unknown", line: `unknown log format`, complete: false},
		{name: "unsupported severity", line: `{"level":"TRACE","msg":"unknown"}`, complete: false},
		{name: "malformed JSON", line: `{"level":"INFO","msg":`, complete: false},
		{name: "blank", line: "", complete: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := MaintenanceEvidence{Complete: true}
			e.ObserveLine(tt.line)
			if e.Snapshots != tt.snapshots || e.Compactions != tt.compactions || e.Retentions != tt.retentions || e.Errors != tt.errors || e.Complete != tt.complete {
				t.Fatalf("evidence=%+v", e)
			}
			if tt.errors > 0 && e.LastError != tt.line {
				t.Fatalf("original error not retained: %q", e.LastError)
			}
		})
	}
}
