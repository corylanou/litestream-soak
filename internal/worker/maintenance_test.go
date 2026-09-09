package worker

import (
	"strings"
	"testing"
)

func TestMaintenanceEvidenceSurvivesLogTailEviction(t *testing.T) {
	log := newLineBuffer(1)
	for _, line := range []string{
		`time=2026-09-09T10:00:00Z level=INFO msg="starting compaction monitor"`,
		`time=2026-09-09T10:00:01Z level=INFO msg="snapshot complete" txid=00000001 size=4096`,
		`{"level":"INFO","msg":"compaction complete","level":1,"size":4096}`,
		`time=2026-09-09T10:00:03Z level=INFO msg="l0 retention enforced" deleted_count=0`,
		`time=2026-09-09T10:00:04Z level=INFO msg="l0 retention enforced" deleted_count=2`,
		`time=2026-09-09T10:00:05Z level=ERROR msg="snapshot complete" error="failed"`,
	} {
		if _, err := log.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 150; i++ {
		_, _ = log.Write([]byte("unrelated output\n"))
	}
	evidence := log.MaintenanceEvidence()
	if evidence.Snapshots != 1 || evidence.Compactions != 1 || evidence.Retentions != 1 || evidence.Errors != 1 || evidence.Epoch == "" {
		t.Fatalf("evidence=%+v", evidence)
	}
	if !strings.Contains(evidence.LastError, "failed") {
		t.Fatalf("error not retained: %+v", evidence)
	}
}

func TestMaintenanceLogClassification(t *testing.T) {
	for _, tt := range []struct {
		line     string
		errors   uint64
		complete bool
	}{
		{`level=INFO msg="retrying failed upload"`, 1, true},
		{`level=INFO msg="recovered after timeout"`, 1, true},
		{`level=DEBUG msg="polling database"`, 0, true},
		{`unknown log format`, 0, false},
		{`{"level":"TRACE","msg":"unrecognized"}`, 0, false},
	} {
		t.Run(tt.line, func(t *testing.T) {
			log := newLineBuffer(1)
			_, _ = log.Write([]byte(tt.line + "\n"))
			e := log.MaintenanceEvidence()
			if e.Errors != tt.errors || e.Complete != tt.complete {
				t.Fatalf("evidence=%+v", e)
			}
		})
	}
}
