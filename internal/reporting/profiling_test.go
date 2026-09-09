package reporting

import (
	"strings"
	"testing"
	"time"
)

func TestProfileIncidentsRetainRecoveryAndMissingHistory(t *testing.T) {
	at := time.Now().UTC()
	record := ProfileRecordEvidence{RunID: "original", Artifact: "goroutine.pprof", CapturedAt: at, Error: "capture interrupted", Upload: "uploaded", UploadFailureCount: 2, UploadFailuresDropped: 1, UploadFailures: []ProfileUploadFailureEvidence{{At: at, Attempt: 1, Stage: "artifact", Error: "signal: killed"}}}
	incidents := record.Incidents()
	if len(incidents) != 3 || incidents[1].ID != "profile:original:goroutine.pprof:upload:1" || !strings.Contains(incidents[1].Message, "signal: killed") || incidents[2].Kind != "profile_history_unavailable" {
		t.Fatalf("lost failure history after recovery: %+v", incidents)
	}
	record.Error = ""
	record.UploadFailureCount = 0
	record.UploadFailuresDropped = 0
	record.UploadFailures = nil
	if got := record.Incidents(); len(got) != 0 {
		t.Fatalf("healthy record has incidents: %+v", got)
	}
	record.UploadHistoryIncomplete = true
	if got := record.Incidents(); len(got) != 1 || got[0].Kind != "profile_history_unavailable" {
		t.Fatalf("legacy history lost: %+v", got)
	}
}
