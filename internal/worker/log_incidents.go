package worker

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

var logCredential = regexp.MustCompile(`(?i)(password|secret|token|authorization|access_key_id|secret_access_key|aws_session_token)("?\s*[=:]\s*)(?:"[^"]*"|[^\s&]+)`)
var logURLCredential = regexp.MustCompile(`(https?://)[^/@\s]+@`)
var logSignedCredential = regexp.MustCompile(`(?i)(X-Amz-(?:Credential|Signature|Security-Token)=)[^&\s"]+`)

func redactLogIncident(line string) string {
	line = strings.ReplaceAll(line, "\x00", "")
	line = logCredential.ReplaceAllString(line, "${1}${2}[REDACTED]")
	line = logURLCredential.ReplaceAllString(line, "${1}[REDACTED]@")
	return logSignedCredential.ReplaceAllString(line, "${1}[REDACTED]")
}

func (r *Runner) observeLogIncidents(cancelRun context.CancelCauseFunc) {
	r.litestreamLog.onIncomplete = cancelRun
	r.litestreamLog.onIncident = func(line string, evidence reporting.MaintenanceEvidence) error {
		event := reporting.WorkerEventPayload{
			WorkerIdentity: workerIdentity(r.cfg), EventType: "litestream_log_error", Message: redactLogIncident(line), SentAt: time.Now().UTC(),
			RuntimePayload: reporting.RuntimePayload{MaintenanceEvidence: evidence},
			WorkloadEvent:  reporting.WorkloadEvent{WorkloadEventID: fmt.Sprintf("log:%s:%d", evidence.Epoch, evidence.Errors)},
		}
		if err := r.persistRunEvidence(event); err != nil {
			cancelRun(fmt.Errorf("preserve Litestream log incident: %w", err))
			return err
		}
		r.requestEvidenceFlush()
		return nil
	}
}
