package worker

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TenantLogIncident struct {
	Line    int    `json:"line"`
	Level   string `json:"level"`
	Message string `json:"message"`
	Raw     string `json:"raw"`
}

type TenantLogEvidence struct {
	Format           string              `json:"format"`
	Status           string              `json:"status"`
	Lines            int                 `json:"lines"`
	IncidentCount    int                 `json:"incident_count"`
	UnparsedCount    int                 `json:"unparsed_count"`
	DetailsTruncated bool                `json:"details_truncated"`
	Incidents        []TenantLogIncident `json:"incidents"`
}

var tenantLogLine = regexp.MustCompile(`^time=\S+ level=(DEBUG|INFO|WARN|ERROR) msg=("(?:\\.|[^"\\])*"|[^"\s]\S*)(?:\s|$)`)
var tenantLogIncident = regexp.MustCompile(`(?i)\b(retry|retrying|retries|retried|fail|failed|failure|failures|error|errors|recovering|recovered|recovery|backoff|reconnecting|reconnected)\b`)

func readTenantLogEvidence(reader io.Reader) (TenantLogEvidence, error) {
	evidence := TenantLogEvidence{Format: "slog-text-v1", Status: "unavailable"}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		evidence.Lines++
		line := scanner.Text()
		fields := tenantLogLine.FindStringSubmatch(line)
		level, message := "UNKNOWN", line
		parsed := len(fields) > 0
		if parsed {
			level, message = fields[1], fields[2]
			if strings.HasPrefix(message, `"`) {
				decoded, err := strconv.Unquote(message)
				if err != nil {
					parsed = false
				} else {
					message = decoded
				}
			}
		}
		if !parsed {
			evidence.UnparsedCount++
		}
		if !parsed || level == "WARN" || level == "ERROR" || tenantLogIncident.MatchString(message) || strings.Contains(line, " error=") {
			evidence.IncidentCount++
			if len(evidence.Incidents) < 1000 {
				evidence.Incidents = append(evidence.Incidents, TenantLogIncident{Line: evidence.Lines, Level: level, Message: sanitizeLine(message), Raw: sanitizeLine(line)})
			} else {
				evidence.DetailsTruncated = true
			}
		}
	}
	switch {
	case scanner.Err() != nil || evidence.UnparsedCount > 0 || evidence.DetailsTruncated:
		evidence.Status = "partial"
	case evidence.Lines > 0:
		evidence.Status = "parsed"
	}
	return evidence, scanner.Err()
}

func (r *tenantRunner) reviewLog() error {
	file, err := os.Open(r.report.ProcessLog)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	evidence, err := readTenantLogEvidence(file)
	r.report.LogEvidence = evidence
	for _, incident := range evidence.Incidents {
		r.event("process-log", tenantID{}, "failed", fmt.Sprintf("line=%d level=%s message=%s", incident.Line, incident.Level, incident.Message))
	}
	if evidence.Status != "parsed" {
		r.event("process-log-coverage", tenantID{}, "failed", fmt.Sprintf("format=%s status=%s unparsed=%d truncated=%t", evidence.Format, evidence.Status, evidence.UnparsedCount, evidence.DetailsTruncated))
	}
	if err != nil {
		return err
	}
	if len(r.report.Failures) > 0 {
		return fmt.Errorf("lifecycle failures retained; see report and full process log")
	}
	return nil
}

func (r *tenantRunner) finish(retErr error) error {
	hadProcess := r.process != nil
	stopErr := r.stop()
	if stopErr != nil {
		r.event("cleanup", tenantID{}, "failed", stopErr.Error())
		retErr = errors.Join(retErr, stopErr)
	}
	if hadProcess {
		if stopErr == nil {
			r.event("cleanup", tenantID{}, "passed", "replication process exited and was reaped after scenario")
		}
		r.frame("cleanup")
	}
	r.report.Failures = append(r.report.Failures, r.ledger.failures...)
	r.report.LogTail = r.log.Lines()
	retErr = errors.Join(retErr, r.fullLog.file.Close(), r.fullLog.err)
	if logErr := r.reviewLog(); logErr != nil {
		retErr = errors.Join(retErr, logErr)
	}
	if retErr != nil || len(r.report.Failures) > 0 {
		r.report.Status = "failed"
	}
	if retErr != nil {
		r.report.Error = retErr.Error()
	}
	if r.report.Interrupted && len(r.report.Failures) == 0 {
		r.report.Status = "incomplete"
	}
	for id, work := range r.ledger.work {
		if work.pending {
			r.report.PendingTenants = append(r.report.PendingTenants, id.name())
		}
		if work.attempt > 0 {
			r.report.AttemptedTenants = append(r.report.AttemptedTenants, id.name())
		}
	}
	sort.Strings(r.report.PendingTenants)
	sort.Strings(r.report.AttemptedTenants)
	r.report.FinishedAt = time.Now().UTC()
	data, encodeErr := json.MarshalIndent(r.report, "", "  ")
	if encodeErr == nil {
		encodeErr = os.WriteFile(filepath.Join(r.dir, "report.json"), data, 0o600)
	}
	retErr = errors.Join(retErr, encodeErr)
	if encodeErr != nil {
		r.report.Status = "failed"
	}
	return retErr
}
