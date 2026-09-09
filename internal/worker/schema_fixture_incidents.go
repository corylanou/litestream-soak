package worker

import (
	"bufio"
	"io"
	"regexp"
	"strconv"
	"strings"
)

const schemaProcessPatternSHA = "4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3"

var schemaProcessLine = regexp.MustCompile(`^time=\S+ level=(DEBUG|INFO|WARN|ERROR) msg=("(?:\\.|[^"\\])*"|[^"\s]\S*)(?:\s|$)`)
var schemaProcessFailure = regexp.MustCompile(`(?i)\b(retry|retrying|retries|retried|fail|failed|failure|failures|error|errors)\b`)

type SchemaProcessIncident struct {
	Line    int    `json:"line"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

type SchemaProcessEvidence struct {
	PatternSHA       string                  `json:"pattern_sha"`
	Status           string                  `json:"status"`
	Lines            int                     `json:"lines"`
	IncidentCount    int                     `json:"incident_count"`
	UnparsedCount    int                     `json:"unparsed_count"`
	UnparsedLines    []int                   `json:"unparsed_lines,omitempty"`
	Incidents        []SchemaProcessIncident `json:"incidents,omitempty"`
	DetailsTruncated bool                    `json:"details_truncated"`
}

func readSchemaProcessEvidence(reader io.Reader) (SchemaProcessEvidence, error) {
	evidence := SchemaProcessEvidence{PatternSHA: schemaProcessPatternSHA, Status: "unavailable"}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		evidence.Lines++
		line := scanner.Text()
		fields := schemaProcessLine.FindStringSubmatch(line)
		if len(fields) == 0 {
			evidence.UnparsedCount++
			if len(evidence.UnparsedLines) < 1000 {
				evidence.UnparsedLines = append(evidence.UnparsedLines, evidence.Lines)
			} else {
				evidence.DetailsTruncated = true
			}
			continue
		}
		message := fields[2]
		if strings.HasPrefix(message, `"`) {
			decoded, err := strconv.Unquote(message)
			if err != nil {
				evidence.UnparsedCount++
				continue
			}
			message = decoded
		}
		if fields[1] == "ERROR" || fields[1] == "WARN" || schemaProcessFailure.MatchString(message) || strings.Contains(line, " error=") {
			evidence.IncidentCount++
			if len(evidence.Incidents) < 1000 {
				evidence.Incidents = append(evidence.Incidents, SchemaProcessIncident{Line: evidence.Lines, Level: fields[1], Message: message})
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

func schemaProcessVerdict(boundaryVerdict string, evidence SchemaProcessEvidence) string {
	if boundaryVerdict != "scenario_success" {
		return boundaryVerdict
	}
	if evidence.IncidentCount > 0 {
		return "recovered_with_incidents"
	}
	if evidence.Status != "parsed" {
		return "inconclusive"
	}
	return boundaryVerdict
}
