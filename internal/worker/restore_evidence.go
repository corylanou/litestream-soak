package worker

import (
	"bytes"
	"strings"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const restoreReplanMessage = "restore file disappeared, recalculating plan"
const restoreEvidenceLimit = 8 * 1024
const restoreOutputTruncated = "[restore output truncated]\n"

type restoreOutput struct {
	pending      []byte
	output       []byte
	head         []byte
	headComplete bool
	finalError   string
	matchTail    string
	replans      int
	requests     map[string]uint64
	planTXID     uint64
	truncated    bool
	dropping     bool
}

func (b *restoreOutput) Write(p []byte) (int, error) {
	n := len(p)
	text := b.matchTail + string(p)
	b.replans += strings.Count(text, restoreReplanMessage)
	b.matchTail = text[max(0, len(text)-len(restoreReplanMessage)+1):]
	for len(p) > 0 {
		end := bytes.IndexByte(p, '\n')
		size := len(p)
		if end >= 0 {
			size = end + 1
		}
		if !b.dropping {
			if len(b.pending)+size > syncDiagnosticOutputLimit {
				b.pending = nil
				b.dropping = true
				b.truncated = true
			} else {
				b.pending = append(b.pending, p[:size]...)
			}
		}
		p = p[size:]
		if end >= 0 {
			b.flushLine()
			b.dropping = false
		}
	}
	return n, nil
}

func (b *restoreOutput) flushLine() {
	if len(b.pending) == 0 {
		return
	}
	if method, ok := restoreRequestMethod(string(b.pending)); ok {
		if b.requests == nil {
			b.requests = make(map[string]uint64)
		}
		b.requests[method]++
	}
	if txid, ok := parseRestorePlanTXID(string(b.pending)); ok {
		b.planTXID = txid
	}
	line := []byte(redactLogIncident(string(b.pending)))
	b.pending = nil
	if final := reporting.RestoreErrorOutput(string(line)); final != "" {
		if len(b.finalError)+len(line)+1 <= syncDiagnosticOutputLimit {
			b.finalError = reporting.RestoreErrorOutput(b.finalError + "\n" + string(line))
		} else {
			b.finalError = final
		}
	}
	limit := (restoreEvidenceLimit - len(restoreOutputTruncated)) / 2
	if !b.headComplete && len(b.head)+len(line) <= limit {
		b.head = append(b.head, line...)
		return
	}
	b.headComplete = true
	if len(line) > limit {
		b.truncated = true
		return
	}
	for len(b.output)+len(line) > limit {
		end := bytes.IndexByte(b.output, '\n')
		if end < 0 {
			b.output = nil
		} else {
			b.output = b.output[end+1:]
		}
		b.truncated = true
	}
	b.output = append(b.output, line...)
}

func (b *restoreOutput) text() string {
	b.flushLine()
	if b.truncated {
		return string(b.head) + restoreOutputTruncated + string(b.output)
	}
	return string(b.head) + string(b.output)
}
