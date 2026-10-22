package worker

import (
	"bytes"
	"strings"
)

const restoreReplanMessage = "restore file disappeared, recalculating plan"
const restoreOutputTruncated = "[restore output truncated]\n"

type restoreOutput struct {
	pending   []byte
	output    []byte
	matchTail string
	replans   int
	truncated bool
	dropping  bool
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
	line := []byte(redactLogIncident(string(b.pending)))
	b.pending = nil
	limit := syncDiagnosticOutputLimit - len(restoreOutputTruncated)
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
		return restoreOutputTruncated + string(b.output)
	}
	return string(b.output)
}
