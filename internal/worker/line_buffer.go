package worker

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/google/uuid"
)

type lineBuffer struct {
	maintenance  reporting.MaintenanceEvidence
	onIncident   func(string, reporting.MaintenanceEvidence) error
	onIncomplete func(error)
	mu           sync.Mutex
	limit        int
	pending      string
	lines        []string
}

func newLineBuffer(limit int) *lineBuffer {
	if limit <= 0 {
		limit = 80
	}
	return &lineBuffer{limit: limit, maintenance: reporting.MaintenanceEvidence{Epoch: uuid.NewString(), StartedAt: time.Now().UTC(), Complete: true}}
}

func (b *lineBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	text := b.pending + string(p)
	parts := strings.Split(text, "\n")
	if len(parts[len(parts)-1]) > churnOutboxMaxBytes {
		b.maintenance.Complete = false
		err := fmt.Errorf("log observation unavailable: incomplete line exceeds evidence capacity")
		if b.onIncomplete != nil {
			b.onIncomplete(err)
		}
		return 0, err
	}
	b.pending = parts[len(parts)-1]
	if b.onIncident == nil {
		if len(b.pending) > 1000 {
			b.maintenance.Complete = false
		}
		b.pending = sanitizeLine(b.pending)
	}
	for _, line := range parts[:len(parts)-1] {
		if err := b.observeLine(line); err != nil {
			return len(p), err
		}

	}
	return len(p), nil
}

func (b *lineBuffer) Lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	lines := append([]string(nil), b.lines...)
	if strings.TrimSpace(b.pending) != "" {
		lines = append(lines, sanitizeLine(b.pending))
	}
	return lines
}

func (b *lineBuffer) append(line string) {
	b.maintenance.ObserveLine(line)
	b.maintenance.LastError = redactLogIncident(b.maintenance.LastError)
	if b.limit <= 0 {
		return
	}
	line = sanitizeLine(line)
	b.lines = append(b.lines, line)
	if len(b.lines) > b.limit {
		copy(b.lines, b.lines[len(b.lines)-b.limit:])
		b.lines = b.lines[:b.limit]
	}
}

func sanitizeLine(line string) string {
	line = strings.ReplaceAll(line, "\x00", "")
	line = strings.TrimRight(line, "\r")
	if len(line) <= 1000 {
		return line
	}
	var buf bytes.Buffer
	buf.WriteString(line[:1000])
	buf.WriteString("...truncated")
	return buf.String()
}

func (b *lineBuffer) observeLine(line string) error {
	before := b.maintenance.Errors
	b.append(strings.TrimRight(line, "\r"))
	if b.maintenance.Errors > before && b.onIncident != nil {
		if err := b.onIncident(line, b.maintenance); err != nil {
			b.maintenance.Complete = false
			return err
		}
	}
	return nil
}

func (b *lineBuffer) FlushPending() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	pending := b.pending
	b.pending = ""
	if strings.TrimSpace(pending) == "" {
		return nil
	}
	return b.observeLine(pending)
}
