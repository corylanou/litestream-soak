package worker

import (
	"bytes"
	"strings"
	"sync"
)

type lineBuffer struct {
	mu      sync.Mutex
	limit   int
	pending string
	lines   []string
}

func newLineBuffer(limit int) *lineBuffer {
	if limit <= 0 {
		limit = 80
	}
	return &lineBuffer{limit: limit}
}

func (b *lineBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	text := b.pending + string(p)
	parts := strings.Split(text, "\n")
	b.pending = sanitizeLine(parts[len(parts)-1])
	for _, line := range parts[:len(parts)-1] {
		b.append(strings.TrimRight(line, "\r"))
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
