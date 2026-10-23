package worker

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

var sqliteBusyLog = regexp.MustCompile(`(?i)SQLITE_BUSY|database is locked`)

var harnessWriters = &harnessWriterTracker{}

type harnessWriterTracker struct {
	mu     sync.Mutex
	next   uint64
	active map[string]reporting.HarnessWriter
}

func (t *harnessWriterTracker) begin(name, dbPath string) func() {
	t.mu.Lock()
	t.next++
	key := fmt.Sprintf("%s#%d", name, t.next)
	t.put(key, name, dbPath)
	t.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			delete(t.active, key)
			t.mu.Unlock()
		})
	}
}

func (t *harnessWriterTracker) setActive(name, dbPath string, active bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := name + "@" + dbPath
	if !active {
		delete(t.active, key)
		return
	}
	if _, ok := t.active[key]; !ok {
		t.put(key, name, dbPath)
	}
}

func (t *harnessWriterTracker) put(key, name, dbPath string) {
	if t.active == nil {
		t.active = map[string]reporting.HarnessWriter{}
	}
	t.active[key] = reporting.HarnessWriter{Name: name, DBPath: dbPath, StartedAt: time.Now().UTC()}
}

func (t *harnessWriterTracker) snapshot() []reporting.HarnessWriter {
	t.mu.Lock()
	writers := make([]reporting.HarnessWriter, 0, len(t.active))
	for _, writer := range t.active {
		writers = append(writers, writer)
	}
	t.mu.Unlock()
	sort.Slice(writers, func(i, j int) bool {
		if !writers[i].StartedAt.Equal(writers[j].StartedAt) {
			return writers[i].StartedAt.Before(writers[j].StartedAt)
		}
		return writers[i].Name < writers[j].Name
	})
	return writers
}
