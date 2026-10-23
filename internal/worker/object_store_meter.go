package worker

import (
	"maps"
	"strings"
	"sync"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/google/uuid"
)

const restoreRequestMessage = `msg="s3 request"`

type objectStoreMeter struct {
	mu          sync.Mutex
	epoch       string
	restore     map[string]uint64
	uploads     uint64
	uploadBytes uint64
}

func newObjectStoreMeter() *objectStoreMeter {
	return &objectStoreMeter{epoch: uuid.NewString(), restore: make(map[string]uint64)}
}

func (m *objectStoreMeter) recordRestoreRequests(requests map[string]uint64) {
	if m == nil || len(requests) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for method, count := range requests {
		m.restore[method] += count
	}
}

func (m *objectStoreMeter) recordObjectUpload(bytes int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploads++
	if bytes > 0 {
		m.uploadBytes += uint64(bytes)
	}
}

func (m *objectStoreMeter) snapshot() reporting.ObjectStoreCounters {
	if m == nil {
		return reporting.ObjectStoreCounters{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	counters := reporting.ObjectStoreCounters{
		ObjectStoreCounterEpoch:         m.epoch,
		EvidenceObjectUploadsTotal:      m.uploads,
		EvidenceUploadPayloadBytesTotal: m.uploadBytes,
	}
	if len(m.restore) > 0 {
		counters.RestoreRequestTotals = maps.Clone(m.restore)
	}
	return counters
}

func restoreRequestMethod(line string) (string, bool) {
	if !strings.Contains(line, restoreRequestMessage) {
		return "", false
	}
	_, rest, ok := strings.Cut(line, " s3.method=")
	if !ok {
		return "", false
	}
	method, _, _ := strings.Cut(rest, " ")
	method = strings.ToUpper(strings.Trim(strings.TrimSpace(method), `"`))
	if method == "" {
		return "", false
	}
	if method == "GET" && strings.Contains(line, "list-type=") {
		return "LIST", true
	}
	return method, true
}
