package compare

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/worker"
)

var errProgressUnsupported = errors.New("read-only binary diagnostics omit replicated_txid; transaction lag unavailable")

type progressSample struct {
	SyncAgeSeconds *float64                           `json:"sync_age_seconds,omitempty"`
	Diagnostic     json.RawMessage                    `json:"diagnostic,omitempty"`
	At             time.Time                          `json:"at"`
	TXID           uint64                             `json:"txid"`
	ReplicatedTXID uint64                             `json:"replicated_txid"`
	Resources      *worker.ComparisonProcessResources `json:"resources,omitempty"`
	ProgressError  string                             `json:"progress_error,omitempty"`
	ResourceError  string                             `json:"resource_error,omitempty"`
}

type monitor struct {
	preserved       bool
	lastDiagnostic  json.RawMessage
	lastSyncAt      *time.Time
	maxSyncAge      float64
	client          *http.Client
	baseURL         string
	path            string
	directory       string
	pid             int
	samples         []progressSample
	errors          []string
	pending         map[uint64]time.Time
	maxLagSeconds   float64
	allocationStart float64
	allocationError string
}

func socketMonitor(socket, path, directory string, pid int) *monitor {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", socket)
	}}
	return &monitor{client: &http.Client{Transport: transport, Timeout: 5 * time.Second}, baseURL: "http://localhost", path: path, directory: directory, pid: pid}
}

func (m *monitor) get(ctx context.Context, path string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", m.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := m.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s returned %s", path, response.Status)
	}
	return data, nil
}

func (m *monitor) progress(ctx context.Context) (uint64, uint64, error) {
	m.lastSyncAt = nil
	m.lastDiagnostic = nil
	data, err := m.get(ctx, "/list")
	if err != nil {
		return 0, 0, err
	}
	var result struct {
		Databases []struct {
			Path       string     `json:"path"`
			TXID       *uint64    `json:"txid"`
			Replicated *uint64    `json:"replicated_txid"`
			LastSyncAt *time.Time `json:"last_sync_at"`
		} `json:"databases"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return 0, 0, err
	}
	for _, db := range result.Databases {
		if db.Path == m.path && db.LastSyncAt != nil {
			m.lastSyncAt = db.LastSyncAt
		}
	}
	for _, db := range result.Databases {
		if db.Path == m.path && db.TXID != nil && db.Replicated != nil {
			return *db.TXID, *db.Replicated, nil
		}
	}
	data, err = m.get(ctx, "/debug/sync-status?path="+url.QueryEscape(m.path))
	if err != nil {
		return 0, 0, err
	}
	m.lastDiagnostic = append(json.RawMessage(nil), data...)
	if err := json.Unmarshal(data, &result); err != nil {
		return 0, 0, err
	}
	if len(result.Databases) == 1 && result.Databases[0].TXID != nil && result.Databases[0].Replicated != nil {
		return *result.Databases[0].TXID, *result.Databases[0].Replicated, nil
	}
	return 0, 0, errProgressUnsupported
}

func (m *monitor) sample(ctx context.Context, at time.Time) {
	s := progressSample{At: at}
	txid, replicated, err := m.progress(ctx)
	if err != nil {
		s.ProgressError = err.Error()
		if !errors.Is(err, errProgressUnsupported) {
			m.errors = append(m.errors, err.Error())
		}
	} else {
		s.TXID = txid
		s.ReplicatedTXID = replicated
		if m.pending == nil {
			m.pending = map[uint64]time.Time{}
		}
		if txid > replicated {
			if _, ok := m.pending[txid]; !ok {
				m.pending[txid] = at
			}
		}
		for id, started := range m.pending {
			m.maxLagSeconds = max(m.maxLagSeconds, at.Sub(started).Seconds())
			if replicated >= id {
				delete(m.pending, id)
			}
		}
	}
	s.Diagnostic = append(json.RawMessage(nil), m.lastDiagnostic...)
	if m.lastSyncAt != nil {
		age := max(0, at.Sub(*m.lastSyncAt).Seconds())
		s.SyncAgeSeconds = &age
		m.maxSyncAge = max(m.maxSyncAge, age)
	}
	resources, err := worker.ReadComparisonProcessResources(m.pid)
	if err != nil {
		s.ResourceError = err.Error()
		if runtime.GOOS == "linux" {
			m.errors = append(m.errors, "process resources: "+err.Error())
		}
	} else {
		s.Resources = &resources
	}
	m.samples = append(m.samples, s)
}

func allocationTotal(data []byte) (float64, error) {
	for _, line := range strings.Split(string(data), "\n") {
		if value, ok := strings.CutPrefix(line, "# TotalAlloc = "); ok {
			n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			return float64(n), err
		}
	}
	return 0, errors.New("binary pprof heap text lacks runtime.MemStats TotalAlloc")
}

func (m *monitor) allocation(ctx context.Context, name string) (float64, error) {
	data, err := m.get(ctx, "/debug/pprof/heap?debug=1")
	if err != nil {
		return 0, err
	}
	if err := writeExclusive(filepath.Join(m.directory, name+"-heap.txt"), data); err != nil {
		return 0, err
	}
	return allocationTotal(data)
}

func (m *monitor) begin(ctx context.Context) {
	m.sample(ctx, time.Now())
	value, err := m.allocation(ctx, "baseline")
	if err != nil {
		m.allocationError = err.Error()
	} else {
		m.allocationStart = value
	}
}

func (m *monitor) finish(ctx context.Context, o *Observation) error {
	m.sample(ctx, time.Now())
	allocation, err := m.allocation(ctx, "end")
	if m.allocationError != "" {
		o.Metrics["allocation_bytes_per_operation"] = Measurement{Unavailable: m.allocationError}
	} else if err != nil {
		o.Metrics["allocation_bytes_per_operation"] = Measurement{Unavailable: err.Error()}
	} else if allocation < m.allocationStart {
		o.Incidents = append(o.Incidents, Incident{Category: "allocation_counter_reset", Detail: "candidate TotalAlloc decreased"})
	} else {
		v := (allocation - m.allocationStart) / float64(o.CompletedOperations)
		o.Metrics["allocation_bytes_per_operation"] = Measurement{Value: &v}
	}
	progressComplete := true
	progressReason := ""
	for _, sample := range m.samples {
		if sample.ProgressError != "" {
			progressComplete = false
			progressReason = sample.ProgressError
		}
	}
	if progressComplete {
		v := m.maxLagSeconds
		o.Metrics["lag_seconds"] = Measurement{Value: &v}
	} else {
		o.Metrics["lag_seconds"] = Measurement{Unavailable: progressReason}
	}
	syncAgeObserved := false
	for _, sample := range m.samples {
		if sample.SyncAgeSeconds != nil {
			syncAgeObserved = true
		}
	}
	if syncAgeObserved {
		age := m.maxSyncAge
		o.Metrics["replica_sync_age_seconds"] = Measurement{Value: &age}
	}
	var first, last, peak *worker.ComparisonProcessResources
	for _, s := range m.samples {
		if s.Resources == nil {
			continue
		}
		if first == nil {
			first = s.Resources
			peak = s.Resources
		}
		last = s.Resources
		if s.Resources.FDs > peak.FDs {
			peak = s.Resources
		}
	}
	if first != nil && last != nil && first.StartTicks == last.StartTicks {
		growth := float64(last.FDs - first.FDs)
		o.Metrics["fd_growth"] = Measurement{Value: &growth}
		o.ResourceSummary = map[string]float64{"fd_baseline": float64(first.FDs), "fd_peak": float64(peak.FDs), "fd_end": float64(last.FDs)}
	} else {
		o.Metrics["fd_growth"] = Measurement{Unavailable: "candidate FD observations unavailable; see process resource sample errors"}
	}
	for _, message := range m.errors {
		o.Incidents = append(o.Incidents, Incident{Category: "measurement_gap", Detail: message})
	}
	return m.preserve(o)
}

func (m *monitor) syncState(ctx context.Context, wait bool) (uint64, uint64, error) {
	body, err := json.Marshal(map[string]any{"path": m.path, "wait": wait, "timeout": 2})
	if err != nil {
		return 0, 0, err
	}
	request, err := http.NewRequestWithContext(ctx, "POST", m.baseURL+"/sync", bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := m.client.Do(request)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("sync returned %s", response.Status)
	}
	var result struct {
		TXID       *uint64 `json:"txid"`
		Replicated *uint64 `json:"replicated_txid"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return 0, 0, err
	}
	if result.TXID == nil || result.Replicated == nil {
		return 0, 0, errors.New("binary sync response lacks transaction progress")
	}
	return *result.TXID, *result.Replicated, nil
}

func (m *monitor) sync(ctx context.Context) error {
	for {
		txid, replicated, err := m.syncState(ctx, true)
		if err != nil {
			return err
		}
		if txid > 0 && replicated >= txid {
			return nil
		}
		m.sample(ctx, time.Now())
		if err := waitContext(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
}

func recordS3Evidence(directory string, evidence worker.ComparisonS3Evidence, o *Observation) error {
	requests := float64(evidence.Requests)
	transferred := float64(evidence.UploadBytes + evidence.DownloadBytes)
	o.Metrics["object_requests"] = Measurement{Value: &requests}
	o.Metrics["transfer_bytes"] = Measurement{Value: &transferred}
	o.Metrics["replica_bytes"] = Measurement{Unavailable: "S3 retained object size is not transfer bytes; no bucket inventory performed"}
	for _, failure := range evidence.Failures {
		o.Incidents = append(o.Incidents, Incident{Category: "object_request", Detail: failure})
	}
	return writeJSON(filepath.Join(directory, "s3.json"), evidence)
}

func (m *monitor) preserve(o *Observation) error {
	if m.preserved {
		return nil
	}
	o.ProgressSamples = len(m.samples)
	err := writeJSON(filepath.Join(m.directory, "progress.json"), m.samples)
	if err == nil {
		m.preserved = true
	}
	return err
}
