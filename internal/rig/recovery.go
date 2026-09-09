package rig

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RecoveryPhase struct {
	Name      string        `json:"name"`
	Duration  time.Duration `json:"duration_ns"`
	WriteRate int           `json:"write_rate"`
	Latency   time.Duration `json:"latency_ns"`
	Status    int           `json:"status,omitempty"`
}

type RecoveryPhases []RecoveryPhase

func RecoverySchedule(duration time.Duration, writeRate int) RecoveryPhases {
	return RecoveryPhases{
		{Name: "idle", Duration: duration},
		{Name: "offline", Duration: duration, WriteRate: writeRate, Status: http.StatusServiceUnavailable},
		{Name: "latency", Duration: duration, WriteRate: writeRate, Latency: duration / 4},
		{Name: "throttle", Duration: duration, WriteRate: writeRate, Status: http.StatusTooManyRequests},
		{Name: "drain", Duration: duration, WriteRate: writeRate},
	}
}

func (s RecoveryPhases) At(elapsed time.Duration) RecoveryPhase {
	for _, p := range s {
		if elapsed < p.Duration {
			return p
		}
		elapsed -= p.Duration
	}
	return RecoveryPhase{Name: "complete"}
}

type RecoveryRequest struct {
	Injected        bool    `json:"injected"`
	ElapsedSeconds  float64 `json:"elapsed_seconds"`
	Phase           string  `json:"phase"`
	Method          string  `json:"method"`
	Path            string  `json:"path"`
	SDKRequest      string  `json:"sdk_request,omitempty"`
	Status          int     `json:"status"`
	DurationSeconds float64 `json:"duration_seconds"`
	Error           string  `json:"error,omitempty"`
}

type RecoveryHandler struct {
	OnRequest func(RecoveryRequest)
	schedule  RecoveryPhases
	elapsed   func() time.Duration
	next      http.Handler
	mu        sync.Mutex
	events    []RecoveryRequest
}

func NewRecoveryHandler(schedule RecoveryPhases, elapsed func() time.Duration, next http.Handler) *RecoveryHandler {
	return &RecoveryHandler{schedule: schedule, elapsed: elapsed, next: next}
}

func (h *RecoveryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	elapsed := h.elapsed()
	p := h.schedule.At(elapsed)
	event := RecoveryRequest{ElapsedSeconds: elapsed.Seconds(), Phase: p.Name, Method: r.Method, Path: r.URL.Path, SDKRequest: r.Header.Get("Amz-Sdk-Request")}
	defer func() {
		event.DurationSeconds = time.Since(start).Seconds()
		h.mu.Lock()
		h.events = append(h.events, event)
		h.mu.Unlock()
		if h.OnRequest != nil {
			h.OnRequest(event)
		}
	}()
	if p.Latency > 0 {
		timer := time.NewTimer(p.Latency)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			event.Error = r.Context().Err().Error()
			return
		case <-timer.C:
		}
	}
	if p.Status != 0 {
		event.Status = p.Status
		event.Injected = true
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(p.Status)
		_, err := w.Write([]byte("<Error><Code>SlowDown</Code><Message>scheduled connectivity fault</Message></Error>"))
		if err != nil {
			event.Error = err.Error()
		}
		return
	}
	recorder := &recoveryResponse{ResponseWriter: w, status: http.StatusOK}
	h.next.ServeHTTP(recorder, r)
	event.Status = recorder.status
}

type recoveryResponse struct {
	http.ResponseWriter
	status int
}

func (w *recoveryResponse) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *recoveryResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (h *RecoveryHandler) Events() []RecoveryRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]RecoveryRequest(nil), h.events...)
}

func (h *RecoveryHandler) Engaged(phase string) bool {
	for _, e := range h.Events() {
		if e.Phase == phase && e.Error == "" {
			return true
		}
	}
	return false
}

type RecoverySample struct {
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	CommittedRows  int64   `json:"committed_rows"`
	ReplicatedRows int64   `json:"replicated_rows"`
	DiskBytes      int64   `json:"disk_bytes"`
	MemoryBytes    uint64  `json:"memory_bytes"`
}

type RecoveryMeasurements struct {
	Samples        []RecoverySample `json:"samples"`
	MaxLagRows     int64            `json:"max_lag_rows"`
	MaxDiskBytes   int64            `json:"max_disk_bytes"`
	MaxMemoryBytes uint64           `json:"max_memory_bytes"`
}

func (m *RecoveryMeasurements) Observe(s RecoverySample) {
	m.Samples = append(m.Samples, s)
	m.MaxLagRows = max(m.MaxLagRows, s.CommittedRows-s.ReplicatedRows)
	m.MaxDiskBytes = max(m.MaxDiskBytes, s.DiskBytes)
	m.MaxMemoryBytes = max(m.MaxMemoryBytes, s.MemoryBytes)
}

func (m RecoveryMeasurements) CatchUpRowsPerSecond() float64 {
	if len(m.Samples) < 2 {
		return 0
	}
	first, last := m.Samples[0], m.Samples[len(m.Samples)-1]
	if last.ElapsedSeconds <= first.ElapsedSeconds {
		return 0
	}
	return float64(last.ReplicatedRows-first.ReplicatedRows) / (last.ElapsedSeconds - first.ElapsedSeconds)
}

func (m RecoveryMeasurements) NetDrainRowsPerSecond() float64 {
	if len(m.Samples) < 2 {
		return 0
	}
	first, last := m.Samples[0], m.Samples[len(m.Samples)-1]
	if last.ElapsedSeconds <= first.ElapsedSeconds {
		return 0
	}
	return float64((first.CommittedRows-first.ReplicatedRows)-(last.CommittedRows-last.ReplicatedRows)) / (last.ElapsedSeconds - first.ElapsedSeconds)
}

func BacklogVerdict(engaged, validated, drained bool, failures int) string {
	if !engaged || !validated || !drained {
		return "inconclusive"
	}
	if failures > 0 {
		return "recovered_with_incidents"
	}
	return "scenario_success"
}

func (m RecoveryMeasurements) DrainedWhileWriting() bool {
	if len(m.Samples) < 2 {
		return false
	}
	first, last := m.Samples[0], m.Samples[len(m.Samples)-1]
	return last.ElapsedSeconds > first.ElapsedSeconds && last.CommittedRows > first.CommittedRows && (m.NetDrainRowsPerSecond() > 0 || last.ReplicatedRows == last.CommittedRows)
}

type RecoveryRequestSummary struct {
	TransportFailures      int `json:"transport_failures"`
	InjectedFailures       int `json:"injected_failures"`
	UnexpectedHTTPFailures int `json:"unexpected_http_failures"`
	SDKRetries             int `json:"sdk_retries"`
}

func SummarizeRecoveryRequests(events []RecoveryRequest) RecoveryRequestSummary {
	var summary RecoveryRequestSummary
	for _, e := range events {
		if e.Error != "" {
			summary.TransportFailures++
		}
		if e.Status >= 400 {
			if e.Injected {
				summary.InjectedFailures++
			} else {
				summary.UnexpectedHTTPFailures++
			}
		}
		for _, part := range strings.Split(e.SDKRequest, ";") {
			key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
			if !ok || key != "attempt" {
				continue
			}
			if n, err := strconv.Atoi(value); err == nil && n > 1 {
				summary.SDKRetries++
			}
		}
	}
	return summary
}

func RecoveryLimits(workDir, cpu, memory, mounts string) map[string]string {
	limits := map[string]string{"work_directory": workDir, "cpu": "unsupported: cgroup v2 CPU limit unavailable", "memory": "unsupported: cgroup v2 memory limit unavailable", "disk": "unsupported: fixture mount unavailable"}
	if cpu = strings.TrimSpace(cpu); cpu != "" {
		limits["cpu"] = cpu
		if strings.HasPrefix(cpu, "max ") {
			limits["cpu"] = "unenforced: " + cpu
		}
	}
	if memory = strings.TrimSpace(memory); memory != "" {
		limits["memory"] = memory
		if memory == "max" {
			limits["memory"] = "unenforced: max"
		}
	}
	longest := -1
	for _, line := range strings.Split(mounts, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		mount := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\134`, `\`).Replace(fields[1])
		if workDir != mount && !strings.HasPrefix(workDir, strings.TrimRight(mount, "/")+"/") {
			continue
		}
		if len(mount) <= longest {
			continue
		}
		longest = len(mount)
		limits["mount"] = mount
		limits["filesystem"] = fields[2]
		limits["disk"] = "unenforced: no per-fixture disk quota observed"
		if fields[2] == "tmpfs" {
			limits["disk"] = fields[3]
		}
	}
	return limits
}
