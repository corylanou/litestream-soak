package worker

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var processObservationStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "soak_process_observation_status",
	Help: "Process collection status: 0 unavailable, 1 fresh, 2 stale, 3 unsupported. CPU is cumulative seconds for the current process lifetime; RSS is bytes; FDs is a count.",
}, []string{"worker_id", "profile", "source", "region", "process"})

func (p *statsPoller) pollProcessObservations(at time.Time) {
	p.collectProcessObservations("/proc", processCollectionSupported(), at)
}

func (p *statsPoller) collectProcessObservations(root string, supported bool, at time.Time) {
	pid := p.currentLitestreamPID()
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	collect := func(name string, pid int, observation *reporting.ProcessObservation, rss *int64, cpu *float64, fds *int) {
		previous := *observation
		observation.PID = pid
		observation.Status = "unavailable"
		status := 0.0
		if !supported {
			observation.Status = "unsupported"
			status = 3
		} else {
			newRSS, newCPU, newFDs, start, err := readProcStatsAt(root, pid)
			if err == nil {
				*rss, *cpu, *fds = newRSS, newCPU, newFDs
				*observation = reporting.ProcessObservation{Status: "fresh", PID: pid, StartTicks: start, CollectedAt: at}
				status = 1
			} else if previous.PID == pid && start != "" && previous.StartTicks == start && !previous.CollectedAt.IsZero() {
				observation.Status = "stale"
				status = 2
			}
		}
		if status == 0 || status == 3 {
			*rss, *cpu, *fds = 0, 0, 0
			observation.CollectedAt = time.Time{}
			observation.StartTicks = ""
		}
		processObservationStatus.WithLabelValues(append(currentMetricLabels(), name)...).Set(status)
	}
	collect("litestream", pid, &p.snapshot.LitestreamProcess, &p.snapshot.LitestreamRSSBytes, &p.snapshot.LitestreamCPUSecondsTotal, &p.snapshot.LitestreamFDs)
	var workerCPU float64
	collect("worker", os.Getpid(), &p.snapshot.WorkerProcess, &p.snapshot.WorkerRSSBytes, &workerCPU, &p.snapshot.WorkerFDs)
	p.publishProcessObservations()
}

func (p *statsPoller) publishProcessObservations() {
	labels := currentMetricLabels()
	value := func(status string, n float64) float64 {
		if status != "fresh" {
			return math.NaN()
		}
		return n
	}
	litestreamRSSBytes.WithLabelValues(labels...).Set(value(p.snapshot.LitestreamProcess.Status, float64(p.snapshot.LitestreamRSSBytes)))
	litestreamCPUSeconds.WithLabelValues(labels...).Set(value(p.snapshot.LitestreamProcess.Status, p.snapshot.LitestreamCPUSecondsTotal))
	litestreamFDs.WithLabelValues(labels...).Set(value(p.snapshot.LitestreamProcess.Status, float64(p.snapshot.LitestreamFDs)))
	workerRSSBytes.WithLabelValues(labels...).Set(value(p.snapshot.WorkerProcess.Status, float64(p.snapshot.WorkerRSSBytes)))
	workerFDs.WithLabelValues(labels...).Set(value(p.snapshot.WorkerProcess.Status, float64(p.snapshot.WorkerFDs)))
}

func (p *statsPoller) processSnapshot() processStatsSnapshot {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	return processStatsSnapshot{
		LitestreamRSSBytes:        p.snapshot.LitestreamRSSBytes,
		LitestreamCPUSecondsTotal: p.snapshot.LitestreamCPUSecondsTotal,
		LitestreamFDs:             p.snapshot.LitestreamFDs,
		WorkerRSSBytes:            p.snapshot.WorkerRSSBytes,
		WorkerFDs:                 p.snapshot.WorkerFDs,
	}
}

type localStateFrame struct {
	dir     *os.File
	entries []os.DirEntry
	ltxRoot string
}

type localStateScanner struct {
	paths []string
	next  int
	stack []localStateFrame
	total int64
	ltx   int64
}

func (s *localStateScanner) close() {
	for _, frame := range s.stack {
		_ = frame.dir.Close()
	}
	s.stack = nil
}

func (s *localStateScanner) step(remaining int, deadline time.Time) (bool, error) {
	for remaining > 0 && time.Now().Before(deadline) {
		remaining--
		if len(s.stack) == 0 {
			if s.next == len(s.paths) {
				return true, nil
			}
			root := litestreamStateDir(s.paths[s.next])
			s.next++
			info, err := os.Lstat(root)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return false, err
			}
			if !info.IsDir() {
				return false, fmt.Errorf("local state root is not a directory")
			}
			dir, err := os.Open(root)
			if err != nil {
				return false, err
			}
			s.stack = append(s.stack, localStateFrame{dir: dir, ltxRoot: filepath.Join(root, "ltx") + string(filepath.Separator)})
			continue
		}
		frame := &s.stack[len(s.stack)-1]
		if len(frame.entries) == 0 {
			entries, err := frame.dir.ReadDir(64)
			if err != nil && err != io.EOF {
				return false, err
			}
			if len(entries) == 0 {
				if err := frame.dir.Close(); err != nil {
					return false, err
				}
				s.stack = s.stack[:len(s.stack)-1]
				continue
			}
			frame.entries = entries
		}
		entry := frame.entries[0]
		frame.entries = frame.entries[1:]
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		child := filepath.Join(frame.dir.Name(), entry.Name())
		if entry.IsDir() {
			if len(s.stack) >= 64 {
				return false, fmt.Errorf("local state directory depth exceeded")
			}
			dir, err := os.Open(child)
			if err != nil {
				return false, err
			}
			s.stack = append(s.stack, localStateFrame{dir: dir, ltxRoot: frame.ltxRoot})
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return false, err
		}
		s.total += info.Size()
		if strings.HasPrefix(child, frame.ltxRoot) {
			s.ltx += info.Size()
		}
	}
	return false, nil
}

func localStateSizes(paths []string, remaining int, deadline time.Time) (int64, int64, error) {
	scanner := localStateScanner{paths: paths}
	defer scanner.close()
	complete, err := scanner.step(remaining, deadline)
	if err != nil {
		return 0, 0, err
	}
	if !complete {
		return 0, 0, fmt.Errorf("local state scan budget exceeded")
	}
	return scanner.total, scanner.ltx, nil
}

var localStateObservationStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "soak_local_state_observation_status",
	Help: "Local state collection status: 0 unavailable, 1 fresh, 2 stale. Totals cover all configured database state directories; LTX bytes are included in total bytes.",
}, []string{"worker_id", "profile", "source", "region"})

var localStateObservationTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "soak_local_state_observation_unixtime",
	Help: "Unix timestamp of the last complete local state scan; NaN until observed. Scan slices have a soft 250ms budget between filesystem calls.",
}, []string{"worker_id", "profile", "source", "region"})

func (p *statsPoller) publishLocalStateObservation() {
	labels := currentMetricLabels()
	status, total, ltx, at := 0.0, math.NaN(), math.NaN(), math.NaN()
	if !p.snapshot.LocalStateCollectedAt.IsZero() {
		at = float64(p.snapshot.LocalStateCollectedAt.Unix())
	}
	switch p.snapshot.LocalStateStatus {
	case "fresh":
		status, total, ltx = 1, float64(p.snapshot.LitestreamDirSizeBytes), float64(p.snapshot.LitestreamLTXSizeBytes)
	case "stale":
		status = 2
	}
	localStateObservationStatus.WithLabelValues(labels...).Set(status)
	localStateObservationTime.WithLabelValues(labels...).Set(at)
	litestreamDirSize.WithLabelValues(labels...).Set(total)
	litestreamLTXSize.WithLabelValues(labels...).Set(ltx)
}
