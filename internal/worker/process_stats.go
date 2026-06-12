package worker

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type processStatsSnapshot struct {
	LitestreamRSSBytes        int64
	LitestreamCPUSecondsTotal float64
	LitestreamGoroutines      int
	LitestreamFDs             int
	WorkerRSSBytes            int64
	WorkerFDs                 int
}

func collectProcessStats(litestreamPID int) processStatsSnapshot {
	workerRSS, _, workerFDs := readProcStats(os.Getpid())
	ltRSS, ltCPU, ltFDs := readProcStats(litestreamPID)
	return processStatsSnapshot{
		LitestreamRSSBytes:        ltRSS,
		LitestreamCPUSecondsTotal: ltCPU,
		LitestreamFDs:             ltFDs,
		WorkerRSSBytes:            workerRSS,
		WorkerFDs:                 workerFDs,
	}
}

func readProcStats(pid int) (rssBytes int64, cpuSeconds float64, fds int) {
	if pid <= 0 {
		return 0, 0, 0
	}

	status, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
	if err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if !strings.HasPrefix(line, "VmRSS:") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, parseErr := strconv.ParseInt(fields[1], 10, 64); parseErr == nil {
					rssBytes = kb * 1024
				}
			}
			break
		}
	}

	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err == nil {
		fields := strings.Fields(string(stat))
		if len(fields) >= 15 {
			utime, _ := strconv.ParseFloat(fields[13], 64)
			stime, _ := strconv.ParseFloat(fields[14], 64)
			cpuSeconds = (utime + stime) / 100
		}
	}

	entries, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(pid), "fd"))
	if err == nil {
		fds = len(entries)
	}
	return rssBytes, cpuSeconds, fds
}
