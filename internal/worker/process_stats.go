package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

func readProcStatsAt(root string, pid int) (rssBytes int64, cpuSeconds float64, fds int, start string, err error) {
	if pid <= 0 {
		return 0, 0, 0, "", fmt.Errorf("process not running")
	}
	dir := filepath.Join(root, strconv.Itoa(pid))
	stat, err := os.ReadFile(filepath.Join(dir, "stat"))
	if err != nil {
		return 0, 0, 0, "", err
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return 0, 0, 0, "", fmt.Errorf("invalid process stat")
	}
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) < 22 {
		return 0, 0, 0, "", fmt.Errorf("short process stat")
	}
	start = fields[19]
	if _, err := strconv.ParseUint(start, 10, 64); err != nil {
		return 0, 0, 0, "", err
	}
	user, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return 0, 0, 0, start, err
	}
	system, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return 0, 0, 0, start, err
	}
	pages, err := strconv.ParseInt(fields[21], 10, 64)
	if err != nil || pages < 0 {
		return 0, 0, 0, start, fmt.Errorf("invalid RSS pages")
	}
	entries, err := os.ReadDir(filepath.Join(dir, "fd"))
	if err != nil {
		return 0, 0, 0, start, err
	}
	check, err := os.ReadFile(filepath.Join(dir, "stat"))
	if err != nil {
		return 0, 0, 0, "", err
	}
	end = strings.LastIndexByte(string(check), ')')
	if end < 0 {
		return 0, 0, 0, "", fmt.Errorf("invalid process identity")
	}
	identity := strings.Fields(string(check[end+1:]))
	if len(identity) < 20 || identity[19] != start {
		return 0, 0, 0, "", fmt.Errorf("process identity changed during collection")
	}
	return pages * int64(os.Getpagesize()), (float64(user) + float64(system)) / 100, len(entries), fields[19], nil
}

func processCollectionSupported() bool { return runtime.GOOS == "linux" }
