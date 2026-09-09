package worker

import (
	"fmt"
)

type ComparisonProcessResources struct {
	RSSBytes   int64   `json:"rss_bytes"`
	CPUSeconds float64 `json:"cpu_seconds"`
	FDs        int     `json:"fds"`
	StartTicks string  `json:"start_ticks"`
}

func ReadComparisonProcessResources(pid int) (ComparisonProcessResources, error) {
	if !processCollectionSupported() {
		return ComparisonProcessResources{}, fmt.Errorf("process resource collection requires Linux /proc")
	}
	rss, cpu, fds, start, err := readProcStatsAt("/proc", pid)
	return ComparisonProcessResources{RSSBytes: rss, CPUSeconds: cpu, FDs: fds, StartTicks: start}, err
}
