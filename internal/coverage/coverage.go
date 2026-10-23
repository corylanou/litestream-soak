package coverage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Requirements struct {
	Backend            string
	MinPassed          int
	Databases          int
	RestoreModes       []string
	LogPatterns        map[string]int
	MinResourceSamples int
}

type Input struct {
	Results   io.Reader
	Log       string
	Resources io.Reader
}

type Summary struct {
	Passed          int            `json:"passed"`
	Failed          int            `json:"failed"`
	Aborted         int            `json:"aborted"`
	RestoreModes    map[string]int `json:"restore_modes,omitempty"`
	ResourceSamples int            `json:"resource_samples"`
	MaxFD           int            `json:"max_fd"`
	MaxRSSKB        int64          `json:"max_rss_kb"`
}

type ShardReport struct {
	Shard   string  `json:"shard"`
	Status  string  `json:"status"`
	Error   string  `json:"error,omitempty"`
	Summary Summary `json:"summary"`
}

type resultRecord struct {
	Backend   string `json:"backend"`
	Databases int    `json:"databases"`
	Result    struct {
		Passed       bool
		Status       string
		ErrorMessage string
		RestoreMode  *string
	} `json:"result"`
}

type resourceSample struct {
	FD    int   `json:"fd"`
	RSSKB int64 `json:"rss_kb"`
}

func Check(input Input, req Requirements) (Summary, error) {
	summary := Summary{RestoreModes: map[string]int{}}
	var problems []error
	modeReported := false
	records := 0
	scanner := bufio.NewScanner(input.Results)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var record resultRecord
		if err := json.Unmarshal([]byte(text), &record); err != nil {
			return summary, fmt.Errorf("parse result line %d: %w", line, err)
		}
		records++
		if record.Backend != req.Backend {
			problems = append(problems, fmt.Errorf("result line %d has backend %q, want %q", line, record.Backend, req.Backend))
		}
		if req.Databases > 0 && record.Databases != req.Databases {
			problems = append(problems, fmt.Errorf("result line %d has databases %d, want %d", line, record.Databases, req.Databases))
		}
		if record.Result.RestoreMode != nil {
			modeReported = true
		}
		switch {
		case record.Result.Passed:
			summary.Passed++
			if record.Result.RestoreMode != nil {
				summary.RestoreModes[*record.Result.RestoreMode]++
			}
		case record.Result.Status == "aborted" || record.Result.Status == "pending":
			summary.Aborted++
		default:
			summary.Failed++
		}
	}
	if err := scanner.Err(); err != nil {
		return summary, fmt.Errorf("read results: %w", err)
	}
	if records == 0 {
		return summary, errors.New("no verification results recorded")
	}
	if summary.Failed > 0 {
		problems = append(problems, fmt.Errorf("%d failed verification cycle(s)", summary.Failed))
	}
	if summary.Passed < req.MinPassed {
		problems = append(problems, fmt.Errorf("%d passed verification cycles, want at least %d", summary.Passed, req.MinPassed))
	}
	if len(req.RestoreModes) > 0 {
		if !modeReported {
			problems = append(problems, errors.New("restore mode rotation is not reported by this worker build"))
		} else {
			for _, mode := range req.RestoreModes {
				if summary.RestoreModes[mode] == 0 {
					problems = append(problems, fmt.Errorf("restore mode %s never passed", mode))
				}
			}
		}
	}
	for pattern, want := range req.LogPatterns {
		if got := strings.Count(input.Log, pattern); got < want {
			problems = append(problems, fmt.Errorf("log %q seen %d times, want at least %d", pattern, got, want))
		}
	}
	if req.MinResourceSamples > 0 {
		if err := readResources(input.Resources, &summary); err != nil {
			problems = append(problems, err)
		} else if summary.ResourceSamples < req.MinResourceSamples {
			problems = append(problems, fmt.Errorf("%d resource samples, want at least %d", summary.ResourceSamples, req.MinResourceSamples))
		}
	}
	return summary, errors.Join(problems...)
}

func readResources(r io.Reader, summary *Summary) error {
	if r == nil {
		return errors.New("resource samples missing")
	}
	scanner := bufio.NewScanner(r)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var sample resourceSample
		if err := json.Unmarshal([]byte(text), &sample); err != nil {
			return fmt.Errorf("parse resource line %d: %w", line, err)
		}
		summary.ResourceSamples++
		summary.MaxFD = max(summary.MaxFD, sample.FD)
		summary.MaxRSSKB = max(summary.MaxRSSKB, sample.RSSKB)
	}
	return scanner.Err()
}

func CheckShards(dir string, expected []string) error {
	if len(expected) == 0 {
		return errors.New("no expected coverage shards")
	}
	var problems []error
	for _, shard := range expected {
		body, err := os.ReadFile(filepath.Join(dir, "coverage-"+shard, "coverage.json"))
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: no coverage report", shard))
			continue
		}
		var report ShardReport
		if err := json.Unmarshal(body, &report); err != nil {
			problems = append(problems, fmt.Errorf("%s: parse coverage report: %w", shard, err))
			continue
		}
		if report.Shard != shard {
			problems = append(problems, fmt.Errorf("%s: report names shard %q", shard, report.Shard))
		}
		if report.Status != "passed" {
			problems = append(problems, fmt.Errorf("%s: status %s %s", shard, report.Status, report.Error))
		}
	}
	return errors.Join(problems...)
}
