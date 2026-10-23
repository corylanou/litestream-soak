package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/corylanou/litestream-soak/internal/coverage"
)

type logPatterns map[string]int

func (p logPatterns) String() string { return fmt.Sprint(map[string]int(p)) }

func (p logPatterns) Set(value string) error {
	pattern, count, ok := strings.Cut(value, "=")
	if !ok {
		return fmt.Errorf("want pattern=count, got %q", value)
	}
	n, err := strconv.Atoi(count)
	if err != nil || n < 1 {
		return fmt.Errorf("invalid count in %q", value)
	}
	p[pattern] = n
	return nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "soakcoverage:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: soakcoverage check|shards [flags]")
	}
	switch args[0] {
	case "check":
		return runCheck(args[1:], stdout)
	case "shards":
		return runShards(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runCheck(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	shard := fs.String("shard", "", "shard name")
	resultsPath := fs.String("results", "", "verification results JSONL")
	logPath := fs.String("log", "", "worker log")
	resourcesPath := fs.String("resources", "", "resource samples JSONL")
	reportPath := fs.String("report", "", "coverage report output")
	backend := fs.String("backend", "", "expected replica backend")
	minPassed := fs.Int("min-passed", 1, "minimum passed verification cycles")
	databases := fs.Int("databases", 0, "expected database count")
	restoreModes := fs.String("restore-modes", "", "comma-separated restore modes that must pass")
	minResources := fs.Int("min-resource-samples", 0, "minimum resource samples")
	patterns := logPatterns{}
	fs.Var(patterns, "require-log", "log substring=minimum count (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shard == "" || *backend == "" || *resultsPath == "" {
		return errors.New("check requires -shard, -backend and -results")
	}

	req := coverage.Requirements{
		Backend:            *backend,
		MinPassed:          *minPassed,
		Databases:          *databases,
		LogPatterns:        patterns,
		MinResourceSamples: *minResources,
	}
	if *restoreModes != "" {
		req.RestoreModes = strings.Split(*restoreModes, ",")
	}

	summary, err := check(*resultsPath, *logPath, *resourcesPath, req)
	report := coverage.ShardReport{Shard: *shard, Status: "passed", Summary: summary}
	if err != nil {
		report.Status = "failed"
		report.Error = err.Error()
	}
	body, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		return errors.Join(err, marshalErr)
	}
	_, _ = fmt.Fprintln(stdout, string(body))
	if *reportPath != "" {
		if writeErr := writeReport(*reportPath, body); writeErr != nil {
			return errors.Join(err, writeErr)
		}
	}
	return err
}

func check(resultsPath, logPath, resourcesPath string, req coverage.Requirements) (coverage.Summary, error) {
	results, err := os.Open(resultsPath)
	if err != nil {
		return coverage.Summary{}, fmt.Errorf("no verification results recorded: %w", err)
	}
	defer func() { _ = results.Close() }()
	input := coverage.Input{Results: results}
	if logPath != "" {
		body, err := os.ReadFile(logPath)
		if err != nil {
			return coverage.Summary{}, fmt.Errorf("read worker log: %w", err)
		}
		input.Log = string(body)
	}
	if resourcesPath != "" {
		resources, err := os.Open(resourcesPath)
		if err != nil {
			return coverage.Summary{}, fmt.Errorf("resource samples missing: %w", err)
		}
		defer func() { _ = resources.Close() }()
		input.Resources = resources
	}
	return coverage.Check(input, req)
}

func writeReport(path string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, append(body, '\n'), 0644)
}

func runShards(args []string) error {
	fs := flag.NewFlagSet("shards", flag.ContinueOnError)
	dir := fs.String("dir", "", "directory of downloaded shard reports")
	expect := fs.String("expect", "", "space- or comma-separated expected shards")
	if err := fs.Parse(args); err != nil {
		return err
	}
	expected := strings.FieldsFunc(*expect, func(r rune) bool { return r == ',' || r == ' ' || r == '\n' })
	return coverage.CheckShards(*dir, expected)
}
