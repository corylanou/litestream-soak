package coverage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func record(backend string, databases int, passed bool, status, mode string) string {
	result := map[string]any{"Passed": passed, "Status": status}
	if mode != "" {
		result["RestoreMode"] = mode
	}
	body, _ := json.Marshal(map[string]any{"backend": backend, "databases": databases, "result": result})
	return string(body)
}

func lines(records ...string) string {
	return strings.Join(records, "\n") + "\n"
}

func TestCheckPassesCompleteCoverage(t *testing.T) {
	t.Parallel()

	results := lines(
		record("sftp", 1, true, "passed", "txid"),
		record("sftp", 1, true, "passed", "timestamp"),
		record("sftp", 1, true, "passed", "latest"),
		record("sftp", 1, false, "aborted", ""),
	)
	summary, err := Check(Input{Results: strings.NewReader(results)}, Requirements{
		Backend: "sftp", MinPassed: 3, Databases: 1, RestoreModes: []string{"txid", "timestamp", "latest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Passed != 3 || summary.RestoreModes["latest"] != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestCheckFailsIncompleteCoverage(t *testing.T) {
	t.Parallel()

	passing := lines(record("file", 1, true, "passed", ""), record("file", 1, true, "passed", ""))
	tests := []struct {
		name      string
		input     Input
		req       Requirements
		wantError string
	}{
		{name: "no results", input: Input{Results: strings.NewReader("")}, req: Requirements{Backend: "file", MinPassed: 1}, wantError: "no verification results"},
		{name: "too few passes", input: Input{Results: strings.NewReader(passing)}, req: Requirements{Backend: "file", MinPassed: 3}, wantError: "2 passed verification cycles, want at least 3"},
		{name: "failed cycle", input: Input{Results: strings.NewReader(passing + record("file", 1, false, "failed", "") + "\n")}, req: Requirements{Backend: "file", MinPassed: 1}, wantError: "1 failed verification cycle"},
		{name: "backend skipped", input: Input{Results: strings.NewReader(passing)}, req: Requirements{Backend: "webdav", MinPassed: 1}, wantError: `backend "file", want "webdav"`},
		{name: "database count", input: Input{Results: strings.NewReader(passing)}, req: Requirements{Backend: "file", MinPassed: 1, Databases: 100}, wantError: "databases 1, want 100"},
		{name: "restore rotation absent", input: Input{Results: strings.NewReader(passing)}, req: Requirements{Backend: "file", MinPassed: 1, RestoreModes: []string{"txid"}}, wantError: "restore mode rotation is not reported"},
		{
			name:      "restore mode missing",
			input:     Input{Results: strings.NewReader(lines(record("file", 1, true, "passed", "txid"), record("file", 1, true, "passed", "latest")))},
			req:       Requirements{Backend: "file", MinPassed: 1, RestoreModes: []string{"txid", "timestamp", "latest"}},
			wantError: "restore mode timestamp never passed",
		},
		{
			name:      "log evidence missing",
			input:     Input{Results: strings.NewReader(passing), Log: "Rotated many database active set\n"},
			req:       Requirements{Backend: "file", MinPassed: 1, LogPatterns: map[string]int{"Rotated many database active set": 2}},
			wantError: `log "Rotated many database active set" seen 1 times, want at least 2`,
		},
		{
			name:      "resource samples missing",
			input:     Input{Results: strings.NewReader(passing), Resources: strings.NewReader(`{"fd":10,"rss_kb":2048}` + "\n")},
			req:       Requirements{Backend: "file", MinPassed: 1, MinResourceSamples: 2},
			wantError: "1 resource samples, want at least 2",
		},
		{name: "malformed result", input: Input{Results: strings.NewReader("{not json\n")}, req: Requirements{Backend: "file", MinPassed: 1}, wantError: "parse result line 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Check(tt.input, tt.req)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Check() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestCheckCountsVerifiedManyDBBatches(t *testing.T) {
	t.Parallel()

	batch := func(errorMessage string) string {
		body, _ := json.Marshal(map[string]any{"backend": "file", "databases": 100, "result": map[string]any{"Passed": false, "Status": "pending", "ErrorMessage": errorMessage}})
		return string(body)
	}
	results := lines(batch(""), batch(""), batch("source boundary unavailable: acquire writer reservation: context deadline exceeded"))

	if _, err := Check(Input{Results: strings.NewReader(results)}, Requirements{Backend: "file", MinPassed: 2, Databases: 100}); err == nil {
		t.Fatal("pending batches must not count without CountBatches")
	}
	summary, err := Check(Input{Results: strings.NewReader(results)}, Requirements{Backend: "file", MinPassed: 2, Databases: 100, CountBatches: true})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Passed != 0 || summary.Batches != 2 || summary.Aborted != 1 {
		t.Fatalf("summary = %+v, want 2 verified batches and 1 inconclusive", summary)
	}
	if _, err := Check(Input{Results: strings.NewReader(results)}, Requirements{Backend: "file", MinPassed: 3, Databases: 100, CountBatches: true}); err == nil || !strings.Contains(err.Error(), "2 passed verification cycles") {
		t.Fatalf("Check() error = %v, want too few verified batches", err)
	}
}

func TestCheckRecordsResourcePeaks(t *testing.T) {
	t.Parallel()

	summary, err := Check(Input{
		Results:   strings.NewReader(lines(record("file", 100, true, "passed", ""))),
		Resources: strings.NewReader(`{"fd":10,"rss_kb":2048}` + "\n" + `{"fd":40,"rss_kb":1024}` + "\n"),
	}, Requirements{Backend: "file", MinPassed: 1, Databases: 100, MinResourceSamples: 2})
	if err != nil {
		t.Fatal(err)
	}
	if summary.MaxFD != 40 || summary.MaxRSSKB != 2048 || summary.ResourceSamples != 2 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestCheckShardsRequiresEveryShardPassed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeShard := func(name, status string) {
		path := filepath.Join(dir, "coverage-"+name, "coverage.json")
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(ShardReport{Shard: name, Status: status})
		if err := os.WriteFile(path, body, 0644); err != nil {
			t.Fatal(err)
		}
	}
	writeShard("backend-file", "passed")
	writeShard("backend-sftp", "failed")

	err := CheckShards(dir, []string{"backend-file", "backend-sftp", "backend-nats"})
	if err == nil {
		t.Fatal("CheckShards() error = nil, want failures")
	}
	for _, want := range []string{"backend-sftp: status failed", "backend-nats: no coverage report"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("CheckShards() error = %v, want %q", err, want)
		}
	}

	writeShard("backend-sftp", "passed")
	writeShard("backend-nats", "passed")
	if err := CheckShards(dir, []string{"backend-file", "backend-sftp", "backend-nats"}); err != nil {
		t.Fatal(err)
	}
	if err := CheckShards(dir, nil); err == nil {
		t.Fatal("CheckShards() with no expected shards must fail")
	}
}
