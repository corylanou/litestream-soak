package compare

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func localFixture(t *testing.T) (Plan, Local) {
	t.Helper()
	dir := t.TempDir()
	fixture := filepath.Join(dir, "fixture.db")
	if err := CreateFixture(context.Background(), fixture, 42, 16); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	p := pinnedPlan(t)
	p.Contract.Hardware, err = LocalHardware()
	if err != nil {
		t.Fatal(err)
	}
	p.Contract.Region = "local"
	p.Contract.Toolchain = runtime.Version()
	p.Contract.FixtureSHA256 = fmt.Sprintf("%x", sha256.Sum256(data))
	p.Contract.FixtureBytes = int64(len(data))
	p.Contract.ConfigSHA256 = LocalConfigSHA256()
	p.Contract.OperationBudget = 8
	p.Contract.GeneratorSHA = strings.Repeat("b", 40)
	p.Contract.OracleSHA = p.Contract.GeneratorSHA
	return p, Local{Fixture: fixture, Directory: filepath.Join(dir, "runs"), HarnessSHA: p.Contract.GeneratorSHA, Timeout: 10 * time.Second}
}

func TestLocalControlExecutesExactBudgetAndRetainsEvidence(t *testing.T) {
	p, l := localFixture(t)
	requests, err := Schedule(p)
	if err != nil {
		t.Fatal(err)
	}
	var r Request
	for _, candidate := range requests {
		if candidate.Scenario == "no-litestream" {
			r = candidate
			break
		}
	}
	o, err := l.Execute(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if o.CompletedOperations != 8 || o.Correctness != "pass" || o.Metrics["latency_p99_seconds"].Value == nil {
		t.Fatalf("observation=%+v", o)
	}
	if _, err := os.Stat(filepath.Join(l.Directory, r.ReplicaPrefix, "harness.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(l.Directory, r.ReplicaPrefix, "observation.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Execute(context.Background(), r); err == nil {
		t.Fatal("overwrote existing run")
	}
}

func TestLocalRejectsChangedFixtureAndHarness(t *testing.T) {
	for _, field := range []string{"fixture", "harness", "config", "binary"} {
		t.Run(field, func(t *testing.T) {
			p, l := localFixture(t)
			runs, err := Schedule(p)
			if err != nil {
				t.Fatal(err)
			}
			r := runs[0]
			switch field {
			case "fixture":
				r.Contract.FixtureSHA256 = strings.Repeat("0", 64)
			case "harness":
				l.HarnessSHA = strings.Repeat("0", 40)
			case "config":
				r.Contract.ConfigSHA256 = strings.Repeat("0", 64)
			}
			if _, err := l.Execute(context.Background(), r); err == nil {
				t.Fatal("accepted invalid local execution")
			}
		})
	}
}

func TestLocalRealBinary(t *testing.T) {
	binary := os.Getenv("SOAK_COMPARE_TEST_BINARY")
	sha := os.Getenv("SOAK_COMPARE_TEST_SHA")
	if binary == "" || sha == "" {
		t.Skip("requires explicitly supplied real Litestream binary and SHA")
	}
	p, l := localFixture(t)
	p.Baseline = sha
	p.Candidate = sha
	p.MainSHA = sha
	p.Repeats = 2
	if budget := os.Getenv("SOAK_COMPARE_TEST_BUDGET"); budget != "" {
		value, err := strconv.ParseInt(budget, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		p.Contract.OperationBudget = value
	}
	if endpoint := os.Getenv("SOAK_COMPARE_TEST_S3_ENDPOINT"); endpoint != "" {
		p.Contract.Backend = "s3"
		p.Contract.ReplicaEndpoint = endpoint
		p.Contract.ReplicaBucket = "litestream-soak"
		p.Contract.ReplicaRegion = "us-east-1"
	}
	l.Binaries = map[string]string{sha: binary}
	l.Timeout = 30 * time.Second
	if timeout := os.Getenv("SOAK_COMPARE_TEST_TIMEOUT"); timeout != "" {
		value, err := time.ParseDuration(timeout)
		if err != nil {
			t.Fatal(err)
		}
		l.Timeout = value
	}
	report, err := Run(context.Background(), p, l.Execute)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range report.Observations {
		if o.Error != "" || o.Correctness != "pass" {
			t.Errorf("%s: correctness=%s error=%s", o.Request.ReplicaPrefix, o.Correctness, o.Error)
		}
	}
	if len(report.Observations) != 32 {
		t.Fatalf("executions=%d", len(report.Observations))
	}
	if output := os.Getenv("SOAK_COMPARE_TEST_REPORT"); output != "" {
		if err := writeJSON(output, report); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLocalRejectsUnmatchedRuntime(t *testing.T) {
	for _, field := range []string{"hardware", "region", "toolchain"} {
		t.Run(field, func(t *testing.T) {
			p, l := localFixture(t)
			runs, err := Schedule(p)
			if err != nil {
				t.Fatal(err)
			}
			r := runs[0]
			switch field {
			case "hardware":
				r.Contract.Hardware = "other-host"
			case "region":
				r.Contract.Region = "other-region"
			case "toolchain":
				r.Contract.Toolchain = "go0"
			}
			if _, err := l.Execute(context.Background(), r); err == nil || !strings.Contains(err.Error(), "runtime") {
				t.Fatalf("runtime mismatch not rejected: %v", err)
			}
		})
	}
}

func TestOracleRejectsMissingChangedAndExtraRows(t *testing.T) {
	for _, statement := range []string{"DELETE FROM operations WHERE id=0", "UPDATE operations SET value=x'ff' WHERE id=0", "INSERT INTO operations VALUES(99,x'ff')", "UPDATE fixture SET value=x'ff' WHERE id=0"} {
		t.Run(statement, func(t *testing.T) {
			p, l := localFixture(t)
			runs, err := Schedule(p)
			if err != nil {
				t.Fatal(err)
			}
			var r Request
			for _, request := range runs {
				if request.Scenario == "no-litestream" {
					r = request
					break
				}
			}
			if _, err := l.Execute(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(l.Directory, r.ReplicaPrefix, "source.db")
			db, err := sql.Open("sqlite", target)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := validateLocal(context.Background(), l.Fixture, target, p.Contract.Seed, p.Contract.OperationBudget, 1024); err == nil {
				t.Fatal("oracle accepted corruption")
			}
		})
	}
}

func TestLatencyPercentileIncludesSmallSampleTail(t *testing.T) {
	if got := percentile([]float64{1, 9}, 0.99); got != 9 {
		t.Fatalf("p99=%v, want slowest sample", got)
	}
}

func TestLocalRejectsUnpinnedS3Destination(t *testing.T) {
	p, l := localFixture(t)
	p.Contract.Backend = "s3"
	p.Contract.ReplicaEndpoint = "https://user:secret@example.com"
	p.Contract.ReplicaBucket = "example"
	p.Contract.ReplicaRegion = "us-east-1"
	if _, err := Schedule(p); err == nil {
		t.Fatal("accepted credential-bearing endpoint")
	}
	_ = l
}

func TestDiskGrowthIncludesLitestreamStagingButExcludesEvidence(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".source.db-litestream"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, size := range map[string]int{"source.db": 100, ".source.db-litestream/staged.ltx": 50, "restored.db": 1000, "replicate.log": 2000} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	size, err := dataBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if size != 150 {
		t.Fatalf("data size=%d, want source plus staging", size)
	}
}

func TestLogIncidentsSurviveFailedRun(t *testing.T) {
	dir := t.TempDir()
	line := "time=2026-09-09T00:00:00Z level=ERROR msg=failed\ntime=2026-09-09T00:00:01Z level=INFO msg=recovered\n"
	if err := os.WriteFile(filepath.Join(dir, "replicate.log"), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	o := Observation{Error: "shutdown failed"}
	if err := recordLogEvidence(dir, &o); err != nil {
		t.Fatal(err)
	}
	if o.Error != "shutdown failed" || len(o.Incidents) != 2 {
		t.Fatalf("lost history: %+v", o)
	}
}
