package litestreamsoak

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/coverage"
	"github.com/corylanou/litestream-soak/internal/workload"
)

const coverageWorkflow = ".github/workflows/coverage-matrix.yml"

func coverageShardList(t *testing.T) []string {
	t.Helper()
	out, err := exec.Command("bash", "scripts/coverage-shard.sh", "list").Output()
	if err != nil {
		t.Fatalf("coverage-shard.sh list: %v", err)
	}
	return strings.Fields(string(out))
}

func TestCoverageMatrixCoversEveryScenario(t *testing.T) {
	t.Parallel()

	match := regexp.MustCompile(`(?m)^\s+shard:\s*\[([^\]]+)\]`).FindSubmatch(readFile(t, coverageWorkflow))
	if match == nil {
		t.Fatal("coverage workflow has no shard matrix")
	}
	var matrix []string
	for _, shard := range strings.Split(string(match[1]), ",") {
		matrix = append(matrix, strings.TrimSpace(shard))
	}
	shards := coverageShardList(t)
	if !slices.Equal(matrix, shards) {
		t.Fatalf("workflow matrix %v does not match coverage-shard.sh list %v", matrix, shards)
	}

	required := []string{"restore-modes", "manydb-100", "manydb-500", "manydb-1000", "large-db"}
	for _, backend := range workload.ReplicaBackends() {
		if backend != workload.ReplicaBackendTigris {
			required = append(required, "backend-"+backend)
		}
	}
	for _, shard := range required {
		if !slices.Contains(shards, shard) {
			t.Errorf("coverage matrix missing shard %s", shard)
		}
	}
}

func TestCoverageSummaryRequiresSuccessfulJobs(t *testing.T) {
	t.Parallel()

	workflow := string(readFile(t, coverageWorkflow))
	for _, want := range []string{
		"BUILD_RESULT: ${{ needs.build.result }}",
		"SHARD_RESULT: ${{ needs.shard.result }}",
		`[ "$BUILD_RESULT" != success ] || [ "$SHARD_RESULT" != success ]`,
	} {
		if !strings.Contains(workflow, want) {
			t.Errorf("coverage summary must gate on job results: missing %q", want)
		}
	}
}

func TestCoverageBackendsKeepCredentialsOutOfFiles(t *testing.T) {
	t.Parallel()

	backend := string(readFile(t, "scripts/coverage-backend.sh"))
	if strings.Contains(backend, "mc alias set") {
		t.Fatal("mc alias set persists MinIO credentials to config.json; use MC_HOST_<alias>")
	}
	if !strings.Contains(backend, "MC_HOST_coverage=") {
		t.Fatal("MinIO bucket creation must use an environment-only mc alias")
	}
}

func TestLargeDBShardRequiresMeasuredSize(t *testing.T) {
	t.Parallel()

	shard := string(readFile(t, "scripts/coverage-shard.sh"))
	if !regexp.MustCompile(`check_args=\(-min-passed 3 -databases 1 -min-db-size-bytes 6000000000\)`).MatchString(shard) {
		t.Fatal("large-db shard must require a measured source database of at least 6 GB")
	}
}

func TestCoverageMatrixPinsEveryBackendVersion(t *testing.T) {
	t.Parallel()

	backend := string(readFile(t, "scripts/coverage-backend.sh"))
	images := regexp.MustCompile(`(?m)^\w+_image="([^"]+)"`).FindAllStringSubmatch(backend, -1)
	if len(images) < 4 {
		t.Fatalf("coverage backends declare %d images, want nats, nats-box, sftp and rclone", len(images))
	}
	pinned := regexp.MustCompile(`^[a-z0-9./-]+:[A-Za-z0-9._-]+@sha256:[a-f0-9]{64}$`)
	for _, image := range images {
		if !pinned.MatchString(image[1]) {
			t.Errorf("image %q must pin a tag and digest", image[1])
		}
	}

	workflow := string(readFile(t, coverageWorkflow))
	minio := string(readFile(t, "Dockerfile.minio"))
	for _, name := range []string{"MINIO_COMMIT", "MC_COMMIT", "MINIO_RELEASE", "MC_RELEASE"} {
		pin := regexp.MustCompile(`(?m)^ARG ` + name + `=(\S+)$`).FindStringSubmatch(minio)
		if pin == nil {
			t.Fatalf("Dockerfile.minio missing %s", name)
		}
		if !strings.Contains(workflow, name+": "+pin[1]) {
			t.Errorf("coverage workflow must pin %s to %s like the local rig", name, pin[1])
		}
	}
	if !strings.Contains(workflow, "WORKLOAD_SHA: ae88b164dd6304bcbb654a681df767ee59042eed") {
		t.Error("coverage workflow must pin the workload generator SHA")
	}
}

func TestCoverageShardFailsIncompleteCoverage(t *testing.T) {
	t.Parallel()

	bin := t.TempDir()
	checker := filepath.Join(bin, "soakcoverage")
	if out, err := exec.Command("go", "build", "-o", checker, "./cmd/soakcoverage").CombinedOutput(); err != nil {
		t.Fatalf("build soakcoverage: %v\n%s", err, out)
	}
	worker := filepath.Join(bin, "fake-soakworker")
	script := `#!/bin/sh
backend="${FAKE_BACKEND:-$REPLICA_BACKEND}"
i=0
while [ "$i" -lt "${FAKE_PASSES:-0}" ]; do
  printf '{"backend":"%s","databases":1,"result":{"Passed":true,"Status":"passed"%s}}\n' "$backend" "${FAKE_RESULT_EXTRA:-}" >>"$SOAK_LOCAL_RESULTS"
  i=$((i + 1))
done
if [ "${FAKE_FAILED:-0}" = 1 ]; then
  printf '{"backend":"%s","databases":1,"result":{"Passed":false,"Status":"failed"}}\n' "$backend" >>"$SOAK_LOCAL_RESULTS"
fi
[ "${FAKE_EXIT:-0}" = 1 ] && exit 0
exec sleep 60
`
	if err := os.WriteFile(worker, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		shard      string
		env        []string
		wantStatus string
		wantError  string
	}{
		{name: "complete", shard: "backend-file", env: []string{"FAKE_PASSES=4"}, wantStatus: "passed"},
		{name: "no results", shard: "backend-file", wantStatus: "failed", wantError: "no verification results"},
		{name: "too few cycles", shard: "backend-file", env: []string{"FAKE_PASSES=2"}, wantStatus: "failed", wantError: "want at least 4"},
		{name: "failed cycle", shard: "backend-file", env: []string{"FAKE_PASSES=4", "FAKE_FAILED=1"}, wantStatus: "failed", wantError: "failed verification cycle"},
		{name: "backend skipped", shard: "backend-sftp", env: []string{"FAKE_PASSES=4", "FAKE_BACKEND=file"}, wantStatus: "failed", wantError: `want "sftp"`},
		{name: "worker exited early", shard: "backend-file", env: []string{"FAKE_PASSES=4", "FAKE_EXIT=1"}, wantStatus: "failed"},
		{name: "restore modes unreported", shard: "restore-modes", env: []string{"FAKE_PASSES=6"}, wantStatus: "failed", wantError: "restore mode rotation is not reported"},
		{name: "many-db rotation missing", shard: "manydb-100", env: []string{"FAKE_PASSES=3"}, wantStatus: "failed", wantError: "Rotated many database active set"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := t.TempDir()
			cmd := exec.Command("bash", "scripts/coverage-shard.sh", "run", tt.shard)
			cmd.Env = append(os.Environ(),
				"COVERAGE_OUT="+out,
				"COVERAGE_DURATION=2",
				"COVERAGE_SAMPLE_SECONDS=1",
				"COVERAGE_SOCKET_DIR="+out,
				"SOAKWORKER_BIN="+worker,
				"SOAKCOVERAGE_BIN="+checker,
			)
			cmd.Env = append(cmd.Env, tt.env...)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			switch {
			case tt.wantStatus == "passed" && err != nil:
				t.Fatalf("shard failed: %v\n%s", err, output)
			case tt.wantStatus != "passed" && !errors.As(err, &exitErr):
				t.Fatalf("shard exit = %v, want failure\n%s", err, output)
			}
			report := readShardReport(t, filepath.Join(out, "coverage.json"))
			if report.Status != tt.wantStatus || !strings.Contains(report.Error, tt.wantError) {
				t.Fatalf("report = status %q error %q, want status %q error containing %q", report.Status, report.Error, tt.wantStatus, tt.wantError)
			}
		})
	}
}

func TestCoverageShardRejectsUnknownShard(t *testing.T) {
	t.Parallel()

	err := exec.Command("bash", "scripts/coverage-shard.sh", "run", "backend-carrier-pigeon").Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 {
		t.Fatalf("unknown shard exit = %v, want 2", err)
	}
}

func readShardReport(t *testing.T, path string) coverage.ShardReport {
	t.Helper()
	var report coverage.ShardReport
	if err := json.Unmarshal(readFile(t, path), &report); err != nil {
		t.Fatal(fmt.Errorf("parse %s: %w", path, err))
	}
	return report
}
