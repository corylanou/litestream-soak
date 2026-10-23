package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const restoreOutputWithS3Requests = `time=2026-10-22T10:00:00.000Z level=INFO msg="restoring snapshot" db=/data/soak.db
time=2026-10-22T10:00:00.001Z level=DEBUG msg="s3 request" s3.method=GET s3.url="https://fly.storage.tigris.dev/bucket?list-type=2&prefix=soak%2Fltx%2F9%2F" s3.x-tigris-consistent=true
time=2026-10-22T10:00:00.002Z level=DEBUG msg="s3 request" s3.method=GET s3.url="https://fly.storage.tigris.dev/bucket?list-type=2&prefix=soak%2Fltx%2F0%2F" s3.x-tigris-consistent=true
time=2026-10-22T10:00:00.003Z level=DEBUG msg="s3 request" s3.method=GET s3.url=https://fly.storage.tigris.dev/bucket/soak/ltx/9/0000000000000001-0000000000000040.ltx s3.x-tigris-consistent=true
time=2026-10-22T10:00:00.004Z level=DEBUG msg="s3 request" s3.method=GET s3.url=https://fly.storage.tigris.dev/bucket/soak/ltx/0/0000000000000041-0000000000000041.ltx s3.x-tigris-consistent=true
time=2026-10-22T10:00:00.005Z level=DEBUG msg="s3 request" s3.method=HEAD s3.url=https://fly.storage.tigris.dev/bucket/soak/ltx/0/0000000000000042-0000000000000042.ltx s3.x-tigris-consistent=true
time=2026-10-22T10:00:00.006Z level=DEBUG msg="read ltx" s3.method=PUT
time=2026-10-22T10:00:00.007Z level=INFO msg="restore complete"
`

func TestRestoreOutputCountsObjectStoreRequestsByMethod(t *testing.T) {
	var output restoreOutput
	for start := 0; start < len(restoreOutputWithS3Requests); start += 11 {
		if _, err := output.Write([]byte(restoreOutputWithS3Requests[start:min(start+11, len(restoreOutputWithS3Requests))])); err != nil {
			t.Fatal(err)
		}
	}
	_ = output.text()
	want := map[string]uint64{"GET": 2, "HEAD": 1, "LIST": 2}
	if !reflect.DeepEqual(output.requests, want) {
		t.Fatalf("requests = %v, want %v", output.requests, want)
	}
}

func TestRestoreOutputCountsRequestsWhenEvidenceTruncates(t *testing.T) {
	var output restoreOutput
	line := `time=now level=DEBUG msg="s3 request" s3.method=GET s3.url=https://example.test/bucket/key` + "\n"
	if _, err := output.Write([]byte(strings.Repeat(line, 5000))); err != nil {
		t.Fatal(err)
	}
	_ = output.text()
	if !output.truncated || output.requests["GET"] != 5000 {
		t.Fatalf("truncated=%v requests=%v", output.truncated, output.requests)
	}
}

func TestObjectStoreMeterSnapshotCopiesCounters(t *testing.T) {
	meter := newObjectStoreMeter()
	meter.recordRestoreRequests(map[string]uint64{"GET": 3, "LIST": 1})
	meter.recordRestoreRequests(map[string]uint64{"GET": 2})
	meter.recordUpload(1024)
	meter.recordUpload(2048)

	first := meter.snapshot()
	if first.ObjectStoreCounterEpoch == "" {
		t.Fatal("missing counter epoch")
	}
	if want := map[string]uint64{"GET": 5, "LIST": 1}; !reflect.DeepEqual(first.RestoreRequestTotals, want) {
		t.Fatalf("restore totals = %v, want %v", first.RestoreRequestTotals, want)
	}
	if first.EvidenceUploadsTotal != 2 || first.EvidenceUploadBytesTotal != 3072 {
		t.Fatalf("uploads = %d/%d", first.EvidenceUploadsTotal, first.EvidenceUploadBytesTotal)
	}
	meter.recordRestoreRequests(map[string]uint64{"GET": 1})
	if first.RestoreRequestTotals["GET"] != 5 {
		t.Fatal("snapshot shares state with the meter")
	}
	if second := meter.snapshot(); second.ObjectStoreCounterEpoch != first.ObjectStoreCounterEpoch || second.RestoreRequestTotals["GET"] != 6 {
		t.Fatalf("second snapshot = %+v", second)
	}
}

func TestVerifierRecordsRestoreRequests(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.LogicalTimeout = 30 * time.Second
	logicalTestDB(t, cfg.DBPath, "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
	logicalTestDB(t, cfg.DBPath+".restored", "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
	startBoundarySyncFixture(t, &cfg, 42)
	script := "#!/bin/sh\ncat <<'EOF'\n" + restoreOutputWithS3Requests + "EOF\n"
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	meter := newObjectStoreMeter()
	v := NewVerifier(cfg)
	v.objectStore = meter
	if passed, err := v.validate(context.Background(), 42); !passed || err != nil {
		t.Fatalf("passed=%v err=%v", passed, err)
	}
	if want := map[string]uint64{"GET": 2, "HEAD": 1, "LIST": 2}; !reflect.DeepEqual(meter.snapshot().RestoreRequestTotals, want) {
		t.Fatalf("restore totals = %v, want %v", meter.snapshot().RestoreRequestTotals, want)
	}
}

func TestPprofUploadRecordsObjectStoreUploads(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3AccessKey = "example-key"
	cfg.S3SecretKey = "example-secret"
	cfg.S3Endpoint = "http://127.0.0.1:9000"
	dir := t.TempDir()
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	artifact := filepath.Join(dir, "example.pprof")
	if err := os.WriteFile(artifact, make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "s3cmd"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	capturer := newPprofCapturer(&cfg)
	meter := newObjectStoreMeter()
	capturer.objectStore = meter
	if err := capturer.upload(context.Background(), artifact, "example.pprof"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "s3cmd"), []byte("#!/bin/sh\nexit 3\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := capturer.upload(context.Background(), artifact, "example.pprof"); err == nil {
		t.Fatal("expected failed upload")
	}
	counters := meter.snapshot()
	if counters.EvidenceUploadsTotal != 1 || counters.EvidenceUploadBytesTotal != 4096 {
		t.Fatalf("uploads = %d/%d, want 1/4096", counters.EvidenceUploadsTotal, counters.EvidenceUploadBytesTotal)
	}
}

func TestStatsPollerKeepsReplicaOperationsAcrossScrapeFailure(t *testing.T) {
	cfg := DefaultConfig()
	poller := newStatsPoller(&cfg)
	now := time.Now().UTC()
	healthy := poller.recordLitestreamMetricsScrape(now, litestreamMetricsSnapshot{
		ReplicaOperationTotals: map[string]uint64{"PUT": 10},
		ReplicaOperationBytes:  map[string]uint64{"PUT": 4096},
	}, nil)
	if healthy.ReplicaOperationTotals["PUT"] != 10 || healthy.ReplicaOperationBytes["PUT"] != 4096 {
		t.Fatalf("healthy runtime = %+v", healthy.ObjectStoreCounters)
	}
	failed := poller.recordLitestreamMetricsScrape(now.Add(time.Second), litestreamMetricsSnapshot{}, errors.New("scrape failed"))
	if failed.ReplicaOperationTotals["PUT"] != 10 || failed.ReplicaOperationBytes["PUT"] != 4096 {
		t.Fatalf("failed scrape dropped counters: %+v", failed.ObjectStoreCounters)
	}
	var snapshot reporting.RuntimePayload
	applyLitestreamMetricsRuntime(&snapshot, failed)
	poller.setLitestreamSnapshot(snapshot)
	if got := poller.currentSnapshot().ReplicaOperationTotals["PUT"]; got != 10 {
		t.Fatalf("snapshot replica PUT = %d, want 10", got)
	}
}

func TestRunnerSnapshotIncludesObjectStoreCounters(t *testing.T) {
	runner := NewRunner(DefaultConfig())
	runner.objectStore.recordRestoreRequests(map[string]uint64{"GET": 4})
	runner.objectStore.recordUpload(512)
	snapshot := runner.currentSnapshot()
	if snapshot.RestoreRequestTotals["GET"] != 4 || snapshot.EvidenceUploadsTotal != 1 || snapshot.EvidenceUploadBytesTotal != 512 || snapshot.ObjectStoreCounterEpoch == "" {
		t.Fatalf("snapshot counters = %+v", snapshot.ObjectStoreCounters)
	}
	if runner.profiles.objectStore != runner.objectStore {
		t.Fatal("profile uploads are not metered")
	}
}
