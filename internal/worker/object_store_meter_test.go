package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
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
	meter.recordObjectUpload(1024)
	meter.recordObjectUpload(2048)

	first := meter.snapshot()
	if first.ObjectStoreCounterEpoch == "" {
		t.Fatal("missing counter epoch")
	}
	if want := map[string]uint64{"GET": 5, "LIST": 1}; !reflect.DeepEqual(first.RestoreRequestTotals, want) {
		t.Fatalf("restore totals = %v, want %v", first.RestoreRequestTotals, want)
	}
	if first.EvidenceObjectUploadsTotal != 2 || first.EvidenceUploadPayloadBytesTotal != 3072 {
		t.Fatalf("uploads = %d/%d", first.EvidenceObjectUploadsTotal, first.EvidenceUploadPayloadBytesTotal)
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
	if counters.EvidenceObjectUploadsTotal != 1 || counters.EvidenceUploadPayloadBytesTotal != 4096 {
		t.Fatalf("uploads = %d/%d, want 1/4096", counters.EvidenceObjectUploadsTotal, counters.EvidenceUploadPayloadBytesTotal)
	}
}

func TestStatsPollerKeepsLogicalOperationsWithTheirEpoch(t *testing.T) {
	cfg := DefaultConfig()
	poller := newStatsPoller(&cfg)
	poller.litestreamPID = func() int { return 4242 }
	now := time.Now().UTC()
	healthy := poller.recordLitestreamMetricsScrape(now, litestreamMetricsSnapshot{
		LitestreamObjectOperationTotals: map[string]uint64{"PUT": 10},
		LitestreamObjectOperationBytes:  map[string]uint64{"PUT": 4096},
		LitestreamCounterEpoch:          "start:100",
	}, nil)
	if healthy.LitestreamObjectOperationTotals["PUT"] != 10 || healthy.LitestreamObjectOperationBytes["PUT"] != 4096 || healthy.LitestreamCounterEpoch != "start:100" {
		t.Fatalf("healthy runtime = %+v", healthy.ObjectStoreCounters)
	}
	failed := poller.recordLitestreamMetricsScrape(now.Add(time.Second), litestreamMetricsSnapshot{}, errors.New("scrape failed"))
	if failed.LitestreamObjectOperationTotals["PUT"] != 10 || failed.LitestreamCounterEpoch != "start:100" {
		t.Fatalf("failed scrape dropped counters: %+v", failed.ObjectStoreCounters)
	}
	var snapshot reporting.RuntimePayload
	applyLitestreamMetricsRuntime(&snapshot, failed)
	poller.setLitestreamSnapshot(snapshot)
	if got := poller.currentSnapshot(); got.LitestreamObjectOperationTotals["PUT"] != 10 || got.LitestreamCounterEpoch != "start:100" {
		t.Fatalf("snapshot = %+v", got.ObjectStoreCounters)
	}
	withoutStart := poller.recordLitestreamMetricsScrape(now.Add(2*time.Second), litestreamMetricsSnapshot{
		LitestreamObjectOperationTotals: map[string]uint64{"PUT": 3},
	}, nil)
	if withoutStart.LitestreamCounterEpoch != "pid:4242" || withoutStart.LitestreamObjectOperationTotals["PUT"] != 3 {
		t.Fatalf("pid fallback runtime = %+v", withoutStart.ObjectStoreCounters)
	}
}

func TestRunnerSnapshotIncludesObjectStoreCounters(t *testing.T) {
	runner := NewRunner(DefaultConfig())
	runner.objectStore.recordRestoreRequests(map[string]uint64{"GET": 4})
	runner.objectStore.recordObjectUpload(512)
	snapshot := runner.currentSnapshot()
	if snapshot.RestoreRequestTotals["GET"] != 4 || snapshot.EvidenceObjectUploadsTotal != 1 || snapshot.EvidenceUploadPayloadBytesTotal != 512 || snapshot.ObjectStoreCounterEpoch == "" {
		t.Fatalf("snapshot counters = %+v", snapshot.ObjectStoreCounters)
	}
	if snapshot.ReplicaRequestTotals != nil {
		t.Fatalf("replica requests without an observe proxy = %v", snapshot.ReplicaRequestTotals)
	}
	if runner.profiles.objectStore != runner.objectStore {
		t.Fatal("profile uploads are not metered")
	}
}

func TestRunnerSnapshotIncludesObserveProxyRequests(t *testing.T) {
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte("ok"))
	})
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Endpoint = upstream.URL
	cfg.S3AccessKey = testS3AccessKey
	cfg.S3SecretKey = testS3SecretKey
	cfg.S3SessionToken = testS3SessionToken
	cfg.S3Region = testS3Region
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyMode = "observe"
	cfg.S3FaultProxyListenAddr = "127.0.0.1:0"
	runner := NewRunner(cfg)
	if err := runner.startS3FaultProxy(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.stopS3FaultProxy)
	resp, err := testS3HTTPClient(time.Second).Get(runner.cfg.S3Endpoint + "/bucket?list-type=2")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	snapshot := runner.currentSnapshot()
	if snapshot.ReplicaRequestTotals["LIST"] != 1 || snapshot.ReplicaResponseBytesReceived["LIST"] != 2 {
		t.Fatalf("snapshot counters = %+v", snapshot.ObjectStoreCounters)
	}
}

func TestS3ObserveProxyFailsOpenWithoutSigningCredentials(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Endpoint = "https://fly.storage.tigris.dev"
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyMode = "observe"
	cfg.S3FaultProxyListenAddr = "127.0.0.1:0"
	runner := NewRunner(cfg)
	if err := runner.startS3FaultProxy(context.Background()); err != nil {
		t.Fatalf("observe proxy start must fail open: %v", err)
	}
	if runner.s3FaultProxy != nil || runner.cfg.S3Endpoint != "https://fly.storage.tigris.dev" {
		t.Fatalf("replication was routed through a proxy that cannot sign: endpoint=%s", runner.cfg.S3Endpoint)
	}
}

func TestS3ObserveProxyResignsAndMetersRequests(t *testing.T) {
	var uploaded atomic.Int64
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		uploaded.Add(n)
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") != "":
			_, _ = w.Write([]byte("<ListBucketResult></ListBucketResult>"))
		case r.Method == http.MethodGet:
			_, _ = w.Write(bytes.Repeat([]byte("g"), 300))
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint: upstream.URL,
		ListenAddr:     "127.0.0.1:0",
		Mode:           s3FaultProxyModeObserve,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := testS3HTTPClient(time.Second)
	do := func(method, path string, body []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, proxy.Endpoint()+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s status = %d", method, path, resp.StatusCode)
		}
	}
	do(http.MethodPut, "/bucket/db/ltx/0/1.ltx?partNumber=1&uploadId=u", bytes.Repeat([]byte("p"), 1000))
	do(http.MethodPut, "/bucket/db/ltx/0/1.ltx?partNumber=2&uploadId=u", bytes.Repeat([]byte("p"), 500))
	do(http.MethodPost, "/bucket?delete", []byte("<Delete></Delete>"))
	do(http.MethodGet, "/bucket?list-type=2&prefix=db/", nil)
	do(http.MethodGet, "/bucket/db/ltx/0/1.ltx", nil)
	do(http.MethodHead, "/bucket/db/ltx/0/1.ltx", nil)

	counters := proxy.RequestCounters()
	if want := map[string]uint64{"PUT": 2, "POST": 1, "LIST": 1, "GET": 1, "HEAD": 1}; !reflect.DeepEqual(counters.Requests, want) {
		t.Fatalf("requests = %v, want %v", counters.Requests, want)
	}
	if want := map[string]uint64{"PUT": 1500, "POST": 17}; !reflect.DeepEqual(counters.BytesSent, want) {
		t.Fatalf("bytes sent = %v, want %v", counters.BytesSent, want)
	}
	if want := map[string]uint64{"LIST": 37, "GET": 300}; !reflect.DeepEqual(counters.BytesReceived, want) {
		t.Fatalf("bytes received = %v, want %v", counters.BytesReceived, want)
	}
	if uploaded.Load() != 1517 {
		t.Fatalf("upstream received %d bytes", uploaded.Load())
	}
}

func TestLTXLevelOfKeyFollowsS3ReplicaLayout(t *testing.T) {
	for key, want := range map[string]string{
		"/bucket/soak/w1/0000/0000000000000001-0000000000000001.ltx":                      "L0",
		"/bucket/soak/w1/0001/0000000000000001-0000000000000040.ltx":                      "L1",
		"/bucket/soak/w1/0008/0000000000000001-0000000000000fff.ltx":                      "L8",
		"/bucket/soak/w1/0009/0000000000000001-00000000000003e8.ltx":                      "L9",
		"soak/w1/0009/0000000000000001-00000000000003e8.ltx":                              "L9",
		"/bucket/soak/manydb/db-0417/0003/0000000000000a00-0000000000000a7f.ltx":          "L3",
		"/bucket/soak/scenarios/pinned-reader/0000/0000000000000002-0000000000000002.ltx": "L0",
		"/bucket/soak/w1/ltx/0/0000000000000001-0000000000000001.ltx":                     "other",
		"/bucket/soak/w1/000a/0000000000000001-0000000000000001.ltx":                      "other",
		"/bucket/soak/w1/0010/0000000000000001-0000000000000001.ltx":                      "other",
		"/bucket/soak/w1/9/0000000000000001-0000000000000001.ltx":                         "other",
		"/bucket/soak/w1/.litestream-replica":                                             "other",
		"/bucket/soak/evidence/pprof/heap.pb.gz":                                          "other",
		"/bucket":                                                                         "other",
		"":                                                                                "other",
	} {
		if got := ltxLevelOfKey(key); got != want {
			t.Errorf("ltxLevelOfKey(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestS3ObserveProxyCountsPutsPerLevelOncePerObject(t *testing.T) {
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})
	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint: upstream.URL,
		ListenAddr:     "127.0.0.1:0",
		Mode:           s3FaultProxyModeObserve,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := testS3HTTPClient(time.Second)
	do := func(method, path string, body []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, proxy.Endpoint()+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s status = %d", method, path, resp.StatusCode)
		}
	}
	l0 := "/bucket/soak/w1/0000/0000000000000001-0000000000000001.ltx"
	l9 := "/bucket/soak/w1/0009/0000000000000001-0000000000000400.ltx"
	l1 := "/bucket/soak/w1/0001/0000000000000001-0000000000000040.ltx"
	do(http.MethodPut, l0, bytes.Repeat([]byte("a"), 100))
	do(http.MethodPost, l9+"?uploads", nil)
	do(http.MethodPut, l9+"?partNumber=1&uploadId=snap", bytes.Repeat([]byte("p"), 1000))
	do(http.MethodPut, l9+"?partNumber=2&uploadId=snap", bytes.Repeat([]byte("p"), 500))
	do(http.MethodPost, l9+"?uploadId=snap", []byte("<CompleteMultipartUpload></CompleteMultipartUpload>"))
	do(http.MethodPost, l1+"?uploads", nil)
	do(http.MethodPut, l1+"?partNumber=1&uploadId=abandoned", bytes.Repeat([]byte("p"), 700))
	do(http.MethodDelete, l1+"?uploadId=abandoned", nil)
	do(http.MethodPut, "/bucket/soak/w1/.litestream-replica", bytes.Repeat([]byte("m"), 20))
	do(http.MethodGet, l0, nil)
	do(http.MethodHead, l9, nil)
	do(http.MethodPost, "/bucket?delete", []byte("<Delete></Delete>"))

	counters := proxy.RequestCounters()
	if want := map[string]uint64{"L0": 1, "L9": 1, "other": 1}; !reflect.DeepEqual(counters.LevelPuts, want) {
		t.Fatalf("level puts = %v, want %v", counters.LevelPuts, want)
	}
	if want := map[string]uint64{"L0": 100, "L9": 1500, "other": 20}; !reflect.DeepEqual(counters.LevelPutBytes, want) {
		t.Fatalf("level put bytes = %v, want %v", counters.LevelPutBytes, want)
	}
	if want := map[string]uint64{"PUT": 5, "POST": 4, "DELETE": 1, "GET": 1, "HEAD": 1}; !reflect.DeepEqual(counters.Requests, want) {
		t.Fatalf("requests = %v, want %v", counters.Requests, want)
	}
	if len(proxy.multipartBytes) != 0 {
		t.Fatalf("multipart uploads still tracked after completion and abort: %v", proxy.multipartBytes)
	}
}

func TestS3FaultProxyBoundsTrackedMultipartUploads(t *testing.T) {
	proxy := newS3FaultProxy(s3FaultProxyConfig{Mode: s3FaultProxyModeObserve})
	part := func(key, uploadID string, n int64) {
		proxy.recordUpload(httptest.NewRequest(http.MethodPut, key+"?partNumber=1&uploadId="+uploadID, nil), n)
	}
	for i := range maxS3FaultProxyMultipartUploads {
		part("/bucket/soak/w1/0000/"+strconv.Itoa(i)+".ltx", "u", 1)
	}
	if len(proxy.multipartBytes) != maxS3FaultProxyMultipartUploads {
		t.Fatalf("tracked uploads = %d, want %d", len(proxy.multipartBytes), maxS3FaultProxyMultipartUploads)
	}
	part("/bucket/soak/w1/0000/0.ltx", "u", 5)
	if got := proxy.multipartBytes["/bucket/soak/w1/0000/0.ltx\x00u"]; got != 6 || len(proxy.multipartBytes) != maxS3FaultProxyMultipartUploads {
		t.Fatalf("existing upload bytes = %d tracked = %d, want 6 and %d", got, len(proxy.multipartBytes), maxS3FaultProxyMultipartUploads)
	}
	part("/bucket/soak/w1/0009/new.ltx", "u", 7)
	if len(proxy.multipartBytes) != 1 || proxy.multipartBytes["/bucket/soak/w1/0009/new.ltx\x00u"] != 7 {
		t.Fatalf("tracked uploads after overflow = %v", proxy.multipartBytes)
	}
	proxy.recordUpload(httptest.NewRequest(http.MethodPost, "/bucket/soak/w1/0009/new.ltx?uploadId=u", nil), 40)
	if proxy.levelPuts["L9"] != 1 || proxy.levelPutBytes["L9"] != 7 {
		t.Fatalf("L9 puts = %d bytes = %d, want 1 and 7", proxy.levelPuts["L9"], proxy.levelPutBytes["L9"])
	}
}
