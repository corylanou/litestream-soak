package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
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
		"/bucket/soak/w1/0000/0000000000000001-0000000000000001.ltx.tmp":                  "other",
		"/bucket/soak/w1/0000/00000000000000001-0000000000000001.ltx":                     "other",
		"/bucket/soak/w1/0000/000000000000000G-0000000000000001.ltx":                      "other",
		"/bucket/soak/w1/0009/snapshot.ltx":                                               "other",
		"/bucket/soak/w1/0009/":                                                           "other",
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

func TestS3ObserveProxyCountsSuccessfulPutsPerLevelOncePerObject(t *testing.T) {
	var failNext atomic.Int32
	const errorCompletion = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>`
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case failNext.Load() > 0:
			failNext.Add(-1)
			w.WriteHeader(http.StatusInternalServerError)
		case r.URL.Query().Get("uploadId") == "errbody" && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(errorCompletion))
		case r.URL.Query().Get("uploadId") == "snap" && r.Method == http.MethodPost:
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><ETag>"e"</ETag></CompleteMultipartUploadResult>`))
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
	do := func(method, path string, body []byte, wantStatus int) {
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
		if resp.StatusCode != wantStatus {
			t.Fatalf("%s %s status = %d, want %d", method, path, resp.StatusCode, wantStatus)
		}
	}
	l0 := "/bucket/soak/w1/0000/0000000000000001-0000000000000001.ltx"
	l1 := "/bucket/soak/w1/0001/0000000000000001-0000000000000040.ltx"
	l2 := "/bucket/soak/w1/0002/0000000000000001-0000000000000080.ltx"
	l9 := "/bucket/soak/w1/0009/0000000000000001-0000000000000400.ltx"

	failNext.Store(1)
	do(http.MethodPut, l0, bytes.Repeat([]byte("a"), 100), http.StatusInternalServerError)
	do(http.MethodPut, l0, bytes.Repeat([]byte("a"), 100), http.StatusOK)

	do(http.MethodPost, l9+"?uploads", nil, http.StatusOK)
	do(http.MethodPut, l9+"?partNumber=1&uploadId=snap", bytes.Repeat([]byte("p"), 1000), http.StatusOK)
	failNext.Store(1)
	do(http.MethodPut, l9+"?partNumber=2&uploadId=snap", bytes.Repeat([]byte("p"), 500), http.StatusInternalServerError)
	do(http.MethodPut, l9+"?partNumber=2&uploadId=snap", bytes.Repeat([]byte("p"), 500), http.StatusOK)
	do(http.MethodPost, l9+"?uploadId=snap", []byte("<CompleteMultipartUpload></CompleteMultipartUpload>"), http.StatusOK)

	do(http.MethodPost, l1+"?uploads", nil, http.StatusOK)
	do(http.MethodPut, l1+"?partNumber=1&uploadId=abandoned", bytes.Repeat([]byte("p"), 700), http.StatusOK)
	do(http.MethodDelete, l1+"?uploadId=abandoned", nil, http.StatusOK)

	do(http.MethodPost, l2+"?uploads", nil, http.StatusOK)
	do(http.MethodPut, l2+"?partNumber=1&uploadId=failed", bytes.Repeat([]byte("p"), 300), http.StatusOK)
	failNext.Store(1)
	do(http.MethodPost, l2+"?uploadId=failed", []byte("<CompleteMultipartUpload></CompleteMultipartUpload>"), http.StatusInternalServerError)
	do(http.MethodPut, l2+"?partNumber=1&uploadId=errbody", bytes.Repeat([]byte("p"), 200), http.StatusOK)
	do(http.MethodPost, l2+"?uploadId=errbody", []byte("<CompleteMultipartUpload></CompleteMultipartUpload>"), http.StatusOK)

	do(http.MethodPut, "/bucket/soak/w1/.litestream-replica", bytes.Repeat([]byte("m"), 20), http.StatusOK)
	do(http.MethodGet, l0, nil, http.StatusOK)
	do(http.MethodHead, l9, nil, http.StatusOK)
	do(http.MethodPost, "/bucket?delete", []byte("<Delete></Delete>"), http.StatusOK)

	counters := proxy.RequestCounters()
	if want := map[string]uint64{"L0": 1, "L9": 1, "other": 1}; !reflect.DeepEqual(counters.LevelPuts, want) {
		t.Fatalf("level puts = %v, want %v", counters.LevelPuts, want)
	}
	if want := map[string]uint64{"L0": 100, "L1": 700, "L2": 500, "L9": 1500, "other": 20}; !reflect.DeepEqual(counters.LevelPutBytes, want) {
		t.Fatalf("level put bytes = %v, want %v", counters.LevelPutBytes, want)
	}
	if want := map[string]uint64{"PUT": 9, "POST": 7, "DELETE": 1, "GET": 1, "HEAD": 1}; !reflect.DeepEqual(counters.Requests, want) {
		t.Fatalf("requests = %v, want %v", counters.Requests, want)
	}
}

func TestRecordUploadIgnoresFailuresAndErrorCompletions(t *testing.T) {
	key := "/bucket/soak/w1/0003/0000000000000001-0000000000000001.ltx"
	for name, tc := range map[string]struct {
		request   *http.Request
		status    int
		completed bool
		puts      map[string]uint64
		bytes     map[string]uint64
	}{
		"put without status": {request: httptest.NewRequest(http.MethodPut, key, nil), status: 0},
		"put 500":            {request: httptest.NewRequest(http.MethodPut, key, nil), status: http.StatusInternalServerError},
		"put 502 from proxy": {request: httptest.NewRequest(http.MethodPut, key, nil), status: http.StatusBadGateway},
		"put 200":            {request: httptest.NewRequest(http.MethodPut, key, nil), status: http.StatusOK, puts: map[string]uint64{"L3": 1}, bytes: map[string]uint64{"L3": 40}},
		"part 200":           {request: httptest.NewRequest(http.MethodPut, key+"?partNumber=3&uploadId=u", nil), status: http.StatusOK, bytes: map[string]uint64{"L3": 40}},
		"part 403":           {request: httptest.NewRequest(http.MethodPut, key+"?partNumber=3&uploadId=u", nil), status: http.StatusForbidden},
		"completion 200":     {request: httptest.NewRequest(http.MethodPost, key+"?uploadId=u", nil), status: http.StatusOK, completed: true, puts: map[string]uint64{"L3": 1}},
		"completion error":   {request: httptest.NewRequest(http.MethodPost, key+"?uploadId=u", nil), status: http.StatusOK},
		"completion 500":     {request: httptest.NewRequest(http.MethodPost, key+"?uploadId=u", nil), status: http.StatusInternalServerError, completed: true},
		"create upload":      {request: httptest.NewRequest(http.MethodPost, key+"?uploads", nil), status: http.StatusOK, completed: true},
		"abort upload":       {request: httptest.NewRequest(http.MethodDelete, key+"?uploadId=u", nil), status: http.StatusNoContent},
		"batch delete":       {request: httptest.NewRequest(http.MethodPost, "/bucket?delete", nil), status: http.StatusOK},
		"other key put 200":  {request: httptest.NewRequest(http.MethodPut, "/bucket/soak/w1/0003/notes.txt", nil), status: http.StatusOK, puts: map[string]uint64{"other": 1}, bytes: map[string]uint64{"other": 40}},
		"other part 200":     {request: httptest.NewRequest(http.MethodPut, "/bucket/soak/evidence/heap.pb.gz?partNumber=1&uploadId=u", nil), status: http.StatusOK, bytes: map[string]uint64{"other": 40}},
		"zero byte put 200":  {request: httptest.NewRequest(http.MethodPut, key, nil), status: http.StatusOK, puts: map[string]uint64{"L3": 1}},
	} {
		t.Run(name, func(t *testing.T) {
			proxy := newS3FaultProxy(s3FaultProxyConfig{Mode: s3FaultProxyModeObserve})
			sent := int64(40)
			if name == "zero byte put 200" {
				sent = 0
			}
			proxy.recordUpload(tc.request, sent, tc.status, tc.completed)
			counters := proxy.RequestCounters()
			if !reflect.DeepEqual(counters.LevelPuts, tc.puts) || !reflect.DeepEqual(counters.LevelPutBytes, tc.bytes) {
				t.Fatalf("puts = %v bytes = %v, want %v and %v", counters.LevelPuts, counters.LevelPutBytes, tc.puts, tc.bytes)
			}
		})
	}
}

func TestCountingResponseWriterScansWholeCompletionBody(t *testing.T) {
	write := func(t *testing.T, contentLength string, chunks ...string) *countingResponseWriter {
		t.Helper()
		recorder := httptest.NewRecorder()
		counted := &countingResponseWriter{ResponseWriter: recorder, scanErrors: true}
		if contentLength != "" {
			counted.Header().Set("Content-Length", contentLength)
		}
		var forwarded bytes.Buffer
		for _, chunk := range chunks {
			if _, err := counted.Write([]byte(chunk)); err != nil {
				t.Fatal(err)
			}
			forwarded.WriteString(chunk)
		}
		if recorder.Body.String() != forwarded.String() {
			t.Fatalf("forwarded body %q, want every byte unchanged %q", recorder.Body.String(), forwarded.String())
		}
		return counted
	}
	whitespace := strings.Repeat(" ", 64<<10)
	const fullResult = "<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><ETag>e</ETag></CompleteMultipartUploadResult>"
	for name, tc := range map[string]struct {
		contentLength string
		chunks        []string
		want          bool
	}{
		"clean result":                    {chunks: []string{fullResult}, want: true},
		"result after 64 KiB whitespace":  {chunks: []string{whitespace, fullResult}, want: true},
		"result split across writes":      {chunks: []string{whitespace + "<CompleteMultipart", "UploadResult><ETag>e</ETag></CompleteMultipartUpl", "oadResult>"}, want: true},
		"result split byte by byte":       {chunks: strings.Split(fullResult, ""), want: true},
		"result with xml header and tail": {chunks: []string{`<?xml version="1.0" encoding="UTF-8"?>` + "\n", fullResult, "\n"}, want: true},
		"opening tag only":                {chunks: []string{"<CompleteMultipartUploadResult>"}},
		"opening tag only chunked":        {chunks: []string{whitespace, "<CompleteMultipartUploadResult", "><Bucket>b</Bucket>"}},
		"self-closing opening tag":        {chunks: []string{"<CompleteMultipartUploadResult/>"}},
		"closing tag only":                {chunks: []string{"</CompleteMultipartUploadResult>"}},
		"closing tag before opening tag":  {chunks: []string{"</CompleteMultipartUploadResult>", "<CompleteMultipartUploadResult>"}},
		"result then error":               {chunks: []string{fullResult, "<Error><Code>InternalError</Code></Error>"}},
		"error inside result":             {chunks: []string{"<CompleteMultipartUploadResult><Error><Code>InternalError</Code></Error></CompleteMultipartUploadResult>"}},
		"error after 64 KiB whitespace":   {chunks: []string{whitespace, "<Error><Code>InternalError</Code></Error>"}},
		"error split across writes":       {chunks: []string{whitespace + "<Err", "or><Code>SlowDown</Code></Error>"}},
		"error split byte by byte":        {chunks: []string{"<", "E", "r", "r", "o", "r", ">"}},
		"error after a full tail":         {chunks: []string{whitespace + "<Erro", "r>"}},
		"truncated 200":                   {contentLength: "100", chunks: []string{"<CompleteMultipartUploadResult>"}},
		"complete with content length":    {contentLength: strconv.Itoa(len(fullResult)), chunks: []string{fullResult}, want: true},
		"unrelated element only":          {contentLength: "9", chunks: []string{"<Result/>"}},
		"whitespace only":                 {chunks: []string{whitespace, "\n"}},
		"empty body":                      {chunks: nil},
	} {
		t.Run(name, func(t *testing.T) {
			counted := write(t, tc.contentLength, tc.chunks...)
			if len(tc.chunks) == 0 {
				counted.WriteHeader(http.StatusOK)
			}
			if got := counted.completionSucceeded(); got != tc.want {
				t.Fatalf("completionSucceeded() = %v, want %v", got, tc.want)
			}
		})
	}
	plain := &countingResponseWriter{ResponseWriter: httptest.NewRecorder()}
	if _, err := plain.Write([]byte("<Error>")); err != nil || plain.completionSucceeded() {
		t.Fatalf("non-completion writer: err=%v succeeded=%v, want never counted as a completion", err, plain.completionSucceeded())
	}
}

func TestS3ObserveProxyIgnoresCompletionErrorsStreamedLate(t *testing.T) {
	key := "/bucket/soak/w1/0009/0000000000000001-0000000000000400.ltx"
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if !isS3MultipartCompletion(r) {
			w.WriteHeader(http.StatusOK)
			return
		}
		switch r.URL.Query().Get("uploadId") {
		case "late":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(strings.Repeat(" ", 64<<10)))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			_, _ = w.Write([]byte("<Err"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			_, _ = w.Write([]byte("or><Code>InternalError</Code></Error>"))
		case "truncated":
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("<CompleteMultipartUploadResult>"))
			panic(http.ErrAbortHandler)
		case "empty":
			w.WriteHeader(http.StatusOK)
		case "whitespace":
			_, _ = w.Write([]byte(strings.Repeat(" ", 1024)))
		case "opening-only":
			_, _ = w.Write([]byte(strings.Repeat(" ", 1024) + "<CompleteMultipartUploadResult"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			_, _ = w.Write([]byte("><Bucket>b</Bucket>"))
		default:
			_, _ = w.Write([]byte("<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><ETag>e</ETag></CompleteMultipartUploadResult>"))
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

	client := testS3HTTPClient(2 * time.Second)
	complete := func(uploadID string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxy.Endpoint()+key+"?uploadId="+uploadID, bytes.NewReader([]byte("<CompleteMultipartUpload/>")))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	complete("late")
	complete("truncated")
	complete("empty")
	complete("whitespace")
	complete("opening-only")
	complete("ok")

	counters := proxy.RequestCounters()
	if want := map[string]uint64{"L9": 1}; !reflect.DeepEqual(counters.LevelPuts, want) {
		t.Fatalf("level puts = %v, want only the clean completion %v", counters.LevelPuts, want)
	}
	if counters.Requests["POST"] != 6 {
		t.Fatalf("requests = %v, want every completion attempt counted as a request", counters.Requests)
	}
}

func TestS3ObserveProxyRequiredFailsClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Endpoint = "https://fly.storage.tigris.dev"
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyMode = "observe"
	cfg.S3FaultProxyListenAddr = "127.0.0.1:0"
	cfg.S3ObserveProxyRequired = true
	runner := NewRunner(cfg)
	err := runner.startS3FaultProxy(context.Background())
	if !errors.Is(err, errS3ObserveProxyRequired) || !errors.Is(err, errS3FaultProxyUpstreamRequestFailed) {
		t.Fatalf("startS3FaultProxy() error = %v, want required-proxy failure wrapping the redacted start error", err)
	}
	if runner.s3FaultProxy != nil || runner.cfg.S3Endpoint != "https://fly.storage.tigris.dev" {
		t.Fatalf("a failed required proxy must not alter replication: endpoint=%s", runner.cfg.S3Endpoint)
	}
	if !cfg.WorkloadConfig().S3ObserveProxyRequired {
		t.Fatal("S3ObserveProxyRequired is missing from the attested workload config")
	}
	parsed, err := WorkloadFromEnvironment(map[string]string{"S3_OBSERVE_PROXY_ENABLED": "true", "S3_OBSERVE_PROXY_REQUIRED": "true"})
	if err != nil || !parsed.S3ObserveProxyRequired || parsed.S3FaultProxyMode != "observe" {
		t.Fatalf("WorkloadFromEnvironment() = %+v, %v", parsed, err)
	}
}

func TestFinalHeartbeatCarriesProxyCountersAfterCancel(t *testing.T) {
	var mu sync.Mutex
	var heartbeats []reporting.HeartbeatPayload
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload reporting.HeartbeatPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		heartbeats = append(heartbeats, payload)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(control.Close)
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})

	cfg := DefaultConfig()
	cfg.ControlBaseURL = control.URL
	cfg.WorkerID = "w-final"
	cfg.ReplicaType = "s3"
	cfg.S3Endpoint = upstream.URL
	cfg.S3AccessKey = testS3AccessKey
	cfg.S3SecretKey = testS3SecretKey
	cfg.S3SessionToken = testS3SessionToken
	cfg.S3Region = testS3Region
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyMode = s3FaultProxyModeObserve
	cfg.S3FaultProxyListenAddr = "127.0.0.1:0"
	runner := NewRunner(cfg)
	runner.reporter = NewReporter(runner.cfg)
	ctx, cancel := context.WithCancel(context.Background())
	if err := runner.startS3FaultProxy(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.stopS3FaultProxy)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, runner.s3FaultProxyEndpoint+"/bucket/soak/w1/0000/0000000000000001-0000000000000001.ltx", bytes.NewReader(bytes.Repeat([]byte("a"), 64)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := testS3HTTPClient(time.Second).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	cancel()
	runner.sendFinalHeartbeat(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(heartbeats) != 1 {
		t.Fatalf("heartbeats after cancel = %d, want 1", len(heartbeats))
	}
	counters := heartbeats[0].ObjectStoreCounters
	if counters.ReplicaLevelPutTotals["L0"] != 1 || counters.ReplicaLevelPutBytes["L0"] != 64 || counters.ReplicaRequestTotals["PUT"] != 1 {
		t.Fatalf("final heartbeat counters = %+v", counters)
	}
}
