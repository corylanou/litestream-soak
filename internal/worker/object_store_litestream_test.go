package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type recordingS3Fixture struct {
	mu       sync.Mutex
	requests map[string]uint64
	hosts    map[string]bool
	unsigned int
}

func (f *recordingS3Fixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests[s3RequestOperation(r)]++
	f.hosts[r.Host] = true
	if !validS3SignatureForTest(r) {
		f.unsigned++
	}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") != "" {
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`))
		return
	}
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
}

func (f *recordingS3Fixture) snapshot() (map[string]uint64, map[string]bool, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	requests := make(map[string]uint64, len(f.requests))
	for key, value := range f.requests {
		requests[key] = value
	}
	hosts := make(map[string]bool, len(f.hosts))
	for key := range f.hosts {
		hosts[key] = true
	}
	return requests, hosts, f.unsigned
}

func TestRealLitestreamRequestsMatchMeters(t *testing.T) {
	if os.Getenv("SOAK_TEST_LITESTREAM_BINARY") != "1" {
		t.Skip("set SOAK_TEST_LITESTREAM_BINARY=1 to run against the installed litestream binary")
	}
	binary, err := exec.LookPath("litestream")
	if err != nil {
		t.Fatalf("litestream binary: %v", err)
	}
	fixture := &recordingS3Fixture{requests: make(map[string]uint64), hosts: make(map[string]bool)}
	upstream := httptest.NewServer(fixture)
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint: upstream.URL,
		ListenAddr:     "127.0.0.1:0",
		Mode:           s3FaultProxyModeObserve,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	var output restoreOutput
	cmd := exec.CommandContext(ctx, binary, "restore", "-o", filepath.Join(t.TempDir(), "restored.db"), "s3://bucket/soak/db?endpoint="+proxy.Endpoint()+"&region=us-east-1&force-path-style=true")
	cmd.Env = append(os.Environ(), "LOG_LEVEL=DEBUG", "AWS_ACCESS_KEY_ID="+testS3AccessKey, "AWS_SECRET_ACCESS_KEY="+testS3SecretKey, "AWS_REGION=us-east-1")
	cmd.Stdout, cmd.Stderr = &output, &output
	_ = cmd.Run()
	text := output.text()

	served, hosts, unsigned := fixture.snapshot()
	if len(served) == 0 || served["LIST"] == 0 {
		t.Fatalf("fixture saw no LIST requests: %v\n%s", served, text)
	}
	if !reflect.DeepEqual(output.requests, served) {
		t.Fatalf("restore log requests = %v, fixture served %v\n%s", output.requests, served, text)
	}
	if proxied := proxy.RequestCounters().Requests; !reflect.DeepEqual(proxied, served) {
		t.Fatalf("observe proxy requests = %v, fixture served %v", proxied, served)
	}
	target := upstream.Listener.Addr().String()
	if unsigned != 0 {
		t.Fatalf("%d proxied requests carried an invalid signature", unsigned)
	}
	if len(hosts) != 1 || !hosts[target] {
		t.Fatalf("fixture Host headers = %v, want only %s", hosts, target)
	}
}
