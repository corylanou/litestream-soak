package worker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestS3FaultProxyResetsFirstUploadPartAttempts(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		MinContentLength:  8,
		ResetAfterBytes:   4,
		FailFirstAttempts: 2,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := &http.Client{Timeout: time.Second}
	url := proxy.Endpoint() + "/bucket/key?partNumber=1&uploadId=test-upload"
	for attempt := 1; attempt <= 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(bytes.Repeat([]byte("x"), 32)))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			t.Fatalf("attempt %d error = nil, want connection reset", attempt)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(bytes.Repeat([]byte("x"), 32)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("third attempt error = %v, want success", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("third attempt status = %d, want 200", resp.StatusCode)
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
}

func TestS3FaultProxyForwardsNonMultipartRequests(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		MinContentLength:  8,
		ResetAfterBytes:   4,
		FailFirstAttempts: 2,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, proxy.Endpoint()+"/bucket/key", bytes.NewReader(bytes.Repeat([]byte("x"), 32)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
}

func TestS3FaultProxyHonorsMaxFailuresForUploadPart(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "uploadpart-reset",
		MinContentLength:  8,
		ResetAfterBytes:   4,
		FailFirstAttempts: 3,
		MaxFailures:       2,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := &http.Client{Timeout: time.Second}
	for part := 1; part <= 3; part++ {
		url := fmt.Sprintf("%s/bucket/key?partNumber=%d&uploadId=test-upload", proxy.Endpoint(), part)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(bytes.Repeat([]byte("x"), 32)))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if part <= 2 {
			if err == nil {
				_ = resp.Body.Close()
				t.Fatalf("part %d error = nil, want induced reset", part)
			}
			continue
		}
		if err != nil {
			t.Fatalf("part %d error = %v, want success after max failures", part, err)
		}
		_ = resp.Body.Close()
	}
	if got := proxy.TotalFailures(); got != 2 {
		t.Fatalf("TotalFailures() = %d, want 2", got)
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
}

func TestS3FaultProxyDropsSourceGETResponseAndTracksObservation(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Length", "64")
		_, _ = w.Write(bytes.Repeat([]byte("x"), 64))
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "source-get-reset",
		SourceLevel:       "0001",
		ResetAfterBytes:   8,
		FailFirstAttempts: 1,
		MaxFailures:       1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	resp, err := http.Get(proxy.Endpoint() + "/bucket/db/0001/0000000000000001-0000000000000002.ltx")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	_, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr == nil {
		t.Fatal("ReadAll() error = nil, want dropped source GET body")
	}
	if got := proxy.ObservedSourceGETs(); got != 1 {
		t.Fatalf("ObservedSourceGETs() = %d, want 1", got)
	}
	if got := proxy.TotalFailures(); got != 1 {
		t.Fatalf("TotalFailures() = %d, want 1", got)
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
}

func TestS3FaultProxyInjectsRequestCanceled408WithoutReset(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "provider-408-requestcanceled",
		FailFirstAttempts: 1,
		MaxFailures:       1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	resp, err := http.Get(proxy.Endpoint() + "/bucket/db?list-type=2")
	if err != nil {
		t.Fatalf("Get() error = %v, want HTTP 408 response", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408", resp.StatusCode)
	}
	if !strings.Contains(string(body), "<Code>RequestCanceled</Code>") {
		t.Fatalf("body = %q, want RequestCanceled XML", body)
	}
	if got := upstreamHits.Load(); got != 0 {
		t.Fatalf("upstream hits = %d, want 0", got)
	}
}

func TestStartS3FaultProxyPreservesEndpointAndSetsLitestreamProxyEnv(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Endpoint = upstream.URL
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyListenAddr = "127.0.0.1:0"

	runner := NewRunner(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runner.startS3FaultProxy(ctx); err != nil {
		t.Fatalf("startS3FaultProxy() error = %v", err)
	}
	t.Cleanup(runner.stopS3FaultProxy)

	if runner.cfg.S3Endpoint != upstream.URL {
		t.Fatalf("S3Endpoint = %q, want original endpoint %q", runner.cfg.S3Endpoint, upstream.URL)
	}
	env := commandEnvMap(runner.litestreamEnv())
	if env["HTTP_PROXY"] != runner.s3FaultProxy.Endpoint() {
		t.Fatalf("HTTP_PROXY = %q, want %q", env["HTTP_PROXY"], runner.s3FaultProxy.Endpoint())
	}
	if env["HTTPS_PROXY"] != runner.s3FaultProxy.Endpoint() {
		t.Fatalf("HTTPS_PROXY = %q, want %q", env["HTTPS_PROXY"], runner.s3FaultProxy.Endpoint())
	}
	if env["NO_PROXY"] != "127.0.0.1,localhost" {
		t.Fatalf("NO_PROXY = %q, want localhost bypass", env["NO_PROXY"])
	}
}

func TestS3FaultProxyResetsConnectTunnelAfterThreshold(t *testing.T) {
	upstream, upstreamBytes := startCountingTCPServer(t)
	defer func() { _ = upstream.Close() }()

	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    "https://" + upstream.Addr().String(),
		ListenAddr:        "127.0.0.1:0",
		Mode:              "connect-reset",
		ResetAfterBytes:   16,
		FailFirstAttempts: 1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	proxyURL, err := url.Parse(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", proxyURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", upstream.Addr().String(), upstream.Addr().String()); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatalf("ReadResponse() error = %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want 200", resp.StatusCode)
	}
	if _, err := conn.Write(bytes.Repeat([]byte("x"), 64)); err != nil {
		t.Fatalf("write tunnel body: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("Read() error = nil, want tunnel close after threshold")
	}
	if !waitUntil(time.Second, 10*time.Millisecond, func() bool {
		return upstreamBytes.Load() >= 16
	}) {
		got := upstreamBytes.Load()
		t.Fatalf("upstream bytes = %d, want at least 16", got)
	}
}

func commandEnvMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			out[key] = value
		}
	}
	return out
}

func startCountingTCPServer(t *testing.T) (net.Listener, *atomic.Int64) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var bytesRead atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				n, _ := io.Copy(io.Discard, conn)
				bytesRead.Add(n)
			}()
		}
	}()
	return listener, &bytesRead
}
