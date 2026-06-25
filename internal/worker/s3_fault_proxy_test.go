package worker

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
