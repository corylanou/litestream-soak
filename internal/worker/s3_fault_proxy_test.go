package worker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	testS3AccessKey    = "AKIDEXAMPLE"
	testS3SecretKey    = "test-secret-key"
	testS3SessionToken = "test-session-token"
	testS3Region       = "auto"
	testS3PayloadHash  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	testS3SigningScope = "s3"
)

func TestS3FaultProxyWithoutResigningForwardsIncomingHost(t *testing.T) {
	upstreamHost := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHost <- r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint: upstream.URL,
		ListenAddr:     "127.0.0.1:0",
		Mode:           s3FaultProxyModeObserve,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.Endpoint()+"/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", testS3PayloadHash)
	signS3RequestForTest(t, req, time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	proxyURL, err := url.Parse(proxy.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	if got := <-upstreamHost; got != proxyURL.Host {
		t.Fatalf("upstream Host = %q, want incoming proxy host %q", got, proxyURL.Host)
	}
}

func TestSigV4HostOnlyRewriteInvalidatesSignature(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:19000/bucket/key%2Fpart?list-type=2", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", testS3PayloadHash)
	signS3RequestForTest(t, req, time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC))
	if !validS3SignatureForTest(req) {
		t.Fatal("signature is invalid before Host rewrite")
	}

	rewritten := req.Clone(context.Background())
	rewritten.URL = cloneURL(req.URL)
	rewritten.URL.Scheme = "https"
	rewritten.URL.Host = "fly.storage.tigris.dev"
	rewritten.Host = rewritten.URL.Host
	if validS3SignatureForTest(rewritten) {
		t.Fatal("signature remains valid after Host-only rewrite")
	}
}

func TestS3FaultProxyResignsForTargetHost(t *testing.T) {
	var targetHost string
	var staleAuthorization string
	var staleSigningTime string
	freshSigningTime := time.Date(2026, 7, 28, 12, 5, 0, 0, time.UTC)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Host != targetHost:
			http.Error(w, "unexpected upstream host", http.StatusMisdirectedRequest)
		case r.URL.EscapedPath() != "/bucket/key%2Fpart":
			http.Error(w, "encoded path changed", http.StatusBadRequest)
		case r.Header.Get("Authorization") == staleAuthorization:
			http.Error(w, "stale authorization forwarded", http.StatusUnauthorized)
		case r.Header.Get("X-Amz-Date") == staleSigningTime:
			http.Error(w, "stale signing time forwarded", http.StatusUnauthorized)
		case r.Header.Get("X-Amz-Date") != freshSigningTime.Format("20060102T150405Z"):
			http.Error(w, "unexpected signing time", http.StatusUnauthorized)
		case r.Header.Get("X-Amz-Security-Token") != testS3SessionToken:
			http.Error(w, "stale session token forwarded", http.StatusUnauthorized)
		case r.Header.Get("X-Amz-Region-Set") != "":
			http.Error(w, "stale region set forwarded", http.StatusUnauthorized)
		case !validS3SignatureForTest(r):
			http.Error(w, "invalid target signature", http.StatusUnauthorized)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(upstream.Close)
	targetURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	targetHost = targetURL.Host

	t.Setenv("AWS_ACCESS_KEY_ID", testS3AccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", testS3SecretKey)
	t.Setenv("AWS_SESSION_TOKEN", testS3SessionToken)
	t.Setenv("AWS_REGION", testS3Region)
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	cfg.ReplicaType = "s3"
	cfg.S3Endpoint = upstream.URL
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyMode = s3FaultProxyModeUploadPartReset
	cfg.S3FaultProxyListenAddr = "127.0.0.1:0"
	cfg.S3FaultProxyFailFirstAttempts = 0
	runner := NewRunner(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runner.startS3FaultProxy(ctx); err != nil {
		t.Fatalf("startS3FaultProxy() error = %v", err)
	}
	t.Cleanup(runner.stopS3FaultProxy)
	signingTransport, ok := runner.s3FaultProxy.proxy.Transport.(*s3FaultSigningTransport)
	if !ok {
		t.Fatalf("transport = %T, want *s3FaultSigningTransport", runner.s3FaultProxy.proxy.Transport)
	}
	signingTransport.now = func() time.Time { return freshSigningTime }

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, runner.s3FaultProxy.Endpoint()+"/bucket/key%2Fpart?list-type=2", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", testS3PayloadHash)
	signS3RequestForTest(t, req, time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC))
	staleAuthorization = req.Header.Get("Authorization")
	staleSigningTime = req.Header.Get("X-Amz-Date")
	req.Header.Set("X-Amz-Security-Token", "stale-session-token")
	req.Header.Set("X-Amz-Region-Set", "stale-region")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestS3FaultProxyRequiresSigningConfigForFaultModes(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*s3FaultProxyConfig)
		wantErr error
	}{
		{
			name: "missing access key",
			mutate: func(cfg *s3FaultProxyConfig) {
				cfg.AccessKeyID = ""
			},
			wantErr: errS3FaultProxySigningCredentialsRequired,
		},
		{
			name: "missing secret key",
			mutate: func(cfg *s3FaultProxyConfig) {
				cfg.SecretAccessKey = ""
			},
			wantErr: errS3FaultProxySigningCredentialsRequired,
		},
		{
			name: "missing region",
			mutate: func(cfg *s3FaultProxyConfig) {
				cfg.Region = ""
			},
			wantErr: errS3FaultProxySigningRegionRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := withTestS3SigningConfig(s3FaultProxyConfig{
				TargetEndpoint: "https://fly.storage.tigris.dev",
				ListenAddr:     "127.0.0.1:0",
			})
			cfg.SessionToken = "sensitive-session-token"
			tt.mutate(&cfg)

			err := newS3FaultProxy(cfg).Start(context.Background())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Start() error = %v, want %v", err, tt.wantErr)
			}
			for _, secret := range []string{cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken} {
				if secret != "" && strings.Contains(err.Error(), secret) {
					t.Fatal("Start() error exposes signing credentials")
				}
			}
		})
	}
}

func TestS3FaultProxyRejectsMissingPayloadHashWithoutExposingSigningData(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)

	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previousLogger)

	accessKey := "SENSITIVEACCESSKEY"
	secretKey := "sensitive-secret-key"
	sessionToken := "sensitive-session-token"
	staleAuthorization := "AWS4-HMAC-SHA256 Credential=SENSITIVEACCESSKEY/20260728/auto/s3/aws4_request, SignedHeaders=host, Signature=sensitive-signature"
	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              s3FaultProxyModeProviderHTTP408,
		FailFirstAttempts: 1,
		AccessKeyID:       accessKey,
		SecretAccessKey:   secretKey,
		SessionToken:      sessionToken,
		Region:            testS3Region,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.Endpoint()+"/bucket/key?X-Amz-Credential=SENSITIVEACCESSKEY&X-Amz-Signature=sensitive-signature", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", staleAuthorization)
	req.Header.Set("X-Amz-Security-Token", sessionToken)
	client := &http.Client{Transport: directHTTPTransport(), Timeout: time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("first status = %d, want 408", resp.StatusCode)
	}

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("retry Do() error = %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if !strings.Contains(string(body), errS3FaultProxyPayloadHashRequired.Error()) {
		t.Fatalf("body = %q, want missing payload hash error", body)
	}
	if got := upstreamHits.Load(); got != 0 {
		t.Fatalf("upstream hits = %d, want 0", got)
	}
	proxy.mu.Lock()
	attemptKeys := make([]string, 0, len(proxy.attempts))
	for key := range proxy.attempts {
		attemptKeys = append(attemptKeys, key)
	}
	proxy.mu.Unlock()
	for _, sensitive := range []string{accessKey, secretKey, sessionToken, staleAuthorization, "sensitive-signature"} {
		if strings.Contains(string(body), sensitive) {
			t.Fatal("HTTP error exposes signing data")
		}
		if strings.Contains(logs.String(), sensitive) {
			t.Fatal("proxy logs expose signing data")
		}
		if strings.Contains(strings.Join(attemptKeys, "\n"), sensitive) {
			t.Fatal("proxy retry state exposes signing data")
		}
	}
}

func signS3RequestForTest(t *testing.T, req *http.Request, signingTime time.Time) {
	t.Helper()
	err := v4.NewSigner().SignHTTP(
		req.Context(),
		testS3Credentials(),
		req,
		req.Header.Get("X-Amz-Content-Sha256"),
		testS3SigningScope,
		testS3Region,
		signingTime,
		func(options *v4.SignerOptions) {
			options.DisableURIPathEscaping = true
		},
	)
	if err != nil {
		t.Fatalf("SignHTTP() error = %v", err)
	}
}

func validS3SignatureForTest(req *http.Request) bool {
	signingTime, err := time.Parse("20060102T150405Z", req.Header.Get("X-Amz-Date"))
	if err != nil {
		return false
	}
	signedHeaders := sigV4SignedHeaders(req.Header.Get("Authorization"))
	if len(signedHeaders) == 0 {
		return false
	}

	expected := req.Clone(context.Background())
	expected.RequestURI = ""
	expected.Header = make(http.Header)
	for _, name := range signedHeaders {
		switch name {
		case "authorization", "content-length", "host", "x-amz-date", "x-amz-security-token":
			continue
		}
		for _, value := range req.Header.Values(name) {
			expected.Header.Add(name, value)
		}
	}
	expected.Host = req.Host
	if expected.Host == "" {
		expected.Host = req.URL.Host
	}
	payloadHash := req.Header.Get("X-Amz-Content-Sha256")
	err = v4.NewSigner().SignHTTP(
		expected.Context(),
		testS3Credentials(),
		expected,
		payloadHash,
		testS3SigningScope,
		testS3Region,
		signingTime,
		func(options *v4.SignerOptions) {
			options.DisableURIPathEscaping = true
		},
	)
	if err != nil {
		return false
	}
	return expected.Header.Get("Authorization") == req.Header.Get("Authorization")
}

func newSignedS3Upstream(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()

	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target, err := url.Parse(upstream.URL)
		if err != nil {
			http.Error(w, "invalid test target", http.StatusInternalServerError)
			return
		}
		if r.Host != target.Host || !validS3SignatureForTest(r) {
			http.Error(w, "invalid target signature", http.StatusUnauthorized)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(upstream.Close)
	return upstream
}

func sigV4SignedHeaders(authorization string) []string {
	for _, part := range strings.Split(authorization, ",") {
		part = strings.TrimSpace(part)
		if value, ok := strings.CutPrefix(part, "SignedHeaders="); ok {
			return strings.Split(value, ";")
		}
	}
	return nil
}

func testS3Credentials() aws.Credentials {
	return aws.Credentials{
		AccessKeyID:     testS3AccessKey,
		SecretAccessKey: testS3SecretKey,
		SessionToken:    testS3SessionToken,
	}
}

func withTestS3SigningConfig(cfg s3FaultProxyConfig) s3FaultProxyConfig {
	cfg.AccessKeyID = testS3AccessKey
	cfg.SecretAccessKey = testS3SecretKey
	cfg.SessionToken = testS3SessionToken
	cfg.Region = testS3Region
	return cfg
}

type testS3PayloadHashTransport struct {
	transport http.RoundTripper
}

func (t testS3PayloadHashTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	out := r.Clone(r.Context())
	out.Header = r.Header.Clone()
	if out.Header.Get("X-Amz-Content-Sha256") == "" {
		out.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	}
	return t.transport.RoundTrip(out)
}

func testS3HTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: testS3PayloadHashTransport{transport: http.DefaultTransport},
		Timeout:   timeout,
	}
}

func TestS3FaultProxyResetsFirstUploadPartAttempts(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		MinContentLength:  8,
		ResetAfterBytes:   4,
		FailFirstAttempts: 2,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := testS3HTTPClient(time.Second)
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
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		MinContentLength:  8,
		ResetAfterBytes:   4,
		FailFirstAttempts: 2,
	}))
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
	resp, err := testS3HTTPClient(time.Second).Do(req)
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
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "uploadpart-reset",
		MinContentLength:  8,
		ResetAfterBytes:   4,
		FailFirstAttempts: 3,
		MaxFailures:       2,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := testS3HTTPClient(time.Second)
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
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Length", "64")
		_, _ = w.Write(bytes.Repeat([]byte("x"), 64))
	})

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "source-get-reset",
		SourceLevel:       "0001",
		ResetAfterBytes:   8,
		FailFirstAttempts: 1,
		MaxFailures:       1,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	resp, err := testS3HTTPClient(time.Second).Get(proxy.Endpoint() + "/bucket/db/0001/0000000000000001-0000000000000002.ltx")
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
	if got := proxy.ObservedSourceRangeGETs(); got != 0 {
		t.Fatalf("ObservedSourceRangeGETs() = %d, want 0", got)
	}
	if got := proxy.TotalFailures(); got != 1 {
		t.Fatalf("TotalFailures() = %d, want 1", got)
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
}

func TestS3FaultProxyTracksResumedSourceRangeGET(t *testing.T) {
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "64")
		_, _ = w.Write(bytes.Repeat([]byte("x"), 64))
	})

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "source-get-reset",
		SourceLevel:       "0001",
		ResetAfterBytes:   8,
		FailFirstAttempts: 1,
		MaxFailures:       1,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.Endpoint()+"/bucket/db/0001/0000000000000001-0000000000000002.ltx", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set("Range", "bytes=8-")

	resp, err := testS3HTTPClient(time.Second).Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if got := proxy.ObservedSourceGETs(); got != 1 {
		t.Fatalf("ObservedSourceGETs() = %d, want 1", got)
	}
	if got := proxy.ObservedSourceRangeGETs(); got != 1 {
		t.Fatalf("ObservedSourceRangeGETs() = %d, want 1", got)
	}
}

func TestS3FaultProxyInjectsHTTP408WithoutRequestCanceledCode(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "provider-http-408",
		FailFirstAttempts: 1,
		MaxFailures:       1,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := testS3HTTPClient(time.Second)
	resp, err := client.Get(proxy.Endpoint() + "/bucket/db?list-type=2")
	if err != nil {
		t.Fatalf("Get() error = %v, want HTTP 408 response", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408", resp.StatusCode)
	}
	if strings.Contains(string(body), "<Code>RequestCanceled</Code>") {
		t.Fatalf("body = %q, want no RequestCanceled XML", body)
	}
	resp, err = client.Get(proxy.Endpoint() + "/bucket/db?list-type=2")
	if err != nil {
		t.Fatalf("retry Get() error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry status = %d, want 200", resp.StatusCode)
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
}

func TestS3FaultProxyInjectsRequestCanceledCodeWithoutHTTP408(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "provider-request-canceled",
		FailFirstAttempts: 1,
		MaxFailures:       1,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := testS3HTTPClient(time.Second)
	resp, err := client.Get(proxy.Endpoint() + "/bucket/db?list-type=2")
	if err != nil {
		t.Fatalf("Get() error = %v, want RequestCanceled response", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if resp.StatusCode == http.StatusRequestTimeout {
		t.Fatalf("status = %d, want non-408 RequestCanceled response", resp.StatusCode)
	}
	if !strings.Contains(string(body), "<Code>RequestCanceled</Code>") {
		t.Fatalf("body = %q, want RequestCanceled XML", body)
	}
	resp, err = client.Get(proxy.Endpoint() + "/bucket/db?list-type=2")
	if err != nil {
		t.Fatalf("retry Get() error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry status = %d, want 200", resp.StatusCode)
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
}

func TestS3FaultProxyResetsFaultCountersForNextCycle(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := newSignedS3Upstream(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "provider-http-408",
		FailFirstAttempts: 1,
		MaxFailures:       1,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := testS3HTTPClient(time.Second)
	for i := 0; i < 2; i++ {
		resp, err := client.Get(proxy.Endpoint() + "/bucket/db?list-type=2")
		if err != nil {
			t.Fatalf("cycle %d first Get() error = %v", i+1, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusRequestTimeout {
			t.Fatalf("cycle %d first status = %d, want 408", i+1, resp.StatusCode)
		}

		resp, err = client.Get(proxy.Endpoint() + "/bucket/db?list-type=2")
		if err != nil {
			t.Fatalf("cycle %d second Get() error = %v", i+1, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("cycle %d second status = %d, want 200", i+1, resp.StatusCode)
		}

		proxy.ResetCycle()
	}

	if got := upstreamHits.Load(); got != 2 {
		t.Fatalf("upstream hits = %d, want 2", got)
	}
}

func TestStartS3FaultProxyRoutesLitestreamEndpointThroughProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Endpoint = upstream.URL
	cfg.S3AccessKey = testS3AccessKey
	cfg.S3SecretKey = testS3SecretKey
	cfg.S3Region = testS3Region
	cfg.S3FaultProxyEnabled = true
	cfg.S3FaultProxyListenAddr = "127.0.0.1:0"

	runner := NewRunner(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runner.startS3FaultProxy(ctx); err != nil {
		t.Fatalf("startS3FaultProxy() error = %v", err)
	}
	t.Cleanup(runner.stopS3FaultProxy)

	if runner.cfg.S3Endpoint != runner.s3FaultProxy.Endpoint() {
		t.Fatalf("S3Endpoint = %q, want proxy endpoint %q", runner.cfg.S3Endpoint, runner.s3FaultProxy.Endpoint())
	}
	if runner.cfg.S3FaultProxyTargetEndpoint != upstream.URL {
		t.Fatalf("S3FaultProxyTargetEndpoint = %q, want original endpoint %q", runner.cfg.S3FaultProxyTargetEndpoint, upstream.URL)
	}
	if runner.cfg.S3FaultProxyEndpoint != runner.s3FaultProxy.Endpoint() {
		t.Fatalf("S3FaultProxyEndpoint = %q, want proxy endpoint %q", runner.cfg.S3FaultProxyEndpoint, runner.s3FaultProxy.Endpoint())
	}
	data := runner.litestreamConfigData()
	if got := data.Databases[0].Replica.S3Endpoint; got != runner.s3FaultProxy.Endpoint() {
		t.Fatalf("litestream replica endpoint = %q, want proxy endpoint %q", got, runner.s3FaultProxy.Endpoint())
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

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    "https://" + upstream.Addr().String(),
		ListenAddr:        "127.0.0.1:0",
		Mode:              "connect-reset",
		ResetAfterBytes:   16,
		FailFirstAttempts: 1,
	}))
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

func TestNormalizeS3FaultProxyModeObserve(t *testing.T) {
	if got := normalizeS3FaultProxyMode("observe"); got != s3FaultProxyModeObserve {
		t.Fatalf("normalizeS3FaultProxyMode(\"observe\") = %q, want %q", got, s3FaultProxyModeObserve)
	}
	if got := normalizeS3FaultProxyMode(""); got != s3FaultProxyModeUploadPartReset {
		t.Fatalf("normalizeS3FaultProxyMode(\"\") = %q, want %q", got, s3FaultProxyModeUploadPartReset)
	}
}

func TestS3FaultProxyObserveModeNeverInjectsFaults(t *testing.T) {
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
		Mode:              "observe",
		SourceLevel:       "0001",
		ResetAfterBytes:   4,
		FailFirstAttempts: 3,
		MaxFailures:       5,
	})
	if proxy.cfg.FailFirstAttempts != 0 {
		t.Fatalf("observe mode FailFirstAttempts = %d, want 0", proxy.cfg.FailFirstAttempts)
	}
	if proxy.cfg.MaxFailures != 0 {
		t.Fatalf("observe mode MaxFailures = %d, want 0", proxy.cfg.MaxFailures)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := &http.Client{Timeout: time.Second}
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, proxy.Endpoint()+"/bucket/key?partNumber=1&uploadId=test-upload", bytes.NewReader(bytes.Repeat([]byte("x"), 32)))
	if err != nil {
		t.Fatal(err)
	}
	putResp, err := client.Do(putReq)
	if err != nil {
		t.Fatalf("multipart PUT error = %v, want pass-through", err)
	}
	_ = putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("multipart PUT status = %d, want 200", putResp.StatusCode)
	}

	getResp, err := client.Get(proxy.Endpoint() + "/bucket/db/0001/0000000000000001-0000000000000002.ltx")
	if err != nil {
		t.Fatalf("source GET error = %v, want pass-through", err)
	}
	if _, err := io.ReadAll(getResp.Body); err != nil {
		t.Fatalf("source GET ReadAll() error = %v, want full body", err)
	}
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("source GET status = %d, want 200", getResp.StatusCode)
	}

	if got := proxy.TotalFailures(); got != 0 {
		t.Fatalf("TotalFailures() = %d, want 0", got)
	}
	if got := upstreamHits.Load(); got != 2 {
		t.Fatalf("upstream hits = %d, want 2", got)
	}
}

func TestS3FaultProxyCountsListRequests(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint: upstream.URL,
		ListenAddr:     "127.0.0.1:0",
		Mode:           "observe",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := &http.Client{Timeout: time.Second}

	listResp, err := client.Get(proxy.Endpoint() + "/bucket?list-type=2&prefix=db/")
	if err != nil {
		t.Fatalf("LIST Get() error = %v", err)
	}
	_ = listResp.Body.Close()
	if got := proxy.ListRequests(); got != 1 {
		t.Fatalf("ListRequests() after LIST = %d, want 1", got)
	}

	objResp, err := client.Get(proxy.Endpoint() + "/bucket/db/key")
	if err != nil {
		t.Fatalf("object Get() error = %v", err)
	}
	_ = objResp.Body.Close()

	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, proxy.Endpoint()+"/bucket/db/key", bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	putResp, err := client.Do(putReq)
	if err != nil {
		t.Fatalf("PUT error = %v", err)
	}
	_ = putResp.Body.Close()

	if got := proxy.ListRequests(); got != 1 {
		t.Fatalf("ListRequests() after non-LIST requests = %d, want 1", got)
	}
}

func TestS3FaultProxyCountsListRequestsInDefaultMode(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint: upstream.URL,
		ListenAddr:     "127.0.0.1:0",
	}))
	if proxy.cfg.Mode != s3FaultProxyModeUploadPartReset {
		t.Fatalf("default mode = %q, want %q", proxy.cfg.Mode, s3FaultProxyModeUploadPartReset)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	resp, err := testS3HTTPClient(time.Second).Get(proxy.Endpoint() + "/bucket?list-type=2")
	if err != nil {
		t.Fatalf("LIST Get() error = %v", err)
	}
	_ = resp.Body.Close()

	if got := proxy.ListRequests(); got != 1 {
		t.Fatalf("ListRequests() = %d, want 1", got)
	}
}

func TestS3FaultProxyResetCyclePreservesListRequests(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	proxy := newS3FaultProxy(withTestS3SigningConfig(s3FaultProxyConfig{
		TargetEndpoint:    upstream.URL,
		ListenAddr:        "127.0.0.1:0",
		Mode:              "provider-http-408",
		FailFirstAttempts: 1,
		MaxFailures:       1,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := proxy.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close(context.Background()) })

	client := testS3HTTPClient(time.Second)
	for i := 0; i < 2; i++ {
		resp, err := client.Get(proxy.Endpoint() + "/bucket/db?list-type=2")
		if err != nil {
			t.Fatalf("Get() %d error = %v", i+1, err)
		}
		_ = resp.Body.Close()
	}

	if got := proxy.TotalFailures(); got != 1 {
		t.Fatalf("TotalFailures() before reset = %d, want 1", got)
	}
	if got := proxy.ListRequests(); got != 2 {
		t.Fatalf("ListRequests() before reset = %d, want 2", got)
	}

	proxy.ResetCycle()

	if got := proxy.TotalFailures(); got != 0 {
		t.Fatalf("TotalFailures() after reset = %d, want 0", got)
	}
	if got := proxy.ObservedSourceGETs(); got != 0 {
		t.Fatalf("ObservedSourceGETs() after reset = %d, want 0", got)
	}
	if got := proxy.ListRequests(); got != 2 {
		t.Fatalf("ListRequests() after reset = %d, want 2", got)
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
