package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type ComparisonS3Evidence struct {
	Requests      int64    `json:"requests"`
	UploadBytes   int64    `json:"upload_bytes"`
	DownloadBytes int64    `json:"download_bytes"`
	Failures      []string `json:"failures"`
}

type ComparisonS3Observer struct {
	server    *http.Server
	endpoint  string
	transport *s3FaultSigningTransport
	mu        sync.Mutex
	evidence  ComparisonS3Evidence
}

func StartComparisonS3Observer(ctx context.Context, endpoint, region, access, secret, token string) (*ComparisonS3Observer, error) {
	cfg := s3FaultProxyConfig{Region: region, AccessKeyID: access, SecretAccessKey: secret}
	if err := validateS3FaultProxySigningConfig(cfg); err != nil {
		return nil, err
	}
	target, err := parseProxyTargetEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	observer := &ComparisonS3Observer{endpoint: "http://" + listener.Addr().String()}
	observer.transport = &s3FaultSigningTransport{transport: directHTTPTransport(), signer: v4.NewSigner(), credentials: aws.Credentials{AccessKeyID: access, SecretAccessKey: secret, SessionToken: token}, region: region}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		observer.mu.Lock()
		observer.evidence.Requests++
		observer.mu.Unlock()
		if r.Body != nil {
			r.Body = &countedBody{ReadCloser: r.Body, failed: func() {
				observer.mu.Lock()
				observer.evidence.Failures = append(observer.evidence.Failures, "S3 request body read failure")
				observer.mu.Unlock()
			}, add: func(n int) { observer.mu.Lock(); observer.evidence.UploadBytes += int64(n); observer.mu.Unlock() }}
		}
		response, err := observer.transport.RoundTrip(r)
		if err != nil {
			observer.mu.Lock()
			observer.evidence.Failures = append(observer.evidence.Failures, "S3 transport failure")
			observer.mu.Unlock()
			return nil, err
		}
		if response.StatusCode >= 400 {
			observer.mu.Lock()
			observer.evidence.Failures = append(observer.evidence.Failures, fmt.Sprintf("%s returned HTTP %d", r.Method, response.StatusCode))
			observer.mu.Unlock()
		}
		response.Body = &countedBody{ReadCloser: response.Body, failed: func() {
			observer.mu.Lock()
			observer.evidence.Failures = append(observer.evidence.Failures, "S3 response body read failure")
			observer.mu.Unlock()
		}, add: func(n int) { observer.mu.Lock(); observer.evidence.DownloadBytes += int64(n); observer.mu.Unlock() }}
		return response, nil
	})
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "S3 observation proxy upstream failure", http.StatusBadGateway)
	}
	observer.server = &http.Server{Handler: proxy, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() { _ = observer.server.Serve(listener) }()
	return observer, nil
}

func (o *ComparisonS3Observer) Endpoint() string { return o.endpoint }
func (o *ComparisonS3Observer) Close(ctx context.Context) error {
	o.transport.CloseIdleConnections()
	return o.server.Shutdown(ctx)
}
func (o *ComparisonS3Observer) Evidence() ComparisonS3Evidence {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := o.evidence
	result.Failures = append([]string(nil), result.Failures...)
	return result
}

type countedBody struct {
	io.ReadCloser
	add    func(int)
	failed func()
}

func (b *countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.add(n)
	if err != nil && !errors.Is(err, io.EOF) && b.failed != nil {
		b.failed()
	}
	return n, err
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
