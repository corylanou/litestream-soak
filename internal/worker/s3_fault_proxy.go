package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

const maxS3FaultProxyAttemptKeys = 10000

type s3FaultProxyConfig struct {
	TargetEndpoint    string
	ListenAddr        string
	MinContentLength  int64
	ResetAfterBytes   int64
	FailFirstAttempts int
}

type s3FaultProxy struct {
	cfg      s3FaultProxyConfig
	server   *http.Server
	listener net.Listener
	endpoint string
	proxy    *httputil.ReverseProxy

	mu       sync.Mutex
	attempts map[string]int
}

func newS3FaultProxy(cfg s3FaultProxyConfig) *s3FaultProxy {
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		cfg.ListenAddr = "127.0.0.1:19000"
	}
	if cfg.ResetAfterBytes <= 0 {
		cfg.ResetAfterBytes = 1
	}
	return &s3FaultProxy{
		cfg:      cfg,
		attempts: make(map[string]int),
	}
}

func (p *s3FaultProxy) Start(ctx context.Context) error {
	target, err := parseProxyTargetEndpoint(p.cfg.TargetEndpoint)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", p.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen for s3 fault proxy: %w", err)
	}
	p.listener = listener
	p.endpoint = "http://" + listener.Addr().String()
	p.proxy = httputil.NewSingleHostReverseProxy(target)
	p.proxy.Transport = directHTTPTransport()
	p.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		slog.Warn("S3 fault proxy upstream request failed", "method", r.Method, "path", r.URL.String(), "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
	p.server = &http.Server{Handler: p}

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		_ = p.Close(context.Background())
		return ctx.Err()
	case err := <-errCh:
		return fmt.Errorf("start s3 fault proxy: %w", err)
	default:
	}

	slog.Info("Started S3 fault proxy", "listen", p.endpoint, "target", target.String())
	return nil
}

func (p *s3FaultProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.proxyConnect(w, r)
		return
	}
	if p.shouldReset(r) {
		p.resetConnection(w, r)
		return
	}
	if r.URL.IsAbs() {
		p.proxyHTTP(w, r)
		return
	}
	p.proxy.ServeHTTP(w, r)
}

func (p *s3FaultProxy) Close(ctx context.Context) error {
	if p.server == nil {
		return nil
	}
	if p.proxy != nil {
		if transport, ok := p.proxy.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	return p.server.Shutdown(ctx)
}

func (p *s3FaultProxy) Endpoint() string {
	return p.endpoint
}

func (p *s3FaultProxy) proxyConnect(w http.ResponseWriter, r *http.Request) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connection hijacking unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		return
	}

	var dialer net.Dialer
	upstreamConn, err := dialer.DialContext(r.Context(), "tcp", r.Host)
	if err != nil {
		_, _ = clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		_ = clientConn.Close()
		return
	}
	_, _ = clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	p.copyTunnel(r.Context(), r.Host, clientConn, upstreamConn)
}

func (p *s3FaultProxy) proxyHTTP(w http.ResponseWriter, r *http.Request) {
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.URL = cloneURL(r.URL)
	out.Host = r.Host
	transport := p.proxy.Transport
	if transport == nil {
		transport = directHTTPTransport()
	}
	resp, err := transport.RoundTrip(out)
	if err != nil {
		slog.Warn("S3 fault proxy HTTP request failed", "method", r.Method, "url", r.URL.String(), "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *s3FaultProxy) shouldReset(r *http.Request) bool {
	if p.cfg.FailFirstAttempts <= 0 || r.Method != http.MethodPut {
		return false
	}
	query := r.URL.Query()
	partNumber := strings.TrimSpace(query.Get("partNumber"))
	uploadID := strings.TrimSpace(query.Get("uploadId"))
	if partNumber == "" || uploadID == "" {
		return false
	}
	if p.cfg.MinContentLength > 0 && r.ContentLength >= 0 && r.ContentLength < p.cfg.MinContentLength {
		return false
	}

	key := r.URL.Path + "\x00" + uploadID + "\x00" + partNumber
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.attempts) > maxS3FaultProxyAttemptKeys {
		p.attempts = make(map[string]int)
	}
	p.attempts[key]++
	return p.attempts[key] <= p.cfg.FailFirstAttempts
}

func (p *s3FaultProxy) resetConnection(w http.ResponseWriter, r *http.Request) {
	if p.cfg.ResetAfterBytes > 0 {
		_, _ = io.CopyN(io.Discard, r.Body, p.cfg.ResetAfterBytes)
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connection reset by fault proxy", http.StatusServiceUnavailable)
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		return
	}
	slog.Warn("S3 fault proxy reset upload part connection", "path", r.URL.Path, "part_number", r.URL.Query().Get("partNumber"))
	_ = conn.Close()
}

func (p *s3FaultProxy) copyTunnel(ctx context.Context, host string, clientConn net.Conn, upstreamConn net.Conn) {
	done := make(chan struct{})
	closeOnce := sync.OnceFunc(func() {
		_ = clientConn.Close()
		_ = upstreamConn.Close()
		close(done)
	})
	go func() {
		_, _ = io.Copy(clientConn, upstreamConn)
		closeOnce()
	}()
	go func() {
		defer closeOnce()
		if p.cfg.FailFirstAttempts <= 0 {
			_, _ = io.Copy(upstreamConn, clientConn)
			return
		}
		threshold := p.cfg.ResetAfterBytes
		if threshold <= 0 {
			threshold = 1
		}
		copied, err := io.CopyN(upstreamConn, clientConn, threshold)
		if err != nil {
			return
		}
		if copied >= threshold && p.recordReset(host) {
			slog.Warn("S3 fault proxy reset CONNECT tunnel", "host", host)
			return
		}
		_, _ = io.Copy(upstreamConn, clientConn)
	}()

	select {
	case <-ctx.Done():
		closeOnce()
	case <-done:
	}
}

func (p *s3FaultProxy) recordReset(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.attempts) > maxS3FaultProxyAttemptKeys {
		p.attempts = make(map[string]int)
	}
	p.attempts[key]++
	return p.attempts[key] <= p.cfg.FailFirstAttempts
}

func parseProxyTargetEndpoint(endpoint string) (*url.URL, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("s3 fault proxy target endpoint is required")
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse s3 fault proxy target endpoint: %w", err)
	}
	if target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("invalid s3 fault proxy target endpoint %q", endpoint)
	}
	return target, nil
}

func (r *Runner) startS3FaultProxy(ctx context.Context) error {
	if !r.cfg.S3FaultProxyEnabled || r.cfg.ReplicaType != "s3" {
		return nil
	}
	targetEndpoint := firstNonEmpty(r.cfg.S3FaultProxyTargetEndpoint, r.cfg.S3Endpoint)
	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    targetEndpoint,
		ListenAddr:        r.cfg.S3FaultProxyListenAddr,
		MinContentLength:  r.cfg.S3FaultProxyMinContentLength,
		ResetAfterBytes:   r.cfg.S3FaultProxyResetAfterBytes,
		FailFirstAttempts: r.cfg.S3FaultProxyFailFirstAttempts,
	})
	if err := proxy.Start(ctx); err != nil {
		return err
	}
	r.s3FaultProxy = proxy
	r.cfg.S3FaultProxyTargetEndpoint = targetEndpoint
	r.s3FaultProxyEndpoint = proxy.Endpoint()
	return nil
}

func (r *Runner) stopS3FaultProxy() {
	if r.s3FaultProxy == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.s3FaultProxy.Close(ctx); err != nil {
		slog.Warn("Failed to stop S3 fault proxy", "error", err)
	}
}

func cloneURL(input *url.URL) *url.URL {
	if input == nil {
		return nil
	}
	clone := *input
	return &clone
}

func copyHeader(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func directHTTPTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return transport
}
