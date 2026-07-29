package worker

import (
	"context"
	"crypto/sha256"
	"errors"
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

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	maxS3FaultProxyAttemptKeys              = 10000
	s3FaultProxyModeUploadPartReset         = "uploadpart-reset"
	s3FaultProxyModeSourceGETReset          = "source-get-reset"
	s3FaultProxyModeProvider408Canceled     = "provider-408-requestcanceled"
	s3FaultProxyModeProviderHTTP408         = "provider-http-408"
	s3FaultProxyModeProviderRequestCanceled = "provider-request-canceled"
	s3FaultProxyModeConnectReset            = "connect-reset"
	s3FaultProxyModeObserve                 = "observe"
	defaultS3FaultProxySourceLevel          = "0001"
	requestCanceledResponseBody             = `<Error><Code>RequestCanceled</Code><Message>Request is canceled.</Message><RequestId>fault-proxy</RequestId></Error>`
	requestTimeoutResponseBody              = `<Error><Code>RequestTimeout</Code><Message>Request timeout.</Message><RequestId>fault-proxy</RequestId></Error>`
)

var (
	errS3FaultProxySigningCredentialsRequired = errors.New("s3 fault proxy signing credentials are required")
	errS3FaultProxySigningRegionRequired      = errors.New("s3 fault proxy signing region is required")
	errS3FaultProxyPayloadHashRequired        = errors.New("s3 fault proxy x-amz-content-sha256 header is required")
	errS3FaultProxySigningFailed              = errors.New("s3 fault proxy request signing failed")
	errS3FaultProxyUpstreamRequestFailed      = errors.New("s3 fault proxy upstream request failed")
)

type s3FaultProxyConfig struct {
	TargetEndpoint    string
	ListenAddr        string
	Mode              string
	MinContentLength  int64
	ResetAfterBytes   int64
	FailFirstAttempts int
	MaxFailures       int
	SourceLevel       string
	AccessKeyID       string
	SecretAccessKey   string
	SessionToken      string
	Region            string
}

type s3FaultProxy struct {
	cfg      s3FaultProxyConfig
	server   *http.Server
	listener net.Listener
	endpoint string
	target   *url.URL
	proxy    *httputil.ReverseProxy

	mu                     sync.Mutex
	attempts               map[string]int
	totalFailures          int
	observedSourceGET      int
	observedSourceRangeGET int
	listRequests           int64
}

func newS3FaultProxy(cfg s3FaultProxyConfig) *s3FaultProxy {
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		cfg.ListenAddr = "127.0.0.1:19000"
	}
	cfg.Mode = normalizeS3FaultProxyMode(cfg.Mode)
	if cfg.Mode == s3FaultProxyModeObserve {
		cfg.FailFirstAttempts = 0
		cfg.MaxFailures = 0
	}
	if cfg.ResetAfterBytes <= 0 {
		cfg.ResetAfterBytes = 1
	}
	if strings.TrimSpace(cfg.SourceLevel) == "" {
		cfg.SourceLevel = defaultS3FaultProxySourceLevel
	}
	return &s3FaultProxy{
		cfg:      cfg,
		attempts: make(map[string]int),
	}
}

func (p *s3FaultProxy) Start(ctx context.Context) error {
	signingEnabled, err := s3FaultProxyModeRequiresSigning(p.cfg.Mode)
	if err != nil {
		return err
	}
	if signingEnabled {
		if err := validateS3FaultProxySigningConfig(p.cfg); err != nil {
			return err
		}
	}

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
	p.target = target
	p.proxy = httputil.NewSingleHostReverseProxy(target)
	transport := directHTTPTransport()
	if signingEnabled {
		p.proxy.Transport = &s3FaultSigningTransport{
			transport: transport,
			signer:    v4.NewSigner(),
			credentials: aws.Credentials{
				AccessKeyID:     p.cfg.AccessKeyID,
				SecretAccessKey: p.cfg.SecretAccessKey,
				SessionToken:    p.cfg.SessionToken,
			},
			region: p.cfg.Region,
		}
	} else {
		p.proxy.Transport = transport
	}
	p.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		safeErr := safeS3FaultProxyError(err)
		slog.Warn("S3 fault proxy upstream request failed", "method", r.Method, "path", r.URL.Path, "error", safeErr)
		http.Error(w, safeErr.Error(), http.StatusBadGateway)
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

	slog.Info("Started S3 fault proxy", "listen", p.endpoint, "target", target.Scheme+"://"+target.Host)
	return nil
}

func (p *s3FaultProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.proxyConnect(w, r)
		return
	}
	if isS3ListRequest(r) {
		p.recordListRequest()
	}
	if p.shouldInjectProviderHTTP408(r) {
		p.injectProviderHTTP408(w, r)
		return
	}
	if p.shouldInjectProviderRequestCanceled(r) {
		p.injectProviderRequestCanceled(w, r)
		return
	}
	if p.shouldDropSourceGET(r) {
		p.dropSourceGETResponse(w, r)
		return
	}
	if p.shouldResetUploadPart(r) {
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
		if transport, ok := p.proxy.Transport.(interface{ CloseIdleConnections() }); ok {
			transport.CloseIdleConnections()
		}
	}
	return p.server.Shutdown(ctx)
}

func (p *s3FaultProxy) Endpoint() string {
	return p.endpoint
}

func (p *s3FaultProxy) TotalFailures() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalFailures
}

func (p *s3FaultProxy) ObservedSourceGETs() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.observedSourceGET
}

func (p *s3FaultProxy) ObservedSourceRangeGETs() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.observedSourceRangeGET
}

func (p *s3FaultProxy) ListRequests() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.listRequests
}

func (p *s3FaultProxy) ResetCycle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts = make(map[string]int)
	p.totalFailures = 0
	p.observedSourceGET = 0
	p.observedSourceRangeGET = 0
	// listRequests is intentionally exempt: it is reported as a monotonic total.
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
	resp, err := p.roundTrip(r)
	if err != nil {
		safeErr := safeS3FaultProxyError(err)
		slog.Warn("S3 fault proxy HTTP request failed", "method", r.Method, "path", r.URL.Path, "error", safeErr)
		http.Error(w, safeErr.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *s3FaultProxy) shouldResetUploadPart(r *http.Request) bool {
	if p.cfg.Mode != s3FaultProxyModeUploadPartReset || r.Method != http.MethodPut {
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
	return p.recordFault("uploadpart\x00" + key)
}

func (p *s3FaultProxy) shouldDropSourceGET(r *http.Request) bool {
	if p.cfg.Mode != s3FaultProxyModeSourceGETReset || r.Method != http.MethodGet {
		return false
	}
	if !p.matchesSourceLevel(r.URL.Path) {
		return false
	}
	p.recordObservedSourceGET(r.Header.Get("Range"))
	return p.recordFault("source-get\x00" + r.URL.Path)
}

func (p *s3FaultProxy) shouldInjectProviderHTTP408(r *http.Request) bool {
	if p.cfg.Mode != s3FaultProxyModeProviderHTTP408 || r.Method != http.MethodGet {
		return false
	}
	return p.recordFault("provider-http-408\x00" + s3FaultRequestKey(r))
}

func (p *s3FaultProxy) shouldInjectProviderRequestCanceled(r *http.Request) bool {
	if p.cfg.Mode != s3FaultProxyModeProviderRequestCanceled || r.Method != http.MethodGet {
		return false
	}
	return p.recordFault("provider-request-canceled\x00" + s3FaultRequestKey(r))
}

func (p *s3FaultProxy) matchesSourceLevel(path string) bool {
	level := strings.Trim(strings.TrimSpace(p.cfg.SourceLevel), "/")
	if level == "" {
		level = defaultS3FaultProxySourceLevel
	}
	return strings.Contains(path, "/"+level+"/")
}

func (p *s3FaultProxy) injectProviderHTTP408(w http.ResponseWriter, r *http.Request) {
	slog.Warn("S3 fault proxy injected provider HTTP 408", "path", r.URL.Path)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("X-Amz-Request-Id", "fault-proxy")
	w.WriteHeader(http.StatusRequestTimeout)
	_, _ = w.Write([]byte(requestTimeoutResponseBody))
}

func (p *s3FaultProxy) injectProviderRequestCanceled(w http.ResponseWriter, r *http.Request) {
	slog.Warn("S3 fault proxy injected provider RequestCanceled", "path", r.URL.Path)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("X-Amz-Request-Id", "fault-proxy")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write([]byte(requestCanceledResponseBody))
}

func (p *s3FaultProxy) dropSourceGETResponse(w http.ResponseWriter, r *http.Request) {
	resp, err := p.roundTrip(r)
	if err != nil {
		safeErr := safeS3FaultProxyError(err)
		slog.Warn("S3 fault proxy source GET upstream request failed", "method", r.Method, "path", r.URL.Path, "error", safeErr)
		http.Error(w, safeErr.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	dropBytes := p.sourceDropBytes(resp)
	if dropBytes > 0 {
		_, _ = io.CopyN(w, resp.Body, dropBytes)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		return
	}
	slog.Warn("S3 fault proxy dropped source GET response", "path", r.URL.Path, "level", p.cfg.SourceLevel, "range", r.Header.Get("Range"), "drop_bytes", dropBytes)
	_ = conn.Close()
}

func (p *s3FaultProxy) sourceDropBytes(resp *http.Response) int64 {
	if p.cfg.ResetAfterBytes <= 0 {
		return 1
	}
	if resp != nil && resp.ContentLength > 0 && resp.ContentLength <= p.cfg.ResetAfterBytes {
		n := resp.ContentLength / 2
		if n < 1 {
			return 1
		}
		return n
	}
	return p.cfg.ResetAfterBytes
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
		if p.cfg.Mode != s3FaultProxyModeConnectReset {
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
		if copied >= threshold && p.recordFault("connect\x00"+host) {
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

func (p *s3FaultProxy) recordFault(key string) bool {
	if p.cfg.FailFirstAttempts <= 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.attempts) > maxS3FaultProxyAttemptKeys {
		p.attempts = make(map[string]int)
	}
	if p.cfg.MaxFailures > 0 && p.totalFailures >= p.cfg.MaxFailures {
		return false
	}
	p.attempts[key]++
	if p.attempts[key] > p.cfg.FailFirstAttempts {
		return false
	}
	p.totalFailures++
	return true
}

func (p *s3FaultProxy) recordObservedSourceGET(rangeHeader string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observedSourceGET++
	if isNonzeroRangeHeader(rangeHeader) {
		p.observedSourceRangeGET++
	}
}

func (p *s3FaultProxy) recordListRequest() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listRequests++
}

func isS3ListRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && r.URL.Query().Get("list-type") != ""
}

func isNonzeroRangeHeader(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if !strings.HasPrefix(value, "bytes=") {
		return false
	}
	value = strings.TrimPrefix(value, "bytes=")
	value = strings.TrimSpace(strings.SplitN(value, "-", 2)[0])
	return value != "" && value != "0"
}

func s3FaultRequestKey(r *http.Request) string {
	queryHash := sha256.Sum256([]byte(r.URL.RawQuery))
	return fmt.Sprintf("%s\x00%x", r.URL.Path, queryHash)
}

func (p *s3FaultProxy) roundTrip(r *http.Request) (*http.Response, error) {
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.URL = cloneURL(r.URL)
	if !out.URL.IsAbs() && p.target != nil {
		out.URL.Scheme = p.target.Scheme
		out.URL.Host = p.target.Host
		out.Host = p.target.Host
	}
	transport := p.proxy.Transport
	if transport == nil {
		transport = directHTTPTransport()
	}
	return transport.RoundTrip(out)
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

func normalizeS3FaultProxyMode(mode string) string {
	mode = strings.TrimSpace(strings.ToLower(mode))
	switch mode {
	case "", s3FaultProxyModeUploadPartReset:
		return s3FaultProxyModeUploadPartReset
	case s3FaultProxyModeProvider408Canceled:
		return s3FaultProxyModeProviderHTTP408
	case s3FaultProxyModeSourceGETReset, s3FaultProxyModeProviderHTTP408, s3FaultProxyModeProviderRequestCanceled, s3FaultProxyModeConnectReset, s3FaultProxyModeObserve:
		return mode
	default:
		return mode
	}
}

func s3FaultProxyModeRequiresSigning(mode string) (bool, error) {
	switch normalizeS3FaultProxyMode(mode) {
	case s3FaultProxyModeObserve:
		return false, nil
	case s3FaultProxyModeUploadPartReset,
		s3FaultProxyModeSourceGETReset,
		s3FaultProxyModeProviderHTTP408,
		s3FaultProxyModeProviderRequestCanceled,
		s3FaultProxyModeConnectReset:
		return true, nil
	default:
		return false, fmt.Errorf("invalid s3 fault proxy mode %q", mode)
	}
}

func validateS3FaultProxySigningConfig(cfg s3FaultProxyConfig) error {
	if strings.TrimSpace(cfg.AccessKeyID) == "" || cfg.SecretAccessKey == "" {
		return errS3FaultProxySigningCredentialsRequired
	}
	if strings.TrimSpace(cfg.Region) == "" {
		return errS3FaultProxySigningRegionRequired
	}
	return nil
}

func safeS3FaultProxyError(err error) error {
	switch {
	case errors.Is(err, errS3FaultProxyPayloadHashRequired):
		return errS3FaultProxyPayloadHashRequired
	case errors.Is(err, errS3FaultProxySigningFailed):
		return errS3FaultProxySigningFailed
	default:
		return errS3FaultProxyUpstreamRequestFailed
	}
}

type s3FaultSigningTransport struct {
	transport   http.RoundTripper
	signer      *v4.Signer
	credentials aws.Credentials
	region      string
	now         func() time.Time
}

func (t *s3FaultSigningTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	out := r.Clone(r.Context())
	out.Header = r.Header.Clone()
	out.URL = cloneURL(r.URL)
	out.Host = out.URL.Host

	payloadHash := strings.TrimSpace(out.Header.Get("X-Amz-Content-Sha256"))
	if payloadHash == "" {
		return nil, errS3FaultProxyPayloadHashRequired
	}
	out.Header.Set("X-Amz-Content-Sha256", payloadHash)
	out.Header.Del("Authorization")
	out.Header.Del("X-Amz-Date")
	out.Header.Del("X-Amz-Security-Token")
	out.Header.Del("X-Amz-Region-Set")

	signingTime := time.Now().UTC()
	if t.now != nil {
		signingTime = t.now().UTC()
	}
	if err := t.signer.SignHTTP(
		out.Context(),
		t.credentials,
		out,
		payloadHash,
		"s3",
		t.region,
		signingTime,
		func(options *v4.SignerOptions) {
			options.DisableURIPathEscaping = true
		},
	); err != nil {
		out.Header.Del("Authorization")
		out.Header.Del("X-Amz-Date")
		out.Header.Del("X-Amz-Security-Token")
		return nil, errS3FaultProxySigningFailed
	}
	return t.transport.RoundTrip(out)
}

func (t *s3FaultSigningTransport) CloseIdleConnections() {
	if transport, ok := t.transport.(interface{ CloseIdleConnections() }); ok {
		transport.CloseIdleConnections()
	}
}

func (r *Runner) startS3FaultProxy(ctx context.Context) error {
	if !r.cfg.S3FaultProxyEnabled || r.cfg.ReplicaType != "s3" {
		return nil
	}
	targetEndpoint := firstNonEmpty(r.cfg.S3FaultProxyTargetEndpoint, r.cfg.S3Endpoint)
	proxy := newS3FaultProxy(s3FaultProxyConfig{
		TargetEndpoint:    targetEndpoint,
		ListenAddr:        r.cfg.S3FaultProxyListenAddr,
		Mode:              r.cfg.S3FaultProxyMode,
		MinContentLength:  r.cfg.S3FaultProxyMinContentLength,
		ResetAfterBytes:   r.cfg.S3FaultProxyResetAfterBytes,
		FailFirstAttempts: r.cfg.S3FaultProxyFailFirstAttempts,
		MaxFailures:       r.cfg.S3FaultProxyMaxFailures,
		SourceLevel:       r.cfg.S3FaultProxySourceLevel,
		AccessKeyID:       r.cfg.S3AccessKey,
		SecretAccessKey:   r.cfg.S3SecretKey,
		SessionToken:      r.cfg.S3SessionToken,
		Region:            r.cfg.S3Region,
	})
	if err := proxy.Start(ctx); err != nil {
		return err
	}
	r.s3FaultProxy = proxy
	r.cfg.S3FaultProxyTargetEndpoint = targetEndpoint
	r.s3FaultProxyEndpoint = proxy.Endpoint()
	r.cfg.S3FaultProxyEndpoint = proxy.Endpoint()
	r.cfg.S3Endpoint = proxy.Endpoint()
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
