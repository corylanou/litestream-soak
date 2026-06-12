package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type pprofCapturer struct {
	cfg *Config
}

func newPprofCapturer(cfg *Config) *pprofCapturer {
	return &pprofCapturer{cfg: cfg}
}

func (c *pprofCapturer) Run(ctx context.Context) {
	c.captureSet(ctx, "baseline")

	hourly := time.NewTicker(time.Hour)
	defer hourly.Stop()
	cpu := time.NewTicker(6 * time.Hour)
	defer cpu.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hourly.C:
			c.captureSet(ctx, "hourly")
		case <-cpu.C:
			c.captureEndpoint(ctx, "cpu", "profile", "profile?seconds=30", 40*time.Second)
		}
	}
}

func (c *pprofCapturer) captureSet(ctx context.Context, label string) {
	c.captureEndpoint(ctx, label, "heap", "heap", 10*time.Second)
	c.captureEndpoint(ctx, label, "allocs", "allocs", 10*time.Second)
	c.captureEndpoint(ctx, label, "goroutine", "goroutine?debug=2", 10*time.Second)
}

func (c *pprofCapturer) captureEndpoint(ctx context.Context, label, name, endpoint string, timeout time.Duration) {
	client := newIPCClient(c.cfg.SocketPath, timeout)
	defer client.CloseIdleConnections()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/debug/pprof/"+endpoint, nil)
	if err != nil {
		slog.Warn("Create pprof request failed", "profile", name, "error", err)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		slog.Warn("Pprof capture failed", "profile", name, "error", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		slog.Warn("Pprof capture returned non-OK status", "profile", name, "status", resp.Status)
		return
	}

	dir := filepath.Join(c.cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Warn("Create pprof directory failed", "profile", name, "error", err)
		return
	}
	ext := "pprof"
	if name == "goroutine" {
		ext = "txt"
	}
	filename := fmt.Sprintf("%s_%s_%s.%s", time.Now().UTC().Format("20060102T150405Z"), label, name, ext)
	target := filepath.Join(dir, filename)
	f, err := os.Create(target)
	if err != nil {
		slog.Warn("Create pprof file failed", "profile", name, "error", err)
		return
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(target)
		slog.Warn("Write pprof file failed", "profile", name, "error", err)
		return
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(target)
		slog.Warn("Close pprof file failed", "profile", name, "error", err)
		return
	}
	slog.Info("Captured Litestream pprof", "profile", name, "path", target)
	c.upload(ctx, target, filename)
}

func (c *pprofCapturer) upload(ctx context.Context, filePath, filename string) {
	if c.cfg.ReplicaType != "s3" || c.cfg.S3Bucket == "" || c.cfg.S3AccessKey == "" || c.cfg.S3SecretKey == "" {
		return
	}
	endpoint, err := url.Parse(c.cfg.S3Endpoint)
	if err != nil {
		slog.Warn("Parse S3 endpoint for pprof upload failed", "endpoint", c.cfg.S3Endpoint, "error", err)
		return
	}
	host := endpoint.Host
	if host == "" {
		host = strings.TrimPrefix(strings.TrimPrefix(c.cfg.S3Endpoint, "https://"), "http://")
	}
	if host == "" {
		return
	}

	prefix := strings.Trim(strings.TrimPrefix(c.cfg.S3Path, "/"), "/")
	target := "s3://" + c.cfg.S3Bucket + "/" + path.Join(prefix, "profiles", filename)
	uploadCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(uploadCtx, "sh", "-c", `s3cmd --access_key="$AWS_ACCESS_KEY_ID" --secret_key="$AWS_SECRET_ACCESS_KEY" --host="$S3CMD_HOST" --host-bucket="%(bucket)s.$S3CMD_HOST" put "$SOAK_PPROF_FILE" "$SOAK_PPROF_TARGET"`)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+c.cfg.S3AccessKey,
		"AWS_SECRET_ACCESS_KEY="+c.cfg.S3SecretKey,
		"S3CMD_HOST="+host,
		"SOAK_PPROF_FILE="+filePath,
		"SOAK_PPROF_TARGET="+target,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		slog.Warn("Pprof S3 upload failed", "target", target, "error", err, "output", tailString(string(output), 2048))
		return
	}
	slog.Info("Uploaded Litestream pprof", "target", target)
}
