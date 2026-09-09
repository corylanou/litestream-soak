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
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	pprofLocalRetentionFiles = 96
	// One baseline set is heap, allocs, goroutine, and memstats.
	pprofBaselineRetentionFiles = 4
)

type pprofCapturer struct {
	cfg          *Config
	gate         chan struct{}
	statusMu     sync.Mutex
	uploadWake   chan struct{}
	events       chan string
	identityOnce sync.Once
	binaries     map[string]profileBinary
	lastCPU      time.Time
}

func newPprofCapturer(cfg *Config) *pprofCapturer {
	return &pprofCapturer{cfg: cfg, events: make(chan string, 16), gate: make(chan struct{}, 1), uploadWake: make(chan struct{}, 1)}
}

// Run captures Litestream pprof profiles for every worker profile (the
// Litestream control socket exposes /debug/pprof regardless of database
// count) unless capture is disabled with SOAK_PPROF_CAPTURE=false.
func (c *pprofCapturer) Run(ctx context.Context) {
	if !c.cfg.PprofCaptureEnabled {
		return
	}

	uploadDone := make(chan struct{})
	go func() { defer close(uploadDone); c.runUploads(ctx) }()
	defer func() { <-uploadDone }()
	defer func() {
		c.recordStatus("collector", "cancelled")
		for {
			select {
			case label := <-c.events:
				c.recordStatus(label, "queue-cancelled")
			default:
				return
			}
		}
	}()
	ready := time.NewTimer(5 * time.Second)
	defer ready.Stop()
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	waiting := true
	for waiting {
		if _, err := os.Stat(c.cfg.SocketPath); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-ready.C:
			waiting = false
		case <-poll.C:
		}
	}
	c.captureSet(ctx, "baseline")
	hourly := time.NewTicker(time.Hour)
	defer hourly.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case label := <-c.events:
			c.captureSet(ctx, label)
		case <-hourly.C:
			c.captureSet(ctx, "hourly")
		}
	}
}

func (c *pprofCapturer) Trigger(label string) {
	if c == nil || !c.cfg.PprofCaptureEnabled {
		return
	}
	select {
	case c.events <- label:
	default:
		c.recordStatus(label, "queue-full")
	}
}

func (c *pprofCapturer) captureSet(ctx context.Context, label string) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		c.recordStatus(label, "cancelled-waiting")
		return
	}
	defer func() { <-c.gate }()
	defer func() {
		if ctx.Err() != nil {
			c.recordStatus(label, "cancelled")
		}
		select {
		case c.uploadWake <- struct{}{}:
		default:
		}
	}()
	if label != "final" {
		cpuLabel := label
		if label == "baseline" {
			cpuLabel = "startup"
		}
		if time.Since(c.lastCPU) >= time.Minute {
			c.lastCPU = time.Now()
			c.captureEndpoint(ctx, cpuLabel, "cpu_profile", "profile?seconds=5", 7*time.Second)
		} else {
			c.recordStatus(label, "cpu-rate-limited")
		}
	} else {
		c.recordStatus(label, "cpu-skipped-shutdown")
	}
	for _, item := range []struct{ name, endpoint string }{
		{"heap", "heap"}, {"allocs", "allocs"}, {"goroutine", "goroutine?debug=2"}, {"memstats", "heap?debug=1"},
	} {
		if ctx.Err() != nil {
			return
		}
		c.captureEndpoint(ctx, label, item.name, item.endpoint, 5*time.Second)
	}
	if label == "final" {
		return
	}

	for _, name := range []string{"block", "mutex", "trace"} {
		if ctx.Err() != nil {
			return
		}
		if os.Getenv("SOAK_PPROF_"+strings.ToUpper(name)) != "true" {
			c.recordStatus(label, name+"-disabled")
			continue
		}
		endpoint := name
		if name == "trace" {
			endpoint += "?seconds=1"
		}
		c.captureEndpoint(ctx, label, name, endpoint, 3*time.Second)
	}
}

func (c *pprofCapturer) captureEndpoint(ctx context.Context, label, name, endpoint string, timeout time.Duration) {
	dir := filepath.Join(c.cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		slog.Warn("Create pprof directory failed", "error", err)
		return
	}
	if !c.captureSpaceAvailable(dir) {
		c.recordStatus(label, "storage-full")
		return
	}
	ext := "pprof"
	if name == "goroutine" || name == "memstats" {
		ext = "txt"
	}
	filename := fmt.Sprintf("%s_%s_%s.%s", time.Now().UTC().Format("20060102T150405.000000000Z"), profileLabel(label), name, ext)
	target := filepath.Join(dir, filename)
	record := c.newRecord(label, name, filename)
	if name == "block" || name == "mutex" {
		record.Sampling = "unverified: target sampling rate is not exposed by this endpoint"
	}
	defer func() {
		c.saveRecord(ctx, target+".json", record)
		c.pruneLocalProfiles(dir, pprofLocalRetentionFiles)
	}()
	client := newIPCClient(c.cfg.SocketPath, timeout)
	defer client.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/debug/pprof/"+endpoint, nil)
	if err != nil {
		record.Error = err.Error()
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		record.Error = err.Error()
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		record.Error = resp.Status
		return
	}
	record.EndpointAvailable = true
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		record.Error = err.Error()
		return
	}
	n, copyErr := io.Copy(f, io.LimitReader(resp.Body, pprofMaxCaptureBytes+1))
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil || n == 0 || n > pprofMaxCaptureBytes {
		record.Error = fmt.Sprintf("incomplete capture: bytes=%d copy=%v close=%v", n, copyErr, closeErr)
		if err := os.Remove(target); err != nil {
			slog.Warn("Remove incomplete profile", "error", err)
		}
		return
	}
	record.Status = "available"
}

type pprofProfileFile struct {
	name    string
	modTime time.Time
}

func (c *pprofCapturer) pruneLocalProfiles(dir string, keep int) {
	if keep <= 0 {
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn("Read pprof directory failed", "dir", dir, "error", err)
		return
	}

	// Baseline captures are pruned separately so the newest baseline set
	// (one file per profile kind) always survives, while restarts cannot
	// accumulate baselines without bound.
	var files, baselines []pprofProfileFile
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if c.pending(filepath.Join(dir, entry.Name())) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			slog.Warn("Read pprof file info failed", "file", filepath.Join(dir, entry.Name()), "error", err)
			continue
		}
		file := pprofProfileFile{name: entry.Name(), modTime: info.ModTime()}
		if strings.Contains(entry.Name(), "_baseline_") {
			baselines = append(baselines, file)
		} else {
			files = append(files, file)
		}
	}
	removeOldest(dir, files, keep)
	removeOldest(dir, baselines, pprofBaselineRetentionFiles)
}

// removeOldest deletes all but the newest keep files.
func removeOldest(dir string, files []pprofProfileFile, keep int) {
	if len(files) <= keep {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime.Equal(files[j].modTime) {
			return files[i].name < files[j].name
		}
		return files[i].modTime.Before(files[j].modTime)
	})
	for _, file := range files[:len(files)-keep] {
		target := filepath.Join(dir, file.name)
		if err := os.Remove(target); err != nil {
			slog.Warn("Remove old pprof file failed", "file", target, "error", err)
		} else if err := os.Remove(target + ".json"); err != nil && !os.IsNotExist(err) {
			slog.Warn("Remove old pprof metadata failed", "error", err)
		}
	}
}

func (c *pprofCapturer) upload(ctx context.Context, filePath, filename string) error {
	if c.cfg.ReplicaType != "s3" {
		return nil
	}
	if c.cfg.S3Bucket == "" || c.cfg.S3AccessKey == "" || c.cfg.S3SecretKey == "" {
		return fmt.Errorf("profile upload credentials or bucket unavailable")
	}
	endpoint, err := url.Parse(c.cfg.S3Endpoint)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return fmt.Errorf("invalid profile upload endpoint")
	}
	target := "s3://" + c.cfg.S3Bucket + "/" + path.Join(strings.Trim(c.cfg.S3Path, "/"), "profiles", filename)
	args := []string{"--host=" + endpoint.Host, "--host-bucket=" + endpoint.Host, "--max-retries=0"}
	if endpoint.Scheme == "http" {
		args = append(args, "--no-ssl")
	} else {
		args = append(args, "--ssl")
	}
	if c.cfg.S3Region != "" {
		args = append(args, "--region="+c.cfg.S3Region)
	}
	args = append(args, "put", filePath, target)
	uploadCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(uploadCtx, "s3cmd", args...)
	cmd.WaitDelay = time.Second
	cmd.Env = append(os.Environ(), "AWS_ACCESS_KEY_ID="+c.cfg.S3AccessKey, "AWS_SECRET_ACCESS_KEY="+c.cfg.S3SecretKey, "AWS_SESSION_TOKEN="+c.cfg.S3SessionToken)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("upload profile: %w", err)
	}
	return nil
}
