package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"
)

func (c *pprofCapturer) beginFTSProfile(ctx context.Context, phase string) func() {
	label := "fts-" + phase + "-during"
	dir := filepath.Join(c.cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		c.recordStatus(label, "storage-unavailable")
		return func() {}
	}
	if !c.captureSpaceAvailable(dir) {
		c.recordStatus(label, "storage-full")
		return func() {}
	}
	filename := fmt.Sprintf("%s_%s_worker_cpu.pprof", time.Now().UTC().Format("20060102T150405.000000000Z"), profileLabel(label))
	target := filepath.Join(dir, filename)
	record := c.newRecord(label, "worker_cpu", filename)
	record.Sampling = "worker CPU sampling starts before the operation and stops after it; short phases may contain no CPU samples"
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		record.Error = err.Error()
		c.saveRecord(ctx, target+".json", record)
		return func() {}
	}
	writer := &ftsProfileWriter{writer: f}
	if err := pprof.StartCPUProfile(writer); err != nil {
		record.Error = fmt.Sprintf("start worker CPU profile: %v; close: %v", err, f.Close())
		c.saveRecord(ctx, target+".json", record)
		return func() {}
	}
	record.EndpointAvailable = true
	return func() {
		pprof.StopCPUProfile()
		closeErr := f.Close()
		if writer.err != nil || closeErr != nil || writer.bytes == 0 {
			record.Error = fmt.Sprintf("incomplete worker CPU profile: bytes=%d write=%v close=%v", writer.bytes, writer.err, closeErr)
		} else {
			record.Status = "available"
		}
		c.saveRecord(ctx, target+".json", record)
		select {
		case c.uploadWake <- struct{}{}:
		default:
		}
	}
}

type ftsProfileWriter struct {
	writer io.Writer
	bytes  int
	err    error
}

func (w *ftsProfileWriter) Write(body []byte) (int, error) {
	if len(body) > pprofMaxCaptureBytes-w.bytes {
		w.err = fmt.Errorf("worker CPU profile exceeds byte limit")
		return 0, w.err
	}
	n, err := w.writer.Write(body)
	w.bytes += n
	if err != nil {
		w.err = err
	}
	return n, err
}
