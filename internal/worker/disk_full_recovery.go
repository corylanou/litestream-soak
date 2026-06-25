package worker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const diskFullRecoveryReserveFile = "soak-disk-full-recovery.reserve"

func (r *Runner) prepareDiskFullRecoveryReserve() error {
	if r.cfg.DiskFullRecoveryReserve <= 0 {
		return nil
	}
	if err := os.MkdirAll(r.cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	path := r.diskFullRecoveryReservePath()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create reserve file: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()

	buf := make([]byte, 1024*1024)
	for i := range buf {
		buf[i] = 0x5a
	}
	remaining := r.cfg.DiskFullRecoveryReserve
	for remaining > 0 {
		n := len(buf)
		if remaining < int64(n) {
			n = int(remaining)
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return fmt.Errorf("write reserve file: %w", err)
		}
		remaining -= int64(n)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync reserve file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close reserve file: %w", err)
	}
	closed = true
	return nil
}

func (r *Runner) freeDiskFullRecoveryReserve() (int64, error) {
	if r.cfg.DiskFullRecoveryReserve <= 0 {
		return 0, nil
	}
	path := r.diskFullRecoveryReservePath()
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("stat reserve file: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return 0, fmt.Errorf("remove reserve file: %w", err)
	}
	return info.Size(), nil
}

func (r *Runner) diskFullRecoveryReservePath() string {
	return filepath.Join(r.cfg.DataDir, diskFullRecoveryReserveFile)
}

func litestreamDiskFullSignal(lines []string) (bool, string) {
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		switch {
		case strings.Contains(lower, "no space left on device"),
			strings.Contains(lower, "database or disk is full"),
			strings.Contains(lower, "database is full"),
			strings.Contains(lower, "disk is full"),
			strings.Contains(lower, "enospc"),
			strings.Contains(lower, "sqlite_full"):
			return true, line
		}
	}
	return false, ""
}

func (c Config) diskFullRecoveryTimeout() time.Duration {
	if c.DiskFullRecoveryTimeout > 0 {
		return c.DiskFullRecoveryTimeout
	}
	return DefaultConfig().DiskFullRecoveryTimeout
}
