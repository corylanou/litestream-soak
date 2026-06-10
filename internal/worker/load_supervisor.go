package worker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type loadSupervisor struct {
	mu       sync.Mutex
	cmd      *exec.Cmd
	exited   bool
	paused   bool
	stopped  bool
	resumeCh chan struct{}
	procDone chan struct{}

	startCmd func(ctx context.Context) (*exec.Cmd, error)

	initialBackoff time.Duration
	maxBackoff     time.Duration
	healthyReset   time.Duration
}

func newLoadSupervisor(startCmd func(ctx context.Context) (*exec.Cmd, error)) *loadSupervisor {
	return &loadSupervisor{
		startCmd:       startCmd,
		initialBackoff: 5 * time.Second,
		maxBackoff:     2 * time.Minute,
		healthyReset:   5 * time.Minute,
	}
}

func (s *loadSupervisor) start(ctx context.Context) error {
	cmd, err := s.startCmd(ctx)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	s.mu.Lock()
	s.cmd = cmd
	s.exited = false
	s.procDone = done
	s.mu.Unlock()
	SetLoadRunning(true)
	go s.supervise(ctx, cmd, done)
	return nil
}

func (s *loadSupervisor) supervise(ctx context.Context, cmd *exec.Cmd, done chan struct{}) {
	backoff := s.initialBackoff
	for {
		started := time.Now()
		err := cmd.Wait()
		s.mu.Lock()
		s.exited = true
		s.mu.Unlock()
		SetLoadRunning(false)
		close(done)
		slog.Warn("Load generator exited", "error", err, "ran_for", time.Since(started).Round(time.Millisecond))

		if ctx.Err() != nil || s.isStopped() {
			return
		}
		if time.Since(started) >= s.healthyReset {
			backoff = s.initialBackoff
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < s.maxBackoff {
				backoff *= 2
				if backoff > s.maxBackoff {
					backoff = s.maxBackoff
				}
			}
			if !s.waitWhilePaused(ctx) {
				return
			}
			next, startErr := s.startCmd(ctx)
			if startErr != nil {
				slog.Error("Failed to restart load generator", "error", startErr)
				continue
			}
			done = make(chan struct{})
			s.mu.Lock()
			s.cmd = next
			s.exited = false
			s.procDone = done
			stopped := s.stopped
			s.mu.Unlock()
			IncLoadRestart("synthetic")
			SetLoadRunning(true)
			slog.Info("Restarted load generator")
			if stopped && next.Process != nil {
				_ = next.Process.Signal(os.Interrupt)
			}
			cmd = next
			break
		}
	}
}

func (s *loadSupervisor) waitWhilePaused(ctx context.Context) bool {
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return false
		}
		if !s.paused {
			s.mu.Unlock()
			return true
		}
		if s.resumeCh == nil {
			s.resumeCh = make(chan struct{})
		}
		resume := s.resumeCh
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return false
		case <-resume:
		}
	}
}

func (s *loadSupervisor) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

func (s *loadSupervisor) Pause(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = true
	if s.resumeCh == nil {
		s.resumeCh = make(chan struct{})
	}
	if s.exited || s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	slog.Info("Pausing load generator")
	if err := s.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		if isProcessGoneError(err) {
			return nil
		}
		return err
	}
	SetLoadRunning(false)
	return nil
}

func (s *loadSupervisor) Resume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = false
	if s.resumeCh != nil {
		close(s.resumeCh)
		s.resumeCh = nil
	}
	if s.exited || s.cmd == nil || s.cmd.Process == nil {
		return
	}
	if err := s.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		if !isProcessGoneError(err) {
			slog.Error("Failed to resume load generator", "error", err)
		}
		return
	}
	SetLoadRunning(true)
	slog.Info("Resumed load generator")
}

func (s *loadSupervisor) Stop() {
	s.mu.Lock()
	s.stopped = true
	if s.resumeCh != nil {
		close(s.resumeCh)
		s.resumeCh = nil
	}
	cmd := s.cmd
	exited := s.exited
	done := s.procDone
	s.mu.Unlock()

	if cmd != nil && cmd.Process != nil && !exited {
		// A SIGSTOPped child that traps SIGINT cannot handle it until it
		// is continued, so resume it before interrupting.
		_ = cmd.Process.Signal(syscall.SIGCONT)
		if err := cmd.Process.Signal(os.Interrupt); err != nil && !isProcessGoneError(err) {
			slog.Warn("Failed to interrupt load generator", "error", err)
		}
	}
	if done != nil {
		<-done
	}
	SetLoadRunning(false)
}

func isProcessGoneError(err error) bool {
	return errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH)
}
