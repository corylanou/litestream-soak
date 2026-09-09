package recovery

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"time"
)

type process struct {
	cmd  *exec.Cmd
	done chan error
	log  *os.File
}

func start(ctx context.Context, binary, logPath string, args ...string) (*process, error) {
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		return nil, errors.Join(err, log.Close())
	}
	p := &process{cmd: cmd, done: make(chan error, 1), log: log}
	go func() { p.done <- cmd.Wait() }()
	return p, nil
}

func (p *process) stop(kill bool) error {
	var signalErr error
	if kill {
		signalErr = p.cmd.Process.Kill()
	} else {
		signalErr = p.cmd.Process.Signal(os.Interrupt)
	}
	if errors.Is(signalErr, os.ErrProcessDone) {
		signalErr = nil
	}
	select {
	case err := <-p.done:
		return errors.Join(signalErr, err, p.log.Close())
	case <-time.After(5 * time.Second):
		killErr := p.cmd.Process.Kill()
		return errors.Join(errors.New("process stop timed out"), signalErr, killErr, <-p.done, p.log.Close())
	}
}

func wait(ctx context.Context, fn func() (bool, error)) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := fn()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
