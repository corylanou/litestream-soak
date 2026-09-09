package recovery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (r *runner) interruptRestore() error {
	output := r.path("interrupted-restore", ".db")
	log := output + ".log"
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Window)
	defer cancel()
	p, err := start(ctx, r.cfg.Binary, log, r.args(output)...)
	if err != nil {
		return err
	}
	engaged := false
	err = wait(ctx, func() (bool, error) {
		entries, err := os.ReadDir(r.cfg.Output)
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), filepath.Base(output)) && !strings.HasSuffix(entry.Name(), ".log") {
				info, err := entry.Info()
				if err != nil {
					return false, err
				}
				if info.Size() > 0 {
					engaged = true
					return true, nil
				}
			}
		}
		select {
		case exit := <-p.done:
			p.done <- exit
			return false, errors.New("restore exited before interruption")
		default:
		}
		return false, nil
	})
	r.recordStop(p, "interrupted-restore", log, true)
	a := &r.result.Attempts[len(r.result.Attempts)-1]
	a.Engaged = a.Engaged && engaged
	a.Expected = a.Engaged
	a.Proof = "nonempty partial restore output observed before child SIGKILL; retry uses separate output"
	if err != nil {
		a.Error = errors.Join(err, errors.New(a.Error)).Error()
		a.Expected = false
	}
	_, retryErr := r.restore("interrupted-restore-retry", r.confirmed)
	return retryErr
}

func (r *runner) follow() error {
	if !r.result.Capabilities.Follow {
		r.result.Attempts = append(r.result.Attempts, Attempt{Name: "follow-resume", Status: "unexecuted", Proof: "follow absent from pinned restore help"})
		return nil
	}
	output := r.path("follow", ".db")
	for step := 0; step < 2; step++ {
		if step > 0 {
			if err := r.write(); err != nil {
				return err
			}
			if err := r.settle(); err != nil {
				return err
			}
		}
		log := r.path("follow", ".log")
		p, err := start(r.ctx, r.cfg.Binary, log, r.args(output, "-f", "-follow-interval", "100ms")...)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Window)
		err = wait(ctx, func() (bool, error) {
			if _, err := os.Stat(output + "-txid"); errors.Is(err, os.ErrNotExist) {
				return false, nil
			} else if err != nil {
				return false, err
			}
			count, err := validate(ctx, r.db, output, r.confirmed)
			if err != nil {
				return false, err
			}
			return count == r.committed, nil
		})
		cancel()
		name := "follow-initial"
		if step > 0 {
			name = "follow-resume"
		}
		r.recordStop(p, name, log, false)
		a := &r.result.Attempts[len(r.result.Attempts)-1]
		if err != nil {
			a.Error = err.Error()
			a.Oracle = false
		}
		if step > 0 {
			a.Engaged = a.Engaged && strings.Contains(readLog(log), "resum")
			a.Proof = "same database and saved -txid sidecar resumed, post-interruption commit validated"
		}
		if err != nil {
			return nil
		}
	}
	return nil
}

func (r *runner) activeRetention(log string) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Window)
	defer cancel()
	started := time.Now()
	engaged := false
	for ctx.Err() == nil {
		before := readLog(log)
		beforeCompactions, beforeDeleted, _ := maintenance(before)
		committedBefore := r.committed
		writerDone := make(chan error, 1)
		writerCtx, stopWriter := context.WithCancel(ctx)
		go func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			count := committedBefore
			for {
				select {
				case <-writerCtx.Done():
					writerDone <- nil
					return
				case <-ticker.C:
					count++
					if err := appendRow(r.ctx, r.db, count); err != nil {
						if writerCtx.Err() != nil {
							writerDone <- nil
						} else {
							writerDone <- err
						}
						return
					}
				}
			}
		}()
		_, restoreErr := r.restore("active-retention", r.confirmed)
		stopWriter()
		writerErr := <-writerDone
		if writerErr != nil {
			return writerErr
		}
		if err := r.db.QueryRowContext(r.ctx, "SELECT count(*) FROM t").Scan(&r.committed); err != nil {
			return err
		}
		a := &r.result.Attempts[len(r.result.Attempts)-1]
		a.Boundary, _ = MeasureBoundary(r.committed, r.confirmed, a.Boundary.Restored)
		compactions, deleted, _ := maintenance(readLog(log))
		a.Engaged = r.committed > committedBefore && restoreExposed(readLog(log), readLog(a.Log), a.Started, a.ProcessFinished)
		a.Proof = fmt.Sprintf("during restore: committed %d->%d, compactions %d->%d, retained L0 deletions %d->%d", committedBefore, r.committed, beforeCompactions, compactions, beforeDeleted, deleted)
		if !a.Engaged && restoreErr == nil {
			a.Status = "observation"
		}
		engaged = engaged || (a.Engaged && a.Oracle)
		if engaged && time.Since(started) > 18*time.Second {
			break
		}
	}
	r.result.Attempts = append(r.result.Attempts, Attempt{Name: "active-retention-exposure", Started: started, Duration: time.Since(started), Engaged: engaged, Oracle: engaged, Proof: "at least one individually exposed valid restore required"})
	return r.confirm("confirmed-after-maintenance")
}
