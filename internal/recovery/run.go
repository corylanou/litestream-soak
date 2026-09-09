package recovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	Binary string        `json:"binary"`
	SHA256 string        `json:"sha256"`
	Output string        `json:"output"`
	Window time.Duration `json:"window_ns"`
}

type Result struct {
	Config       Config       `json:"config"`
	Version      string       `json:"version"`
	Capabilities Capabilities `json:"capabilities"`
	Scope        string       `json:"scope"`
	Status       string       `json:"status"`
	Attempts     []Attempt    `json:"attempts"`
}

type runner struct {
	ctx       context.Context
	cfg       Config
	result    *Result
	db        *sql.DB
	config    string
	source    string
	committed int
	confirmed int
	sequence  int
}

func Run(ctx context.Context, cfg Config) (result Result, runErr error) {
	hash, err := hashFile(cfg.Binary)
	if err != nil {
		return result, err
	}
	if len(cfg.SHA256) != 64 || hash != cfg.SHA256 {
		return result, errors.New("binary SHA-256 does not match explicit pin")
	}
	cfg.Binary, err = filepath.Abs(cfg.Binary)
	if err != nil {
		return result, err
	}
	if cfg.Output == "" {
		return result, errors.New("new output directory is required")
	}
	cfg.Output, err = filepath.Abs(cfg.Output)
	if err != nil {
		return result, err
	}
	if cfg.Window <= 0 {
		cfg.Window = 20 * time.Second
	}
	if err := os.Mkdir(cfg.Output, 0700); err != nil {
		return result, err
	}
	result = Result{Config: cfg, Scope: "local-file-replica; process kill, not power loss; fleet/provider validation unexecuted"}
	defer func() {
		seen := map[string]bool{}
		for _, a := range result.Attempts {
			if a.Log == "" || seen[a.Log] {
				continue
			}
			seen[a.Log] = true
			_, _, incidents := maintenance(readLog(a.Log))
			for _, incident := range incidents {
				event := Attempt{Name: a.Name + "-incident", Error: incident, Log: a.Log, Started: time.Now().UTC()}
				if strings.Contains(incident, "scheduling retry for unready dbs") {
					event.Status = "observation"
					event.Expected = true
					event.Proof = "startup readiness scheduling; retained diagnostic"
				}
				result.Attempts = append(result.Attempts, event)
			}
		}
		if runErr != nil {
			result.Attempts = append(result.Attempts, Attempt{Name: "runner-error", Error: runErr.Error(), Started: time.Now().UTC()})
		}
		result.Status = Verdict(result.Attempts)
		data, err := json.MarshalIndent(result, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(cfg.Output, "result.json"), data, 0600)
		}
		runErr = errors.Join(runErr, err)
	}()
	version, err := exec.CommandContext(ctx, cfg.Binary, "version").CombinedOutput()
	if err != nil {
		return result, err
	}
	result.Version = strings.TrimSpace(string(version))
	help, err := exec.CommandContext(ctx, cfg.Binary, "restore", "-h").CombinedOutput()
	if err != nil {
		return result, fmt.Errorf("probe restore capabilities: %w: %s", err, help)
	}
	result.Capabilities = DetectCapabilities(hash, string(help))
	source := filepath.Join(cfg.Output, "source.db")
	db, err := openDB(source)
	if err != nil {
		return result, err
	}
	defer func() { runErr = errors.Join(runErr, db.Close()) }()
	if _, err := db.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT NOT NULL)"); err != nil {
		return result, err
	}
	r := &runner{ctx: ctx, cfg: cfg, result: &result, db: db, source: source, config: filepath.Join(cfg.Output, "litestream.yml")}
	for i := 0; i < 512; i++ {
		if err := r.write(); err != nil {
			return result, err
		}
	}
	config := fmt.Sprintf("socket:\n  enabled: false\nlogging:\n  level: debug\nsnapshot:\n  interval: 2s\n  retention: 15s\nlevels:\n  - interval: 1s\nl0-retention: 1s\nl0-retention-check-interval: 1s\ndbs:\n  - path: %q\n    replica:\n      path: %q\n      sync-interval: 100ms\n", source, filepath.Join(cfg.Output, "replica"))
	if err := os.WriteFile(r.config, []byte(config), 0600); err != nil {
		return result, err
	}
	if err := r.scenarios(); err != nil {
		return result, err
	}
	return result, nil
}

func (r *runner) write() error {
	if err := appendRow(r.ctx, r.db, r.committed+1); err != nil {
		return err
	}
	r.committed++
	return nil
}

func (r *runner) path(name, suffix string) string {
	r.sequence++
	return filepath.Join(r.cfg.Output, fmt.Sprintf("%03d-%s%s", r.sequence, name, suffix))
}

func (r *runner) args(output string, options ...string) []string {
	args := []string{"restore", "-config", r.config, "-no-expand-env", "-o", output}
	args = append(args, options...)
	return append(args, r.source)
}

func (r *runner) restore(name string, minimum int, options ...string) (int, error) {
	a := Attempt{Name: name, Started: time.Now().UTC(), Engaged: true, Proof: "restore process executed against fixture replica"}
	output := r.path(name, ".db")
	a.Log = output + ".log"
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Window)
	defer cancel()
	p, err := start(ctx, r.cfg.Binary, a.Log, r.args(output, options...)...)
	count := 0
	if err == nil {
		err = errors.Join(<-p.done, p.log.Close())
		a.ProcessFinished = time.Now().UTC()
		if err == nil {
			count, err = validate(ctx, r.db, output, minimum)
		}
	}
	a.Oracle = err == nil
	a.Boundary, _ = MeasureBoundary(max(r.committed, count), min(r.confirmed, minimum), count)
	a.Duration = time.Since(a.Started)
	if err != nil {
		a.Error = err.Error()
	}
	r.result.Attempts = append(r.result.Attempts, a)
	return count, err
}

func (r *runner) replicator(name string) (*process, string, error) {
	log := r.path(name, ".log")
	p, err := start(r.ctx, r.cfg.Binary, log, "replicate", "-config", r.config, "-no-expand-env")
	if err != nil {
		return nil, log, err
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.Window)
	defer cancel()
	err = wait(ctx, func() (bool, error) {
		data, err := os.ReadFile(log)
		return strings.Contains(string(data), "ltx file uploaded"), err
	})
	if err != nil {
		return nil, log, errors.Join(err, p.stop(true))
	}
	return p, log, nil
}

func (r *runner) recordStop(p *process, name, log string, kill bool) {
	a := Attempt{Name: name, Started: time.Now().UTC(), Log: log}
	err := p.stop(kill)
	a.Duration = time.Since(a.Started)
	if err != nil {
		a.Error = err.Error()
	}
	if kill {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			if status, ok := exit.Sys().(syscall.WaitStatus); ok {
				a.Engaged = status.Signaled() && status.Signal() == syscall.SIGKILL
			}
		}
		a.Expected = a.Engaged
		a.Proof = "child WaitStatus must report SIGKILL; not a power-loss simulation"
	} else {
		a.Engaged = err == nil
		a.Oracle = err == nil
		a.Proof = "child exited after interrupt"
	}
	r.result.Attempts = append(r.result.Attempts, a)

}

func (r *runner) confirm(name string) error {
	count, err := r.restore(name, r.confirmed)
	if err == nil {
		r.confirmed = count
	}
	return err
}

func (r *runner) settle() error {
	ctx, cancel := context.WithTimeout(r.ctx, 1500*time.Millisecond)
	defer cancel()
	<-ctx.Done()
	return r.ctx.Err()
}

func (r *runner) scenarios() (runErr error) {
	p, log, err := r.replicator("initial-replicate")
	if err != nil {
		return err
	}
	defer func() {
		if p != nil {
			r.recordStop(p, "replicator-stop", log, false)
		}
	}()
	if err := r.settle(); err != nil {
		return err
	}
	if err := r.confirm("confirmed-before-fault"); err != nil {
		return err
	}
	target := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	targetCount := r.confirmed
	if err := r.settle(); err != nil {
		return err
	}
	r.recordStop(p, "process-kill", log, true)
	p = nil
	for i := 0; i < 4; i++ {
		if err := r.write(); err != nil {
			return err
		}
	}
	if _, err := r.restore("offline-loss-window", r.confirmed); err != nil {
		return err
	}
	p, log, err = r.replicator("restart")
	if err != nil {
		return err
	}
	if err := r.settle(); err != nil {
		return err
	}
	if err := r.confirm("restart"); err != nil {
		return err
	}
	if r.result.Capabilities.Timestamp {
		count, err := r.restore("pit", targetCount, "-timestamp", target.Format(time.RFC3339))
		r.result.Attempts[len(r.result.Attempts)-1].historical(targetCount, false)
		if err == nil && count != targetCount {
			r.result.Attempts[len(r.result.Attempts)-1].Error = fmt.Sprintf("PIT restored %d rows, expected %d", count, targetCount)
		}
	} else {
		r.result.Attempts = append(r.result.Attempts, Attempt{Name: "pit", Status: "unexecuted", Proof: "timestamp absent from pinned restore help"})
	}
	if err := r.interruptRestore(); err != nil {
		return err
	}
	if err := r.follow(); err != nil {
		return err
	}
	if err := r.activeRetention(log); err != nil {
		return err
	}
	if r.result.Capabilities.Timestamp {
		count, err := r.restore("retention-boundary", 0, "-timestamp", target.Format(time.RFC3339))
		a := &r.result.Attempts[len(r.result.Attempts)-1]
		if err != nil && strings.Contains(readLog(a.Log), "no matching backup files") {
			a.Expected = true
			a.historical(targetCount, true)
			a.Proof = "previously validated PIT target now rejected after retention; latest recovery checked independently"
		}
		if err == nil {
			a.historical(targetCount, false)
		}
		if err == nil && count != targetCount {
			a.Error = fmt.Sprintf("retained PIT restored %d rows, expected %d", count, targetCount)
			a.Oracle = false
		}
	}
	r.recordStop(p, "before-local-loss", log, false)
	p = nil
	if err := r.db.Close(); err != nil {
		return err
	}
	quarantine := filepath.Join(r.cfg.Output, "quarantined-local-state")
	if err := os.Mkdir(quarantine, 0700); err != nil {
		return err
	}
	moved := 0
	for _, name := range []string{"source.db", "source.db-wal", "source.db-shm", ".source.db-litestream"} {
		path := filepath.Join(r.cfg.Output, name)
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if err := os.Rename(path, filepath.Join(quarantine, name)); err != nil {
			return err
		}
		moved++
	}
	oracle, err := sql.Open("sqlite", "file:"+filepath.Join(quarantine, "source.db")+"?mode=ro")
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, oracle.Close()) }()
	r.db = oracle
	_, err = r.restore("local-loss", r.confirmed)
	a := &r.result.Attempts[len(r.result.Attempts)-1]
	a.Engaged = moved > 0
	a.Proof = fmt.Sprintf("quarantined %d local database/sidecar/cache entries; replica retained", moved)
	return err
}

func readLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return string(data)
}
