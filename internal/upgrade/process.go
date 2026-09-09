package upgrade

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/corylanou/litestream-soak/internal/worker"
)

type capture struct {
	mu   sync.Mutex
	data bytes.Buffer
	file *os.File
}

func (c *capture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.data.Write(p); err != nil {
		return 0, err
	}
	return c.file.Write(p)
}

func (c *capture) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.data.Bytes())
}

var deletedPattern = regexp.MustCompile(`deleted_count[=:]([0-9]+)`)

func observedAge(log []byte) Age {
	a := Age{}
	for _, line := range strings.Split(string(log), "\n") {
		lower := strings.ToLower(line)
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") || strings.Contains(lower, "retry") || strings.Contains(lower, "self-heal") {
			a.Incidents++
		}
		if strings.Contains(line, `msg="snapshot complete"`) {
			a.Snapshots++
		}
		if strings.Contains(line, `msg="compaction complete"`) {
			a.Compactions++
		}
		if strings.Contains(line, `msg="l0 retention enforced"`) {
			if match := deletedPattern.FindStringSubmatch(line); len(match) == 2 {
				n, _ := strconv.Atoi(match[1])
				a.RetentionDeleted += n
			}
		}
	}
	return a
}

func seed(state string) error {
	db, err := sql.Open("sqlite", filepath.Join(state, "db"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	_, err = db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT NOT NULL); INSERT INTO t VALUES (1,'seed'),(2,'seed'),(3,'seed')")
	return err
}

func exercise(ctx context.Context, binary Binary, state, logPath string, limit time.Duration, aging bool, continuation time.Duration) (Age, error) {
	if err := ctx.Err(); err != nil {
		return Age{}, err
	}
	if err := verifyPin(binary); err != nil {
		return Age{}, err
	}
	config, err := configFile(state)
	if err != nil {
		return Age{}, err
	}
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return Age{}, err
	}
	defer func() { _ = log.Close() }()
	output := &capture{file: log}
	cmd := exec.Command(binary.Path, "replicate", "-config", config, "-no-expand-env")
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		return Age{}, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stop := func() error {
		if err := cmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			killErr := cmd.Process.Kill()
			waitErr := <-done
			return errors.Join(errors.New("replicator did not stop cleanly"), killErr, waitErr)
		}
	}
	db, err := sql.Open("sqlite", filepath.Join(state, "db"))
	if err != nil {
		return Age{}, errors.Join(err, stop())
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL"); err != nil {
		return Age{}, errors.Join(err, db.Close(), stop())
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	window := limit
	if !aging {
		window += 5 * time.Second
	}
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	writes := 0
	var runErr error
	active := true
	for active {
		select {
		case <-ctx.Done():
			runErr = ctx.Err()
			active = false
		case err := <-done:
			_ = db.Close()
			return observedAge(output.bytes()), errors.Join(errors.New("replicator exited before quiescence"), err)
		case <-deadline.C:
			if !aging {
				runErr = errors.New("continuation did not complete required churn")
			}
			active = false
		case <-ticker.C:
			if !bytes.Contains(output.bytes(), []byte(`msg="replicating to"`)) {
				continue
			}
			tx, err := db.BeginTx(ctx, nil)
			if err == nil {
				_, err = tx.ExecContext(ctx, "UPDATE t SET value=? WHERE id=1; DELETE FROM t WHERE id=3; INSERT INTO t(id,value) VALUES(3,?)", fmt.Sprintf("update-%d", writes), fmt.Sprintf("insert-%d", writes))
				if err == nil {
					err = tx.Commit()
				} else {
					err = errors.Join(err, tx.Rollback())
				}
			}
			if err != nil {
				runErr = err
				active = false
				continue
			}
			writes++
			a := observedAge(output.bytes())
			a.Updates = writes
			a.Deletes = writes
			if (aging && a.Ready()) || (!aging && writes >= max(1, int(continuation/(200*time.Millisecond)))) {
				active = false
			}
		}
	}
	closeErr := db.Close()
	if runErr == nil {
		select {
		case <-ctx.Done():
			runErr = ctx.Err()
		case <-time.After(min(continuation, time.Second)):
		}
	}
	stopErr := stop()
	a := observedAge(output.bytes())
	a.Updates = writes
	a.Deletes = writes
	if a.Incidents > 0 {
		runErr = errors.Join(runErr, fmt.Errorf("replicator logged %d warning/error/retry/self-heal incidents; see retained log", a.Incidents))
	}
	return a, errors.Join(runErr, closeErr, stopErr)
}

func restore(ctx context.Context, binary Binary, state, prefix string) error {
	if err := verifyPin(binary); err != nil {
		return err
	}
	config, err := configFile(state)
	if err != nil {
		return err
	}
	log, err := os.OpenFile(prefix+".log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()
	restoreCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(restoreCtx, binary.Path, "restore", "-config", config, "-o", prefix+".db", "-no-expand-env", filepath.Join(state, "db"))
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("restore: %w (see retained log)", err)
	}
	data, err := os.ReadFile(prefix + ".log")
	if err != nil {
		return err
	}
	if incidents := observedAge(data).Incidents; incidents > 0 {
		return fmt.Errorf("restore logged %d warning/error/retry/self-heal incidents; see retained log", incidents)
	}
	return compare(filepath.Join(state, "db"), prefix+".db")
}

func compare(source, restored string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, path := range []string{source, restored} {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			return err
		}
		var integrity string
		err = db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity)
		closeErr := db.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
		if integrity != "ok" {
			return fmt.Errorf("integrity_check: %s", integrity)
		}
	}
	return worker.CompareLogicalDatabases(ctx, source, restored)
}
