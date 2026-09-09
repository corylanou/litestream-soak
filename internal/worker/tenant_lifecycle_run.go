package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type TenantResourceFrame struct {
	Registered           int       `json:"registered"`
	RegistryStatus       string    `json:"registry_status"`
	ProcessExited        bool      `json:"process_exited"`
	Phase                string    `json:"phase"`
	At                   time.Time `json:"at"`
	Live                 int       `json:"live"`
	Retained             int       `json:"retained"`
	Pending              int       `json:"pending"`
	OldestPendingSeconds float64   `json:"oldest_pending_seconds"`
	ProcessStatus        string    `json:"process_status"`
	RSSBytes             int64     `json:"rss_bytes,omitempty"`
	CPUSeconds           float64   `json:"cpu_seconds,omitempty"`
	FDs                  int       `json:"fds,omitempty"`
}

type TenantLifecycleReport struct {
	LogEvidence      TenantLogEvidence      `json:"log_evidence"`
	StartedAt        time.Time              `json:"started_at"`
	FinishedAt       time.Time              `json:"finished_at"`
	OperationTimeout string                 `json:"operation_timeout"`
	SyncTimeout      string                 `json:"sync_timeout"`
	CPULimit         string                 `json:"cpu_limit"`
	MemoryLimit      string                 `json:"memory_limit"`
	LimitsStatus     string                 `json:"limits_status"`
	Error            string                 `json:"error,omitempty"`
	Capabilities     string                 `json:"capabilities"`
	ProcessLog       string                 `json:"process_log"`
	Attempts         []TenantLifecycleEvent `json:"attempts"`
	Status           string                 `json:"status"`
	LitestreamSHA    string                 `json:"litestream_sha"`
	Mode             string                 `json:"mode"`
	Tenants          int                    `json:"tenants"`
	RunDirectory     string                 `json:"run_directory"`
	Events           []TenantLifecycleEvent `json:"events"`
	Failures         []TenantLifecycleEvent `json:"failures"`
	Resources        []TenantResourceFrame  `json:"resources"`
	LogTail          []string               `json:"log_tail"`
}

type tenantRunner struct {
	options  TenantLifecycleOptions
	report   *TenantLifecycleReport
	ledger   *tenantLedger
	dir      string
	socket   string
	config   string
	process  *exec.Cmd
	done     chan error
	log      *lineBuffer
	fullLog  *tenantProcessLog
	verifier *Verifier
	expected map[tenantID]logicalSnapshot
}

func RunTenantLifecycle(ctx context.Context, options TenantLifecycleOptions) (report TenantLifecycleReport, retErr error) {
	defer func() {
		if retErr != nil {
			report.Error = retErr.Error()
		}
		if report.FinishedAt.IsZero() {
			report.FinishedAt = time.Now().UTC()
		}
	}()
	report = TenantLifecycleReport{Status: "failed", Mode: options.Mode, Tenants: options.Tenants, Capabilities: options.Capabilities, StartedAt: time.Now().UTC(), OperationTimeout: options.Timeout.String(), SyncTimeout: options.SyncTimeout.String(), LimitsStatus: "unsupported"}
	if processCollectionSupported() {
		report.LimitsStatus = "unavailable"
		cpu, cpuErr := os.ReadFile("/sys/fs/cgroup/cpu.max")
		memory, memoryErr := os.ReadFile("/sys/fs/cgroup/memory.max")
		if cpuErr == nil && memoryErr == nil {
			report.LimitsStatus = "fresh"
			report.CPULimit = strings.TrimSpace(string(cpu))
			report.MemoryLimit = strings.TrimSpace(string(memory))
		}
	}
	if err := options.validate(); err != nil {
		return report, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()
	versionCtx, versionCancel := context.WithTimeout(ctx, options.Timeout)
	output, err := exec.CommandContext(versionCtx, options.Binary, "version").CombinedOutput()
	versionCancel()
	report.LitestreamSHA = strings.TrimSpace(string(output))
	if err != nil || report.LitestreamSHA != options.SHA {
		report.Status = "unsupported"
		return report, fmt.Errorf("tenant capability contract requires Litestream %s; got %q: %w", options.SHA, report.LitestreamSHA, errors.Join(err, errors.New("unsupported binary")))
	}
	root, err := filepath.Abs(options.Root)
	if err != nil {
		return report, err
	}
	dir, err := os.MkdirTemp(root, "tenant-lifecycle-")
	if err != nil {
		return report, err
	}
	report.RunDirectory = dir
	r := &tenantRunner{options: options, report: &report, ledger: newTenantLedger(options.Tenants), dir: dir, log: newLineBuffer(120), expected: make(map[tenantID]logicalSnapshot)}
	r.socket = filepath.Join(os.TempDir(), fmt.Sprintf("tenant-%s.sock", filepath.Base(dir)))
	r.config = filepath.Join(dir, "litestream.yml")
	cfg := DefaultConfig()
	cfg.SocketPath = r.socket
	r.verifier = NewVerifier(cfg)
	r.verifier.httpClient.Timeout = options.Timeout
	report.ProcessLog = filepath.Join(dir, "process.log")
	logFile, err := os.OpenFile(report.ProcessLog, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return report, err
	}
	r.fullLog = &tenantProcessLog{file: logFile, remaining: 64 << 20}
	defer func() { retErr = r.finish(retErr) }()
	if err := os.Mkdir(filepath.Join(dir, "dbs"), 0o700); err != nil {
		return report, err
	}
	config := fmt.Sprintf("socket:\n  enabled: true\n  path: %q\ndbs:\n  - dir: %q\n    pattern: '*.db'\n    watch: %t\n    checkpoint-interval: 100ms\n    min-checkpoint-page-count: 1\n    replicas:\n      - path: %q\n        sync-interval: 100ms\n", r.socket, filepath.Join(dir, "dbs"), options.Mode == "watch", filepath.Join(dir, "replicas"))
	if err := os.WriteFile(r.config, []byte(config), 0o600); err != nil {
		return report, err
	}
	if err := r.run(ctx); err != nil {
		r.event("scenario", tenantID{}, "failed", err.Error())
		return report, err
	}
	report.Status = "passed"
	return report, nil
}

func (r *tenantRunner) event(phase string, id tenantID, status, detail string) {
	event := TenantLifecycleEvent{Phase: phase, Status: status, Detail: detail, At: time.Now().UTC()}
	if id.Generation > 0 {
		event.Tenant = id.name()
	}
	r.report.Events = append(r.report.Events, event)
	if status == "failed" {
		r.report.Failures = append(r.report.Failures, event)
	}
}

func (r *tenantRunner) path(id tenantID) string { return filepath.Join(r.dir, "dbs", id.name()) }

func (r *tenantRunner) create(ctx context.Context, tenant int) (tenantID, error) {
	id, err := r.ledger.create(tenant, time.Now())
	if err != nil {
		return id, err
	}
	db, err := sql.Open("sqlite", manyDBDSN(r.path(id)))
	if err != nil {
		return id, err
	}
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(ctx, "CREATE TABLE tenant_identity(tenant INTEGER, generation INTEGER); INSERT INTO tenant_identity VALUES(?,?); CREATE TABLE payload(id INTEGER PRIMARY KEY, value BLOB NOT NULL)", id.Tenant, id.Generation)
	return id, err
}

func (r *tenantRunner) write(ctx context.Context, id tenantID, rows int) error {
	db, err := sql.Open("sqlite", manyDBDSN(r.path(id)))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for i := 0; i < rows; i++ {
		if _, err := tx.ExecContext(ctx, "INSERT INTO payload(value) VALUES(randomblob(512))"); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.ledger.dirty(id, time.Now())
	return nil
}

func (r *tenantRunner) start(ctx context.Context) error {
	r.process = exec.CommandContext(ctx, r.options.Binary, "replicate", "-config", r.config)
	r.process.Stdout = io.MultiWriter(r.fullLog, r.log)
	r.process.Stderr = r.process.Stdout
	if err := r.process.Start(); err != nil {
		r.process = nil
		return err
	}
	r.done = make(chan error, 1)
	cmd := r.process
	go func() { r.done <- cmd.Wait() }()
	return r.wait(ctx, "startup", tenantID{}, func(checkCtx context.Context) (bool, error) { _, err := r.list(checkCtx); return err == nil, err })
}

func (r *tenantRunner) stop() error {
	if r.process == nil {
		return nil
	}
	cmd := r.process
	if err := cmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case err := <-r.done:
		r.process = nil
		return err
	case <-time.After(10 * time.Second):
		killErr := cmd.Process.Kill()
		waitErr := <-r.done
		r.process = nil
		return errors.Join(errors.New("litestream did not stop within cleanup bound"), killErr, waitErr)
	}
}

func (r *tenantRunner) wait(ctx context.Context, phase string, id tenantID, check func(context.Context) (bool, error)) error {
	waitCtx, cancel := context.WithTimeout(ctx, r.options.Timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		ok, err := check(waitCtx)
		attempt := TenantLifecycleEvent{Phase: phase, Status: "pending", At: time.Now().UTC()}
		if id.Generation > 0 {
			attempt.Tenant = id.name()
		}
		if ok {
			attempt.Status = "ready"
		}
		if err != nil {
			attempt.Detail = err.Error()
			if phase != "startup" {
				r.event(phase, id, "failed", err.Error())
			}
		}
		r.report.Attempts = append(r.report.Attempts, attempt)
		if ok {
			return nil
		}
		last = err
		select {
		case <-waitCtx.Done():
			return errors.Join(waitCtx.Err(), last)
		case <-ticker.C:
		}
	}
}

func (r *tenantRunner) list(ctx context.Context) (map[string]bool, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost/list", nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.verifier.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list returned %d", resp.StatusCode)
	}
	var result struct {
		Databases []struct {
			Path string `json:"path"`
		} `json:"databases"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return nil, err
	}
	paths := make(map[string]bool, len(result.Databases))
	for _, db := range result.Databases {
		paths[db.Path] = true
	}
	return paths, nil
}

func (r *tenantRunner) discovered(ctx context.Context, id tenantID, present bool) error {
	return r.wait(ctx, "discovery", id, func(checkCtx context.Context) (bool, error) {
		paths, err := r.list(checkCtx)
		return err == nil && paths[r.path(id)] == present, err
	})
}

func (r *tenantRunner) verify(ctx context.Context, id tenantID) error {
	opCtx, cancel := context.WithTimeout(ctx, r.options.Timeout)
	defer cancel()
	if !r.ledger.work[id].retired {
		if err := r.discovered(opCtx, id, true); err != nil {
			return err
		}
		if err := r.wait(opCtx, "sync", id, func(checkCtx context.Context) (bool, error) {
			deadline, _ := checkCtx.Deadline()
			response, err := r.verifier.syncOnceDB(checkCtx, min(r.options.SyncTimeout, time.Until(deadline)), r.path(id))
			return err == nil && response.TXID > 0 && response.ReplicatedTXID >= response.TXID, err
		}); err != nil {
			return err
		}
		expected, err := readLogicalSnapshot(opCtx, r.path(id), r.verifier.cfg.logicalLimits())
		if err != nil {
			return err
		}
		r.expected[id] = expected
	}
	restoreDir, err := os.MkdirTemp(r.dir, "restore-")
	if err != nil {
		return err
	}
	restored := filepath.Join(restoreDir, "restored.db")
	replicaURL := url.URL{Scheme: "file", Path: filepath.Join(r.dir, "replicas", id.name())}
	cmd := exec.CommandContext(opCtx, r.options.Binary, "restore", "-o", restored, replicaURL.String())
	cmd.Stdout = io.MultiWriter(r.fullLog, r.log)
	cmd.Stderr = cmd.Stdout
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("restore %s: %w", id.name(), err)
	}
	actual, err := readLogicalSnapshot(opCtx, restored, r.verifier.cfg.logicalLimits())
	if err != nil {
		return err
	}
	if err := compareLogicalSnapshots(r.expected[id], actual); err != nil {
		return err
	}
	if err := removeRestoredArtifacts(restored); err != nil {
		return err
	}
	return os.Remove(restoreDir)
}

func (r *tenantRunner) verifyPending(ctx context.Context, phase string) error {
	var failures []error
	for _, id := range r.ledger.pending() {
		err := r.verify(ctx, id)
		detail := "logical_match=true"
		status := "passed"
		if err != nil {
			detail = err.Error()
			status = "failed"
			failures = append(failures, err)
		}
		r.ledger.attempt(id, "", time.Now())
		r.event(phase, id, status, detail)
		if err == nil {
			r.ledger.acknowledge(id, time.Now())
		}
	}
	r.frame(phase)
	return errors.Join(failures...)
}

func (r *tenantRunner) frame(phase string) {
	frame := TenantResourceFrame{Phase: phase, At: time.Now().UTC(), Pending: len(r.ledger.pending()), OldestPendingSeconds: r.ledger.oldestAge(time.Now()), ProcessStatus: "unsupported"}
	frame.RegistryStatus = "unavailable"
	if r.process == nil {
		frame.ProcessExited = true
		frame.RegistryStatus = "process-exited"
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), r.options.Timeout)
		paths, err := r.list(ctx)
		cancel()
		if err == nil {
			frame.RegistryStatus = "fresh"
			frame.Registered = len(paths)
		}
	}
	for _, w := range r.ledger.work {
		if w.retired {
			frame.Retained++
		} else {
			frame.Live++
		}
	}
	if processCollectionSupported() {
		frame.ProcessStatus = "unavailable"
		if r.process != nil {
			rss, cpu, fds, _, err := readProcStatsAt("/proc", r.process.Process.Pid)
			if err == nil {
				frame.ProcessStatus = "fresh"
				frame.RSSBytes = rss
				frame.CPUSeconds = cpu
				frame.FDs = fds
			}
		}
	}
	r.report.Resources = append(r.report.Resources, frame)
}

func (r *tenantRunner) run(ctx context.Context) error {
	for i := 0; i < r.options.Tenants-1; i++ {
		if _, err := r.create(ctx, i); err != nil {
			return err
		}
	}
	if err := r.start(ctx); err != nil {
		return err
	}
	if err := r.verifyPending(ctx, "initial"); err != nil {
		return err
	}
	runtimeID, err := r.create(ctx, r.options.Tenants-1)
	if err != nil {
		return err
	}
	r.frame("runtime-create-pending")
	if r.options.Mode == "static" {
		if err := r.staticAbsence(ctx, runtimeID); err != nil {
			return err
		}
		r.event("static-discovery", runtimeID, "passed", "runtime tenant absent before restart, as required by pinned static capability")
		if err := r.stop(); err != nil {
			return err
		}
		if err := r.start(ctx); err != nil {
			return err
		}
	}
	if err := r.verifyPending(ctx, "runtime-create"); err != nil {
		return err
	}
	for id, w := range r.ledger.work {
		if !w.retired {
			rows := 1
			if id.Tenant == 0 {
				rows = 10
			}
			if err := r.write(ctx, id, rows); err != nil {
				return err
			}
		}
	}
	r.frame("skew-pending")
	if err := r.verifyPending(ctx, "skew"); err != nil {
		return err
	}
	r.frame("before-removal")
	old := tenantID{0, 1}
	if err := r.ledger.retire(old, time.Now()); err != nil {
		return err
	}
	if r.options.Mode == "static" {
		if err := r.stop(); err != nil {
			return err
		}
	}
	if err := os.Rename(r.path(old), r.path(old)+".retired"); err != nil {
		return err
	}
	if r.options.Mode == "static" {
		if err := r.start(ctx); err != nil {
			return err
		}
	}
	if err := r.discovered(ctx, old, false); err != nil {
		return err
	}
	r.event("removal", old, "passed", "generation absent from replication registry")
	if err := r.checkRetiredHandles(ctx, old); err != nil {
		return err
	}
	if err := r.verifyPending(ctx, "retained"); err != nil {
		return err
	}
	recreated, err := r.create(ctx, 0)
	if err != nil {
		return err
	}
	if r.options.Mode == "static" {
		if err := r.stop(); err != nil {
			return err
		}
		if err := r.start(ctx); err != nil {
			return err
		}
	}
	if err := r.verifyPending(ctx, "recreate"); err != nil {
		return err
	}
	if err := r.stop(); err != nil {
		return err
	}
	if err := r.start(ctx); err != nil {
		return err
	}
	for id := range r.ledger.work {
		r.ledger.dirty(id, time.Now())
	}
	if err := r.verifyPending(ctx, "restart"); err != nil {
		return err
	}
	if err := r.discovered(ctx, old, false); err != nil {
		return err
	}
	if err := r.discovered(ctx, recreated, true); err != nil {
		return err
	}
	r.frame("before-cleanup")
	if err := r.stop(); err != nil {
		return err
	}
	r.event("cleanup", tenantID{}, "passed", "replication process exited and was reaped; run artifacts retained")
	r.frame("cleanup")
	return nil
}

func (r *tenantRunner) staticAbsence(ctx context.Context, id tenantID) error {
	opCtx, cancel := context.WithTimeout(ctx, r.options.Timeout)
	defer cancel()
	until := time.Now().Add(time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		paths, err := r.list(opCtx)
		attempt := TenantLifecycleEvent{Phase: "static-absence", Tenant: id.name(), Status: "absent", At: time.Now().UTC()}
		if err != nil {
			attempt.Detail = err.Error()
			attempt.Status = "failed"
		}
		r.report.Attempts = append(r.report.Attempts, attempt)
		if err != nil {
			return err
		}
		if paths[r.path(id)] {
			return fmt.Errorf("static mode unexpectedly discovered runtime tenant")
		}
		if time.Now().After(until) {
			return nil
		}
		select {
		case <-opCtx.Done():
			return opCtx.Err()
		case <-ticker.C:
		}
	}
}

func (r *tenantRunner) checkRetiredHandles(ctx context.Context, id tenantID) error {
	if !processCollectionSupported() {
		r.event("retired-handles", id, "unsupported", "requires Linux /proc")
		return nil
	}
	err := r.wait(ctx, "retired-handles", id, func(context.Context) (bool, error) {
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", r.process.Process.Pid))
		if err != nil {
			return false, err
		}
		count := 0
		for _, entry := range entries {
			target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", r.process.Process.Pid, entry.Name()))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return false, err
			}
			if strings.HasPrefix(target, r.path(id)) {
				count++
			}
		}
		return count == 0, nil
	})
	if err != nil {
		return err
	}
	r.event("retired-handles", id, "passed", "no descriptors reference retired source or sidecars")
	return nil
}
