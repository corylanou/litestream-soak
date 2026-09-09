package compare

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/worker"
	_ "modernc.org/sqlite"
)

const localConfig = "file-replica;sync=100ms;checkpoint=100ms;wal;autocheckpoint=0;busy_timeout=5000ms;favorable=128B/1ms;representative=1024B/1ms;saturation=16384B/unpaced;no-litestream=1024B/1ms;startup-probe=1s;sync-confirmed=2s;sample=100ms;readonly-sync-diagnostic;pprof=TotalAlloc;oracle=soak-logical-v1"

type Local struct {
	Fixture    string            `json:"fixture"`
	Directory  string            `json:"directory"`
	HarnessSHA string            `json:"harness_sha"`
	Binaries   map[string]string `json:"binaries"`
	Timeout    time.Duration     `json:"timeout"`
}

func LocalConfigSHA256() string { return fmt.Sprintf("%x", sha256.Sum256([]byte(localConfig))) }

func CreateFixture(ctx context.Context, path string, seed int64, rows int) error {
	if rows < 0 {
		return errors.New("fixture rows must not be negative")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE fixture(id INTEGER PRIMARY KEY, value BLOB NOT NULL); CREATE TABLE operations(id INTEGER PRIMARY KEY, value BLOB NOT NULL)"); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for i := 0; i < rows; i++ {
		if _, err := tx.ExecContext(ctx, "INSERT INTO fixture VALUES (?, ?)", i, payload(seed, int64(i), 1024)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func payload(seed, id int64, size int) []byte {
	value := sha256.Sum256([]byte(fmt.Sprintf("%d/%d", seed, id)))
	b := make([]byte, size)
	for i := range b {
		b[i] = value[i%len(value)]
	}
	return b
}

func (l Local) Execute(ctx context.Context, r Request) (observation Observation, resultErr error) {
	observation = Observation{StartedAt: time.Now().UTC(), Request: r, Correctness: "unavailable", Reliability: "unavailable", Metrics: map[string]Measurement{}}
	hardware, err := LocalHardware()
	if err != nil {
		return observation, err
	}
	if r.Contract.Hardware != hardware || r.Contract.Region != "local" || r.Contract.Toolchain != runtime.Version() {
		return observation, errors.New("local runtime hardware, region or toolchain mismatch")
	}
	if l.Timeout <= 0 {
		return observation, errors.New("positive per-run timeout required")
	}
	if !validHex(l.HarnessSHA, 40) || r.Contract.GeneratorSHA != l.HarnessSHA || r.Contract.OracleSHA != l.HarnessSHA {
		return observation, errors.New("generator and oracle must match the pinned harness source")
	}
	if r.Contract.ConfigSHA256 != LocalConfigSHA256() {
		return observation, errors.New("local runtime configuration digest mismatch")
	}
	if !filepath.IsLocal(r.ReplicaPrefix) {
		return observation, errors.New("unsafe replica prefix")
	}
	data, err := os.ReadFile(l.Fixture)
	if err != nil {
		return observation, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != r.Contract.FixtureSHA256 || int64(len(data)) != r.Contract.FixtureBytes {
		return observation, errors.New("fixture digest or size mismatch")
	}
	dir := filepath.Join(l.Directory, r.ReplicaPrefix)
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return observation, err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return observation, fmt.Errorf("reserve fresh run directory: %w", err)
	}
	defer func() {
		if resultErr != nil {
			observation.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, recordLogEvidence(dir, &observation))
		if resultErr != nil {
			observation.Error = resultErr.Error()
		}
		observation.FinishedAt = time.Now().UTC()
		attachRunEvidence(&observation)
		resultErr = errors.Join(resultErr, writeJSON(filepath.Join(dir, "observation.json"), observation))
	}()
	executable, err := os.Executable()
	if err != nil {
		return observation, err
	}
	harnessData, err := os.ReadFile(executable)
	if err != nil {
		return observation, err
	}
	harness := map[string]string{"binary_sha256": fmt.Sprintf("%x", sha256.Sum256(harnessData)), "declared_source_sha": l.HarnessSHA, "toolchain": runtime.Version()}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if strings.HasPrefix(setting.Key, "vcs.") {
				harness[setting.Key] = setting.Value
			}
		}
	}
	if err := writeJSON(filepath.Join(dir, "harness.json"), harness); err != nil {
		return observation, err
	}
	ctx, cancel := context.WithTimeout(ctx, l.Timeout)
	defer cancel()
	source := filepath.Join(dir, "source.db")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		return observation, err
	}
	if err := writeJSON(filepath.Join(dir, "request.json"), r); err != nil {
		return observation, err
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		return observation, err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0"); err != nil {
		return observation, err
	}
	baselineDisk, err := dataBytes(dir)
	if err != nil {
		return observation, err
	}
	binary := l.Binaries[r.SHA]
	var process *exec.Cmd
	var logPath string
	var telemetry *monitor
	var s3Observer *worker.ComparisonS3Observer
	if r.Litestream {
		if binary == "" {
			return observation, errors.New("no binary supplied for pinned SHA " + r.SHA)
		}
		binary, err = filepath.Abs(binary)
		if err != nil {
			return observation, err
		}
		version, err := exec.CommandContext(ctx, binary, "version").Output()
		if err != nil {
			return observation, fmt.Errorf("binary version: %w", err)
		}
		if !strings.Contains(string(version), r.SHA) {
			return observation, errors.New("binary version does not contain pinned SHA")
		}
		info, err := buildinfo.ReadFile(binary)
		if err != nil {
			return observation, fmt.Errorf("read candidate compiler metadata: %w", err)
		}
		if info.GoVersion != r.Contract.Toolchain {
			return observation, errors.New("candidate runtime compiler mismatch")
		}
		binaryData, err := os.ReadFile(binary)
		if err != nil {
			return observation, err
		}
		metadata := map[string]string{"sha256": fmt.Sprintf("%x", sha256.Sum256(binaryData)), "version": string(version), "harness_sha": l.HarnessSHA, "toolchain": runtime.Version()}
		if err := writeJSON(filepath.Join(dir, "binary.json"), metadata); err != nil {
			return observation, err
		}
		socketDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(dir)))
		socket := filepath.Join(os.TempDir(), "compare-"+socketDigest[:20]+".sock")
		replicaConfig := fmt.Sprintf("      - path: %q\n        sync-interval: 100ms\n", filepath.Join(dir, "replica"))
		if r.Contract.Backend == "s3" {
			s3Observer, err = worker.StartComparisonS3Observer(ctx, r.Contract.ReplicaEndpoint, r.Contract.ReplicaRegion, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN"))
			if err != nil {
				return observation, err
			}
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
				defer closeCancel()
				resultErr = errors.Join(resultErr, s3Observer.Close(closeCtx), recordS3Evidence(dir, s3Observer.Evidence(), &observation))
			}()
			replicaConfig = fmt.Sprintf("      - type: s3\n        bucket: %q\n        path: %q\n        region: %q\n        endpoint: %q\n        force-path-style: true\n        sync-interval: 100ms\n", r.Contract.ReplicaBucket, r.ReplicaPrefix, r.Contract.ReplicaRegion, s3Observer.Endpoint())
		}
		config := fmt.Sprintf("socket:\n  enabled: true\n  path: %q\ndbs:\n  - path: %q\n    min-checkpoint-page-count: 1\n    checkpoint-interval: 100ms\n    replicas:\n%s", socket, source, replicaConfig)
		if err := os.WriteFile(filepath.Join(dir, "litestream.yml"), []byte(config), 0o600); err != nil {
			return observation, err
		}
		logPath = filepath.Join(dir, "replicate.log")
		log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return observation, err
		}
		defer func() { _ = log.Close() }()
		process = exec.CommandContext(ctx, binary, "replicate", "-config", filepath.Join(dir, "litestream.yml"))
		process.Stdout = log
		process.Stderr = log
		if err := process.Start(); err != nil {
			return observation, err
		}
		defer func() {
			if process.ProcessState == nil {
				_ = process.Process.Kill()
				_ = process.Wait()
			}
		}()
		telemetry = socketMonitor(socket, source, dir, process.Process.Pid)
		defer telemetry.client.CloseIdleConnections()
		defer func() { resultErr = errors.Join(resultErr, telemetry.preserve(&observation)) }()
		readinessDeadline := time.Now().Add(time.Second)
		for time.Now().Before(readinessDeadline) {
			if _, _, err := telemetry.progress(ctx); err == nil {
				break
			}
			if err := waitContext(ctx, 20*time.Millisecond); err != nil {
				return observation, err
			}
		}
		telemetry.begin(ctx)
	}
	size := 1024
	pace := time.Millisecond
	if r.Scenario == "favorable" {
		size = 128
	}
	if r.Scenario == "saturation" {
		size = 16384
		pace = 0
	}
	latencies := make([]float64, 0)
	lastSample := time.Now()
	workloadStarted := time.Now()
	for id := int64(0); id < r.Contract.OperationBudget; id++ {
		started := time.Now()
		observation.WorkloadAttempts++
		if _, err := db.ExecContext(ctx, "INSERT INTO operations VALUES (?, ?)", id, payload(r.Contract.Seed, id, size)); err != nil {
			return observation, err
		}
		latencies = append(latencies, time.Since(started).Seconds())
		observation.CompletedOperations++
		if telemetry != nil && time.Since(lastSample) >= 100*time.Millisecond {
			telemetry.sample(ctx, time.Now())
			lastSample = time.Now()
		}
		if pace > 0 {
			if err := waitContext(ctx, pace); err != nil {
				return observation, err
			}
		}
	}
	observation.WorkloadSeconds = time.Since(workloadStarted).Seconds()
	sort.Float64s(latencies)
	for metric, q := range map[string]float64{"latency_p50_seconds": 0.50, "latency_p95_seconds": 0.95, "latency_p99_seconds": 0.99} {
		value := percentile(latencies, q)
		observation.Metrics[metric] = Measurement{Value: &value}
	}
	target := source
	if r.Litestream {
		telemetry.sample(ctx, time.Now())
		if err := telemetry.sync(ctx); err != nil {
			observation.Incidents = append(observation.Incidents, Incident{Category: "sync", Detail: err.Error()})
		}
		if err := telemetry.finish(ctx, &observation); err != nil {
			return observation, err
		}
		if err := process.Process.Signal(os.Interrupt); err != nil {
			return observation, err
		}
		if err := process.Wait(); err != nil {
			return observation, fmt.Errorf("replicate exit: %w", err)
		}
		cpu := (process.ProcessState.UserTime() + process.ProcessState.SystemTime()).Seconds() / float64(observation.CompletedOperations)
		observation.Metrics["cpu_seconds_per_operation"] = Measurement{Value: &cpu}
		if usage, ok := process.ProcessState.SysUsage().(*syscall.Rusage); ok {
			rss := float64(usage.Maxrss)
			if runtime.GOOS == "linux" {
				rss *= 1024
			}
			observation.Metrics["rss_bytes"] = Measurement{Value: &rss}
		}

		target = filepath.Join(dir, "restored.db")
		started := time.Now()
		output, restoreErr := exec.CommandContext(ctx, binary, "restore", "-config", filepath.Join(dir, "litestream.yml"), "-o", target, source).CombinedOutput()
		if err := os.WriteFile(filepath.Join(dir, "restore.log"), output, 0o600); err != nil {
			return observation, err
		}
		duration := time.Since(started).Seconds()
		observation.Metrics["restore_seconds"] = Measurement{Value: &duration}
		if restoreErr != nil {
			observation.Incidents = append(observation.Incidents, Incident{Category: "restore", Detail: restoreErr.Error()})
			return observation, fmt.Errorf("restore: %w", restoreErr)
		}
		if s3Observer == nil {
			replicaBytes, err := directoryBytes(filepath.Join(dir, "replica"))
			if err != nil {
				return observation, err
			}
			value := float64(replicaBytes)
			observation.Metrics["replica_bytes"] = Measurement{Value: &value}
		}
	}
	if err := validateLocal(ctx, l.Fixture, target, r.Contract.Seed, r.Contract.OperationBudget, size); err != nil {
		observation.Correctness = "fail"
		observation.VerifiedAt = time.Now().UTC()
		observation.Incidents = append(observation.Incidents, Incident{Category: "correctness", Detail: err.Error()})
		return observation, err
	}
	if err := worker.CompareLogicalDatabases(ctx, source, target); err != nil {
		observation.Correctness = "fail"
		observation.VerifiedAt = time.Now().UTC()
		observation.Incidents = append(observation.Incidents, Incident{Category: "logical_schema_or_data", Detail: err.Error()})
		return observation, err
	}
	observation.Correctness = "pass"
	observation.VerifiedAt = time.Now().UTC()
	finalDisk, err := dataBytes(dir)
	if err != nil {
		return observation, err
	}
	growth := float64(finalDisk - baselineDisk)
	observation.Metrics["disk_growth_bytes"] = Measurement{Value: &growth}
	for _, metric := range Metrics() {
		if _, ok := observation.Metrics[metric]; !ok {
			reason := "local executor does not instrument " + metric
			switch metric {
			case "allocation_bytes_per_operation":
				reason = "no candidate allocation profile collected; harness allocations are not candidate allocations"
			case "lag_seconds":
				reason = "local executor does not sample replication progress; drain delay is not measured lag"
			case "fd_growth":
				reason = "no portable time series of candidate open descriptors collected"
			case "object_requests":
				reason = "file replica backend has no object-store requests"
			case "cpu_seconds_per_operation", "rss_bytes", "restore_seconds", "replica_bytes":
				reason = "no-Litestream control has no replication process or restore"
			}
			observation.Metrics[metric] = Measurement{Unavailable: reason}
		}
	}
	return observation, nil
}

func validateLocal(ctx context.Context, fixture, target string, seed, budget int64, size int) error {
	db, err := sql.Open("sqlite", target)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "ATTACH DATABASE ? AS original", fixture); err != nil {
		return err
	}
	var differences int
	if err := db.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM (SELECT * FROM fixture EXCEPT SELECT * FROM original.fixture)) + (SELECT count(*) FROM (SELECT * FROM original.fixture EXCEPT SELECT * FROM fixture))").Scan(&differences); err != nil {
		return err
	}
	if differences != 0 {
		return errors.New("restored fixture differs from immutable fixture")
	}
	rows, err := db.QueryContext(ctx, "SELECT id,value FROM operations ORDER BY id")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var count int64
	for rows.Next() {
		var id int64
		var value []byte
		if err := rows.Scan(&id, &value); err != nil {
			return err
		}
		if id != count || string(value) != string(payload(seed, id, size)) {
			return fmt.Errorf("operation %d differs from independent oracle", count)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != budget {
		return fmt.Errorf("restored %d operations, expected %d", count, budget)
	}
	return nil
}

func directoryBytes(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func waitContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(data, '\n'))
	return errors.Join(writeErr, f.Close())
}

func LocalHardware() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/%s/cpus=%d", host, runtime.GOOS, runtime.GOARCH, runtime.NumCPU()), nil
}

func dataBytes(dir string) (int64, error) {
	var total int64
	for _, name := range []string{"source.db", "source.db-wal", "source.db-shm", ".source.db-litestream", "replica"} {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if info.IsDir() {
			size, err := directoryBytes(path)
			if err != nil {
				return 0, err
			}
			total += size
		} else {
			total += info.Size()
		}
	}
	return total, nil
}

func percentile(sorted []float64, q float64) float64 {
	return sorted[int(math.Ceil(q*float64(len(sorted))))-1]
}

func writeExclusive(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	return errors.Join(writeErr, f.Close())
}

func recordLogEvidence(directory string, o *Observation) error {
	data, err := os.ReadFile(filepath.Join(directory, "replicate.log"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	e := reporting.MaintenanceEvidence{Epoch: o.Request.ReplicaPrefix, StartedAt: o.StartedAt, Complete: true}
	for _, line := range strings.Split(string(data), "\n") {
		before := e.Errors
		e.ObserveLine(line)
		if e.Errors > before {
			o.Incidents = append(o.Incidents, Incident{Category: "replication_log", Detail: line})
		}
	}
	o.Maintenance = &e
	return nil
}
