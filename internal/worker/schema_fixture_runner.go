package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"

	"github.com/corylanou/litestream-soak/internal/rig"
	"github.com/corylanou/litestream-soak/internal/s3util"
)

type SchemaFixtureOptions struct {
	S3Endpoint       string `json:"s3_endpoint,omitempty"`
	S3Bucket         string `json:"s3_bucket,omitempty"`
	S3Environment    string `json:"s3_environment,omitempty"`
	Directory        string `json:"directory"`
	Binary           string `json:"binary"`
	SHA              string `json:"litestream_sha"`
	Rows             int    `json:"rows"`
	PayloadBytes     int    `json:"payload_bytes"`
	Vacuum           string `json:"vacuum"`
	MaxHeadroomBytes uint64 `json:"max_headroom_bytes"`
}

type SchemaFixtureBoundary struct {
	SyncAttempts        []SchemaFixtureSyncAttempt `json:"sync_attempts"`
	Name                string                     `json:"name"`
	OperationSeconds    float64                    `json:"operation_seconds"`
	VerificationSeconds float64                    `json:"verification_seconds"`
	DatabaseBytes       int64                      `json:"database_bytes"`
	WALBytes            int64                      `json:"wal_bytes"`
	LocalBytes          int64                      `json:"local_bytes"`
	ReplicaBytes        int64                      `json:"replica_bytes"`
	RemoteBytes         *int64                     `json:"remote_bytes"`
	HeadroomBytes       uint64                     `json:"headroom_bytes"`
	Pages               int64                      `json:"pages"`
	FreePages           int64                      `json:"free_pages"`
	TXID                uint64                     `json:"txid"`
	LogicalMatch        bool                       `json:"logical_match"`
	Error               string                     `json:"error,omitempty"`
}

type SchemaFixtureSyncAttempt struct {
	TXID           uint64 `json:"txid"`
	ReplicatedTXID uint64 `json:"replicated_txid"`
	Error          string `json:"error,omitempty"`
}

type SchemaFixtureResult struct {
	RemotePrefix     string                  `json:"remote_prefix,omitempty"`
	Options          SchemaFixtureOptions    `json:"options"`
	Verdict          string                  `json:"verdict"`
	Error            string                  `json:"error,omitempty"`
	ReplicaTransport string                  `json:"replica_transport"`
	Boundaries       []SchemaFixtureBoundary `json:"boundaries"`
}

func RunSchemaFixture(ctx context.Context, options SchemaFixtureOptions) (result SchemaFixtureResult, retErr error) {
	result.Verdict = "inconclusive"
	result.ReplicaTransport = "file; remote measurement unexecuted"
	var remote *s3util.Client
	if options.S3Endpoint != "" {
		endpoint, err := url.Parse(options.S3Endpoint)
		if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
			return result, fmt.Errorf("S3 endpoint must be an HTTP(S) URL without credentials, query or fragment")
		}
		if options.S3Environment != "emulator" && options.S3Environment != "provider" {
			return result, fmt.Errorf("S3 environment must be emulator or provider")
		}
		remote, err = s3util.NewClient(s3util.Config{Bucket: options.S3Bucket, Endpoint: options.S3Endpoint, Region: os.Getenv("AWS_REGION"), AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"), SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY")})
		if err != nil {
			return result, err
		}
		result.RemotePrefix = "schema-fixture/" + uuid.NewString()
		result.ReplicaTransport = "s3-" + options.S3Environment
	}
	result.Options = options
	steps, err := schemaFixtureSteps(options.Rows, options.PayloadBytes, options.Vacuum)
	if err != nil {
		return result, err
	}
	if options.Directory == "" || options.Binary == "" || len(options.SHA) != 40 {
		return result, fmt.Errorf("new fixture directory, binary and full pinned SHA are required")
	}
	dir, err := filepath.Abs(options.Directory)
	if err != nil {
		return result, err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return result, fmt.Errorf("create exclusive fixture directory: %w", err)
	}
	evidence, err := os.OpenFile(filepath.Join(dir, "boundaries.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return result, err
	}
	defer func() { _ = evidence.Close() }()
	defer func() {
		if retErr != nil {
			result.Error = retErr.Error()
			if result.Verdict == "scenario_success" {
				result.Verdict = "unrelated_failure"
			}
		}
		data, err := json.MarshalIndent(result, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, "result.json"), data, 0o600)
		}
		retErr = errors.Join(retErr, err)
		if retErr != nil {
			result.Error = retErr.Error()
			if result.Verdict == "scenario_success" {
				result.Verdict = "unrelated_failure"
			}
		}
	}()
	version, err := exec.CommandContext(ctx, options.Binary, "version").CombinedOutput()
	if err != nil || strings.TrimSpace(string(version)) != options.SHA {
		return result, fmt.Errorf("pinned binary version mismatch: %q: %v", strings.TrimSpace(string(version)), err)
	}
	var disk unix.Statfs_t
	if err := unix.Statfs(dir, &disk); err != nil {
		return result, err
	}
	if options.MaxHeadroomBytes > 0 && disk.Bavail*uint64(disk.Bsize) > options.MaxHeadroomBytes {
		return result, fmt.Errorf("low-headroom fixture not engaged: available=%d maximum=%d", disk.Bavail*uint64(disk.Bsize), options.MaxHeadroomBytes)
	}
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.ReplicaPath = filepath.Join(dir, "replica")
	cfg.SocketPath = filepath.Join("/tmp", "schema-"+uuid.NewString()+".sock")
	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return result, err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	config := fmt.Sprintf("socket:\n  enabled: true\n  path: %q\ndbs:\n  - path: %q\n    replicas:\n      - path: %q\n        sync-interval: 100ms\n", cfg.SocketPath, cfg.DBPath, cfg.ReplicaPath)
	if remote != nil {
		config = fmt.Sprintf("socket:\n  enabled: true\n  path: %q\ndbs:\n  - path: %q\n    replicas:\n      - url: %q\n        endpoint: %q\n        force-path-style: true\n        sync-interval: 100ms\n", cfg.SocketPath, cfg.DBPath, "s3://"+options.S3Bucket+"/"+result.RemotePrefix, options.S3Endpoint)
	}
	if err := os.WriteFile(cfg.ConfigPath, []byte(config), 0o600); err != nil {
		return result, err
	}
	log, err := os.OpenFile(filepath.Join(dir, "replicate.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return result, err
	}
	defer func() { _ = log.Close() }()
	processCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(processCtx, options.Binary, "replicate", "-config", cfg.ConfigPath)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		return result, err
	}
	defer func() { cancel(); _ = cmd.Wait() }()
	v := NewVerifier(cfg)
	recovery := rig.RecoveryEvidence{}
	for _, step := range steps {
		boundary := SchemaFixtureBoundary{Name: step.name}
		started := time.Now()
		err := step.run(ctx, db)
		boundary.OperationSeconds = time.Since(started).Seconds()
		if err == nil {
			err = step.validate(ctx, db)
		}
		if err == nil {
			started = time.Now()
			err = verifySchemaFixture(ctx, v, options.Binary, step, dir, &boundary)
			boundary.VerificationSeconds = time.Since(started).Seconds()
		}
		if observeErr := observeSchemaFixture(ctx, db, cfg, &boundary); observeErr != nil {
			err = errors.Join(err, observeErr)
		}
		if err == nil {
			switch step.name {
			case "delete":
				if boundary.FreePages == 0 {
					err = fmt.Errorf("deletion did not expose free pages")
				}
			case "reuse":
				if boundary.FreePages >= result.Boundaries[len(result.Boundaries)-1].FreePages {
					err = fmt.Errorf("insertion did not reuse free pages")
				}
			case "reclaim":
				if boundary.FreePages != 0 || boundary.Pages >= result.Boundaries[len(result.Boundaries)-1].Pages {
					err = fmt.Errorf("vacuum did not reclaim pages")
				}
			}
		}
		if remote != nil {
			size, measureErr := remote.MeasurePrefix(ctx, result.RemotePrefix)
			if measureErr != nil {
				err = errors.Join(err, measureErr)
			} else if size <= 0 && err == nil {
				err = fmt.Errorf("completed boundary has no remote object bytes")
			} else {
				boundary.RemoteBytes = &size
			}
		}
		if err != nil {
			boundary.Error = err.Error()
			if ctx.Err() != nil {
				recovery.Aborted = true
			} else {
				recovery.OtherFailures++
			}
		} else {
			recovery.ValidRestores++
			recovery.ExposedRestores++
		}
		result.Boundaries = append(result.Boundaries, boundary)
		result.Verdict = recovery.Verdict()
		if writeErr := json.NewEncoder(evidence).Encode(boundary); writeErr != nil {
			return result, errors.Join(err, writeErr)
		}
		if syncErr := evidence.Sync(); syncErr != nil {
			return result, errors.Join(err, syncErr)
		}
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func observeSchemaFixture(ctx context.Context, db *sql.DB, cfg Config, boundary *SchemaFixtureBoundary) error {
	for _, item := range []struct {
		path   string
		target *int64
	}{{cfg.DBPath, &boundary.DatabaseBytes}, {cfg.DBPath + "-wal", &boundary.WALBytes}} {
		info, err := os.Stat(item.path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		*item.target = info.Size()
	}
	if err := db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&boundary.Pages); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&boundary.FreePages); err != nil {
		return err
	}
	var disk unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(cfg.DBPath), &disk); err != nil {
		return err
	}
	boundary.HeadroomBytes = disk.Bavail * uint64(disk.Bsize)
	if err := filepath.WalkDir(filepath.Dir(cfg.DBPath), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		boundary.LocalBytes += info.Size()
		return nil
	}); err != nil {
		return err
	}
	err := filepath.WalkDir(cfg.ReplicaPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		boundary.ReplicaBytes += info.Size()
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func verifySchemaFixture(ctx context.Context, v *Verifier, binary string, step schemaFixtureStep, dir string, boundary *SchemaFixtureBoundary) error {
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		synced, err := v.syncOnceDB(syncCtx, time.Second, v.cfg.DBPath)
		attempt := SchemaFixtureSyncAttempt{TXID: synced.TXID, ReplicatedTXID: synced.ReplicatedTXID}
		if err != nil {
			attempt.Error = err.Error()
		}
		boundary.SyncAttempts = append(boundary.SyncAttempts, attempt)
		if err == nil && synced.TXID > 0 && synced.ReplicatedTXID >= synced.TXID {
			boundary.TXID = synced.TXID
			break
		}
		if err != nil {
			_, socketErr := os.Stat(v.cfg.SocketPath)
			if step.name != "initialize" || !errors.Is(socketErr, os.ErrNotExist) {
				return err
			}
		}
		select {
		case <-syncCtx.Done():
			return fmt.Errorf("sync boundary: %w", syncCtx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	expected, err := readLogicalSnapshot(ctx, v.cfg.DBPath, v.cfg.logicalLimits())
	if err != nil {
		return err
	}
	path := filepath.Join(dir, step.name+"-restored.db")
	output, err := exec.CommandContext(ctx, binary, "restore", "-config", v.cfg.ConfigPath, "-txid", formatTXID(boundary.TXID), "-o", path, v.cfg.DBPath).CombinedOutput()
	if logErr := os.WriteFile(filepath.Join(dir, step.name+"-restore.log"), output, 0o600); logErr != nil {
		return errors.Join(err, logErr)
	}
	if err != nil {
		return fmt.Errorf("restore: %w: %s", err, output)
	}
	actual, err := readLogicalSnapshot(ctx, path, v.cfg.logicalLimits())
	if err != nil {
		return err
	}
	if err := compareLogicalSnapshots(expected, actual); err != nil {
		return err
	}
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	restored, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer func() { _ = restored.Close() }()
	if err := step.validate(ctx, restored); err != nil {
		return err
	}
	boundary.LogicalMatch = true
	return nil
}
