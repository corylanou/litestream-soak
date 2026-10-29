package worker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	replicaL9Level         = "0009"
	replicaL9PurgeTimeout  = 10 * time.Minute
	replicaL9DeleteTimeout = 60 * time.Second
)

var errReplicaL9Outside = errors.New("replica L9 purge refused: object outside the worker's own replica prefix")

// replicaL9Purge is the record of one switch-point purge of the lane's L9
// level, persisted with the lane state so the evidence says exactly which
// objects were removed and what the candidate build found at startup.
type replicaL9Purge struct {
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Prefix      string    `json:"prefix"`
	Listed      int       `json:"objects_listed"`
	Deleted     []string  `json:"deleted"`
	RemainingL9 int       `json:"remaining_l9"`
	Error       string    `json:"error,omitempty"`
}

// replicaL9Purger lists and deletes replica objects. list returns every
// object key under the worker's replica prefix (bucket-relative for S3,
// ReplicaPath-relative for file replicas); remove deletes exactly one key.
type replicaL9Purger struct {
	list   func(context.Context, Config) ([]string, error)
	remove func(context.Context, Config, string) error
}

func newReplicaL9Purger() replicaL9Purger {
	return replicaL9Purger{list: listReplicaObjects, remove: removeReplicaObject}
}

// replicaOwnPrefix is the only key prefix this worker may delete under.
func replicaOwnPrefix(cfg Config) string {
	if cfg.ReplicaType == "file" {
		return filepath.Clean(cfg.ReplicaPath)
	}
	return strings.Trim(strings.TrimPrefix(cfg.S3Path, "/"), "/")
}

func replicaKeyWithinPrefix(cfg Config, key string) bool {
	prefix := replicaOwnPrefix(cfg)
	if prefix == "" || prefix == "." || prefix == "/" {
		return false
	}
	if cfg.ReplicaType == "file" {
		rel, err := filepath.Rel(prefix, filepath.Join(prefix, key))
		return err == nil && rel != "." && !strings.HasPrefix(rel, "..")
	}
	return strings.HasPrefix(key, prefix+"/") && !strings.Contains(key, "/../")
}

// purgeReplicaL9 deletes every L9 object under the worker's own replica
// prefix and verifies none remain. It must only run while no Litestream
// process is replicating this database: the base build snapshots as soon as
// it sees an empty L9, which would defeat the purge.
func (p replicaL9Purger) purgeReplicaL9(ctx context.Context, cfg Config, now func() time.Time) (replicaL9Purge, error) {
	ctx, cancel := context.WithTimeout(ctx, replicaL9PurgeTimeout)
	defer cancel()
	record := replicaL9Purge{StartedAt: now().UTC(), Prefix: replicaOwnPrefix(cfg), Deleted: []string{}}
	finish := func(err error) (replicaL9Purge, error) {
		record.CompletedAt = now().UTC()
		if err != nil {
			record.Error = err.Error()
		}
		return record, err
	}
	if record.Prefix == "" || record.Prefix == "." || record.Prefix == "/" {
		return finish(fmt.Errorf("%w: empty prefix", errReplicaL9Outside))
	}
	keys, err := p.list(ctx, cfg)
	if err != nil {
		return finish(fmt.Errorf("list replica objects: %w", err))
	}
	record.Listed = len(keys)
	var targets []string
	for _, key := range keys {
		if !replicaKeyWithinPrefix(cfg, key) {
			return finish(fmt.Errorf("%w: %s (prefix %s)", errReplicaL9Outside, key, record.Prefix))
		}
		if level, ok := replicaLevelOfKey(key); ok && level == replicaL9Level {
			targets = append(targets, key)
		}
	}
	slices.Sort(targets)
	for _, key := range targets {
		if err := p.remove(ctx, cfg, key); err != nil {
			return finish(fmt.Errorf("delete %s: %w", key, err))
		}
		record.Deleted = append(record.Deleted, key)
		slog.Info("Deleted replica L9 object at upgrade switch", "key", key)
	}
	remaining, err := p.list(ctx, cfg)
	if err != nil {
		return finish(fmt.Errorf("list replica objects after purge: %w", err))
	}
	for _, key := range remaining {
		if level, ok := replicaLevelOfKey(key); ok && level == replicaL9Level {
			record.RemainingL9++
		}
	}
	if record.RemainingL9 > 0 {
		return finish(fmt.Errorf("replica L9 purge incomplete: %d L9 object(s) remain under %s", record.RemainingL9, record.Prefix))
	}
	return finish(nil)
}

func listReplicaObjects(ctx context.Context, cfg Config) ([]string, error) {
	if cfg.ReplicaType == "file" {
		return listFileReplicaObjects(cfg)
	}
	return listS3ReplicaObjects(ctx, cfg)
}

func removeReplicaObject(ctx context.Context, cfg Config, key string) error {
	if cfg.ReplicaType == "file" {
		return os.Remove(filepath.Join(replicaOwnPrefix(cfg), key))
	}
	return removeS3ReplicaObject(ctx, cfg, key)
}

func listFileReplicaObjects(cfg Config) ([]string, error) {
	root := replicaOwnPrefix(cfg)
	var keys []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return keys, err
}

// s3cmdArgs builds the connection arguments shared by every s3cmd call the
// worker makes against its replica endpoint. Credentials come from the AWS_*
// environment, never the command line.
func s3cmdArgs(cfg Config) ([]string, error) {
	endpoint := cfg.directS3Endpoint()
	host := endpointHost(endpoint)
	if host == "" {
		return nil, fmt.Errorf("replica endpoint %q has no host", endpoint)
	}
	args := []string{"--host=" + host}
	if strings.HasPrefix(strings.ToLower(endpoint), "http://") {
		args = append(args, "--no-ssl", "--host-bucket="+host)
	} else {
		args = append(args, "--host-bucket=%(bucket)s."+host)
	}
	if region := os.Getenv("AWS_REGION"); region != "" {
		args = append(args, "--region="+region)
	}
	return args, nil
}

func listS3ReplicaObjects(ctx context.Context, cfg Config) ([]string, error) {
	args, err := s3cmdArgs(cfg)
	if err != nil {
		return nil, err
	}
	prefix := replicaOwnPrefix(cfg)
	prefixURL := fmt.Sprintf("s3://%s/%s/", cfg.S3Bucket, prefix)
	args = append(args, "ls", "--recursive", prefixURL)
	ctx, cancel := context.WithTimeout(ctx, replicaLevelListTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "s3cmd", args...)
	cmd.WaitDelay = replicaLevelKillGrace
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	keys, parseErr := parseS3ListingKeys(stdout, cfg.S3Bucket)
	waitErr := cmd.Wait()
	switch {
	case ctx.Err() != nil:
		return nil, ctx.Err()
	case waitErr != nil:
		return nil, fmt.Errorf("%w: %s", waitErr, tailString(strings.TrimSpace(stderr.String()), 512))
	case parseErr != nil:
		return nil, parseErr
	}
	return keys, nil
}

// parseS3ListingKeys turns `s3cmd ls --recursive` rows (DATE TIME SIZE
// s3://bucket/key, where the key may contain spaces) into bucket-relative keys.
func parseS3ListingKeys(r interface{ Read([]byte) (int, error) }, bucket string) ([]string, error) {
	var keys []string
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	marker := "s3://" + bucket + "/"
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 {
			continue
		}
		url := strings.Join(fields[3:], " ")
		if !strings.HasPrefix(url, marker) {
			continue
		}
		keys = append(keys, strings.TrimPrefix(url, marker))
	}
	return keys, scanner.Err()
}

func removeS3ReplicaObject(ctx context.Context, cfg Config, key string) error {
	args, err := s3cmdArgs(cfg)
	if err != nil {
		return err
	}
	args = append(args, "del", "s3://"+path.Join(cfg.S3Bucket, key))
	ctx, cancel := context.WithTimeout(ctx, replicaL9DeleteTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "s3cmd", args...)
	cmd.WaitDelay = replicaLevelKillGrace
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, tailString(strings.TrimSpace(string(output)), 512))
	}
	return nil
}
