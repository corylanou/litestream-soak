package worker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	// replicaLevelListTimeout bounds one recursive listing of the replica prefix.
	replicaLevelListTimeout = 90 * time.Second
	// replicaLevelKillGrace is how long Wait may block after the context kills
	// s3cmd before its pipes are abandoned.
	replicaLevelKillGrace = 5 * time.Second
)

// replicaLevel is the object count and total size of one LTX level on the
// replica, aggregated across every database under the worker's prefix.
type replicaLevel struct {
	Level   string
	Objects int
	Bytes   int64
}

// replicaLevelPoller periodically lists the replica's LTX levels and publishes
// per-level object counts and sizes, so compaction lag (an L0 or L1 backlog
// growing while restores are in flight) is visible in Grafana rather than only
// in a failure debug snapshot.
//
// One recursive listing of the worker's prefix is streamed and aggregated by
// level in Go, so memory stays flat regardless of backlog size and the same
// pass covers single-database (prefix/000N/) and many-database
// (prefix/<db>/000N/) layouts.
type replicaLevelPoller struct {
	cfg   *Config
	list  func(context.Context, Config) ([]replicaLevel, error)
	known map[string]struct{} // levels published by the previous successful poll
}

func newReplicaLevelPoller(cfg *Config) *replicaLevelPoller {
	return &replicaLevelPoller{cfg: cfg, list: listReplicaLevels, known: make(map[string]struct{})}
}

func (p *replicaLevelPoller) enabled() bool {
	return p.cfg.ReplicaType == "s3" && p.cfg.S3Bucket != "" && p.cfg.ReplicaLevelPollInterval > 0
}

func (p *replicaLevelPoller) Run(ctx context.Context) {
	if !p.enabled() || ctx.Err() != nil {
		return
	}
	p.poll(ctx)
	ticker := time.NewTicker(p.cfg.ReplicaLevelPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *replicaLevelPoller) poll(ctx context.Context) {
	levels, err := p.list(ctx, *p.cfg)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("Replica level listing failed", "error", err)
		}
		SetReplicaLevelListingOK(false)
		return
	}
	// A level that has been compacted away since the last poll must read
	// zero, not keep its old count.
	current := make(map[string]struct{}, len(levels))
	for _, level := range levels {
		current[level.Level] = struct{}{}
	}
	for level := range p.known {
		if _, ok := current[level]; !ok {
			levels = append(levels, replicaLevel{Level: level})
		}
	}
	p.known = current
	SetReplicaLevelStats(levels)
	SetReplicaLevelListingOK(true)
}

// listReplicaLevels runs one recursive s3cmd listing of the worker's replica
// prefix, streaming the output and aggregating objects and bytes per
// four-digit level directory. Credentials come from the AWS_* environment that
// s3cmd reads itself, so they never appear on the command line.
func listReplicaLevels(ctx context.Context, cfg Config) ([]replicaLevel, error) {
	host := endpointHost(cfg.S3Endpoint)
	if host == "" {
		return nil, fmt.Errorf("replica endpoint %q has no host", cfg.S3Endpoint)
	}
	prefixURL := fmt.Sprintf("s3://%s/%s/", cfg.S3Bucket, strings.Trim(strings.TrimPrefix(cfg.S3Path, "/"), "/"))
	args := []string{"--host=" + host}
	if strings.HasPrefix(strings.ToLower(cfg.S3Endpoint), "http://") {
		// Plain-http endpoints are the local fault proxy or MinIO: no TLS
		// and path-style addressing, since %(bucket)s.127.0.0.1 never
		// resolves.
		args = append(args, "--no-ssl", "--host-bucket="+host)
	} else {
		args = append(args, "--host-bucket=%(bucket)s."+host)
	}
	if region := os.Getenv("AWS_REGION"); region != "" {
		args = append(args, "--region="+region)
	}
	args = append(args, "ls", "--recursive", prefixURL)

	ctx, cancel := context.WithTimeout(ctx, replicaLevelListTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "s3cmd", args...)
	cmd.WaitDelay = replicaLevelKillGrace
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("list replica levels: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("list replica levels: %w", err)
	}
	levels, parseErr := aggregateReplicaListing(stdout)
	waitErr := cmd.Wait()
	switch {
	case ctx.Err() != nil:
		return nil, fmt.Errorf("list replica levels: %w", ctx.Err())
	case waitErr != nil:
		return nil, fmt.Errorf("list replica levels: %w: %s", waitErr, tailString(strings.TrimSpace(stderr.String()), 512))
	case parseErr != nil:
		return nil, fmt.Errorf("list replica levels: %w", parseErr)
	}
	return levels, nil
}

// aggregateReplicaListing consumes `s3cmd ls --recursive` output
// (DATE TIME SIZE KEY, where KEY may contain spaces) and sums objects and bytes
// per four-digit directory that directly contains each object.
func aggregateReplicaListing(r io.Reader) ([]replicaLevel, error) {
	totals := make(map[string]*replicaLevel)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 {
			continue
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			continue // "DIR" rows and other non-object lines
		}
		key := strings.Join(fields[3:], " ")
		level, ok := replicaLevelOfKey(key)
		if !ok {
			continue
		}
		entry := totals[level]
		if entry == nil {
			entry = &replicaLevel{Level: level}
			totals[level] = entry
		}
		entry.Objects++
		entry.Bytes += size
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read listing: %w", err)
	}
	levels := make([]replicaLevel, 0, len(totals))
	for _, entry := range totals {
		levels = append(levels, *entry)
	}
	slices.SortFunc(levels, func(a, b replicaLevel) int { return strings.Compare(a.Level, b.Level) })
	return levels, nil
}

// replicaLevelOfKey returns the four-digit level directory that directly
// contains key (…/0001/<file>), if any. Only LTX's real levels 0000–0009
// (compaction levels 0–8 and the snapshot level 9) are accepted, so the
// level label stays bounded to ten values.
func replicaLevelOfKey(key string) (string, bool) {
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return "", false
	}
	level := parts[len(parts)-2]
	if len(level) != 4 || level[:3] != "000" || level[3] < '0' || level[3] > '9' {
		return "", false
	}
	return level, true
}
