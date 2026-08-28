package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

// replicaLevelListTimeout bounds one recursive listing of the replica prefix.
const replicaLevelListTimeout = 90 * time.Second

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
// A single recursive listing of the worker's prefix is aggregated by level in
// awk, so the output stays a few lines regardless of backlog size and the
// same command covers single-database (prefix/000N/) and many-database
// (prefix/<db>/000N/) layouts.
type replicaLevelPoller struct {
	cfg  *Config
	list func(context.Context, Config) ([]replicaLevel, error)
}

func newReplicaLevelPoller(cfg *Config) *replicaLevelPoller {
	return &replicaLevelPoller{cfg: cfg, list: listReplicaLevels}
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
	SetReplicaLevelStats(levels)
	SetReplicaLevelListingOK(true)
}

// listReplicaLevels runs one recursive s3cmd listing of the worker's replica
// prefix and aggregates object counts and bytes per four-digit level directory.
func listReplicaLevels(ctx context.Context, cfg Config) ([]replicaLevel, error) {
	host := endpointHost(cfg.S3Endpoint)
	if host == "" {
		return nil, fmt.Errorf("replica endpoint %q has no host", cfg.S3Endpoint)
	}
	prefixURL := fmt.Sprintf("s3://%s/%s/", cfg.S3Bucket, strings.Trim(strings.TrimPrefix(cfg.S3Path, "/"), "/"))
	ssl := ""
	if strings.HasPrefix(strings.ToLower(cfg.S3Endpoint), "http://") {
		ssl = " --no-ssl"
	}
	command := fmt.Sprintf(
		`s3cmd --access_key="$AWS_ACCESS_KEY_ID" --secret_key="$AWS_SECRET_ACCESS_KEY" --host=%s --host-bucket='%%(bucket)s.%s' --region="$AWS_REGION"%s ls --recursive %s | awk '%s'`,
		shellQuote(host), host, ssl, shellQuote(prefixURL), replicaLevelAwk,
	)

	ctx, cancel := context.WithTimeout(ctx, replicaLevelListTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	output, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("list replica levels: %w", ctx.Err())
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			return nil, fmt.Errorf("list replica levels: %w: %s", err, tailString(strings.TrimSpace(string(exitErr.Stderr)), 512))
		}
		return nil, fmt.Errorf("list replica levels: %w", err)
	}
	return parseReplicaLevels(string(output))
}

// replicaLevelAwk aggregates `s3cmd ls --recursive` lines (DATE TIME SIZE KEY)
// by the four-digit directory that contains each object, emitting
// "<level> <objects> <bytes>" per level.
const replicaLevelAwk = `NF >= 4 { n = split($4, parts, "/"); if (n >= 2 && parts[n-1] ~ /^[0-9][0-9][0-9][0-9]$/) { objects[parts[n-1]]++; bytes[parts[n-1]] += $3 } } END { for (level in objects) printf "%s %d %d\n", level, objects[level], bytes[level] }`

// parseReplicaLevels parses the awk output into levels sorted by level name.
func parseReplicaLevels(output string) ([]replicaLevel, error) {
	var levels []replicaLevel
	for _, line := range nonEmptyLines(output) {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("unexpected replica level line %q", line)
		}
		objects, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, fmt.Errorf("parse object count in %q: %w", line, err)
		}
		bytes, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse bytes in %q: %w", line, err)
		}
		levels = append(levels, replicaLevel{Level: fields[0], Objects: objects, Bytes: bytes})
	}
	slices.SortFunc(levels, func(a, b replicaLevel) int { return strings.Compare(a.Level, b.Level) })
	return levels, nil
}
