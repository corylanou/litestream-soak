package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

// replicaLevelPoller periodically lists the replica's LTX levels and publishes
// per-level object counts and sizes, so compaction lag (an L0 or L1 backlog
// growing while restores are in flight) is visible in Grafana rather than only
// in a failure debug snapshot.
type replicaLevelPoller struct {
	cfg  *Config
	list func(Config) []reporting.ObjectStorageLevelSnapshot
}

func newReplicaLevelPoller(cfg *Config) *replicaLevelPoller {
	return &replicaLevelPoller{cfg: cfg, list: listReplicaLevels}
}

func (p *replicaLevelPoller) enabled() bool {
	return p.cfg.ReplicaType == "s3" && p.cfg.S3Bucket != "" && p.cfg.ReplicaLevelPollInterval > 0
}

func (p *replicaLevelPoller) Run(ctx context.Context) {
	if !p.enabled() {
		return
	}
	p.poll()
	ticker := time.NewTicker(p.cfg.ReplicaLevelPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll()
		}
	}
}

func (p *replicaLevelPoller) poll() {
	levels := p.list(*p.cfg)
	for _, level := range levels {
		if level.Error != "" {
			slog.Debug("Replica level listing failed", "level", level.Level, "error", level.Error)
		}
	}
	SetReplicaLevelStats(levels)
}

func listReplicaLevels(cfg Config) []reporting.ObjectStorageLevelSnapshot {
	return collectObjectStoragePrefix(cfg).LevelListings
}
