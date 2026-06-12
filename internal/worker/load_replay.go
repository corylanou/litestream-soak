package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/replay"
)

type loadReplayManager struct {
	cfg *Config

	loadSup      *loadSupervisor
	replayEngine *replay.Engine
	manyDBLoad   *manyDBLoad
	loadLog      *lineBuffer
}

func newLoadReplayManager(cfg *Config) loadReplayManager {
	return loadReplayManager{
		cfg:     cfg,
		loadLog: newLineBuffer(120),
	}
}

func (r *Runner) startLoad(ctx context.Context) error {
	if err := r.loadReplayManager.startLoad(ctx); err != nil {
		return err
	}
	r.sendHeartbeat(ctx)
	return nil
}

func (r *Runner) startManyDBLoad(ctx context.Context) error {
	if err := r.loadReplayManager.startManyDBLoad(ctx); err != nil {
		return err
	}
	r.sendHeartbeat(ctx)
	return nil
}

func (m *loadReplayManager) startManyDBLoad(ctx context.Context) error {
	load := newManyDBLoad(m.cfg)
	if err := load.Start(ctx); err != nil {
		return err
	}
	m.manyDBLoad = load
	return nil
}

func (m *loadReplayManager) startLoad(ctx context.Context) error {
	slog.Info("Starting load generator",
		"profile", m.cfg.ProfileName,
		"write_rate", m.cfg.WriteRate,
		"pattern", m.cfg.Pattern,
	)

	m.loadSup = newLoadSupervisor(func(ctx context.Context) (*exec.Cmd, error) {
		cmd := exec.CommandContext(ctx, "litestream-test", "load",
			"-db", m.cfg.DBPath,
			"-write-rate", strconv.Itoa(m.cfg.WriteRate),
			"-duration", m.cfg.LoadDuration.String(),
			"-pattern", m.cfg.Pattern,
			"-payload-size", strconv.Itoa(m.cfg.PayloadSize),
			"-read-ratio", fmt.Sprintf("%.2f", m.cfg.ReadRatio),
			"-workers", strconv.Itoa(m.cfg.Workers),
		)
		cmd.Stdout = io.MultiWriter(os.Stdout, m.loadLog)
		cmd.Stderr = io.MultiWriter(os.Stderr, m.loadLog)

		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("start load: %w", err)
		}
		return cmd, nil
	})

	return m.loadSup.start(ctx)
}

func (r *Runner) startReplay(ctx context.Context) error {
	return r.loadReplayManager.startReplay(ctx)
}

func (m *loadReplayManager) startReplay(ctx context.Context) error {
	if m.cfg.ReplayDataset == "" {
		return fmt.Errorf("REPLAY_DATASET is required for replay mode")
	}
	if err := m.prepareReplayData(ctx); err != nil {
		return fmt.Errorf("prepare replay data: %w", err)
	}
	if m.cfg.ReplayDataPath == "" {
		return fmt.Errorf("REPLAY_DATA_PATH or REPLAY_DATA_URL is required for replay mode")
	}

	var adapter replay.Adapter
	switch m.cfg.ReplayDataset {
	case "taxi":
		adapter = replay.NewTaxiAdapter(m.cfg.ReplayDataPath)
	case "gharchive":
		adapter = replay.NewGHArchiveAdapter(m.cfg.ReplayDataPath)
	case "orders":
		adapter = replay.NewOrdersAdapter(m.cfg.ReplayDataPath)
	default:
		return fmt.Errorf("unknown replay dataset: %s", m.cfg.ReplayDataset)
	}

	engine := replay.NewEngine(replay.Config{
		Dataset:         m.cfg.ReplayDataset,
		DataPath:        m.cfg.ReplayDataPath,
		DBPath:          m.cfg.DBPath,
		SpeedMultiplier: m.cfg.ReplaySpeed,
		Loop:            m.cfg.ReplayLoop,
		WorkerID:        m.cfg.WorkerID,
		ProfileName:     m.cfg.ProfileName,
		Source:          m.cfg.Source,
	}, adapter)

	m.replayEngine = engine

	go superviseReplay(ctx, m.cfg.ReplayDataset, m.cfg.ReplayLoop,
		5*time.Second, 2*time.Minute, 5*time.Minute, engine.Run)

	slog.Info("Replay engine started", "dataset", m.cfg.ReplayDataset, "speed", m.cfg.ReplaySpeed)
	return nil
}

func superviseReplay(ctx context.Context, dataset string, loop bool, initialBackoff, maxBackoff, healthyReset time.Duration, run func(context.Context) error) {
	backoff := initialBackoff
	for {
		started := time.Now()
		err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil && !loop {
			slog.Info("Replay complete", "dataset", dataset)
			return
		}
		if err != nil {
			slog.Error("Replay engine failed", "dataset", dataset, "error", err)
		} else {
			slog.Warn("Replay engine exited unexpectedly", "dataset", dataset)
		}
		if time.Since(started) >= healthyReset {
			backoff = initialBackoff
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		IncLoadRestart("replay")
		slog.Info("Restarting replay engine", "dataset", dataset)
	}
}

func (m *loadReplayManager) prepareReplayData(ctx context.Context) error {
	if strings.TrimSpace(m.cfg.ReplayDataPath) != "" {
		if _, err := os.Stat(m.cfg.ReplayDataPath); err == nil {
			return nil
		}
		if strings.TrimSpace(m.cfg.ReplayDataURL) == "" {
			return fmt.Errorf("replay data path %s not found", m.cfg.ReplayDataPath)
		}
	}
	if strings.TrimSpace(m.cfg.ReplayDataURL) == "" {
		return nil
	}

	dataURL, err := url.Parse(m.cfg.ReplayDataURL)
	if err != nil {
		return fmt.Errorf("parse replay data url: %w", err)
	}

	name := filepath.Base(dataURL.Path)
	if name == "." || name == "/" || name == "" {
		return fmt.Errorf("replay data url must include a file name")
	}

	targetDir := filepath.Join(m.cfg.DataDir, "datasets")
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create replay dataset dir: %w", err)
	}

	targetPath := filepath.Join(targetDir, name)
	if _, err := os.Stat(targetPath); err == nil {
		m.cfg.ReplayDataPath = targetPath
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.ReplayDataURL, nil)
	if err != nil {
		return fmt.Errorf("create replay data request: %w", err)
	}

	slog.Info("Downloading replay dataset", "dataset", m.cfg.ReplayDataset, "url", m.cfg.ReplayDataURL, "target", targetPath)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download replay data: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download replay data returned %d", resp.StatusCode)
	}

	tmpPath := targetPath + ".download"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create replay dataset file: %w", err)
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write replay dataset: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close replay dataset: %w", err)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("activate replay dataset: %w", err)
	}

	m.cfg.ReplayDataPath = targetPath
	return nil
}

func (r *Runner) stopLoad() {
	r.loadReplayManager.stopLoad()
}

func (m *loadReplayManager) stopLoad() {
	if m.loadSup == nil {
		if m.manyDBLoad != nil {
			slog.Info("Stopping many database load")
			m.manyDBLoad.Stop()
		}
		return
	}
	slog.Info("Stopping load generator")
	m.loadSup.Stop()
}
