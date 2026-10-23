package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	StressScenarioOverloadTruncate0 = "overload-truncate0"
	StressScenarioPinnedReader      = "pinned-reader"
)

var errStressScenarioPhaseEnded = errors.New("stress scenario phase ended")

func knownStressScenario(name string) bool {
	return name == StressScenarioOverloadTruncate0 || name == StressScenarioPinnedReader
}

func parseStressScenarios(raw string) ([]string, error) {
	var scenarios []string
	seen := map[string]bool{}
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if !knownStressScenario(name) {
			return nil, fmt.Errorf("unknown stress scenario %q", name)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate stress scenario %q", name)
		}
		seen[name] = true
		scenarios = append(scenarios, name)
	}
	return scenarios, nil
}

func (c Config) StressScenario() string {
	return c.stressScenario
}

func (c Config) ForStressScenario(name string) (Config, error) {
	if !knownStressScenario(name) {
		return Config{}, fmt.Errorf("unknown stress scenario %q", name)
	}
	base := c
	phase := c
	phase.stressBase = &base
	phase.stressScenario = name
	phase.DataDir = filepath.Join(c.DataDir, "scenarios", name)
	phase.DBPath = filepath.Join(phase.DataDir, filepath.Base(c.DBPath))
	phase.ReplicaPath = filepath.Join(c.ReplicaPath, "scenarios", name)
	phase.S3Path = path.Join(strings.Trim(c.S3Path, "/"), "scenarios", name)
	phase.PayloadSize = 2048
	phase.ReadRatio = 0.1
	switch name {
	case StressScenarioOverloadTruncate0:
		zero := 0
		phase.WriteRate = 600
		phase.Workers = 8
		phase.TruncatePageN = &zero
		phase.PinnedReaderHold = 0
		phase.PinnedReaderPause = 0
	case StressScenarioPinnedReader:
		phase.WriteRate = 200
		phase.Workers = 4
		phase.TruncatePageN = nil
		phase.PinnedReaderHold = 4 * time.Minute
		phase.PinnedReaderPause = 45 * time.Second
	}
	return phase, nil
}

func stressScenarioAt(now time.Time, count int, phase time.Duration) (int, time.Time) {
	slot := stressScenarioSlot(now, phase)
	return stressScenarioSlotIndex(slot, count), stressScenarioSlotEnd(slot, phase)
}

func stressScenarioSlot(now time.Time, phase time.Duration) int64 {
	return now.UnixNano() / int64(phase)
}

func stressScenarioSlotIndex(slot int64, count int) int {
	return int(slot % int64(count))
}

func stressScenarioSlotEnd(slot int64, phase time.Duration) time.Time {
	return time.Unix(0, (slot+1)*int64(phase)).UTC()
}

func RunStressScenarioSchedule(ctx context.Context, cfg Config, run func(context.Context, Config) error) error {
	return runStressScenarioSchedule(ctx, cfg, time.Now, run)
}

func runStressScenarioSchedule(ctx context.Context, cfg Config, clock func() time.Time, run func(context.Context, Config) error) error {
	if len(cfg.StressScenarios) == 0 || cfg.StressScenarioPhase <= 0 {
		return errors.New("stress scenario schedule is not configured")
	}
	previous := int64(-1)
	for {
		if ctx.Err() != nil {
			return nil
		}
		now := clock()
		slot := max(stressScenarioSlot(now, cfg.StressScenarioPhase), previous+1)
		previous = slot
		index, end := stressScenarioSlotIndex(slot, len(cfg.StressScenarios)), stressScenarioSlotEnd(slot, cfg.StressScenarioPhase)
		name := cfg.StressScenarios[index]
		phase, err := cfg.ForStressScenario(name)
		if err != nil {
			return err
		}
		slog.Info("Starting stress scenario phase", "scenario", name, "until", end)
		phaseCtx, cancel := context.WithCancelCause(ctx)
		timer := time.AfterFunc(end.Sub(now), func() { cancel(errStressScenarioPhaseEnded) })
		err = run(phaseCtx, phase)
		timer.Stop()
		ended := errors.Is(context.Cause(phaseCtx), errStressScenarioPhaseEnded)
		cancel(nil)
		switch {
		case ctx.Err() != nil:
			return nil
		case ended:
			slog.Info("Stress scenario phase ended", "scenario", name)
		case err != nil:
			return fmt.Errorf("stress scenario %s: %w", name, err)
		default:
			return nil
		}
	}
}
