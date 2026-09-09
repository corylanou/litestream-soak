package upgrade

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type BuildMetadata struct {
	GoVersion     string `json:"go_version"`
	ModuleVersion string `json:"module_version"`
	Revision      string `json:"revision"`
	Modified      string `json:"modified"`
}

type Binary struct {
	Build  BuildMetadata `json:"build"`
	Path   string        `json:"path"`
	SHA256 string        `json:"sha256"`
}

type Config struct {
	Fixture     string        `json:"fixture,omitempty"`
	Mode        string        `json:"mode"`
	Output      string        `json:"output"`
	Baseline    Binary        `json:"baseline"`
	Candidate   Binary        `json:"candidate"`
	Transition  string        `json:"transition"`
	Rollback    bool          `json:"rollback_supported"`
	AgeTimeout  time.Duration `json:"age_timeout"`
	ContinueFor time.Duration `json:"continue_for"`
}

type Age struct {
	Incidents        int `json:"incidents"`
	Snapshots        int `json:"snapshots"`
	Compactions      int `json:"compactions"`
	RetentionDeleted int `json:"retention_deleted"`
	Updates          int `json:"updates"`
	Deletes          int `json:"deletes"`
}

func (a Age) Ready() bool {
	return a.Snapshots >= 2 && a.Compactions >= 2 && a.RetentionDeleted > 0 && a.Updates > 0 && a.Deletes > 0
}

type Check struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type Result struct {
	Scope              string    `json:"scope"`
	ProviderValidation string    `json:"provider_validation"`
	Config             Config    `json:"config"`
	Age                Age       `json:"age"`
	Checks             []Check   `json:"checks"`
	Status             string    `json:"status"`
	Started            time.Time `json:"started"`
}

func (r Result) Verdict() string {
	if len(r.Checks) == 0 {
		return "inconclusive"
	}
	status := "passed"
	for _, c := range r.Checks {
		if c.Status == "failed" {
			return "failed"
		}
		if c.Status != "passed" {
			status = "inconclusive"
		}
	}
	return status
}

func Run(ctx context.Context, cfg Config) (result Result, runErr error) {
	for _, b := range []*Binary{&cfg.Baseline, &cfg.Candidate} {
		path, err := filepath.Abs(b.Path)
		if err != nil {
			return result, err
		}
		b.Path = path
		if len(b.SHA256) != 64 {
			return result, errors.New("binary requires an explicit SHA-256 pin")
		}
		hash, err := fileHash(b.Path)
		if err != nil {
			return result, err
		}
		if hash != b.SHA256 {
			return result, errors.New("binary SHA-256 does not match pin")
		}
		info, err := buildinfo.ReadFile(b.Path)
		if err != nil {
			return result, fmt.Errorf("read binary build metadata: %w", err)
		}
		b.Build.GoVersion = info.GoVersion
		b.Build.ModuleVersion = info.Main.Version
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				b.Build.Revision = setting.Value
			case "vcs.modified":
				b.Build.Modified = setting.Value
			}
		}
	}
	if cfg.Mode != "fresh-start" && cfg.Mode != "persistent-upgrade" {
		return result, errors.New("mode must be fresh-start or persistent-upgrade")
	}
	if cfg.Fixture != "" && cfg.Mode != "persistent-upgrade" {
		return result, errors.New("fixture reuse requires persistent-upgrade mode")
	}
	if cfg.Output == "" {
		return result, errors.New("output directory is required")
	}
	if cfg.AgeTimeout <= 0 {
		cfg.AgeTimeout = time.Minute
	}
	if cfg.ContinueFor <= 0 {
		cfg.ContinueFor = 5 * time.Second
	}
	output, err := filepath.Abs(cfg.Output)
	if err != nil {
		return result, err
	}
	cfg.Output = output
	if err := os.Mkdir(cfg.Output, 0700); err != nil {
		return result, err
	}
	result = Result{Config: cfg, Started: time.Now().UTC(), Scope: "local-file-replica", ProviderValidation: "unexecuted"}
	for _, name := range []string{"fixture", "pre-baseline", "pre-candidate", "baseline-continuation", "candidate-upgrade", "post-baseline", "post-candidate", "rollback", "post-rollback"} {
		result.Checks = append(result.Checks, Check{Name: name, Status: "unexecuted"})
	}
	defer func() {
		result.Status = result.Verdict()
		data, err := json.MarshalIndent(result, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(cfg.Output, "result.json"), data, 0600)
		}
		runErr = errors.Join(runErr, err)
	}()
	record := func(name string, err error) {
		for i := range result.Checks {
			if result.Checks[i].Name == name {
				result.Checks[i].Status = "passed"
				if err != nil {
					result.Checks[i].Status = "failed"
					result.Checks[i].Error = err.Error()
				}
			}
		}
	}
	if cfg.Transition != "ltx-v1" {
		for i := range result.Checks {
			result.Checks[i].Status = "unsupported"
			result.Checks[i].Error = "only explicitly declared ltx-v1 to ltx-v1 transitions are supported"
		}
		return result, nil
	}
	fixture := filepath.Join(cfg.Output, "fixture")
	var age Age
	if cfg.Fixture != "" {
		age, err = importFixture(cfg.Fixture, fixture, cfg.Baseline.SHA256)
	} else {
		err = os.Mkdir(fixture, 0700)
		if err == nil {
			err = seed(fixture)
		}
		if err == nil {
			limit := cfg.ContinueFor
			if cfg.Mode == "persistent-upgrade" {
				limit = cfg.AgeTimeout
			}
			age, err = exercise(ctx, cfg.Baseline, fixture, filepath.Join(cfg.Output, "fixture.log"), limit, cfg.Mode == "persistent-upgrade", cfg.ContinueFor)
		}
		if logData, readErr := os.ReadFile(filepath.Join(cfg.Output, "fixture.log")); readErr == nil {
			err = errors.Join(err, os.WriteFile(filepath.Join(fixture, "evidence.log"), logData, 0600))
		}
		failure := ""
		if err != nil {
			failure = err.Error()
		}
		err = errors.Join(err, sealFixture(fixture, age, cfg.Baseline.SHA256, failure))
	}
	result.Age = age
	record("fixture", err)
	if err != nil {
		return result, err
	}
	if cfg.Mode == "persistent-upgrade" && !age.Ready() {
		result.Checks[0].Status = "inconclusive"
		result.Checks[0].Error = "fixture did not reach observed maintenance and churn requirements"
		return result, nil
	}
	for _, arm := range []string{"baseline", "candidate"} {
		state := filepath.Join(cfg.Output, arm)
		if cfg.Mode == "persistent-upgrade" {
			_, err = importFixture(fixture, state, cfg.Baseline.SHA256)
		} else {
			err = os.Mkdir(state, 0700)
			if err == nil {
				err = seed(state)
			}
			if err == nil {
				binary := cfg.Baseline
				if arm == "candidate" {
					binary = cfg.Candidate
				}
				_, err = exercise(ctx, binary, state, filepath.Join(cfg.Output, arm+"-fresh.log"), cfg.ContinueFor, false, cfg.ContinueFor)
			}
		}
		if err != nil {
			record("pre-"+arm, err)
			return result, err
		}
	}
	for _, arm := range []string{"baseline", "candidate"} {
		binary := cfg.Baseline
		if arm == "candidate" {
			binary = cfg.Candidate
		}
		state := filepath.Join(cfg.Output, arm)
		record("pre-"+arm, restore(ctx, binary, state, filepath.Join(cfg.Output, "pre-"+arm)))
		_, err = exercise(ctx, binary, state, filepath.Join(cfg.Output, arm+".log"), cfg.ContinueFor, false, cfg.ContinueFor)
		name := "baseline-continuation"
		if arm == "candidate" {
			name = "candidate-upgrade"
		}
		record(name, err)
		record("post-"+arm, restore(ctx, binary, state, filepath.Join(cfg.Output, "post-"+arm)))
	}
	if !cfg.Rollback {
		for i := range result.Checks {
			if result.Checks[i].Name == "rollback" || result.Checks[i].Name == "post-rollback" {
				result.Checks[i].Status = "unsupported"
				result.Checks[i].Error = "rollback support was not explicitly declared"
			}
		}
		return result, nil
	}
	rollback := filepath.Join(cfg.Output, "rollback")
	if err := copyState(filepath.Join(cfg.Output, "candidate"), rollback); err != nil {
		record("rollback", err)
		return result, err
	}
	_, err = exercise(ctx, cfg.Baseline, rollback, filepath.Join(cfg.Output, "rollback.log"), cfg.ContinueFor, false, cfg.ContinueFor)
	record("rollback", err)
	record("post-rollback", restore(ctx, cfg.Baseline, rollback, filepath.Join(cfg.Output, "post-rollback")))
	return result, nil
}

func configFile(state string) (string, error) {
	text := fmt.Sprintf("socket:\n  enabled: false\nlogging:\n  level: info\nsnapshot:\n  interval: 2s\nlevels:\n  - interval: 1s\nl0-retention: 1s\nl0-retention-check-interval: 1s\ndbs:\n  - path: %q\n    replica:\n      path: %q\n      sync-interval: 100ms\n", filepath.Join(state, "db"), filepath.Join(state, "replica"))
	path := state + ".yml"
	return path, os.WriteFile(path, []byte(text), 0600)
}
