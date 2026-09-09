package compare

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
)

func CLI(ctx context.Context, args []string, output io.Writer) error {
	info, _ := debug.ReadBuildInfo()
	return cli(ctx, args, output, info)
}

func cli(ctx context.Context, args []string, output io.Writer, info *debug.BuildInfo) error {
	flags := flag.NewFlagSet("soakcompare", flag.ContinueOnError)
	planPath := flags.String("plan", "", "experiment plan JSON")
	localPath := flags.String("local", "", "local executor JSON; explicitly executes the entire matrix")
	pin := flags.Bool("pin", false, "resolve references once and emit immutable plan; do not execute")
	inspect := flags.String("inspect", "", "emit measured local contract fields for a fixture; do not execute")
	fixture := flags.String("fixture", "", "create a new deterministic SQLite fixture; do not execute")
	seed := flags.Int64("seed", 42, "fixture seed")
	rows := flags.Int("rows", 1000, "fixture rows")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *inspect != "" {
		if *fixture != "" || *planPath != "" || *localPath != "" || *pin {
			return errors.New("inspection is a separate mode")
		}
		data, err := os.ReadFile(*inspect)
		if err != nil {
			return err
		}
		hardware, err := LocalHardware()
		if err != nil {
			return err
		}
		contract := Contract{Backend: "file", FixtureSHA256: fmt.Sprintf("%x", sha256.Sum256(data)), FixtureBytes: int64(len(data)), ConfigSHA256: LocalConfigSHA256(), Hardware: hardware, Region: "local", Toolchain: runtime.Version(), Seed: *seed}
		if info, ok := debug.ReadBuildInfo(); ok {
			revision, modified := "", ""
			for _, setting := range info.Settings {
				switch setting.Key {
				case "vcs.revision":
					revision = setting.Value
				case "vcs.modified":
					modified = setting.Value
				}
			}
			if modified == "false" {
				contract.GeneratorSHA = revision
				contract.OracleSHA = revision
			}
		}
		return json.NewEncoder(output).Encode(contract)
	}
	if *fixture != "" {
		if *planPath != "" || *localPath != "" || *pin {
			return errors.New("fixture creation is a separate mode")
		}
		return CreateFixture(ctx, *fixture, *seed, *rows)
	}
	if *planPath == "" || (*pin == (*localPath != "")) {
		return errors.New("provide -plan and exactly one of -pin or -local")
	}
	var p Plan
	if err := readJSON(*planPath, &p); err != nil {
		return err
	}
	if *pin {
		pinned, err := Pin(ctx, p, func(ctx context.Context, ref string) (string, error) {
			return resolveReference(ctx, ref, func(ctx context.Context, endpoint string) ([]byte, error) {
				return exec.CommandContext(ctx, "gh", "api", endpoint).Output()
			})
		})
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(pinned)
	}
	var local Local
	if err := readJSON(*localPath, &local); err != nil {
		return err
	}
	if err := validateHarnessBuild(info, local.HarnessSHA); err != nil {
		return err
	}
	report, runErr := Run(ctx, p, local.Execute)
	encodeErr := json.NewEncoder(output).Encode(report)
	if report.Verdict == "adverse" {
		runErr = errors.Join(runErr, errors.New("experiment retained adverse evidence"))
	}
	return errors.Join(runErr, encodeErr)
}

func readJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("expected exactly one JSON document")
	}
	return nil
}

func resolveReference(ctx context.Context, ref string, get func(context.Context, string) ([]byte, error)) (string, error) {
	if ref == "main" || strings.HasPrefix(ref, "branch:") {
		name := strings.TrimPrefix(ref, "branch:")
		if name == "" {
			return "", errors.New("empty branch")
		}
		data, err := get(ctx, "repos/benbjohnson/litestream/git/ref/heads/"+url.PathEscape(name))
		if err != nil {
			return "", err
		}
		var branch struct {
			Object struct {
				SHA  string `json:"sha"`
				Type string `json:"type"`
			} `json:"object"`
		}
		if err := json.Unmarshal(data, &branch); err != nil {
			return "", err
		}
		if branch.Object.Type != "commit" || !validHex(branch.Object.SHA, 40) {
			return "", errors.New("branch did not resolve to a commit")
		}
		return branch.Object.SHA, nil
	}
	if ref == "latest-release" {
		data, err := get(ctx, "repos/benbjohnson/litestream/releases/latest")
		if err != nil {
			return "", err
		}
		var release struct {
			Tag string `json:"tag_name"`
		}
		if err := json.Unmarshal(data, &release); err != nil {
			return "", err
		}
		ref = release.Tag
	}
	ref = strings.TrimPrefix(ref, "branch:")
	if ref == "" {
		return "", errors.New("empty reference")
	}
	data, err := get(ctx, "repos/benbjohnson/litestream/commits/"+url.PathEscape(ref))
	if err != nil {
		return "", err
	}
	var commit struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(data, &commit); err != nil {
		return "", err
	}
	if !validHex(commit.SHA, 40) {
		return "", fmt.Errorf("reference %q did not resolve to a full commit", ref)
	}
	return commit.SHA, nil
}

func validateHarnessBuild(info *debug.BuildInfo, declared string) error {
	revision, modified := "", ""
	if info != nil {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value
			}
		}
	}
	if modified != "false" || !validHex(revision, 40) || revision != declared {
		return errors.New("clean harness build provenance must match declared source SHA")
	}
	return nil
}
