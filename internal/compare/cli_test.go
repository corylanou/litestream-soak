package compare

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

func TestResolveReferenceUsesExactCommitEndpoint(t *testing.T) {
	for _, tc := range []struct {
		ref  string
		want []string
	}{{"main", []string{"repos/benbjohnson/litestream/git/ref/heads/main"}}, {"branch:fix/a", []string{"repos/benbjohnson/litestream/git/ref/heads/fix%2Fa"}}, {"latest-release", []string{"repos/benbjohnson/litestream/releases/latest", "repos/benbjohnson/litestream/commits/v1.2.3"}}} {
		t.Run(tc.ref, func(t *testing.T) {
			var calls []string
			sha, err := resolveReference(context.Background(), tc.ref, func(_ context.Context, endpoint string) ([]byte, error) {
				calls = append(calls, endpoint)
				if strings.HasSuffix(endpoint, "latest") {
					return []byte(`{"tag_name":"v1.2.3"}`), nil
				}
				if strings.Contains(endpoint, "/git/ref/") {
					return []byte(`{"object":{"type":"commit","sha":"` + strings.Repeat("a", 40) + `"}}`), nil
				}
				return []byte(`{"sha":"` + strings.Repeat("a", 40) + `"}`), nil
			})
			if err != nil || sha != strings.Repeat("a", 40) || strings.Join(calls, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("sha=%s calls=%v err=%v", sha, calls, err)
			}
		})
	}
}

func TestCLIRejectsImplicitExecution(t *testing.T) {
	var output bytes.Buffer
	if err := CLI(context.Background(), nil, &output); err == nil {
		t.Fatal("ran without explicit mode")
	}
}

func TestCLIRunsRealControlAndWritesReport(t *testing.T) {
	p, l := localFixture(t)
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	localPath := filepath.Join(dir, "local.json")
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(localPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err = cli(context.Background(), []string{"-plan", planPath, "-local", localPath}, &output, &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: l.HarnessSHA}, {Key: "vcs.modified", Value: "false"}}})
	if err == nil {
		t.Fatal("missing candidate binary must return nonzero")
	}
	var report Report
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Verdict != "adverse" || len(report.Observations) != 64 {
		t.Fatalf("verdict=%s observations=%d", report.Verdict, len(report.Observations))
	}
}

func TestCLIInspectsMeasuredLocalContract(t *testing.T) {
	_, l := localFixture(t)
	var output bytes.Buffer
	if err := CLI(context.Background(), []string{"-inspect", l.Fixture}, &output); err != nil {
		t.Fatal(err)
	}
	var contract Contract
	if err := json.Unmarshal(output.Bytes(), &contract); err != nil {
		t.Fatal(err)
	}
	if contract.FixtureBytes <= 0 || contract.ConfigSHA256 != LocalConfigSHA256() || contract.Region != "local" {
		t.Fatalf("contract=%+v", contract)
	}
}

func TestCLIRejectsUnattestedHarnessBeforeExecution(t *testing.T) {
	p, l := localFixture(t)
	dir := t.TempDir()
	planPath, localPath := filepath.Join(dir, "plan.json"), filepath.Join(dir, "local.json")
	if err := writeJSON(planPath, p); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(localPath, l); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := CLI(context.Background(), []string{"-plan", planPath, "-local", localPath}, &output)
	if err == nil || !strings.Contains(err.Error(), "clean harness build provenance") {
		t.Fatalf("accepted unattested harness: %v", err)
	}
	if output.Len() != 0 {
		t.Fatal("executed before provenance validation")
	}
	if _, err := os.Stat(l.Directory); !os.IsNotExist(err) {
		t.Fatalf("created runs before provenance validation: %v", err)
	}
}

func TestHarnessBuildRequiresCleanMatchingRevision(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, tc := range []struct {
		name, revision, modified string
		want                     bool
	}{
		{"clean", sha, "false", true}, {"dirty", sha, "true", false}, {"unknown", sha, "", false}, {"mismatch", strings.Repeat("b", 40), "false", false}, {"missing", "", "false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: tc.revision}, {Key: "vcs.modified", Value: tc.modified}}}
			if err := validateHarnessBuild(info, sha); (err == nil) != tc.want {
				t.Fatalf("err=%v", err)
			}
		})
	}
	if err := validateHarnessBuild(nil, sha); err == nil {
		t.Fatal("accepted absent metadata")
	}
}
