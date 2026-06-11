package workflows

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var workflowFiles = []string{
	"deploy-main.yml",
	"soak-pr.yml",
	"sync-upstream-main.yml",
}

func TestCheckoutActionsUseV5(t *testing.T) {
	t.Parallel()

	for _, file := range workflowFiles {
		t.Run(file, func(t *testing.T) {
			t.Parallel()

			content := readWorkflow(t, file)
			if strings.Contains(content, "actions/checkout@v4") {
				t.Fatalf("%s still references actions/checkout@v4", file)
			}
			if !strings.Contains(content, "actions/checkout@v5") {
				t.Fatalf("%s does not reference actions/checkout@v5", file)
			}
		})
	}
}

func TestSetupGoUsesNode24Action(t *testing.T) {
	t.Parallel()

	content := readWorkflow(t, "deploy-main.yml")
	if strings.Contains(content, "actions/setup-go@v5") {
		t.Fatal("deploy-main.yml still references actions/setup-go@v5")
	}
	if !strings.Contains(content, "actions/setup-go@v6") {
		t.Fatal("deploy-main.yml does not reference actions/setup-go@v6")
	}
}

func TestFlyctlImageRefParsingReportsContext(t *testing.T) {
	t.Parallel()

	for _, file := range workflowFiles {
		t.Run(file, func(t *testing.T) {
			t.Parallel()

			content := readWorkflow(t, file)
			if !strings.Contains(content, "--build-only") {
				return
			}

			checks := []string{
				"^registry[.]fly[.]io/litestream-soak:",
				"expected a line matching: image: registry.fly.io/litestream-soak:",
				"last 40 lines of flyctl output:",
				`tail -n 40 "${log_file}"`,
			}
			for _, check := range checks {
				if !strings.Contains(content, check) {
					t.Fatalf("%s image-ref parsing does not include %q", file, check)
				}
			}
			if strings.Contains(content, `awk '/^image:/{print $2}'`) {
				t.Fatalf("%s still uses unvalidated flyctl image-ref parsing", file)
			}
		})
	}
}

func TestDeployMainDetectsDockerEntrypointChangesForBothImages(t *testing.T) {
	t.Parallel()

	content := readWorkflow(t, "deploy-main.yml")
	tests := []struct {
		name   string
		output string
	}{
		{
			name:   "control image",
			output: "control_changed=true",
		},
		{
			name:   "worker image",
			output: "worker_changed=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pattern := deployMainCasePatternForOutput(t, content, tt.output)
			if !casePatternMatches(pattern, "docker-entrypoint.sh") {
				t.Fatalf("deploy-main.yml %s pattern must match docker-entrypoint.sh, got %q", tt.output, pattern)
			}
		})
	}
}

func readWorkflow(t *testing.T, file string) string {
	t.Helper()

	path := filepath.Join("..", "..", ".github", "workflows", file)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func deployMainCasePatternForOutput(t *testing.T, content, output string) string {
	t.Helper()

	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) != output {
			continue
		}
		for j := i - 1; j >= 0; j-- {
			pattern := strings.TrimSpace(lines[j])
			if strings.HasSuffix(pattern, ")") && strings.Contains(pattern, "|") {
				return strings.TrimSuffix(pattern, ")")
			}
		}
	}
	t.Fatalf("could not find output assignment %s", output)
	return ""
}

func casePatternMatches(pattern, file string) bool {
	for _, candidate := range strings.Split(pattern, "|") {
		if candidate == file {
			return true
		}
		if strings.HasSuffix(candidate, "*") && strings.HasPrefix(file, strings.TrimSuffix(candidate, "*")) {
			return true
		}
	}
	return false
}
