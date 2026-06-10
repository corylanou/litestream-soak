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

func readWorkflow(t *testing.T, file string) string {
	t.Helper()

	path := filepath.Join("..", "..", ".github", "workflows", file)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
