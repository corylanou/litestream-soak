package workflows

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

func TestFlyctlActionUsesImmutableRef(t *testing.T) {
	t.Parallel()

	const pinnedRef = "superfly/flyctl-actions/setup-flyctl@ed8efb33836e8b2096c7fd3ba1c8afe303ebbff1"
	for _, file := range workflowFiles {
		t.Run(file, func(t *testing.T) {
			t.Parallel()

			content := readWorkflow(t, file)
			if strings.Contains(content, "superfly/flyctl-actions/setup-flyctl@master") {
				t.Fatalf("%s uses the mutable setup-flyctl master branch", file)
			}
			if strings.Contains(content, "superfly/flyctl-actions/setup-flyctl@") && !strings.Contains(content, pinnedRef) {
				t.Fatalf("%s does not pin setup-flyctl to %s", file, pinnedRef)
			}
		})
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

func TestSoakPRDoesNotInterpolateDispatchInputsIntoShell(t *testing.T) {
	t.Parallel()

	content := readWorkflow(t, "soak-pr.yml")
	for _, unsafe := range []string{
		`pr_number="${{ github.event.client_payload`,
		`repo_full_name="${{ github.event.client_payload`,
		`pr_sha="${{ github.event.client_payload`,
		`actor_login="${{ github.event.client_payload`,
		`requested_label="${{ github.event.client_payload`,
	} {
		if strings.Contains(content, unsafe) {
			t.Fatalf("soak-pr.yml interpolates dispatch input into shell with %q", unsafe)
		}
	}
	for _, required := range []string{
		`PR_NUMBER: ${{ github.event.client_payload.pr_number || github.event.inputs.pr_number }}`,
		`REPO_FULL_NAME: ${{ github.event.client_payload.repo_full_name || github.event.inputs.repo_full_name || 'benbjohnson/litestream' }}`,
		`REPO_FULL_NAME: ${{ needs.resolve-request.outputs.repo_full_name }}`,
		`PR_SHA: ${{ github.event.client_payload.pr_sha || github.event.inputs.pr_sha || '' }}`,
		`if [[ ! "${pr_number}" =~ ^[0-9]+$ ]]; then`,
		`if [[ ! "${pr_sha}" =~ ^[0-9a-fA-F]{40}$ ]]; then`,
	} {
		if !strings.Contains(content, required) {
			t.Fatalf("soak-pr.yml is missing input hardening %q", required)
		}
	}
}

func TestNotifyDeploymentReadyRequiresImageAndLitestreamSHA(t *testing.T) {
	t.Parallel()

	script := filepath.Join("..", "..", "scripts", "notify-deployment-ready.sh")
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "missing image and litestream SHA",
			args: []string{"abc123"},
		},
		{
			name: "missing image",
			args: []string{"abc123", "main", "manual", "", "litestream123"},
		},
		{
			name: "missing litestream SHA",
			args: []string{"abc123", "main", "manual", "registry.fly.io/example:image", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			output, err := exec.Command(script, tt.args...).CombinedOutput()
			if err == nil {
				t.Fatal("notify-deployment-ready.sh succeeded with incomplete deployment metadata")
			}
			for _, expected := range []string{
				"sha, image-ref, and litestream-sha are required",
				"usage:",
				"<image-ref> <litestream-sha>",
			} {
				if !strings.Contains(string(output), expected) {
					t.Fatalf("output %q does not contain %q", output, expected)
				}
			}
		})
	}
}

func TestNotifyDeploymentReadyRequiresRepositoryForPRSource(t *testing.T) {
	t.Parallel()

	script := filepath.Join("..", "..", "scripts", "notify-deployment-ready.sh")
	output, err := exec.Command(
		script,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"pr-177",
		"manual",
		"registry.fly.io/example:image",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	).CombinedOutput()
	if err == nil {
		t.Fatal("notify-deployment-ready.sh succeeded without PR repository")
	}
	if !strings.Contains(string(output), "repository in owner/name format is required for PR sources") {
		t.Fatalf("output %q does not contain repository requirement", output)
	}
}

func TestSoakPRPlumbsRepositoryToDeploymentNotification(t *testing.T) {
	t.Parallel()

	workflow := readWorkflow(t, "soak-pr.yml")
	for _, required := range []string{
		`REPO_FULL_NAME: ${{ needs.resolve-request.outputs.repo_full_name }}`,
		`"${PR_SHA}" \`,
		`"${REPO_FULL_NAME}"`,
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("soak-pr.yml is missing repository notification plumbing %q", required)
		}
	}

	scriptPath := filepath.Join("..", "..", "scripts", "notify-deployment-ready.sh")
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("os.ReadFile(%s) error = %v", scriptPath, err)
	}
	for _, required := range []string{
		`repository="${6:-}"`,
		`--arg repository "$repository"`,
		`repository: $repository`,
	} {
		if !strings.Contains(string(script), required) {
			t.Fatalf("notify-deployment-ready.sh is missing repository plumbing %q", required)
		}
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

func TestDeployMainAffectedComponents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		file    string
		control bool
		worker  bool
	}{
		{"internal/reporting/types.go", true, true},
		{"internal/workload/config.go", true, true},
		{"internal/s3util/delete.go", true, false},
		{"internal/model/db.go", true, false},
		{"internal/flyapi/client.go", true, false},
		{"cmd/soakctl/main.go", true, false},
		{"internal/orchestrator/ui/index.html", true, false},
		{"cmd/soakworker/main.go", false, true},
		{"internal/worker/runner.go", false, true},
		{"internal/replay/engine.go", false, true},
		{"datasets/example.csv", false, true},
		{"Dockerfile.control", true, false},
		{"Dockerfile.worker", false, true},
		{"fly.control.toml", true, false},
		{"fly.toml", false, true},
		{"docker-entrypoint.sh", true, true},
		{".dockerignore", true, true},
		{"go.mod", true, true},
		{"go.sum", true, true},
		{".github/workflows/deploy-main.yml", true, true},
		{"scripts/notify-deployment-ready.sh", false, true},
		{"README.md", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			before := initDeployRepository(t, dir)
			commitDeployFile(t, dir, tt.file)
			runDeployDetection(t, dir, "push", before, "HEAD", tt.control, tt.worker)
		})
	}
}

func TestDeployMainRevisionRange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		event   string
		before  string
		after   string
		control bool
		worker  bool
	}{
		{"multi-commit push", "push", "", "HEAD", true, true},
		{"manual dispatch", "workflow_dispatch", "HEAD", "HEAD", true, true},
		{"new branch", "push", strings.Repeat("0", 40), "HEAD", true, true},
		{"unavailable before", "push", strings.Repeat("a", 40), "HEAD", true, true},
		{"unavailable after", "push", "", strings.Repeat("a", 40), true, true},
		{"empty diff", "push", "HEAD", "HEAD", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			before := initDeployRepository(t, dir)
			commitDeployFile(t, dir, "cmd/soakctl/main.go")
			commitDeployFile(t, dir, "cmd/soakworker/main.go")
			commitDeployFile(t, dir, "README.md")
			if tt.before != "" {
				before = tt.before
			}
			runDeployDetection(t, dir, tt.event, before, tt.after, tt.control, tt.worker)
		})
	}
}

func initDeployRepository(t *testing.T, dir string) string {
	t.Helper()
	deployGit(t, dir, "init", "-q")
	deployGit(t, dir, "-c", "user.name=Workflow Test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-qm", "Initial")
	return deployGit(t, dir, "rev-parse", "HEAD")
}

func commitDeployFile(t *testing.T, dir, file string) {
	t.Helper()
	path := filepath.Join(dir, file)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("example\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deployGit(t, dir, "add", "--", file)
	deployGit(t, dir, "-c", "user.name=Workflow Test", "-c", "user.email=test@example.com", "commit", "-qm", "Change")
}

func deployGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func runDeployDetection(t *testing.T, dir, event, before, after string, control, worker bool) {
	t.Helper()
	workflow := readWorkflow(t, "deploy-main.yml")
	_, block, ok := strings.Cut(workflow, "      - id: detect\n")
	if !ok {
		t.Fatal("missing detection step")
	}
	_, block, ok = strings.Cut(block, "        run: |\n")
	if !ok {
		t.Fatal("missing detection script")
	}
	var lines []string
	for _, line := range strings.Split(block, "\n") {
		if line != "" && !strings.HasPrefix(line, "          ") {
			break
		}
		lines = append(lines, strings.TrimPrefix(line, "          "))
	}
	script := strings.NewReplacer("${{ github.event_name }}", event, "${{ github.event.before }}", before, "${{ github.sha }}", after).Replace(strings.Join(lines, "\n"))
	outputPath := filepath.Join(t.TempDir(), "outputs")
	cmd := exec.Command("bash", "-c", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GITHUB_OUTPUT="+outputPath, "EVENT_NAME="+event, "BEFORE_SHA="+before, "AFTER_SHA="+after)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("detect: %v\n%s", err, output)
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	expected := "control_changed=" + strconv.FormatBool(control) + "\nworker_changed=" + strconv.FormatBool(worker) + "\n"
	if string(data) != expected {
		t.Fatalf("outputs = %q, want %q\n%s", data, expected, output)
	}
	if event == "push" && (before == strings.Repeat("a", 40) || after == strings.Repeat("a", 40)) && !strings.Contains(string(output), "::warning::") {
		t.Fatalf("unavailable revision did not emit a warning: %s", output)
	}
}

func TestDeployMainCheckoutIncludesPushBase(t *testing.T) {
	t.Parallel()
	workflow := readWorkflow(t, "deploy-main.yml")
	changes, _, ok := strings.Cut(workflow, "  verify:\n")
	if !ok || !strings.Contains(changes, "fetch-depth: 0") {
		t.Fatal("change detection must fetch full history for multi-commit pushes")
	}
	for _, binding := range []string{
		"EVENT_NAME: ${{ github.event_name }}",
		"BEFORE_SHA: ${{ github.event.before }}",
		"AFTER_SHA: ${{ github.sha }}",
	} {
		if !strings.Contains(changes, binding) {
			t.Fatalf("missing detector input binding %q", binding)
		}
	}
}

func TestDeployMainRenamedAndDeletedInputs(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"rename", "delete"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			initDeployRepository(t, dir)
			commitDeployFile(t, dir, "internal/reporting/types.go")
			before := deployGit(t, dir, "rev-parse", "HEAD")
			if operation == "rename" {
				deployGit(t, dir, "mv", "internal/reporting/types.go", "archived.txt")
			} else {
				deployGit(t, dir, "rm", "internal/reporting/types.go")
			}
			deployGit(t, dir, "-c", "user.name=Workflow Test", "-c", "user.email=test@example.com", "commit", "-qm", "Remove input")
			runDeployDetection(t, dir, "push", before, "HEAD", true, true)
		})
	}
}
