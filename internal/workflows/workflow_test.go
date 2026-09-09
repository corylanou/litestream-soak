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

	for _, file := range []string{"deploy-main.yml", "soak-pr.yml"} {
		content := readWorkflow(t, file)
		if strings.Contains(content, "superfly/flyctl-actions/setup-flyctl@") {
			t.Fatalf("%s bypasses the maintained operational build", file)
		}
		if !strings.Contains(content, "uses: ./.github/actions/setup-flyctl") {
			t.Fatalf("%s does not use the maintained operational build", file)
		}
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
		"BEFORE_SHA: ${{ steps.baseline.outputs.sha }}",
		"AFTER_SHA: ${{ steps.target.outputs.sha }}",
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

func TestDeployMainRetainsSupersededChanges(t *testing.T) {
	t.Parallel()
	for _, first := range []string{"cmd/soakctl/main.go", "cmd/soakworker/main.go"} {
		t.Run(first, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			baseline := initDeployRepository(t, dir)
			commitDeployFile(t, dir, first)
			before := deployGit(t, dir, "rev-parse", "HEAD")
			second := "cmd/soakctl/main.go"
			if first == second {
				second = "cmd/soakworker/main.go"
			}
			commitDeployFile(t, dir, second)
			selected := runDeployBaseline(t, dir, baseline, "HEAD")
			if selected == "" {
				t.Fatal("missing successful deployment baseline")
			}
			if selected == before {
				t.Fatal("selected cancelled predecessor")
			}
			runDeployDetection(t, dir, "push", selected, "HEAD", true, true)
		})
	}
}

func runDeployBaseline(t *testing.T, dir, successful, after string) string {
	t.Helper()
	workflow := readWorkflow(t, "deploy-main.yml")
	_, block, ok := strings.Cut(workflow, "      - id: baseline\n")
	if !ok {
		t.Fatal("missing successful deployment baseline lookup")
	}
	_, block, ok = strings.Cut(block, "        run: |\n")
	if !ok {
		t.Fatal("missing baseline script")
	}
	var lines []string
	for _, line := range strings.Split(block, "\n") {
		if line != "" && !strings.HasPrefix(line, "          ") {
			break
		}
		lines = append(lines, strings.TrimPrefix(line, "          "))
	}
	bin := t.TempDir()
	mock := `#!/bin/bash
if [[ "$*" == *'actions/workflows/deploy-main.yml/runs?branch=main&status=success&per_page=100'* ]]; then
  [[ "$*" == *'select(.event == "push" or .event == "workflow_dispatch")'* ]] || exit 2
  [[ "$SUCCESSFUL_SHA" != error ]] || exit 1
  echo 1
elif [[ "$*" == *'actions/runs/1/artifacts?per_page=100'* ]]; then
  [[ "$*" == *'select(.expired == false)'* ]] || exit 2
  [[ "$*" == *'main-deployment-checkpoint-v1-'* ]] || exit 2
  printf '%s\n' "$SUCCESSFUL_SHA"
else
  exit 2
fi
`
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(mock), 0700); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(t.TempDir(), "output")
	cmd := exec.Command("bash", "-c", strings.Join(lines, "\n"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "SUCCESSFUL_SHA="+successful, "AFTER_SHA="+after, "GITHUB_REPOSITORY=example/repo", "GITHUB_OUTPUT="+outputPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("baseline: %v\n%s", err, output)
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(strings.TrimSpace(string(data)), "sha=")
}

func TestDeployMainBaselineRecovery(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"partial failure", "cancelled", "queued replacement", "legacy success without evidence", "no history", "history unavailable", "non-ancestor", "missing revision"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			baseline := initDeployRepository(t, dir)
			commitDeployFile(t, dir, "cmd/soakctl/main.go")
			commitDeployFile(t, dir, "cmd/soakworker/main.go")
			commitDeployFile(t, dir, "README.md")
			successful := baseline
			switch scenario {
			case "no history", "legacy success without evidence":
				successful = ""
			case "history unavailable":
				successful = "error"
			case "non-ancestor":
				target := deployGit(t, dir, "rev-parse", "HEAD")
				deployGit(t, dir, "checkout", "--orphan", "unrelated")
				deployGit(t, dir, "-c", "user.name=Workflow Test", "-c", "user.email=test@example.com", "commit", "-qm", "Unrelated")
				successful = deployGit(t, dir, "rev-parse", "HEAD")
				deployGit(t, dir, "checkout", target)
			case "missing revision":
				successful = strings.Repeat("a", 40)
			}
			selected := runDeployBaseline(t, dir, successful, "HEAD")
			if scenario == "partial failure" || scenario == "cancelled" || scenario == "queued replacement" {
				if selected != baseline {
					t.Fatalf("baseline = %q, want %q", selected, baseline)
				}
			} else if selected != "" {
				t.Fatalf("unusable history selected %q", selected)
			}
			runDeployDetection(t, dir, "push", selected, "HEAD", true, true)
		})
	}
}

func TestDeployMainBaselineAdvancesAfterSuccess(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	old := initDeployRepository(t, dir)
	commitDeployFile(t, dir, "cmd/soakctl/main.go")
	deployed := deployGit(t, dir, "rev-parse", "HEAD")
	commitDeployFile(t, dir, "cmd/soakworker/main.go")
	selected := runDeployBaseline(t, dir, deployed+"\n"+old, "HEAD")
	if selected != deployed {
		t.Fatalf("baseline = %q, want latest successful ancestor %q", selected, deployed)
	}
	runDeployDetection(t, dir, "push", selected, "HEAD", false, true)
}

func TestDeployMainPublishesComponentEvidence(t *testing.T) {
	t.Parallel()
	workflow := readWorkflow(t, "deploy-main.yml")
	for _, required := range []string{
		"actions: read",
		"--image-label \"control-${TARGET_SHA}\"",
		"### Worker deployment request accepted",
		"Image: ${IMAGE_REF}",
		"Litestream revision: ${LITESTREAM_SHA}",
		"not evidence of fleet convergence",
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("missing component evidence %q", required)
		}
	}
}

func TestDeployMainSerializesAllMainWriters(t *testing.T) {
	t.Parallel()
	main := readWorkflow(t, "deploy-main.yml")
	for _, required := range []string{"cancel-in-progress: false", "ref: main", "target_sha: ${{ steps.target.outputs.sha }}", "main-deployment-checkpoint-v1-", "flyctl machine list", "actions/upload-artifact@v4"} {
		if !strings.Contains(main, required) {
			t.Fatalf("missing serialized deployment contract %q", required)
		}
	}
	sync := readWorkflow(t, "sync-upstream-main.yml")
	if strings.Contains(sync, "flyctl deploy") || strings.Contains(sync, "./scripts/notify-deployment-ready.sh") {
		t.Fatal("upstream sync must not independently publish a main worker image")
	}
	if !strings.Contains(sync, "gh workflow run deploy-main.yml --ref main") {
		t.Fatal("upstream sync must submit through the main deployment queue")
	}
}

func TestDeploymentSnapshotUsesActualImagesWithoutSecrets(t *testing.T) {
	t.Parallel()
	workflow := readWorkflow(t, "deploy-main.yml")
	_, block, ok := strings.Cut(workflow, "      - name: Record actual component artifacts and health\n")
	if !ok {
		t.Fatal("missing snapshot step")
	}
	_, block, ok = strings.Cut(block, "        run: |\n")
	if !ok {
		t.Fatal("missing snapshot script")
	}
	var lines []string
	for _, line := range strings.Split(block, "\n") {
		if line != "" && !strings.HasPrefix(line, "          ") {
			break
		}
		lines = append(lines, strings.TrimPrefix(line, "          "))
	}
	dir := t.TempDir()
	mock := `#!/bin/bash
printf '%s\n' '[{"id":"machine-example","state":"started","region":"ord","image_ref":{"digest":"sha256:actual-digest"},"config":{"image":"registry.fly.io/example:actual-version","env":{"SECRET":"must-not-publish"}},"checks":[{"name":"health","status":"passing","output":"must-not-publish"}]}]'
`
	if err := os.WriteFile(filepath.Join(dir, "flyctl"), []byte(mock), 0700); err != nil {
		t.Fatal(err)
	}
	summary := filepath.Join(dir, "summary")
	cmd := exec.Command("bash", "-c", strings.Join(lines, "\n"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "TARGET_SHA=newest-repository-revision", "WORKER_IMAGE=accepted-worker-image", "LITESTREAM_SHA=pinned-upstream", "GITHUB_STEP_SUMMARY="+summary)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("snapshot: %v\n%s", err, output)
	}
	for _, file := range []string{"summary", "evidence/control.json", "evidence/worker.json"} {
		data, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}
		for _, actual := range []string{"actual-digest", "actual-version", "passing"} {
			if !strings.Contains(string(data), actual) {
				t.Fatalf("%s missing %q: %s", file, actual, data)
			}
		}
		if strings.Contains(string(data), "must-not-publish") {
			t.Fatalf("%s exposed private fields", file)
		}
	}
}

func TestQueuedPushAndUpstreamSyncRetainBothComponents(t *testing.T) {
	t.Parallel()
	for _, lastEvent := range []string{"push", "workflow_dispatch"} {
		t.Run(lastEvent, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			baseline := initDeployRepository(t, dir)
			commitDeployFile(t, dir, "cmd/soakctl/main.go")
			commitDeployFile(t, dir, "cmd/soakworker/main.go")
			selected := runDeployBaseline(t, dir, baseline, "HEAD")
			runDeployDetection(t, dir, lastEvent, selected, "HEAD", true, true)
			workflow := readWorkflow(t, "deploy-main.yml")
			if strings.Count(workflow, "ref: ${{ needs.changes.outputs.target_sha }}") != 5 {
				t.Fatal("all verifying, building, and publishing checkouts must use the selected current main revision")
			}
			if strings.Contains(workflow, "${GITHUB_SHA}") || strings.Contains(workflow, "${GITHUB_SHA::") {
				t.Fatal("queued event revision must not label current component artifacts")
			}
		})
	}
}

func TestFlyBuildProvenanceUsesSelectedTarget(t *testing.T) {
	t.Parallel()
	for _, step := range []string{"      - env:\n          FLY_API_TOKEN:", "      - id: build\n"} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()
			workflow := readWorkflow(t, "deploy-main.yml")
			_, block, ok := strings.Cut(workflow, step)
			if !ok {
				t.Fatal("missing Fly step")
			}
			_, block, ok = strings.Cut(block, "        run: |\n")
			if !ok {
				t.Fatal("missing Fly script")
			}
			var lines []string
			for _, line := range strings.Split(block, "\n") {
				if line != "" && !strings.HasPrefix(line, "          ") {
					break
				}
				lines = append(lines, strings.TrimPrefix(line, "          "))
			}
			dir := t.TempDir()
			mock := `#!/bin/bash
printf '%s\n' "$GITHUB_SHA" > "$PROVENANCE_OUTPUT"
printf 'image: registry.fly.io/litestream-soak:sha-%s-ls-%s\n' "${TARGET_SHA:0:12}" "${SOAK_LITESTREAM_SHA:0:12}"
`
			if err := os.WriteFile(filepath.Join(dir, "flyctl"), []byte(mock), 0700); err != nil {
				t.Fatal(err)
			}
			target := strings.Repeat("b", 40)
			provenance := filepath.Join(dir, "provenance")
			cmd := exec.Command("bash", "-c", strings.Join(lines, "\n"))
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "GITHUB_SHA="+strings.Repeat("a", 40), "TARGET_SHA="+target, "SOAK_LITESTREAM_SHA="+strings.Repeat("c", 40), "PROVENANCE_OUTPUT="+provenance, "GITHUB_STEP_SUMMARY="+filepath.Join(dir, "summary"), "GITHUB_OUTPUT="+filepath.Join(dir, "outputs"))
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("Fly script: %v\n%s", err, output)
			}
			data, err := os.ReadFile(provenance)
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(string(data)) != target {
				t.Fatalf("Fly GH_SHA provenance = %q, want selected target %q", data, target)
			}
		})
	}
}
