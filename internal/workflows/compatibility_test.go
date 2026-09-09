package workflows

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompatibilityRejectsMutableCandidate(t *testing.T) {
	cmd := exec.Command("bash", filepath.Join("..", "..", "scripts", "test-compatibility.sh"), "main")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "unsupported: candidate must be a full lowercase commit SHA") {
		t.Fatalf("mutable candidate did not fail explicitly: %v: %s", err, output)
	}
}

func TestCompatibilityRunsBeforeFleetNotification(t *testing.T) {
	for _, name := range []string{"deploy-main.yml", "soak-pr.yml"} {
		content := readWorkflow(t, name)
		start := strings.Index(content, "  notify-worker-fleet:")
		if start < 0 {
			t.Fatal("missing notification job")
		}
		job := content[start:]
		check := strings.Index(job, "bash scripts/test-compatibility.sh")
		notify := strings.Index(job, "./scripts/notify-deployment-ready.sh")
		if check < 0 || notify < 0 || check > notify {
			t.Fatalf("%s does not gate notification on actual candidate compatibility", name)
		}
	}
	if !strings.Contains(readWorkflow(t, "ci.yml"), "bash scripts/test-compatibility.sh") {
		t.Fatal("PR CI does not exercise real binaries")
	}
}

func TestCompatibilitySharedComponentDetection(t *testing.T) {
	for _, file := range []string{"scripts/test-compatibility.sh", "internal/workflows/compatibility_test.go"} {
		t.Run(file, func(t *testing.T) {
			dir := t.TempDir()
			before := initDeployRepository(t, dir)
			commitDeployFile(t, dir, file)
			runDeployDetection(t, dir, "push", before, "HEAD", true, true)
		})
	}
}

func TestDeployComponentsCoverProductionDependencies(t *testing.T) {
	const module = "github.com/corylanou/litestream-soak/"
	dependencies := map[string][2]bool{}
	for component, command := range []string{"soakctl", "soakworker"} {
		cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", "./cmd/"+command)
		cmd.Dir = filepath.Join("..", "..")
		cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("production dependencies: %v: %s", err, output)
		}
		for _, dependency := range strings.Fields(string(output)) {
			if !strings.HasPrefix(dependency, module) {
				continue
			}
			packagePath := strings.TrimPrefix(dependency, module)
			targets := dependencies[packagePath]
			targets[component] = true
			dependencies[packagePath] = targets
		}
	}
	dependencies["internal/s3util"] = [2]bool{true, true}
	for packagePath, targets := range dependencies {
		t.Run(packagePath, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			before := initDeployRepository(t, dir)
			commitDeployFile(t, dir, packagePath+"/example.go")
			runDeployDetection(t, dir, "push", before, "HEAD", targets[0], targets[1])
		})
	}
}
