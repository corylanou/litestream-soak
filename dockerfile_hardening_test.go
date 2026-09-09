package litestreamsoak

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var dockerfiles = []string{
	"Dockerfile.control",
	"Dockerfile.worker",
}

var dockerfileRuntimes = []struct {
	path      string
	command   string
	dataDBEnv string
}{
	{
		path:      "Dockerfile.control",
		command:   `CMD ["/usr/local/bin/soakctl"]`,
		dataDBEnv: "SOAK_DATA_DB=/data/soakctl.db",
	},
	{
		path:      "Dockerfile.worker",
		command:   `CMD ["/usr/local/bin/soakworker"]`,
		dataDBEnv: "SOAK_DATA_DB=/data/test.db",
	},
}

func TestDockerfileBaseImagesAreDigestPinned(t *testing.T) {
	fromRE := regexp.MustCompile(`(?i)^FROM\s+(?:--platform=\S+\s+)?(\S+)`)
	digestRE := regexp.MustCompile(`@sha256:[a-f0-9]{64}$`)
	bases := []string{"golang:", "debian:"}

	for _, path := range dockerfiles {
		t.Run(path, func(t *testing.T) {
			for _, line := range readDockerfileLines(t, path) {
				match := fromRE.FindStringSubmatch(strings.TrimSpace(line))
				if match == nil {
					continue
				}

				image := match[1]
				for _, base := range bases {
					if strings.HasPrefix(image, base) {
						if !digestRE.MatchString(image) {
							t.Fatalf("%s must pin %s by digest, got %q", path, base, image)
						}
					}
				}
			}
		})
	}
}

func TestRuntimeStagesDropPrivilegesWithEntrypoint(t *testing.T) {
	for _, runtime := range dockerfileRuntimes {
		t.Run(runtime.path, func(t *testing.T) {
			stageLines := finalDockerfileStage(readDockerfileLines(t, runtime.path))
			if user := lastDirectiveArg(stageLines, "USER"); user != "" {
				t.Fatalf("%s final stage must start as root for volume ownership repair, got USER %q", runtime.path, user)
			}

			stage := strings.Join(stageLines, "\n")
			for _, want := range []string{
				"COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint",
				"chmod 0755 /usr/local/bin/docker-entrypoint",
				runtime.dataDBEnv,
				`ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]`,
				runtime.command,
			} {
				if !strings.Contains(stage, want) {
					t.Fatalf("%s final stage must drop privileges through docker-entrypoint, missing %q", runtime.path, want)
				}
			}
		})
	}
}

func TestWorkerDockerfileDisablesLitestreamTestWALAutocheckpoint(t *testing.T) {
	content := string(readFile(t, "Dockerfile.worker"))
	if !strings.Contains(content, "SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0") {
		t.Fatal("Dockerfile.worker must build litestream-test with WAL autocheckpoint disabled")
	}
}

func TestWorkerDockerfileQuotesLitestreamCheckoutRef(t *testing.T) {
	content := string(readFile(t, "Dockerfile.worker"))
	if !strings.Contains(content, `git checkout --detach "${LITESTREAM_SHA}"`) {
		t.Fatal("Dockerfile.worker must quote the Litestream checkout ref and use --detach (a bare -- makes git treat the SHA as a pathspec and the build fails)")
	}
}

func TestDockerEntrypointRepairsDataBeforeDroppingPrivileges(t *testing.T) {
	content := string(readFile(t, "docker-entrypoint.sh"))
	for _, want := range []string{
		`stat -c "%u:%g"`,
		`10001:10001`,
		`needs_chown "$DATA_DIR"`,
		`needs_chown "$DB_PATH"`,
		`chown -R soak:soak "$DATA_DIR"`,
		`exec setpriv --reuid soak --regid soak --init-groups "$@"`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("docker-entrypoint.sh must repair /data ownership then drop privileges, missing %q", want)
		}
	}

	chownIndex := strings.Index(content, `chown -R soak:soak "$DATA_DIR"`)
	setprivIndex := strings.Index(content, `exec setpriv --reuid soak --regid soak --init-groups "$@"`)
	if chownIndex == -1 || setprivIndex == -1 || chownIndex > setprivIndex {
		t.Fatal("docker-entrypoint.sh must chown /data before execing setpriv")
	}

	for _, forbidden := range []string{"gosu", "su-exec"} {
		if strings.Contains(content, forbidden) {
			t.Fatalf("docker-entrypoint.sh must use setpriv, not %s", forbidden)
		}
	}
}

func TestDockerEntrypointOnlyChownsWhenDataOrDBOwnerDiffers(t *testing.T) {
	tests := []struct {
		name      string
		dataOwner string
		dbOwner   string
		wantChown bool
	}{
		{
			name:      "ownership already correct",
			dataOwner: "10001:10001",
			dbOwner:   "10001:10001",
		},
		{
			name:      "database owner differs",
			dataOwner: "10001:10001",
			dbOwner:   "0:0",
			wantChown: true,
		},
		{
			name:      "data directory owner differs",
			dataOwner: "0:0",
			dbOwner:   "10001:10001",
			wantChown: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			binDir := filepath.Join(dir, "bin")
			dataDir := filepath.Join(dir, "data")
			dbPath := filepath.Join(dataDir, "test.db")
			logPath := filepath.Join(dir, "calls.log")

			if err := os.Mkdir(binDir, 0o755); err != nil {
				t.Fatalf("mkdir bin: %v", err)
			}
			if err := os.Mkdir(dataDir, 0o755); err != nil {
				t.Fatalf("mkdir data: %v", err)
			}
			if err := os.WriteFile(dbPath, nil, 0o644); err != nil {
				t.Fatalf("write db: %v", err)
			}

			writeExecutable(t, filepath.Join(binDir, "stat"), `#!/bin/sh
if [ "$1" != "-c" ] || [ "$2" != "%u:%g" ]; then
  exit 64
fi
printf 'stat %s\n' "$3" >> "$LOG"
case "$3" in
  "$DATA_DIR") printf '%s\n' "$STAT_DATA_OWNER" ;;
  "$SOAK_DATA_DB") printf '%s\n' "$STAT_DB_OWNER" ;;
  *) printf '10001:10001\n' ;;
esac
`)
			writeExecutable(t, filepath.Join(binDir, "chown"), `#!/bin/sh
printf 'chown %s\n' "$*" >> "$LOG"
`)
			writeExecutable(t, filepath.Join(binDir, "setpriv"), `#!/bin/sh
printf 'setpriv %s\n' "$*" >> "$LOG"
`)

			cmd := exec.Command("sh", "docker-entrypoint.sh", "/usr/local/bin/soakworker")
			cmd.Env = append(os.Environ(),
				"PATH="+binDir,
				"DATA_DIR="+dataDir,
				"SOAK_DATA_DB="+dbPath,
				"LOG="+logPath,
				"STAT_DATA_OWNER="+tt.dataOwner,
				"STAT_DB_OWNER="+tt.dbOwner,
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("run docker-entrypoint.sh: %v\n%s", err, output)
			}

			log := string(readFile(t, logPath))
			chownCall := "chown -R soak:soak " + dataDir
			gotChown := strings.Contains(log, chownCall)
			if gotChown != tt.wantChown {
				t.Fatalf("chown call = %v, want %v\n%s", gotChown, tt.wantChown, log)
			}

			setprivCall := "setpriv --reuid soak --regid soak --init-groups /usr/local/bin/soakworker"
			if !strings.Contains(log, setprivCall) {
				t.Fatalf("missing setpriv call %q\n%s", setprivCall, log)
			}
			if tt.wantChown && strings.Index(log, chownCall) > strings.Index(log, setprivCall) {
				t.Fatalf("chown must happen before setpriv\n%s", log)
			}
		})
	}
}

func TestDockerEntrypointSetsHomeForDroppedPrivilegeUser(t *testing.T) {
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	dataDir := filepath.Join(dir, "data")
	dbPath := filepath.Join(dataDir, "test.db")
	logPath := filepath.Join(dir, "calls.log")

	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	if err := os.Mkdir(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	if err := os.WriteFile(dbPath, nil, 0o644); err != nil {
		t.Fatalf("write db: %v", err)
	}

	writeExecutable(t, filepath.Join(binDir, "stat"), `#!/bin/sh
printf '10001:10001\n'
`)
	writeExecutable(t, filepath.Join(binDir, "setpriv"), `#!/bin/sh
printf 'HOME=%s\n' "$HOME" >> "$LOG"
printf 'setpriv %s\n' "$*" >> "$LOG"
`)

	cmd := exec.Command("sh", "docker-entrypoint.sh", "/usr/local/bin/soakctl")
	cmd.Env = append(os.Environ(),
		"PATH="+binDir,
		"DATA_DIR="+dataDir,
		"SOAK_DATA_DB="+dbPath,
		"HOME=/root",
		"LOG="+logPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run docker-entrypoint.sh: %v\n%s", err, output)
	}

	log := string(readFile(t, logPath))
	if !strings.Contains(log, "HOME=/data\n") {
		t.Fatalf("dropped-privilege runtime HOME must be /data\n%s", log)
	}
	if strings.Contains(log, "HOME=/root") {
		t.Fatalf("dropped-privilege runtime must not inherit root HOME\n%s", log)
	}
}

func TestEntrypointStillRunsRuntimeCommandAsNonRoot(t *testing.T) {
	content := string(readFile(t, "docker-entrypoint.sh"))
	setprivRE := regexp.MustCompile(`exec\s+setpriv\s+.*--reuid\s+soak\s+.*--regid\s+soak\s+.*--init-groups\s+"\$@"`)
	if !setprivRE.MatchString(content) {
		t.Fatal("docker-entrypoint.sh must exec the runtime command as soak with initialized groups")
	}

	for _, runtime := range dockerfileRuntimes {
		t.Run(runtime.path, func(t *testing.T) {
			entrypoint := lastDirectiveArg(finalDockerfileStage(readDockerfileLines(t, runtime.path)), "ENTRYPOINT")
			if entrypoint == "" {
				t.Fatalf("%s final stage must declare an ENTRYPOINT", runtime.path)
			}
			if !strings.Contains(entrypoint, "docker-entrypoint") {
				t.Fatalf("%s final stage ENTRYPOINT must use docker-entrypoint, got %q", runtime.path, entrypoint)
			}
		})
	}
}

func TestRuntimeStagesDoNotRunAsRootByUserDirective(t *testing.T) {
	for _, path := range dockerfiles {
		t.Run(path, func(t *testing.T) {
			user := lastDirectiveArg(finalDockerfileStage(readDockerfileLines(t, path)), "USER")
			if user == "" {
				return
			}
			if isRootUser(user) {
				t.Fatalf("%s final stage USER must not be root, got %q", path, user)
			}
		})
	}
}

func TestRuntimeStagesPrepareWritableDataDir(t *testing.T) {
	for _, path := range dockerfiles {
		t.Run(path, func(t *testing.T) {
			stage := strings.Join(finalDockerfileStage(readDockerfileLines(t, path)), "\n")
			for _, want := range []string{"mkdir -p /data", "chown", "/data", "VOLUME /data"} {
				if !strings.Contains(stage, want) {
					t.Fatalf("%s final stage must prepare writable /data, missing %q", path, want)
				}
			}
		})
	}
}

func TestControlDockerfileRebuildsPinnedFlyctl(t *testing.T) {
	build := string(readFile(t, "scripts/build-flyctl.sh"))
	for _, want := range []string{"source_sha=203d7369ecb26c9adecadb501cd95682decdb527", "version=0.4.101-soak.1", `git -C "$source_dir" checkout --detach "$source_sha"`, "CGO_ENABLED=0 go build", `go version -m "$binary"`, "go mod edit -require=golang.org/x/crypto@v0.56.0"} {
		if !strings.Contains(build, want) {
			t.Errorf("maintained tool build missing %q", want)
		}
	}
	content := string(readFile(t, "Dockerfile.control"))
	for _, want := range []string{
		"golang:1.26.6-bookworm@sha256:",
		"bash /opt/build/build-flyctl.sh /src/flyctl /usr/local/bin/flyctl",

		"go version -m /usr/local/bin/flyctl",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("flyctl must preserve source identity and use a patched compiler: missing %q", want)
		}
	}
}

func readDockerfileLines(t *testing.T, path string) []string {
	t.Helper()
	return strings.Split(string(readFile(t, path)), "\n")
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func finalDockerfileStage(lines []string) []string {
	start := 0
	for i, line := range lines {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "FROM ") {
			start = i
		}
	}
	return lines[start:]
}

func lastDirectiveArg(lines []string, directive string) string {
	prefix := strings.ToUpper(directive) + " "
	var arg string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(trimmed), prefix) {
			arg = strings.TrimSpace(trimmed[len(prefix):])
		}
	}
	return arg
}

func isRootUser(user string) bool {
	fields := strings.Fields(user)
	if len(fields) == 0 {
		return true
	}
	name := strings.Split(fields[0], ":")[0]
	return name == "root" || name == "0"
}

func TestBuildsUsePatchedToolchain(t *testing.T) {
	for _, path := range dockerfiles {
		t.Run(path, func(t *testing.T) {
			content := string(readFile(t, path))
			builders := 0
			for _, stage := range strings.Split(content, "FROM ")[1:] {
				if !strings.HasPrefix(stage, "golang:") {
					continue
				}
				builders++
				if !strings.HasPrefix(stage, "golang:1.25.13-bookworm@sha256:") && !strings.HasPrefix(stage, "golang:1.26.6-bookworm@sha256:") {
					t.Error("Go builder must pin the patched compiler image")
				}
				if !strings.Contains(stage, "ENV GOTOOLCHAIN=local") {
					t.Error("builder must prevent implicit compiler changes")
				}
				if !strings.Contains(stage, "go version -m") {
					t.Error("builder must record actual binary compiler metadata")
				}
			}
			if builders == 0 {
				t.Fatal("no Go builders checked")
			}
		})
	}
	if !strings.Contains(string(readFile(t, "go.mod")), "\ngo 1.25.13\n") {
		t.Error("maintained module must require the patched compiler")
	}
}

func TestLocalBuildsPinAndAttributeToolchain(t *testing.T) {
	for _, tc := range []struct {
		path string
		want []string
	}{
		{"Makefile", []string{"export GOTOOLCHAIN := go1.25.13", "go version -m"}},
		{"scripts/local-rig-one-shot.sh", []string{"export GOTOOLCHAIN=go1.25.13", `cache_key="$scenario-$sha-$GOTOOLCHAIN"`, "go version -m", `toolchain_metadata=`}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			content := string(readFile(t, tc.path))
			for _, want := range tc.want {
				if !strings.Contains(content, want) {
					t.Errorf("missing compiler pin or attribution: %s", want)
				}
			}
		})
	}
}

func TestWorkerWorkloadBuildIsIndependentFromCandidate(t *testing.T) {
	content := string(readFile(t, "Dockerfile.worker"))
	for _, want := range []string{
		"ARG WORKLOAD_SHA=ae88b164dd6304bcbb654a681df767ee59042eed",
		`git checkout --detach "${WORKLOAD_SHA}"`,
		"COPY --from=workload-builder /usr/local/bin/litestream-test",
		"ENV WORKLOAD_SHA=${WORKLOAD_SHA}",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("independent workload build missing %q", want)
		}
	}
}
