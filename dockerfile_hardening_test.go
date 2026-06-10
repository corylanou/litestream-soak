package soak_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var dockerfiles = []string{
	"Dockerfile.control",
	"Dockerfile.worker",
}

func TestDockerfileBaseImagesAreDigestPinned(t *testing.T) {
	fromRE := regexp.MustCompile(`(?i)^FROM\s+(?:--platform=\S+\s+)?(\S+)`)
	digestRE := regexp.MustCompile(`@sha256:[a-f0-9]{64}$`)
	bases := []string{"golang:1.25", "debian:bookworm-slim"}

	for _, path := range dockerfiles {
		t.Run(path, func(t *testing.T) {
			for _, line := range readDockerfileLines(t, path) {
				match := fromRE.FindStringSubmatch(strings.TrimSpace(line))
				if match == nil {
					continue
				}

				image := match[1]
				for _, base := range bases {
					if image == base || strings.HasPrefix(image, base+"@") {
						if !strings.HasPrefix(image, base+"@sha256:") || !digestRE.MatchString(image) {
							t.Fatalf("%s must pin %s by digest, got %q", path, base, image)
						}
					}
				}
			}
		})
	}
}

func TestRuntimeStagesRunAsNonRoot(t *testing.T) {
	for _, path := range dockerfiles {
		t.Run(path, func(t *testing.T) {
			user := lastDirectiveArg(finalDockerfileStage(readDockerfileLines(t, path)), "USER")
			if user == "" {
				t.Fatalf("%s final stage must declare a non-root USER", path)
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

func TestControlDockerfilePinsFlyctlWithChecksum(t *testing.T) {
	content := string(readFile(t, "Dockerfile.control"))
	for _, want := range []string{
		"ARG FLYCTL_VERSION=",
		"ARG FLYCTL_AMD64_SHA256=",
		"ARG FLYCTL_ARM64_SHA256=",
		"https://github.com/superfly/flyctl/releases/download/",
		"sha256sum -c",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("Dockerfile.control must pin flyctl with checksum verification, missing %q", want)
		}
	}

	if strings.Contains(content, "https://fly.io/install.sh") {
		t.Fatal("Dockerfile.control must not use the fly.io install script")
	}

	if strings.Contains(content, "ARG TARGETARCH=") || !strings.Contains(content, "ARG TARGETARCH\n") {
		t.Fatal("Dockerfile.control must use BuildKit's TARGETARCH without overriding it")
	}

	curlPipeShRE := regexp.MustCompile(`(?m)curl[^\n|]*\|[^\n]*(?:^|\s)sh(?:\s|$)`)
	if curlPipeShRE.MatchString(content) {
		t.Fatal("Dockerfile.control must not pipe curl output into sh")
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
