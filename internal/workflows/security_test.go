package workflows

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBinarySecurityEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, role, output, stderr, status string
		exit                               int
		wantFailure                        bool
		metadataFailure                    bool
	}{
		{name: "clean", role: "operational", output: `{"config":{"scanner_version":"v1.7.0"}}`, status: "supported"},
		{name: "candidate findings", role: "candidate", output: `{"config":{}}
{"finding":{"osv":"GO-EXAMPLE","trace":[{"module":"example.org/module","package":"example.org/module/pkg","function":"Run"}]}}`, status: "supported"},
		{name: "operational findings", role: "operational", output: `{"config":{}}
{"finding":{"osv":"GO-EXAMPLE","trace":[{"function":"Run"}]}}`, status: "supported", wantFailure: true},
		{name: "reviewed daemon residual", role: "operational", output: `{"config":{}}
{"finding":{"osv":"GO-2026-4883","trace":[{"module":"github.com/docker/docker","function":"init"}]}}`, status: "supported"},
		{name: "residual ID wrong module", role: "operational", output: `{"config":{}}
{"finding":{"osv":"GO-2026-4883","trace":[{"module":"example.org/module","function":"Run"}]}}`, status: "supported", wantFailure: true},
		{name: "module evidence", role: "operational", output: `{"config":{}}
{"finding":{"osv":"GO-EXAMPLE","trace":[{"module":"example.org/module"}]}}`, status: "supported"},
		{name: "empty output", role: "candidate", status: "error", wantFailure: true},
		{name: "invalid output", role: "candidate", output: `broken`, status: "error", wantFailure: true},
		{name: "unsupported candidate", role: "candidate", stderr: "binary format is not supported", exit: 1, status: "unsupported"},
		{name: "unsupported operational", role: "operational", stderr: "binary format is not supported", exit: 1, status: "unsupported", wantFailure: true},
		{name: "missing build metadata", role: "candidate", output: `{"config":{}}`, status: "error", metadataFailure: true, wantFailure: true},
		{name: "scanner error", role: "candidate", stderr: "database unavailable", exit: 1, status: "error", wantFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, body := range map[string]string{
				"go":          "#!/bin/sh\necho 'build metadata'\nexit \"$GO_EXIT\"\n",
				"timeout":     "#!/bin/sh\nshift\nexec \"$@\"\n",
				"govulncheck": "#!/bin/sh\nprintf '%s\\n' \"$SCAN_OUTPUT\"\nprintf '%s\\n' \"$SCAN_STDERR\" >&2\nexit \"$SCAN_EXIT\"\n",
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0700); err != nil {
					t.Fatal(err)
				}
			}
			binary := filepath.Join(dir, "flyctl")
			if err := os.WriteFile(binary, []byte("binary"), 0600); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(dir, "evidence")
			cmd := exec.Command("bash", "../../scripts/scan-binary.sh", tc.role, "203d7369ecb26c9adecadb501cd95682decdb527", binary, out)
			goExit := "0"
			if tc.metadataFailure {
				goExit = "1"
			}
			cmd.Env = append(os.Environ(), "GO_EXIT="+goExit, "PATH="+dir+":"+os.Getenv("PATH"), "SCAN_OUTPUT="+tc.output, "SCAN_STDERR="+tc.stderr, "SCAN_EXIT="+string(rune('0'+tc.exit)))
			output, err := cmd.CombinedOutput()
			if (err != nil) != tc.wantFailure {
				t.Fatalf("failure=%v want %v: %s", err, tc.wantFailure, output)
			}
			data, err := os.ReadFile(filepath.Join(out, "summary.json"))
			if err != nil {
				t.Fatal(err)
			}
			var summary struct {
				Status    string `json:"status"`
				SourceSHA string `json:"source_sha"`
				Role      string `json:"role"`
			}
			if err := json.Unmarshal(data, &summary); err != nil {
				t.Fatal(err)
			}
			if summary.Status != tc.status || summary.Role != tc.role || summary.SourceSHA != "203d7369ecb26c9adecadb501cd95682decdb527" {
				t.Fatalf("unexpected summary: %s", data)
			}
			for _, name := range []string{"binary.json", "binary.stderr", "buildinfo.txt", "sha256.txt"} {
				if _, err := os.Stat(filepath.Join(out, name)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestOperationalToolBuildIsExplicitlyPatched(t *testing.T) {
	data, err := os.ReadFile("../../scripts/build-flyctl.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"203d7369ecb26c9adecadb501cd95682decdb527", "v0.56.0", "0.4.101-soak.1", "go mod verify", "git diff -- go.mod go.sum"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("missing %s", want)
		}
	}
}
