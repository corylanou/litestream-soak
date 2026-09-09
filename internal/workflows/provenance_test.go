package workflows

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNotificationUsesTrustedGeneratorBuildProvenance(t *testing.T) {
	candidate := strings.Repeat("a", 40)
	generator := strings.Repeat("b", 40)
	cases := []struct {
		name, dockerfile, override, want string
		fails                            bool
	}{
		{"split default", "ARG WORKLOAD_SHA=" + generator + "\n", "", generator, false},
		{"explicit build override", "ARG WORKLOAD_SHA=" + generator + "\n", strings.Repeat("c", 40), strings.Repeat("c", 40), false},
		{"shared legacy build", "COPY --from=litestream-builder /usr/local/bin/litestream-test /usr/local/bin/litestream-test\n", "", candidate, false},
		{"split without pin", "ARG WORKLOAD_SHA\nCOPY --from=litestream-builder /usr/local/bin/litestream-test /usr/local/bin/litestream-test\n", "", "", true},
		{"invalid override", "ARG WORKLOAD_SHA=" + generator + "\n", "main", "", true},
	}
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "notify-deployment-ready.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "scripts"), 0755); err != nil {
				t.Fatal(err)
			}
			scriptPath := filepath.Join(root, "scripts", "notify-deployment-ready.sh")
			if err := os.WriteFile(scriptPath, script, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "Dockerfile.worker"), []byte(tc.dockerfile), 0600); err != nil {
				t.Fatal(err)
			}
			received := make(chan map[string]string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]string
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				received <- body
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			cmd := exec.Command("bash", scriptPath, strings.Repeat("d", 40), "main", "test", "registry.example/image", candidate, "", tc.override)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CONTROL_BASE_URL=" + server.URL, "SOAK_ADMIN_BEARER_TOKEN=test-token"}
			output, err := cmd.CombinedOutput()
			if tc.fails {
				if err == nil {
					t.Fatal("unproven generator accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("notification: %v: %s", err, output)
			}
			select {
			case body := <-received:
				if body["workload_sha"] != tc.want || body["litestream_sha"] != candidate {
					t.Fatalf("version provenance = %v", body)
				}
			default:
				t.Fatal("notification missing")
			}
		})
	}
}
