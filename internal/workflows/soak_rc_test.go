package workflows

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const (
	rcCandidateSHA = "ABCDEF0123456789ABCDEF0123456789ABCDEF01"
	rcBaselineSHA  = "1234567890abcdef1234567890abcdef12345678"
	// rcRunID is 0x2dfdc1c35; the workflow keeps the last eight hex digits.
	rcRunID       = "12345678901"
	rcRunIDSuffix = "dfdc1c35"
)

func workflowStepScript(t *testing.T, workflow, marker string) string {
	t.Helper()
	_, block, ok := strings.Cut(workflow, marker)
	if !ok {
		t.Fatalf("missing step %q", marker)
	}
	_, block, ok = strings.Cut(block, "        run: |\n")
	if !ok {
		t.Fatalf("missing script for %q", marker)
	}
	var lines []string
	for _, line := range strings.Split(block, "\n") {
		if line != "" && !strings.HasPrefix(line, "          ") {
			break
		}
		lines = append(lines, strings.TrimPrefix(line, "          "))
	}
	return strings.Join(lines, "\n")
}

func runSoakRCResolve(t *testing.T, env map[string]string) (map[string]string, string, error) {
	t.Helper()
	script := workflowStepScript(t, readWorkflow(t, "soak-rc.yml"), "      - id: resolve\n")
	dir := t.TempDir()
	mock := `#!/bin/bash
case "$*" in
  "api repos/"*"/commits/v0.5.0-rc1 --jq .sha") echo "$CANDIDATE_RESPONSE" ;;
  "api repos/benbjohnson/litestream/commits/main --jq .sha") echo "$BASELINE_RESPONSE" ;;
  "api repos/benbjohnson/litestream/commits/1234567 --jq .sha") echo "$BASELINE_RESPONSE" ;;
  *) echo "unexpected gh call: $*" >&2; exit 2 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(mock), 0700); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(dir, "outputs")
	values := map[string]string{
		"CANDIDATE_REF":      "v0.5.0-rc1",
		"REPO_FULL_NAME":     "benbjohnson/litestream",
		"BASELINE_REF":       "",
		"DURATION":           "72h",
		"TAIL":               "false",
		"PROFILES":           "",
		"GITHUB_RUN_ID":      rcRunID,
		"CANDIDATE_RESPONSE": rcCandidateSHA,
		"BASELINE_RESPONSE":  rcBaselineSHA,
	}
	for key, value := range env {
		values[key] = value
	}
	cmd := exec.Command("bash", "-c", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "GITHUB_OUTPUT="+outputPath, "GITHUB_STEP_SUMMARY="+filepath.Join(dir, "summary"))
	for key, value := range values {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	outputs := map[string]string{}
	if data, readErr := os.ReadFile(outputPath); readErr == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if key, value, ok := strings.Cut(line, "="); ok {
				outputs[key] = value
			}
		}
	}
	return outputs, string(output), err
}

func TestSoakRCResolvesRefsToPinnedFullSHAs(t *testing.T) {
	t.Parallel()
	fullSHA := regexp.MustCompile(`^[0-9a-f]{40}$`)
	for name, env := range map[string]map[string]string{
		"default baseline":    {},
		"explicit baseline":   {"BASELINE_REF": "1234567", "TAIL": "true", "DURATION": "24h"},
		"large-db only":       {"PROFILES": "gate-large-db", "DURATION": "24h"},
		"two profiles spaced": {"PROFILES": " gate-stress, gate-remote ", "TAIL": "true"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			outputs, log, err := runSoakRCResolve(t, env)
			if err != nil {
				t.Fatalf("resolve: %v\n%s", err, log)
			}
			for _, key := range []string{"candidate_sha", "baseline_sha"} {
				if !fullSHA.MatchString(outputs[key]) {
					t.Fatalf("%s = %q, want pinned 40-hex SHA", key, outputs[key])
				}
			}
			if outputs["candidate_sha"] != strings.ToLower(rcCandidateSHA) || outputs["baseline_sha"] != rcBaselineSHA {
				t.Fatalf("outputs = %v", outputs)
			}
			if outputs["candidate_source"] != "rc-abcdef01-"+rcRunIDSuffix || outputs["baseline_source"] != "rcbase-12345678-"+rcRunIDSuffix || outputs["source_suffix"] != rcRunIDSuffix {
				t.Fatalf("sources = %v", outputs)
			}
			wantDuration, wantTail := firstNonEmpty(env["DURATION"], "72h"), firstNonEmpty(env["TAIL"], "false")
			if outputs["duration"] != wantDuration || outputs["tail"] != wantTail || outputs["candidate_ref"] != "v0.5.0-rc1" {
				t.Fatalf("outputs = %v", outputs)
			}
			if want := strings.ReplaceAll(env["PROFILES"], " ", ""); outputs["profiles"] != want {
				t.Fatalf("profiles = %q, want %q", outputs["profiles"], want)
			}
		})
	}
}

func TestSoakRCSourceSuffixFollowsTheWorkflowRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		runID      string
		wantSuffix string
		wantErr    bool
	}{
		{name: "long run id keeps its last eight hex digits", runID: "12345678901", wantSuffix: "dfdc1c35"},
		{name: "short run id is used whole", runID: "255", wantSuffix: "ff"},
		{name: "same run id gives the same sources", runID: rcRunID, wantSuffix: rcRunIDSuffix},
		{name: "missing run id is refused", runID: "", wantErr: true},
		{name: "non-numeric run id is refused", runID: "abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			outputs, log, err := runSoakRCResolve(t, map[string]string{"GITHUB_RUN_ID": tt.runID})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolve succeeded without a run id: %v\n%s", outputs, log)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve: %v\n%s", err, log)
			}
			if outputs["source_suffix"] != tt.wantSuffix || outputs["candidate_source"] != "rc-abcdef01-"+tt.wantSuffix || outputs["baseline_source"] != "rcbase-12345678-"+tt.wantSuffix {
				t.Fatalf("outputs = %v, want suffix %s", outputs, tt.wantSuffix)
			}
		})
	}
}

func TestSoakRCRejectsUnpinnableRequests(t *testing.T) {
	t.Parallel()
	for name, env := range map[string]map[string]string{
		"short sha response":       {"CANDIDATE_RESPONSE": "abcdef0"},
		"empty sha response":       {"BASELINE_RESPONSE": ""},
		"same commit":              {"BASELINE_RESPONSE": strings.ToLower(rcCandidateSHA)},
		"duration too long":        {"DURATION": "169h"},
		"duration not hours":       {"DURATION": "3d"},
		"invalid tail":             {"TAIL": "yes"},
		"repo not allowlisted":     {"REPO_FULL_NAME": "someone/litestream"},
		"fork even if allowlisted": {"REPO_FULL_NAME": "someone/litestream", "SOAK_PR_REPO_ALLOWLIST": "benbjohnson/litestream,someone/litestream"},
		"ref injection":            {"CANDIDATE_REF": "main;echo"},
		"unknown profile":          {"PROFILES": "gate-large-db,gate-huge"},
		"profile injection":        {"PROFILES": "gate-large-db;echo"},
		"tail without gate-stress": {"PROFILES": "gate-large-db", "TAIL": "true"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			outputs, log, err := runSoakRCResolve(t, env)
			if err == nil {
				t.Fatalf("resolve succeeded: %v\n%s", outputs, log)
			}
			if outputs["candidate_sha"] != "" {
				t.Fatalf("published outputs on failure: %v", outputs)
			}
		})
	}
}

func TestSoakRCPinsBuildsAndNotifiesResolvedSHAs(t *testing.T) {
	t.Parallel()
	workflow := readWorkflow(t, "soak-rc.yml")
	for _, required := range []string{
		"workflow_dispatch:",
		"candidate_ref:",
		"default: benbjohnson/litestream",
		"baseline_sha:",
		"default: 72h",
		"type: boolean",
		`local image_label="sha-${GITHUB_SHA::12}-${source_name}-ls-${litestream_sha::12}"`,
		`--build-arg "LITESTREAM_SHA=${litestream_sha}"`,
		`build_gate_image "${CANDIDATE_SOURCE}" "${CANDIDATE_SHA}" candidate_image_ref`,
		`build_gate_image "${BASELINE_SOURCE}" "${BASELINE_SHA}" baseline_image_ref`,
		"./scripts/register-rc-gate.sh",
		"GATE_PROFILES: ${{ needs.resolve-request.outputs.profiles }}",
		"GATE_SOURCE_SUFFIX: ${{ needs.resolve-request.outputs.source_suffix }}",
		`"${CANDIDATE_IMAGE_REF}" \` + "\n" + `            "${CANDIDATE_SHA}"`,
		`"${BASELINE_IMAGE_REF}" \` + "\n" + `            "${BASELINE_SHA}"`,
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("soak-rc.yml is missing %q", required)
		}
	}
	for _, line := range strings.Split(workflow, "\n") {
		if !strings.Contains(line, "github.event.inputs") || strings.HasPrefix(strings.TrimSpace(line), "group:") {
			continue
		}
		if !regexp.MustCompile(`^\s+[A-Z_]+: \$\{\{ github\.event\.inputs\.`).MatchString(line) {
			t.Fatalf("dispatch input reaches shell outside an env binding: %q", line)
		}
	}
}

func TestRegisterRCGateRequiresPinnedSHAs(t *testing.T) {
	t.Parallel()
	script := filepath.Join("..", "..", "scripts", "register-rc-gate.sh")
	valid := []string{"benbjohnson/litestream", "v1", strings.ToLower(rcCandidateSHA), rcBaselineSHA}
	for name, tc := range map[string]struct {
		args []string
		env  []string
	}{
		"short candidate": {args: []string{"benbjohnson/litestream", "v1", "abcdef0", rcBaselineSHA}},
		"uppercase":       {args: []string{"benbjohnson/litestream", "v1", rcCandidateSHA, rcBaselineSHA}},
		"bad tail":        {args: append(slices.Clone(valid), "72h", "maybe")},
		"bad profiles":    {args: valid, env: []string{"GATE_PROFILES=gate-large-db;echo"}},
		"bad suffix":      {args: valid, env: []string{"GATE_SOURCE_SUFFIX=Run_1"}},
		"long suffix":     {args: valid, env: []string{"GATE_SOURCE_SUFFIX=123456789"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(script, tc.args...)
			cmd.Env = append(os.Environ(), "SOAK_ADMIN_BEARER_TOKEN=", "CONTROL_BASE_URL=http://127.0.0.1:1")
			cmd.Env = append(cmd.Env, tc.env...)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("register-rc-gate.sh accepted %v %v: %s", tc.args, tc.env, output)
			}
			if strings.Contains(string(output), "curl") {
				t.Fatalf("register-rc-gate.sh reached curl for %v %v: %s", tc.args, tc.env, output)
			}
		})
	}
}

func TestRegisterRCGatePayloadCarriesProfilesAndSuffix(t *testing.T) {
	t.Parallel()
	script := filepath.Join("..", "..", "scripts", "register-rc-gate.sh")
	tests := []struct {
		name string
		env  []string
		want []string
		omit []string
	}{
		{name: "defaults omit both", omit: []string{`"profiles"`, `"source_suffix"`}},
		{name: "large-db rerun", env: []string{"GATE_PROFILES=gate-large-db", "GATE_SOURCE_SUFFIX=dfdc1c35"}, want: []string{`"profiles":["gate-large-db"]`, `"source_suffix":"dfdc1c35"`}},
		{name: "two profiles", env: []string{"GATE_PROFILES=gate-stress, gate-remote"}, want: []string{`"profiles":["gate-stress","gate-remote"]`}, omit: []string{`"source_suffix"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			capture := filepath.Join(dir, "payload")
			mock := "#!/bin/bash\nwhile [ $# -gt 0 ]; do if [ \"$1\" = \"-d\" ]; then printf '%s' \"$2\" > \"$CURL_CAPTURE\"; fi; shift; done\n"
			if err := os.WriteFile(filepath.Join(dir, "curl"), []byte(mock), 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(script, "benbjohnson/litestream", "pull/1514/head", strings.ToLower(rcCandidateSHA), rcBaselineSHA, "24h", "false")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "SOAK_ADMIN_BEARER_TOKEN=token", "CONTROL_BASE_URL=http://127.0.0.1:1", "CURL_CAPTURE="+capture)
			cmd.Env = append(cmd.Env, tt.env...)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("register-rc-gate.sh: %v\n%s", err, output)
			}
			raw, err := os.ReadFile(capture)
			if err != nil {
				t.Fatalf("payload not sent: %v", err)
			}
			payload := strings.Join(strings.Fields(string(raw)), "")
			for _, want := range tt.want {
				if !strings.Contains(payload, want) {
					t.Fatalf("payload %s is missing %s", payload, want)
				}
			}
			for _, omit := range tt.omit {
				if strings.Contains(payload, omit) {
					t.Fatalf("payload %s should omit %s", payload, omit)
				}
			}
			for _, required := range []string{`"candidate_sha":"` + strings.ToLower(rcCandidateSHA) + `"`, `"duration":"24h"`, `"kind":"rc"`} {
				if !strings.Contains(payload, required) {
					t.Fatalf("payload %s is missing %s", payload, required)
				}
			}
		})
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
