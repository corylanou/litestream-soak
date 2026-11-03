package workflows

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
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
		"vfs only":            {"PROFILES": "gate-vfs", "DURATION": "2h"},
		"metered large-db":    {"PROFILES": "gate-large-db", "DURATION": "24h", "OBSERVE_PROXY": "true"},
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
			if want := firstNonEmpty(env["OBSERVE_PROXY"], "false"); outputs["observe_proxy"] != want {
				t.Fatalf("observe_proxy = %q, want %q", outputs["observe_proxy"], want)
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
		"invalid observe_proxy":    {"OBSERVE_PROXY": "yes"},
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
		"GATE_OBSERVE_PROXY: ${{ needs.resolve-request.outputs.observe_proxy }}",
		"      observe_proxy:\n        description:",
		"OBSERVE_PROXY: ${{ github.event.inputs.observe_proxy || 'false' }}",
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
		"bad observe":     {args: valid, env: []string{"GATE_OBSERVE_PROXY=maybe"}},
		"observe inject":  {args: valid, env: []string{"GATE_OBSERVE_PROXY=true,\"x\":1"}},
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
		{name: "defaults omit both", omit: []string{`"profiles"`, `"source_suffix"`, `"observe_proxy"`}},
		{name: "large-db rerun", env: []string{"GATE_PROFILES=gate-large-db", "GATE_SOURCE_SUFFIX=dfdc1c35"}, want: []string{`"profiles":["gate-large-db"]`, `"source_suffix":"dfdc1c35"`}, omit: []string{`"observe_proxy"`}},
		{name: "metered large-db gate", env: []string{"GATE_PROFILES=gate-large-db", "GATE_OBSERVE_PROXY=true"}, want: []string{`"profiles":["gate-large-db"]`, `"observe_proxy":true`}},
		{name: "observe proxy false is omitted", env: []string{"GATE_OBSERVE_PROXY=false"}, omit: []string{`"observe_proxy"`}},
		{name: "two profiles", env: []string{"GATE_PROFILES=gate-stress, gate-remote"}, want: []string{`"profiles":["gate-stress","gate-remote"]`}, omit: []string{`"source_suffix"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			capture := filepath.Join(dir, "payload")
			mock := "#!/bin/bash\nout=\"\"\nwhile [ $# -gt 0 ]; do case \"$1\" in -d) printf '%s' \"$2\" > \"$CURL_CAPTURE\"; shift;; -o) out=\"$2\"; shift;; esac; shift; done\n[ -n \"$out\" ] && printf '{\"id\":1}' > \"$out\"\nprintf 201\n"
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

// registerGateMockCurl stands in for curl. POST_CODES lists the HTTP code each
// successive POST returns (28 means curl timed out, the last entry repeats);
// GET_LEDGERS lists the ledger file each successive GET serves (timeout means
// curl timed out). Every call is counted per method under CURL_STATE.
const registerGateMockCurl = `#!/bin/bash
method=GET; out=""
while [ $# -gt 0 ]; do
  case "$1" in
    -X) method="$2"; shift ;;
    -o) out="$2"; shift ;;
    --max-time) echo "$2" >> "$CURL_STATE/max-time.log"; shift ;;
    -d|-w|-H|-u) shift ;;
  esac
  shift
done
count_file="$CURL_STATE/$method.count"
n=$(( $(cat "$count_file" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$count_file"
pick() { local i="$1"; shift; local last=""; for v in "$@"; do last="$v"; if [ "$i" -eq 1 ]; then echo "$v"; return; fi; i=$((i-1)); done; echo "$last"; }
if [ "$method" = "POST" ]; then
  # shellcheck disable=SC2086
  code="$(pick "$n" $POST_CODES)"
  [ "$code" = "28" ] && exit 28
  case "$code" in 2*) printf '{"id":9,"candidate_source":"registered-now"}' > "$out" ;; *) printf 'control plane said %s' "$code" > "$out" ;; esac
  printf '%s' "$code"
  exit 0
fi
# shellcheck disable=SC2086
ledger="$(pick "$n" $GET_LEDGERS)"
[ "$ledger" = "timeout" ] && exit 28
cp "$CURL_STATE/ledger-$ledger.json" "$out"
printf 200
`

func TestRegisterRCGateRecoversFromSlowControlPlane(t *testing.T) {
	t.Parallel()
	script := filepath.Join("..", "..", "scripts", "register-rc-gate.sh")
	candidate := strings.ToLower(rcCandidateSHA)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	gateEntry := func(id int, status, startedAt, profiles string, observe bool, duration string) string {
		return fmt.Sprintf(`{"id":%d,"kind":"rc","status":"%s","started_at":"%s","candidate_source":"rc-%s-dfdc1c35","baseline_source":"rcbase-%s-dfdc1c35","candidate_sha":"%s","baseline_sha":"%s","duration":"%s","tail":false,"profiles":[%s],"observe_proxy":%t,"resources":[]}`,
			id, status, startedAt, candidate[:8], rcBaselineSHA[:8], candidate, rcBaselineSHA, duration, profiles, observe)
	}
	gate := func(profiles string, observe bool, duration string) string {
		return `{"gates":[` + gateEntry(21, "running", now, profiles, observe, duration) + `]}`
	}
	oldClosed := gateEntry(4, "completed", "2026-01-01T00:00:00Z", `"gate-large-db"`, true, "24h0m0s")
	upgradeGate := func(warmup string) string {
		return fmt.Sprintf(`{"gates":[{"id":7,"kind":"upgrade","status":"running","candidate_source":"up-%s-dfdc1c35","baseline_source":"upbase-%s-dfdc1c35","candidate_sha":"%s","baseline_sha":"%s","duration":"24h0m0s","upgrade_warmup":"%s","tail":false,"resources":[]}]}`,
			candidate[:8], rcBaselineSHA[:8], candidate, rcBaselineSHA, warmup)
	}
	ledgers := map[string]string{
		"empty":        `{"gates":[]}`,
		"match":        gate(`"gate-large-db"`, true, "24h0m0s"),
		"match-1h":     gate(`"gate-large-db"`, true, "1h0m0s"),
		"match-90m":    gate(`"gate-large-db"`, true, "1h30m0s"),
		"match-two":    gate(`"gate-stress","gate-large-db"`, true, "24h0m0s"),
		"closed":       strings.Replace(gate(`"gate-large-db"`, true, "24h0m0s"), `"status":"running"`, `"status":"aborted"`, 1),
		"completed":    strings.Replace(gate(`"gate-large-db"`, true, "24h0m0s"), `"status":"running"`, `"status":"completed"`, 1),
		"mismatch":     gate(`"gate-stress","gate-large-db"`, true, "24h0m0s"),
		"unmeter":      gate(`"gate-large-db"`, false, "24h0m0s"),
		"upgrade-30m":  upgradeGate("30m0s"),
		"upgrade-2h":   upgradeGate("2h0m0s"),
		"upgrade-45m":  upgradeGate("45m0s"),
		"other-source": strings.ReplaceAll(gate(`"gate-large-db"`, true, "24h0m0s"), "-dfdc1c35", "-other"),
		"old-closed":   `{"gates":[` + oldClosed + `]}`,
		"old-and-new":  `{"gates":[` + gateEntry(21, "running", now, `"gate-large-db"`, true, "24h0m0s") + `,` + oldClosed + `]}`,
		"fresh-closed": `{"gates":[` + gateEntry(22, "aborted", now, `"gate-large-db"`, true, "24h0m0s") + `,` + oldClosed + `]}`,
	}
	tests := []struct {
		name      string
		postCodes string
		ledgers   string
		env       []string
		args      []string
		duration  string
		profiles  string
		wantExit  bool
		wantPosts int
		wantGets  int
		wantOut   string
		wantErr   string
	}{
		{name: "upgrade gate warm-up matches normalized", postCodes: "28", ledgers: "upgrade-30m", args: []string{"upgrade", "30m"}, profiles: "", wantPosts: 1, wantGets: 1, wantOut: `"id":7`},
		{name: "upgrade gate default warm-up matches 2h0m0s", postCodes: "28", ledgers: "upgrade-2h", args: []string{"upgrade"}, profiles: "", wantPosts: 1, wantGets: 1, wantOut: `"id":7`},
		{name: "upgrade gate warm-up mismatch fails", postCodes: "28", ledgers: "upgrade-45m", args: []string{"upgrade", "30m"}, profiles: "", wantExit: true, wantPosts: 1, wantGets: 1, wantErr: "does not match"},
		{name: "a completed gate on the sources blocks a second registration", postCodes: "28", ledgers: "completed", wantExit: true, wantPosts: 1, wantGets: 1, wantErr: "rc gate 21 is completed"},
		{name: "an old closed gate plus a new open match succeeds", postCodes: "28", ledgers: "old-and-new", wantPosts: 1, wantGets: 1, wantOut: `"id":21`},
		{name: "an old closed gate alone keeps polling then re-posts", postCodes: "28 201", ledgers: "old-closed", wantPosts: 2, wantGets: 1, wantOut: `"candidate_source":"registered-now"`},
		{name: "a closed gate started during this attempt fails", postCodes: "28", ledgers: "fresh-closed", wantExit: true, wantPosts: 1, wantGets: 1, wantErr: "rc gate 22 is aborted and started during this attempt"},
		{name: "a gate on other sources does not block the retry", postCodes: "28 201", ledgers: "other-source", wantPosts: 2, wantGets: 1, wantOut: `"candidate_source":"registered-now"`},
		{name: "zero request timeout falls back to the default", postCodes: "201", env: []string{"GATE_REGISTER_TIMEOUT=0"}, wantPosts: 1, wantGets: 0, wantOut: `"candidate_source":"registered-now"`, wantErr: "using 300"},
		{name: "duplicate profiles in the request still match the deduplicated gate", postCodes: "28", ledgers: "match", profiles: "gate-large-db,gate-large-db", wantPosts: 1, wantGets: 1, wantOut: `"id":21`},
		{name: "profiles match regardless of order", postCodes: "28", ledgers: "match-two", profiles: "gate-large-db,gate-stress", wantPosts: 1, wantGets: 1, wantOut: `"id":21`},
		{name: "60m matches the ledger's 1h0m0s", postCodes: "28", ledgers: "match-1h", duration: "60m", wantPosts: 1, wantGets: 1, wantOut: `"id":21`},
		{name: "90m matches the ledger's 1h30m0s", postCodes: "28", ledgers: "match-90m", duration: "90m", wantPosts: 1, wantGets: 1, wantOut: `"id":21`},
		{name: "1h30m matches the ledger's 1h30m0s", postCodes: "28", ledgers: "match-90m", duration: "1h30m", wantPosts: 1, wantGets: 1, wantOut: `"id":21`},
		{name: "2h does not match a 1h gate", postCodes: "28", ledgers: "match-1h", duration: "2h", wantExit: true, wantPosts: 1, wantGets: 1},
		{name: "timeout then the ledger already has the gate", postCodes: "28", ledgers: "match", wantPosts: 1, wantGets: 1, wantOut: `"candidate_source":"rc-` + candidate[:8] + `-dfdc1c35"`},
		{name: "timeout then empty ledger then the retry registers", postCodes: "28 201", ledgers: "empty", wantPosts: 2, wantGets: 1, wantOut: `"candidate_source":"registered-now"`},
		{name: "5xx then the gate appears on the second poll", postCodes: "500", ledgers: "empty match", env: []string{"GATE_REGISTER_POLL_ATTEMPTS=2"}, wantPosts: 1, wantGets: 2, wantOut: `"id":21`},
		{name: "an aborted gate on the sources blocks a second registration", postCodes: "28", ledgers: "closed", wantExit: true, wantPosts: 1, wantGets: 1, wantErr: "rc gate 21 is aborted"},
		{name: "unreadable ledger never retries the POST", postCodes: "28", ledgers: "timeout", env: []string{"GATE_REGISTER_POLL_ATTEMPTS=2"}, wantExit: true, wantPosts: 1, wantGets: 2, wantErr: "could not read the rc gate ledger"},
		{name: "a different gate owns the sources", postCodes: "28", ledgers: "mismatch", wantExit: true, wantPosts: 1, wantGets: 1, wantErr: "rc gate 21 (running) already owns"},
		{name: "same gate without the observe proxy is a mismatch", postCodes: "28", ledgers: "unmeter", wantExit: true, wantPosts: 1, wantGets: 1, wantErr: "does not match"},
		{name: "409 is final without polling", postCodes: "409", wantExit: true, wantPosts: 1, wantGets: 0},
		{name: "400 is final without polling", postCodes: "400", wantExit: true, wantPosts: 1, wantGets: 0},
		{name: "deadline bounds the retries and every request", postCodes: "28", ledgers: "empty", env: []string{"GATE_REGISTER_DEADLINE=2", "GATE_REGISTER_POLL_INTERVAL=1"}, wantExit: true, wantPosts: -1, wantGets: -1, wantErr: "did not complete within 2s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "curl"), []byte(registerGateMockCurl), 0700); err != nil {
				t.Fatal(err)
			}
			for name, body := range ledgers {
				if err := os.WriteFile(filepath.Join(dir, "ledger-"+name+".json"), []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			args := append([]string{"benbjohnson/litestream", "pull/1514/head", candidate, rcBaselineSHA, firstNonEmpty(tt.duration, "24h"), "false"}, tt.args...)
			profiles := "gate-large-db"
			if tt.profiles != "" || tt.args != nil {
				profiles = tt.profiles
			}
			observe := "true"
			if tt.args != nil {
				observe = "false"
			}
			cmd := exec.Command(script, args...)
			cmd.Env = append(os.Environ(),
				"PATH="+dir+":"+os.Getenv("PATH"), "SOAK_ADMIN_BEARER_TOKEN=token", "CONTROL_BASE_URL=http://127.0.0.1:1",
				"CURL_STATE="+dir, "POST_CODES="+tt.postCodes, "GET_LEDGERS="+tt.ledgers,
				"GATE_PROFILES="+profiles, "GATE_OBSERVE_PROXY="+observe, "GATE_SOURCE_SUFFIX=dfdc1c35",
				"GATE_REGISTER_POLL_INTERVAL=0", "GATE_REGISTER_POLL_ATTEMPTS=1", "GATE_REGISTER_DEADLINE=30",
			)
			cmd.Env = append(cmd.Env, tt.env...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			started := time.Now()
			err := cmd.Run()
			if elapsed := time.Since(started); elapsed > 10*time.Second {
				t.Fatalf("register-rc-gate.sh ran for %s, want the deadline to bound it", elapsed)
			}
			if (err != nil) != tt.wantExit {
				t.Fatalf("register-rc-gate.sh error = %v, want failure %v\nstdout: %s\nstderr: %s", err, tt.wantExit, stdout.String(), stderr.String())
			}
			if tt.wantOut != "" && !strings.Contains(stdout.String(), tt.wantOut) {
				t.Fatalf("stdout = %q, want %q\nstderr: %s", stdout.String(), tt.wantOut, stderr.String())
			}
			if tt.wantErr != "" && !strings.Contains(stderr.String(), tt.wantErr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tt.wantErr)
			}
			if raw, err := os.ReadFile(filepath.Join(dir, "max-time.log")); err == nil {
				deadline := 30
				for _, setting := range tt.env {
					if value, ok := strings.CutPrefix(setting, "GATE_REGISTER_DEADLINE="); ok {
						deadline, _ = strconv.Atoi(value)
					}
				}
				for _, line := range strings.Fields(string(raw)) {
					maxTime, err := strconv.Atoi(line)
					if err != nil || maxTime < 1 || maxTime > deadline {
						t.Fatalf("curl --max-time %q, want between 1 and the remaining budget (%ds)", line, deadline)
					}
					if slices.Contains(tt.env, "GATE_REGISTER_TIMEOUT=0") && maxTime != 30 {
						t.Fatalf("curl --max-time %d with a zero timeout, want the 300s default capped by the 30s budget", maxTime)
					}
				}
			}
			count := func(method string) int {
				raw, err := os.ReadFile(filepath.Join(dir, method+".count"))
				if err != nil {
					return 0
				}
				n, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
				return n
			}
			posts, gets := count("POST"), count("GET")
			if tt.wantPosts < 0 {
				if posts < 1 || posts > 3 || gets < 1 || gets > 3 {
					t.Fatalf("curl calls = %d POST %d GET, want a bounded handful of retries\nstderr: %s", posts, gets, stderr.String())
				}
				return
			}
			if posts != tt.wantPosts || gets != tt.wantGets {
				t.Fatalf("curl calls = %d POST %d GET, want %d POST %d GET\nstderr: %s", posts, gets, tt.wantPosts, tt.wantGets, stderr.String())
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
