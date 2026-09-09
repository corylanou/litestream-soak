#!/bin/bash
set -euo pipefail
role=${1:?role required}
source_sha=${2:?source SHA required}
binary=${3:?binary required}
output=${4:?output directory required}
source_dir=${5:-}
[[ "$role" = operational || "$role" = candidate ]] || exit 2
[[ "$source_sha" =~ ^[0-9a-f]{40}$ ]] || exit 2
mkdir -p "$output"
output=$(cd "$output" && pwd)
metadata_status=supported
go version -m "$binary" >"$output/buildinfo.txt" 2>"$output/buildinfo.stderr" || metadata_status=unavailable
shasum -a 256 "$binary" >"$output/sha256.txt"
scan_exit=0
timeout 300 govulncheck -json -mode=binary "$binary" >"$output/binary.json" 2>"$output/binary.stderr" || scan_exit=$?
status=supported
if [[ "$scan_exit" != 0 ]]; then
    status=error
    if rg -qi 'binary format is not supported|unsupported binary|unrecognized binary format' "$output/binary.stderr"; then
        status=unsupported
    fi
elif ! jq -se 'length > 0 and any(.[]; has("config"))' "$output/binary.json" >/dev/null 2>&1; then
    status=error
fi
if [[ "$metadata_status" = unavailable && "$status" = supported ]]; then
    status=error
fi
source_status=not_requested
if [[ -n "$source_dir" ]]; then
    source_status=supported
    git -C "$source_dir" rev-parse HEAD >"$output/source.sha"
    [[ "$(cat "$output/source.sha")" = "$source_sha" ]] || exit 2
    git -C "$source_dir" diff -- go.mod go.sum >"$output/dependency.patch"
    cp "$source_dir/go.mod" "$source_dir/go.sum" "$output/"
    (cd "$source_dir" && go list -m -json all) >"$output/modules.json"
    if ! (cd "$source_dir" && timeout 300 govulncheck -json -tags "${SCAN_TAGS:-}" ./...) >"$output/source.json" 2>"$output/source.stderr"; then
        source_status=error
    elif ! jq -se 'length > 0 and any(.[]; has("config"))' "$output/source.json" >/dev/null 2>&1; then
        source_status=error
    fi
fi
findings='[]'
if [[ "$status" = supported ]]; then
    findings=$(jq -s '[.[] | select(.finding) | .finding | {id: .osv, fixed_version, trace, reachability: (if .trace[0].function then "binary-symbol" elif .trace[0].package then "package" else "module" end)}]' "$output/binary.json")
fi
jq -n --arg role "$role" --arg sha "$source_sha" --arg status "$status" --arg source_status "$source_status" --arg metadata_status "$metadata_status" --argjson exit_code "$scan_exit" --argjson findings "$findings" \
    '{role:$role,source_sha:$sha,status:$status,metadata_status:$metadata_status,source_status:$source_status,scanner_exit:$exit_code,findings:$findings}' >"$output/summary.json"
cat "$output/summary.json"
[[ "$status" != error && "$source_status" != error ]] || exit 1
if [[ "$role" = operational ]]; then
    [[ "$status" = supported ]] || exit 1
    inputs=("$output/binary.json")
    [[ "$source_status" != supported ]] || inputs+=("$output/source.json")
    jq -se --arg name "$(basename "$binary")" --arg sha "$source_sha" '
      def reviewed:
        if .trace[0].module == "github.com/docker/docker" or .trace[0].module == "github.com/docker/docker-credential-helpers" then
          .osv == "GO-2026-4883" or .osv == "GO-2026-4887"
        elif .trace[0].module == "github.com/chrismellard/docker-credential-acr-env" then
          .osv == "GO-2026-6225"
        else false end;
      [.[] | select(.finding) | .finding | select(.trace[0].function != null) |
       select(($name == "flyctl" and $sha == "203d7369ecb26c9adecadb501cd95682decdb527" and reviewed) | not)] | length == 0
    ' "${inputs[@]}" >/dev/null
fi
