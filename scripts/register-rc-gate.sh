#!/usr/bin/env bash
set -euo pipefail

repository="${1:-}"
candidate_ref="${2:-}"
candidate_sha="${3:-}"
baseline_sha="${4:-}"
duration="${5:-72h}"
tail="${6:-false}"
kind="${7:-rc}"
upgrade_warmup="${8:-}"
lane_variant="${9:-}"
profiles="${GATE_PROFILES:-}"
profiles="${profiles//[[:space:]]/}"
source_suffix="${GATE_SOURCE_SUFFIX:-}"
base_url="${CONTROL_BASE_URL:-https://litestream-soak-ctl.fly.dev}"

if [ "$#" -lt 4 ]; then
  echo "usage: $0 <repository> <candidate-ref> <candidate-sha> <baseline-sha> [duration] [tail] [kind] [upgrade-warmup] [lane-variant]" >&2
  echo "  GATE_PROFILES=<comma-separated profiles> runs a subset of the gate fleet (default: all)" >&2
  echo "  GATE_SOURCE_SUFFIX=<1-8 lowercase alphanumerics> names the sources <prefix>-<sha8>-<suffix>" >&2
  exit 1
fi
if [ -n "${profiles}" ] && [[ ! "${profiles}" =~ ^[a-z0-9-]+(,[a-z0-9-]+)*$ ]]; then
  echo "GATE_PROFILES must be a comma-separated list of profile names" >&2
  exit 1
fi
if [ -n "${source_suffix}" ] && [[ ! "${source_suffix}" =~ ^[0-9a-z]{1,8}$ ]]; then
  echo "GATE_SOURCE_SUFFIX must be 1 to 8 lowercase letters or digits" >&2
  exit 1
fi
if [[ ! "${repository}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
  echo "repository must use owner/name format" >&2
  exit 1
fi
for sha in "${candidate_sha}" "${baseline_sha}"; do
  if [[ ! "${sha}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "candidate and baseline SHAs must be pinned 40-character lowercase hex commit SHAs" >&2
    exit 1
  fi
done
if [ "${tail}" != "true" ] && [ "${tail}" != "false" ]; then
  echo "tail must be true or false" >&2
  exit 1
fi
if [ "${kind}" != "rc" ] && [ "${kind}" != "upgrade" ]; then
  echo "kind must be rc or upgrade" >&2
  exit 1
fi
if [ -n "${upgrade_warmup}" ] && [[ ! "${upgrade_warmup}" =~ ^[1-9][0-9]*[mh]$ ]]; then
  echo "upgrade-warmup must be whole minutes or hours, such as 30m or 2h" >&2
  exit 1
fi
if [ -n "${lane_variant}" ] && [ "${lane_variant}" != "nol9" ]; then
  echo "lane-variant must be empty or nol9" >&2
  exit 1
fi

if [ -n "${SOAK_ADMIN_BEARER_TOKEN:-}" ]; then
  auth_args=(-H "Authorization: Bearer ${SOAK_ADMIN_BEARER_TOKEN}")
elif [ -n "${SOAK_BASIC_AUTH_USERNAME:-}" ] && [ -n "${SOAK_BASIC_AUTH_PASSWORD:-}" ]; then
  auth_args=(-u "${SOAK_BASIC_AUTH_USERNAME}:${SOAK_BASIC_AUTH_PASSWORD}")
else
  echo "set SOAK_ADMIN_BEARER_TOKEN or SOAK_BASIC_AUTH_USERNAME/SOAK_BASIC_AUTH_PASSWORD" >&2
  exit 1
fi

payload="$(jq -n \
  --arg repository "$repository" \
  --arg candidate_ref "$candidate_ref" \
  --arg candidate_sha "$candidate_sha" \
  --arg baseline_sha "$baseline_sha" \
  --arg duration "$duration" \
  --argjson tail "$tail" \
  --arg kind "$kind" \
  --arg upgrade_warmup "$upgrade_warmup" \
  --arg lane_variant "$lane_variant" \
  --arg profiles "$profiles" \
  --arg source_suffix "$source_suffix" \
  '{
    repository: $repository,
    candidate_ref: $candidate_ref,
    candidate_sha: $candidate_sha,
    baseline_sha: $baseline_sha,
    duration: $duration,
    tail: $tail,
    kind: $kind
  }
  + (if $upgrade_warmup == "" then {} else {upgrade_warmup: $upgrade_warmup} end)
  + (if $lane_variant == "" then {} else {lane_variant: $lane_variant} end)
  + (if $profiles == "" then {} else {profiles: ($profiles | split(","))} end)
  + (if $source_suffix == "" then {} else {source_suffix: $source_suffix} end)')"

curl -sS --fail-with-body --max-time 60 -X POST \
  "${auth_args[@]}" \
  -H "Content-Type: application/json" \
  "${base_url}/api/admin/rc-gates" \
  -d "$payload"
