#!/usr/bin/env bash
set -euo pipefail

repository="${1:-}"
candidate_ref="${2:-}"
candidate_sha="${3:-}"
baseline_sha="${4:-}"
duration="${5:-72h}"
tail="${6:-false}"
base_url="${CONTROL_BASE_URL:-https://litestream-soak-ctl.fly.dev}"

if [ "$#" -lt 4 ]; then
  echo "usage: $0 <repository> <candidate-ref> <candidate-sha> <baseline-sha> [duration] [tail]" >&2
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
  '{
    repository: $repository,
    candidate_ref: $candidate_ref,
    candidate_sha: $candidate_sha,
    baseline_sha: $baseline_sha,
    duration: $duration,
    tail: $tail
  }')"

curl -sS --fail-with-body --max-time 60 -X POST \
  "${auth_args[@]}" \
  -H "Content-Type: application/json" \
  "${base_url}/api/admin/rc-gates" \
  -d "$payload"
