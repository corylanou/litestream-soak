#!/usr/bin/env bash
set -euo pipefail

sha="${1:-}"
source_name="${2:-main}"
trigger="${3:-deploy_ready}"
image_ref="${4:-}"
litestream_sha="${5:-}"
base_url="${CONTROL_BASE_URL:-https://litestream-soak-ctl.fly.dev}"

if [ -z "$sha" ]; then
  echo "usage: $0 <sha> [source] [trigger] [image-ref] [litestream-sha]" >&2
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
  --arg sha "$sha" \
  --arg source "$source_name" \
  --arg trigger "$trigger" \
  --arg image_ref "$image_ref" \
  --arg litestream_sha "$litestream_sha" \
  '{
    sha: $sha,
    source: $source,
    trigger: $trigger
  }
  + (if $image_ref == "" then {} else {image_ref: $image_ref} end)
  + (if $litestream_sha == "" then {} else {litestream_sha: $litestream_sha} end)')"

curl -sS --fail-with-body --max-time 180 -X POST \
  "${auth_args[@]}" \
  -H "Content-Type: application/json" \
  "${base_url}/api/admin/deployments/ready" \
  -d "$payload"
