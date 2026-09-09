#!/usr/bin/env bash
set -euo pipefail

sha="${1:-}"
source_name="${2:-main}"
trigger="${3:-deploy_ready}"
image_ref="${4:-}"
litestream_sha="${5:-}"
repository="${6:-}"
workload_sha="${7:-${WORKLOAD_SHA:-}}"
base_url="${CONTROL_BASE_URL:-https://litestream-soak-ctl.fly.dev}"

if [ "$#" -lt 5 ] || [ -z "$sha" ] || [ -z "$image_ref" ] || [ -z "$litestream_sha" ]; then
  echo "sha, image-ref, and litestream-sha are required" >&2
  echo "usage: $0 <sha> <source> <trigger> <image-ref> <litestream-sha> [repository] [workload-sha]" >&2
  exit 1
fi

if [[ "${source_name}" =~ ^pr-[1-9][0-9]*$ ]]; then
  if [[ ! "${repository}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
    echo "repository in owner/name format is required for PR sources" >&2
    exit 1
  fi
elif [ -n "${repository}" ]; then
  echo "repository is only valid for PR sources" >&2
  exit 1
fi

if [ -z "$workload_sha" ]; then
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  dockerfile="$script_dir/../Dockerfile.worker"
  workload_sha="$(awk -F= '/^ARG WORKLOAD_SHA=/ { print $2; exit }' "$dockerfile")"
  if [ -z "$workload_sha" ] && awk '
    /^COPY --from=litestream-builder \/usr\/local\/bin\/litestream-test \/usr\/local\/bin\/litestream-test$/ { shared = 1 }
    /^ARG WORKLOAD_SHA/ { split_build = 1 }
    END { exit !(shared && !split_build) }
  ' "$dockerfile"; then
    workload_sha="$litestream_sha"
  fi
fi
if [[ ! "$workload_sha" =~ ^[0-9a-fA-F]{40}$ ]]; then
  echo "a full workload-sha from the worker image build is required" >&2
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
  --arg repository "$repository" \
  --arg workload_sha "$workload_sha" \
  '{
    sha: $sha,
    source: $source,
    trigger: $trigger,
    image_ref: $image_ref,
    litestream_sha: $litestream_sha,
    repository: $repository,
    workload_sha: $workload_sha
  }')"

curl -sS --fail-with-body --max-time 180 -X POST \
  "${auth_args[@]}" \
  -H "Content-Type: application/json" \
  "${base_url}/api/admin/deployments/ready" \
  -d "$payload"
