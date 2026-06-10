#!/usr/bin/env bash
set -euo pipefail

pr_number="${1:-}"
repo_full_name="${2:-benbjohnson/litestream}"
pr_sha="${3:-}"
allowed_repos="${SOAK_PR_REPO_ALLOWLIST:-benbjohnson/litestream}"

if [ -z "${pr_number}" ]; then
  echo "usage: $0 <pr-number> [repo-full-name] [pr-head-sha]" >&2
  exit 1
fi

case ",${allowed_repos}," in
  *,"${repo_full_name}",*) ;;
  *)
    echo "repo ${repo_full_name} is not in SOAK_PR_REPO_ALLOWLIST (${allowed_repos})" >&2
    exit 1
    ;;
esac

if [ -z "${pr_sha}" ]; then
  pr_sha="$(gh api "repos/${repo_full_name}/pulls/${pr_number}" --jq .head.sha)"
fi
if [ -z "${pr_sha}" ]; then
  echo "failed to resolve PR head SHA" >&2
  exit 1
fi

soak_sha="$(git rev-parse HEAD)"
short_soak_sha="${soak_sha::12}"
source_name="pr-${pr_number}"
image_label="sha-${short_soak_sha}-pr-${pr_number}-ls-${pr_sha::12}"
log_file="$(mktemp)"
trap 'rm -f "${log_file}"' EXIT

fly deploy \
  --config fly.toml \
  --app litestream-soak \
  --build-only \
  --push \
  --build-arg "LITESTREAM_SHA=${pr_sha}" \
  --image-label "${image_label}" 2>&1 | tee "${log_file}"

image_ref="$(awk '/^image:/{print $2}' "${log_file}" | tail -n 1)"
if [ -z "${image_ref}" ]; then
  echo "failed to parse image ref from fly deploy output" >&2
  exit 1
fi

scripts/notify-deployment-ready.sh \
  "${soak_sha}" \
  "${source_name}" \
  manual_pr_soak \
  "${image_ref}" \
  "${pr_sha}"
