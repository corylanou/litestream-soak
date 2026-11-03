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
observe_proxy="${GATE_OBSERVE_PROXY:-false}"
base_url="${CONTROL_BASE_URL:-https://litestream-soak-ctl.fly.dev}"
request_timeout="${GATE_REGISTER_TIMEOUT:-300}"
register_deadline="${GATE_REGISTER_DEADLINE:-900}"
poll_interval="${GATE_REGISTER_POLL_INTERVAL:-15}"
poll_attempts="${GATE_REGISTER_POLL_ATTEMPTS:-4}"

if [ "$#" -lt 4 ]; then
  echo "usage: $0 <repository> <candidate-ref> <candidate-sha> <baseline-sha> [duration] [tail] [kind] [upgrade-warmup] [lane-variant]" >&2
  echo "  GATE_PROFILES=<comma-separated profiles> runs a subset of the gate fleet (default: all)" >&2
  echo "  GATE_SOURCE_SUFFIX=<1-8 lowercase alphanumerics> names the sources <prefix>-<sha8>-<suffix>" >&2
  echo "  GATE_OBSERVE_PROXY=true meters every gate worker's replication through the S3 observe proxy" >&2
  echo "  GATE_REGISTER_TIMEOUT / GATE_REGISTER_DEADLINE (seconds) bound one request and the whole registration" >&2
  exit 1
fi
if [ "${observe_proxy}" != "true" ] && [ "${observe_proxy}" != "false" ]; then
  echo "GATE_OBSERVE_PROXY must be true or false" >&2
  exit 1
fi
for setting in "GATE_REGISTER_TIMEOUT=${request_timeout}" "GATE_REGISTER_DEADLINE=${register_deadline}" "GATE_REGISTER_POLL_INTERVAL=${poll_interval}" "GATE_REGISTER_POLL_ATTEMPTS=${poll_attempts}"; do
  if [[ ! "${setting#*=}" =~ ^[0-9]+$ ]]; then
    echo "${setting%%=*} must be a whole number of seconds (or attempts)" >&2
    exit 1
  fi
done
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
  --argjson observe_proxy "$observe_proxy" \
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
  + (if $source_suffix == "" then {} else {source_suffix: $source_suffix} end)
  + (if $observe_proxy then {observe_proxy: true} else {} end)')"

case "${kind}:${lane_variant}" in
  rc:*) candidate_prefix="rc"; baseline_prefix="rcbase" ;;
  upgrade:) candidate_prefix="up"; baseline_prefix="upbase" ;;
  upgrade:*) candidate_prefix="up-${lane_variant}"; baseline_prefix="upbase-${lane_variant}" ;;
esac
candidate_source="${candidate_prefix}-${candidate_sha:0:8}${source_suffix:+-${source_suffix}}"
baseline_source="${baseline_prefix}-${baseline_sha:0:8}${source_suffix:+-${source_suffix}}"
# The ledger reports Go's Duration.String() form (24h -> 24h0m0s, 30m -> 30m0s).
case "${duration}" in
  *h) ledger_duration="${duration}0m0s" ;;
  *m) ledger_duration="${duration}0s" ;;
  *) ledger_duration="${duration}" ;;
esac

body_file="$(mktemp)"
trap 'rm -f "${body_file}"' EXIT
started_at="$(date +%s)"
elapsed() { echo $(( $(date +%s) - started_at )); }

# ledger_gate prints the open gate that owns this run's sources, or nothing.
ledger_gate() {
  local code
  : > "${body_file}"
  code="$(curl -sS --max-time "${request_timeout}" -o "${body_file}" -w '%{http_code}' \
    "${auth_args[@]}" "${base_url}/api/rc-gates" 2>/dev/null)" || return 1
  [ "${code}" = "200" ] || return 1
  jq -c --arg candidate "${candidate_source}" --arg baseline "${baseline_source}" \
    '[.gates[]? | select(.candidate_source == $candidate and .baseline_source == $baseline and (.status == "running" or .status == "tail"))] | first // empty' \
    "${body_file}" 2>/dev/null
}

# ledger_gate_matches reports whether the gate found in the ledger is the one
# this request describes; a different gate on the same sources would also be
# refused by the control plane's idempotent re-registration with 409.
ledger_gate_matches() {
  jq -e --arg candidate_sha "${candidate_sha}" --arg baseline_sha "${baseline_sha}" --arg duration "${ledger_duration}" \
    --arg kind "${kind}" --argjson tail "${tail}" --argjson observe_proxy "${observe_proxy}" --arg profiles "${profiles}" \
    '.candidate_sha == $candidate_sha and .baseline_sha == $baseline_sha and .duration == $duration and .kind == $kind
      and .tail == $tail and ((.observe_proxy // false) == $observe_proxy)
      and (((.profiles // []) | sort) == (if $profiles == "" then [] else ($profiles | split(",") | sort) end))' \
    >/dev/null <<<"$1"
}

attempt=0
while :; do
  attempt=$((attempt + 1))
  : > "${body_file}"
  curl_exit=0
  code="$(curl -sS --max-time "${request_timeout}" -o "${body_file}" -w '%{http_code}' -X POST \
    "${auth_args[@]}" \
    -H "Content-Type: application/json" \
    "${base_url}/api/admin/rc-gates" \
    -d "$payload")" || curl_exit=$?
  case "${curl_exit}:${code}" in
    0:2*)
      cat "${body_file}"
      exit 0
      ;;
    0:4*)
      echo "rc gate registration refused (HTTP ${code}):" >&2
      cat "${body_file}" >&2
      exit 1
      ;;
  esac
  echo "rc gate registration attempt ${attempt} did not complete (curl exit ${curl_exit}, HTTP ${code:-none}); checking the ledger for ${candidate_source} / ${baseline_source}" >&2

  # The control plane may have registered the gate after the client gave up:
  # the request that produced gate 21 timed out client-side but succeeded
  # server-side, and the workflow then never sent the deployment-ready
  # notifications that provision the workers. Find it before retrying.
  poll=0
  while [ "${poll}" -lt "${poll_attempts}" ]; do
    poll=$((poll + 1))
    gate="$(ledger_gate || true)"
    if [ -n "${gate}" ]; then
      if ledger_gate_matches "${gate}"; then
        echo "rc gate for ${candidate_source} / ${baseline_source} is registered; continuing" >&2
        printf '%s\n' "${gate}"
        exit 0
      fi
      echo "an open gate already owns ${candidate_source} / ${baseline_source} but does not match this request:" >&2
      printf '%s\n' "${gate}" >&2
      exit 1
    fi
    if [ "$(elapsed)" -ge "${register_deadline}" ]; then
      break
    fi
    sleep "${poll_interval}"
  done
  if [ "$(elapsed)" -ge "${register_deadline}" ]; then
    echo "rc gate registration for ${candidate_source} / ${baseline_source} did not complete within ${register_deadline}s" >&2
    exit 1
  fi
done
