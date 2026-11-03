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
if [ "${request_timeout}" -le 0 ]; then
  echo "GATE_REGISTER_TIMEOUT must be positive; using 300" >&2
  request_timeout=300
fi
if [ "${register_deadline}" -le 0 ]; then
  echo "GATE_REGISTER_DEADLINE must be positive; using 900" >&2
  register_deadline=900
fi
if [ "${poll_attempts}" -le 0 ]; then
  poll_attempts=1
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
# The ledger reports the registered request in the control plane's canonical
# form: profiles deduplicated and the duration as Go's Duration.String()
# (24h -> 24h0m0s, 60m -> 1h0m0s, 90m -> 1h30m0s), so normalize the request
# the same way before comparing. An input that is not whole h/m/s units is
# compared verbatim.
jq_go_duration='def go_duration:
  . as $raw
  | [scan("([0-9]+)(h|m|s)")] as $parts
  | if ($parts | length) == 0 or (($parts | map(.[0] + .[1]) | join("")) != $raw) then $raw
    else ($parts | map((.[0] | tonumber) * ({"h": 3600, "m": 60, "s": 1}[.[1]])) | add) as $total
      | (($total / 3600) | floor) as $h
      | ((($total - $h * 3600) / 60) | floor) as $m
      | ($total - $h * 3600 - $m * 60) as $s
      | if $h > 0 then "\($h)h\($m)m\($s)s" elif $m > 0 then "\($m)m\($s)s" else "\($s)s" end
    end;'
ledger_duration="$(jq -rn --arg duration "${duration}" "${jq_go_duration} \$duration | go_duration")"
ledger_profiles="$(jq -cn --arg profiles "${profiles}" '$profiles | split(",") | map(select(. != "")) | unique')"
# An upgrade gate registered without a warm-up gets the control plane's 2h
# default, and the ledger reports it like the duration.
ledger_warmup=""
if [ "${kind}" = "upgrade" ]; then
  ledger_warmup="$(jq -rn --arg warmup "${upgrade_warmup:-2h}" "${jq_go_duration} \$warmup | go_duration")"
fi

body_file="$(mktemp)"
trap 'rm -f "${body_file}"' EXIT
started_at="$(date +%s)"
sources="${candidate_source} / ${baseline_source}"

# remaining prints the seconds left in the registration budget and exits the
# script once it is spent, so no request or sleep can outlive the deadline.
remaining() {
  local left=$(( register_deadline - ($(date +%s) - started_at) ))
  if [ "${left}" -le 0 ]; then
    echo "rc gate registration for ${sources} did not complete within ${register_deadline}s" >&2
    exit 1
  fi
  echo "${left}"
}
capped() { local left; left="$(remaining)"; if [ "$1" -lt "${left}" ]; then echo "$1"; else echo "${left}"; fi; }

# ledger_gate prints the most recent gate, open or closed, that owns this
# run's sources (the ledger lists newest first), or nothing when the ledger
# was read and has none. It fails when the ledger could not be read.
ledger_gate() {
  local code
  : > "${body_file}"
  code="$(curl -sS --max-time "$(capped "${request_timeout}")" -o "${body_file}" -w '%{http_code}' \
    "${auth_args[@]}" "${base_url}/api/rc-gates" 2>/dev/null)" || return 1
  [ "${code}" = "200" ] || return 1
  jq -c --arg candidate "${candidate_source}" --arg baseline "${baseline_source}" \
    '[.gates[]? | select(.candidate_source == $candidate and .baseline_source == $baseline)] | first // empty' \
    "${body_file}" 2>/dev/null
}

# ledger_gate_matches reports whether the gate found in the ledger is the one
# this request describes, comparing the same fields the control plane's
# idempotent re-registration compares; a different gate on the same sources
# would be refused by it with 409.
ledger_gate_matches() {
  jq -e --arg candidate_sha "${candidate_sha}" --arg baseline_sha "${baseline_sha}" --arg duration "${ledger_duration}" \
    --arg kind "${kind}" --arg lane_variant "${lane_variant}" --arg warmup "${ledger_warmup}" \
    --argjson tail "${tail}" --argjson observe_proxy "${observe_proxy}" --argjson profiles "${ledger_profiles}" \
    '.candidate_sha == $candidate_sha and .baseline_sha == $baseline_sha and .duration == $duration and .kind == $kind
      and ((.lane_variant // "") == $lane_variant) and ((.upgrade_warmup // "") == $warmup)
      and .tail == $tail and ((.observe_proxy // false) == $observe_proxy)
      and (((.profiles // []) | unique) == $profiles)' \
    >/dev/null <<<"$1"
}

attempt=0
while :; do
  attempt=$((attempt + 1))
  : > "${body_file}"
  curl_exit=0
  code="$(curl -sS --max-time "$(capped "${request_timeout}")" -o "${body_file}" -w '%{http_code}' -X POST \
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
  echo "rc gate registration attempt ${attempt} did not complete (curl exit ${curl_exit}, HTTP ${code:-none}); checking the ledger for ${sources}" >&2

  # The control plane may have registered the gate after the client gave up:
  # the request that produced gate 21 timed out client-side but succeeded
  # server-side, and the workflow then never sent the deployment-ready
  # notifications that provision the workers. The POST is only retried once
  # the ledger has been read and shows no gate at all on these sources: the
  # control plane keeps sources unique only while a gate is open, so retrying
  # against a closed gate would register a second one.
  ledger_read=false
  poll=0
  while [ "${poll}" -lt "${poll_attempts}" ]; do
    poll=$((poll + 1))
    if gate="$(ledger_gate)"; then
      ledger_read=true
      if [ -n "${gate}" ]; then
        gate_id="$(jq -r '.id' <<<"${gate}")"
        gate_status="$(jq -r '.status' <<<"${gate}")"
        case "${gate_status}" in
          running|tail)
            if ledger_gate_matches "${gate}"; then
              echo "rc gate ${gate_id} (${gate_status}) for ${sources} is registered; continuing" >&2
              printf '%s\n' "${gate}"
              exit 0
            fi
            echo "rc gate ${gate_id} (${gate_status}) already owns ${sources} but does not match this request; not registering another:" >&2
            ;;
          *)
            echo "rc gate ${gate_id} is ${gate_status} and still owns ${sources}; a new POST would register a second gate on them, refusing:" >&2
            ;;
        esac
        printf '%s\n' "${gate}" >&2
        exit 1
      fi
    fi
    if [ "${poll}" -lt "${poll_attempts}" ]; then
      sleep "$(capped "${poll_interval}")"
    fi
  done
  if [ "${ledger_read}" != "true" ]; then
    echo "could not read the rc gate ledger after ${poll_attempts} attempts; refusing to retry the registration without knowing whether a gate for ${sources} exists" >&2
    exit 1
  fi
  sleep "$(capped "${poll_interval}")"
done
