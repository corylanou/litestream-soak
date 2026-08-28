#!/usr/bin/env bash
# Pull the newest Litestream pprof captures off a soak worker via flyctl.
#
#   scripts/pull-profiles.sh <source> <profile> [count]
#
# Example: scripts/pull-profiles.sh pr-1483 many-dbs-100-dir 3
#
# Downloads the newest <count> (default 3) heap, allocs, and CPU captures from
# /data/profiles on worker-<source>-<profile> into tmp/profiles/<source>/<profile>/
# and prints pprof commands, including -diff_base comparisons against the same
# profile from main when tmp/profiles/main/<profile>/ has captures.
set -euo pipefail

source_name="${1:-}"
profile_name="${2:-}"
count="${3:-3}"
app="${SOAK_FLY_APP:-litestream-soak}"

if [ -z "$source_name" ] || [ -z "$profile_name" ]; then
  printf 'usage: %s <source> <profile> [count]\n' "$0" >&2
  exit 2
fi
if ! [[ "$count" =~ ^[0-9]+$ ]] || [ "$count" -lt 1 ]; then
  printf 'count must be a positive integer, got %q\n' "$count" >&2
  exit 2
fi

if [ -z "${FLY_ACCESS_TOKEN:-}" ] && [ -f "$HOME/.fly/config.yml" ]; then
  FLY_ACCESS_TOKEN="$(sed -n 's/^access_token: *//p' "$HOME/.fly/config.yml" | tr -d '"' | head -1)"
  export FLY_ACCESS_TOKEN
fi
if [ -z "${FLY_ACCESS_TOKEN:-}" ]; then
  echo "FLY_ACCESS_TOKEN is not set and ~/.fly/config.yml has no access_token; run 'fly auth login'" >&2
  exit 1
fi

# <profile> may be the worker-name suffix (low-vol, high-vol, gharchive, ...)
# or the profile name (low-volume, high-volume, gharchive-replay, ...). Try the
# suffix first; fall back to resolving the profile name through the control
# plane when SOAK_BASIC_AUTH_USERNAME/PASSWORD are set (e.g. via .envrc).
machines_json="$(fly machines list -a "$app" --json)"
resolve_machine() {
  printf '%s' "$machines_json" | python3 -c '
import json, sys
name = sys.argv[1]
for m in json.load(sys.stdin):
    if m.get("name") == name and m.get("state") in ("started", "running"):
        print(m["id"]); break
' "$1"
}
worker="worker-${source_name}-${profile_name}"
machine="$(resolve_machine "$worker")"
if [ -z "$machine" ] && [ -n "${SOAK_BASIC_AUTH_USERNAME:-}" ] && [ -n "${SOAK_BASIC_AUTH_PASSWORD:-}" ]; then
  ctl="${SOAK_CONTROL_URL:-https://litestream-soak-ctl.fly.dev}"
  resolved="$(curl -sf --user "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" "$ctl/api/workers" | python3 -c '
import json, sys
source, profile = sys.argv[1], sys.argv[2]
d = json.load(sys.stdin)
rows = d if isinstance(d, list) else (d.get("workers") or d.get("items") or [])
for w in rows:
    if w.get("source") == source and w.get("profile_name") == profile and w.get("status") == "running":
        print(w["name"]); break
' "$source_name" "$profile_name" || true)"
  if [ -n "$resolved" ]; then
    worker="$resolved"
    machine="$(resolve_machine "$worker")"
  fi
fi
if [ -z "$machine" ]; then
  printf 'no started machine for source %s profile %s in app %s (tried %s)\n' "$source_name" "$profile_name" "$app" "$worker" >&2
  exit 1
fi

dest="tmp/profiles/${source_name}/${profile_name}"
mkdir -p "$dest"

# Newest <count> of each capture kind (heap, allocs, cpu), by name (timestamp-prefixed).
files="$(fly ssh console -a "$app" --machine "$machine" -C "sh -c 'ls /data/profiles'" 2>/dev/null \
  | tr -d '\r' | grep -E '^[0-9]{8}T[0-9]{6}Z_' || true)"
if [ -z "$files" ]; then
  printf 'no captures under /data/profiles on %s (%s)\n' "$worker" "$machine" >&2
  exit 1
fi

selected=""
for kind in _heap.pprof _allocs.pprof _cpu_profile.pprof; do
  picked="$(printf '%s\n' "$files" | grep -F -- "$kind" | sort | tail -n "$count" || true)"
  selected="$(printf '%s\n%s' "$selected" "$picked")"
done
selected="$(printf '%s\n' "$selected" | sed '/^$/d')"
if [ -z "$selected" ]; then
  printf 'no heap/allocs/cpu captures on %s yet (found: %s)\n' "$worker" "$(printf '%s' "$files" | tr '\n' ' ')" >&2
  exit 1
fi

printf 'worker %s machine %s -> %s\n' "$worker" "$machine" "$dest"
while IFS= read -r file; do
  [ -z "$file" ] && continue
  if [ -s "$dest/$file" ]; then
    printf '  have %s\n' "$file"
    continue
  fi
  printf '  get  %s\n' "$file"
  # Download into a scratch dir and move into place only when complete, so an
  # interrupted transfer never masquerades as a finished capture on retry.
  scratch="$(mktemp -d "${TMPDIR:-/tmp}/pull-profiles.XXXXXX")"
  if (cd "$scratch" && fly ssh sftp get -a "$app" --machine "$machine" "/data/profiles/$file" >/dev/null 2>&1) \
    && [ -s "$scratch/$file" ]; then
    mv "$scratch/$file" "$dest/$file"
    rm -rf "$scratch"
  else
    rm -rf "$scratch"
    printf 'download failed: %s\n' "$file" >&2
    exit 1
  fi
done <<< "$selected"

# newest <dir> <suffix>: lexically last match (names are timestamp-prefixed).
newest() {
  local match=""
  for candidate in "$1"/*"$2"; do
    [ -e "$candidate" ] && match="$candidate"
  done
  printf '%s' "$match"
}
heap="$(newest "$dest" _heap.pprof)"
cpu="$(newest "$dest" _cpu_profile.pprof)"

echo
echo "pprof commands:"
[ -n "$heap" ] && printf '  go tool pprof -top -sample_index=inuse_space -nodecount=20 %s\n' "$heap"
[ -n "$cpu" ] && printf '  go tool pprof -top -nodecount=20 %s\n' "$cpu"
if [ "$source_name" != "main" ]; then
  base_heap="$(newest "tmp/profiles/main/${profile_name}" _heap.pprof)"
  if [ -n "$base_heap" ] && [ -n "$heap" ]; then
    printf '  go tool pprof -top -sample_index=inuse_space -nodecount=20 -diff_base=%s %s\n' "$base_heap" "$heap"
  else
    printf '  (pull main first for a diff: %s main %s)\n' "$0" "$profile_name"
  fi
fi
