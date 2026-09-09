#!/usr/bin/env bash
set -euo pipefail
export GOTOOLCHAIN=go1.25.13

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scenario="${1:-}"
ref="${2:-main}"
runs="${3:-1}"

if [ -z "$scenario" ]; then
  printf 'usage: %s <scenario> [litestream-ref] [runs]\n' "$0" >&2
  exit 2
fi

case "$scenario" in
  offline-backlog|compaction-source-stream-drop|uploadpart-retry-quota|provider-http-408|provider-request-canceled|constrained-disk|l0-gap-heal|snapshot-compaction-overlap|restore-retention-race) ;;
  *)
    printf 'unknown scenario: %s\n' "$scenario" >&2
    exit 2
    ;;
esac

if [ "$scenario" = "offline-backlog" ]; then
  if [ -z "${ONE_SHOT_PROVIDER_CLASS:-}" ] && [ -z "${S3_ENDPOINT:-}" ]; then
    export ONE_SHOT_PROVIDER_CLASS=local-emulator
  fi
  case "${ONE_SHOT_PROVIDER_CLASS:-}" in
    local-emulator|real-provider) ;;
    *) printf 'offline-backlog requires ONE_SHOT_PROVIDER_CLASS=local-emulator or real-provider for an explicit endpoint\n' >&2; exit 2 ;;
  esac
fi

sha="$("$root/scripts/resolve-litestream-sha.sh" "$ref")"
cache_dir="$root/.local-rig/one-shot"
cache_key="$scenario-$sha-$GOTOOLCHAIN"
src_dir="$cache_dir/litestream-src/$cache_key"
mod_dir="$cache_dir/harness/$cache_key"
bin_dir="$cache_dir/litestream-bin/$cache_key"
results_dir="$cache_dir/results"
endpoint="${S3_ENDPOINT:-http://$(hostname):9000}"
bucket="${S3_BUCKET:-litestream-soak}"
container_arch="${ONE_SHOT_DOCKER_ARCH:-$(go env GOHOSTARCH)}"

mkdir -p "$cache_dir/litestream-src" "$mod_dir" "$bin_dir" "$results_dir"

if [ ! -d "$src_dir/.git" ]; then
  git clone https://github.com/benbjohnson/litestream.git "$src_dir"
fi

git -C "$src_dir" fetch --quiet origin "$sha"
git -C "$src_dir" checkout --quiet "$sha"

if [ ! -x "$bin_dir/litestream" ] || [ ! -x "$bin_dir/litestream-test" ]; then
  (
    cd "$src_dir"
    go build \
      -ldflags "-s -w -X 'main.Version=$sha'" \
      -tags osusergo,netgo,sqlite_omit_load_extension \
      -o "$bin_dir/litestream" ./cmd/litestream
    CGO_CFLAGS="-DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0" go build \
      -ldflags "-s -w -X 'main.Version=$sha'" \
      -o "$bin_dir/litestream-test" ./cmd/litestream-test
  )
fi

cp "$root/scripts/local-rig-one-shot/main.go.tmpl" "$mod_dir/main.go"
cp "$root/scripts/local-rig-one-shot/main_test.go.tmpl" "$mod_dir/main_test.go"
cp "$root/scripts/local-rig-one-shot/recovery.go.tmpl" "$mod_dir/recovery.go"
cp "$root/scripts/local-rig-one-shot/recovery_test.go.tmpl" "$mod_dir/recovery_test.go"
cat >"$mod_dir/go.mod" <<EOF
module github.com/corylanou/litestream-soak/local-rig-one-shot

go 1.25.13

require github.com/benbjohnson/litestream v0.0.0
require github.com/corylanou/litestream-soak v0.0.0

replace github.com/benbjohnson/litestream => $src_dir
replace github.com/corylanou/litestream-soak => $root
EOF

(
  cd "$mod_dir"
  go mod tidy
  go test .
)

if [ "$scenario" != "constrained-disk" ] && [ "${ONE_SHOT_PROVIDER_CLASS:-local-emulator}" = "local-emulator" ] && [ "${ONE_SHOT_SKIP_EMULATOR_START:-0}" != "1" ]; then
  docker compose -f "$root/docker-compose.yml" up -d minio minio-init >/dev/null
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$results_dir/${scenario}-${sha:0:12}-${GOTOOLCHAIN}-$stamp.jsonl"
toolchain_metadata="${result_file%.jsonl}.buildinfo"
{
  go version
  printf 'litestream_sha=%s\n' "$sha"
  go version -m "$bin_dir/litestream" "$bin_dir/litestream-test"
} >"$toolchain_metadata"

run_host() {
  local run_id="$1"
  (
    cd "$mod_dir"
    AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}" \
      AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
      go run . \
      --scenario "$scenario" \
      --litestream-ref "$ref" \
      --litestream-sha "$sha" \
      --run "$run_id" \
      --work-dir "$cache_dir/work/$scenario/$sha/$stamp/$run_id" \
      --s3-endpoint "$endpoint" \
      --s3-bucket "$bucket" \
      --s3-prefix "one-shot/$scenario/$sha/$stamp/$run_id"
  )
}

run_constrained() {
  local run_id="$1"
  local binary="$mod_dir/one-shot-linux-$container_arch"
  (
    cd "$mod_dir"
    GOOS=linux GOARCH="$container_arch" CGO_ENABLED=0 go build -o "$binary" .
  )
  go version -m "$binary" >>"$toolchain_metadata"
  local container="litestream-one-shot-${scenario}-${sha:0:8}-$stamp-$run_id"
  docker run \
    --name "$container" \
    --mount "type=bind,source=$binary,target=/usr/local/bin/one-shot,readonly" \
    --mount "type=tmpfs,destination=/data,tmpfs-size=${ONE_SHOT_TMPFS_SIZE:-384m}" \
    --mount "type=volume,source=litestream-one-shot-replica-${sha:0:8}-$stamp-$run_id,target=/replica" \
    alpine:3.20 \
    /usr/local/bin/one-shot \
      --scenario "$scenario" \
      --litestream-ref "$ref" \
      --litestream-sha "$sha" \
      --run "$run_id" \
      --work-dir /data/work \
      --file-replica /replica
}

run_recovery_constrained() {
  local run_id="$1"
  local binary="$mod_dir/recovery-linux-$container_arch"
  (cd "$mod_dir" && GOOS=linux GOARCH="$container_arch" CGO_ENABLED=0 go build -o "$binary" .)
  go version -m "$binary" >>"$toolchain_metadata"
  local container="litestream-recovery-${sha:0:8}-$stamp-$run_id"
  local evidence_dir="$results_dir/${scenario}-${sha:0:12}-$stamp-$run_id-evidence"
  mkdir -p "$evidence_dir"
  local env_args=()
  for key in ONE_SHOT_PROVIDER_CLASS ONE_SHOT_RECOVERY_PHASE_SECONDS ONE_SHOT_RECOVERY_WRITE_RATE ONE_SHOT_RECOVERY_WORKERS ONE_SHOT_RECOVERY_PAYLOAD_BYTES ONE_SHOT_RECOVERY_TRUNCATE_PAGE_N ONE_SHOT_RECOVERY_PIN_HOLD_SECONDS ONE_SHOT_RECOVERY_PIN_PAUSE_SECONDS AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY; do
    env_args+=(--env "$key")
  done
  local run_status=0
  docker run \
    --name "$container" \
    --cpus "${ONE_SHOT_CPUS:-1}" \
    --memory "${ONE_SHOT_MEMORY:-512m}" \
    --memory-swap "${ONE_SHOT_MEMORY:-512m}" \
    --mount "type=bind,source=$binary,target=/usr/local/bin/one-shot,readonly" \
    --mount "type=tmpfs,destination=/data,tmpfs-size=${ONE_SHOT_TMPFS_SIZE:-384m}" \
    --mount "type=bind,source=$evidence_dir,target=/evidence" \
    --env ONE_SHOT_RECOVERY_EVIDENCE_DIR=/evidence \
    "${env_args[@]}" \
    alpine:3.20 /usr/local/bin/one-shot \
    --scenario "$scenario" --litestream-ref "$ref" --litestream-sha "$sha" \
    --run "$run_id" --work-dir /data/work \
    --s3-endpoint "$endpoint" --s3-bucket "$bucket" \
    --s3-prefix "one-shot/$scenario/$sha/$stamp/$run_id" || run_status=$?
  docker inspect --format '{{json .State}}' "$container" > "$evidence_dir/container-state.json" || run_status=1
  return "$run_status"
}

# A failing run no longer aborts the remaining runs: multi-run invocations are
# how flaky or statistical scenarios (restore-retention-race) get their
# counts. The exit status still reports whether every run passed.
status=0
for run_id in $(seq 1 "$runs"); do
  if [ "$scenario" = "offline-backlog" ] && [ "${ONE_SHOT_RECOVERY_CONTAINER:-0}" = "1" ]; then
    run_recovery_constrained "$run_id" | tee -a "$result_file" || status=1
  elif [ "$scenario" = "constrained-disk" ]; then
    run_constrained "$run_id" | tee -a "$result_file" || status=1
  else
    run_host "$run_id" | tee -a "$result_file" || status=1
  fi
done

printf 'toolchain_metadata=%s\n' "$toolchain_metadata"
printf 'result_file=%s\n' "$result_file"
printf 'litestream_sha=%s\n' "$sha"
printf 'litestream_bin=%s\n' "$bin_dir/litestream"
exit "$status"
