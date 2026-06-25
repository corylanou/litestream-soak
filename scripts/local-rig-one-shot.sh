#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scenario="${1:-}"
ref="${2:-main}"
runs="${3:-1}"

if [ -z "$scenario" ]; then
  printf 'usage: %s <scenario> [litestream-ref] [runs]\n' "$0" >&2
  exit 2
fi

case "$scenario" in
  compaction-source-stream-drop|uploadpart-retry-quota|provider-http-408|provider-request-canceled|constrained-disk) ;;
  *)
    printf 'unknown scenario: %s\n' "$scenario" >&2
    exit 2
    ;;
esac

sha="$("$root/scripts/resolve-litestream-sha.sh" "$ref")"
cache_dir="$root/.local-rig/one-shot"
cache_key="$scenario-$sha"
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
cat >"$mod_dir/go.mod" <<EOF
module litestream-local-rig-one-shot

go 1.25

require github.com/benbjohnson/litestream v0.0.0

replace github.com/benbjohnson/litestream => $src_dir
EOF

(
  cd "$mod_dir"
  go mod tidy
)

if [ "$scenario" != "constrained-disk" ]; then
  docker compose -f "$root/docker-compose.yml" up -d minio minio-init >/dev/null
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="$results_dir/${scenario}-${sha:0:12}-$stamp.jsonl"

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

for run_id in $(seq 1 "$runs"); do
  if [ "$scenario" = "constrained-disk" ]; then
    run_constrained "$run_id" | tee -a "$result_file"
  else
    run_host "$run_id" | tee -a "$result_file"
  fi
done

printf 'result_file=%s\n' "$result_file"
printf 'litestream_sha=%s\n' "$sha"
printf 'litestream_bin=%s\n' "$bin_dir/litestream"
