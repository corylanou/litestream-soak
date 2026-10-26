#!/usr/bin/env bash
set -euo pipefail

shards=(
  backend-file
  backend-minio
  backend-sftp
  backend-nats
  backend-webdav
  restore-modes
  manydb-100
  manydb-500
  manydb-1000
  large-db
)

usage() {
  printf 'usage: %s list | run <shard>\n' "$0" >&2
  exit 2
}

command="${1:-}"
case "$command" in
  list)
    printf '%s\n' "${shards[@]}"
    exit 0
    ;;
  run) ;;
  *) usage ;;
esac

shard="${2:-}"
[ -n "$shard" ] || usage

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${COVERAGE_OUT:-$root/.local/coverage/$shard}"
worker="${SOAKWORKER_BIN:-soakworker}"
checker="${SOAKCOVERAGE_BIN:-soakcoverage}"

backend=file
duration=1200
check_args=(-min-passed 4 -databases 1)
shard_env=()

case "$shard" in
  backend-file) ;;
  backend-minio)
    backend=minio
    shard_env+=(
      "REPLICA_URL=${COVERAGE_MINIO_URL:-http://127.0.0.1:9000}"
      "REPLICA_BUCKET=${COVERAGE_MINIO_BUCKET:-litestream-soak}"
      "REPLICA_ACCESS_KEY_ID=${COVERAGE_MINIO_ACCESS_KEY:-}"
      "REPLICA_SECRET_ACCESS_KEY=${COVERAGE_MINIO_SECRET_KEY:-}"
    )
    ;;
  backend-sftp)
    backend=sftp
    shard_env+=("REPLICA_URL=${COVERAGE_SFTP_URL:-sftp://soak@127.0.0.1:2222/upload}" "REPLICA_PASSWORD=${COVERAGE_SFTP_PASSWORD:-}")
    ;;
  backend-nats)
    backend=nats
    shard_env+=("REPLICA_URL=${COVERAGE_NATS_URL:-nats://127.0.0.1:4222}")
    ;;
  backend-webdav)
    backend=webdav
    shard_env+=("REPLICA_URL=${COVERAGE_WEBDAV_URL:-webdav://soak@127.0.0.1:8080}" "REPLICA_PASSWORD=${COVERAGE_WEBDAV_PASSWORD:-}")
    ;;
  restore-modes)
    duration=1500
    check_args=(-min-passed 6 -databases 1 -restore-modes txid,timestamp,latest)
    ;;
  manydb-100|manydb-500|manydb-1000)
    databases="${shard#manydb-}"
    case "$databases" in
      100) duration=2100 ;;
      500) duration=2700 ;;
      1000) duration=3600 ;;
    esac
    shard_env+=(
      "NUM_DATABASES=$databases"
      "CONFIG_MODE=dir"
      "ACTIVE_PERCENT=5"
      "ACTIVE_ROTATE_INTERVAL=2m"
      "VERIFY_SAMPLE_SIZE=5"
      "VERIFY_CHANGED_LIMIT=10"
      "WRITE_RATE=20"
      "PAYLOAD_SIZE=512"
      "INITIAL_SIZE=1MB"
    )
    check_args=(-min-passed 3 -count-batches -databases "$databases" -require-log "Rotated many database active set=3" -min-resource-samples 10)
    ;;
  large-db)
    duration=9000
    shard_env+=("INITIAL_SIZE=6GB" "SNAPSHOT_INTERVAL=1h" "VERIFY_INTERVAL=20m" "VERIFY_SYNC_TIMEOUT=60m" "WRITE_RATE=10")
    check_args=(-min-passed 3 -databases 1 -min-db-size-bytes 6000000000)
    ;;
  *)
    printf 'unknown shard: %s\n' "$shard" >&2
    exit 2
    ;;
esac
duration="${COVERAGE_DURATION:-$duration}"

rm -rf "$out"
mkdir -p "$out/data"
results="$out/results.jsonl"
log="$out/worker.log"
resources="$out/resources.jsonl"
: >"$resources"

ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

env_args=(
  "DATA_DIR=$out/data"
  "SOAK_LOCAL_RESULTS=$results"
  "LITESTREAM_SOCKET_PATH=${COVERAGE_SOCKET_DIR:-/tmp}/litestream-$shard.sock"
  "SOAK_PPROF_CAPTURE=false"
  "WORKER_ID=coverage-$shard"
  "WORKER_NAME=coverage-$shard"
  "SOURCE=coverage"
  "REPLICA_BACKEND=$backend"
  "S3_PATH=coverage-$shard"
  "LOAD_MODE=synthetic"
  "WRITE_RATE=50"
  "PATTERN=constant"
  "PAYLOAD_SIZE=1024"
  "LOAD_WORKERS=2"
  "INITIAL_SIZE=10MB"
  "VERIFY_INTERVAL=${COVERAGE_VERIFY_INTERVAL:-2m}"
  "VERIFY_TYPE=integrity"
  "MONITOR_INTERVAL=15s"
  "SNAPSHOT_INTERVAL=5m"
  "SYNC_INTERVAL=1s"
  "L1_COMPACTION_INTERVAL=30s"
  "L2_COMPACTION_INTERVAL=2m"
  "L3_COMPACTION_INTERVAL=5m"
  "L0_RETENTION=1m"
  "L0_RETENTION_CHECK_INTERVAL=15s"
)
env_args+=(${shard_env[@]+"${shard_env[@]}"})

sample_resources() {
  local pid fd rss
  pid="$(pgrep -f "litestream replicate -config $out/data/litestream.yml" | head -n 1 || true)"
  [ -n "$pid" ] || return 0
  if [ -d "/proc/$pid/fd" ]; then
    fd="$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l | tr -d ' ')"
  else
    fd="$(lsof -p "$pid" 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')"
  fi
  rss="$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')"
  [ -n "$rss" ] || return 0
  printf '{"fd":%s,"rss_kb":%s}\n' "${fd:-0}" "$rss" >>"$resources"
}

printf 'shard=%s backend=%s duration=%ss out=%s\n' "$shard" "$backend" "$duration" "$out"
env "${env_args[@]}" "$worker" >"$log" 2>&1 &
worker_pid=$!

status=0
deadline=$((SECONDS + duration))
while [ "$SECONDS" -lt "$deadline" ]; do
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    wait "$worker_pid" || true
    printf 'worker exited before the shard duration elapsed\n' >&2
    status=1
    break
  fi
  sample_resources
  sleep "${COVERAGE_SAMPLE_SECONDS:-30}"
done

if [ "$status" -eq 0 ] && ! kill -0 "$worker_pid" 2>/dev/null; then
  printf 'worker exited before the shard duration elapsed\n' >&2
  status=1
fi
if kill -0 "$worker_pid" 2>/dev/null; then
  kill -TERM "$worker_pid" 2>/dev/null || true
  for _ in $(seq 1 60); do
    kill -0 "$worker_pid" 2>/dev/null || break
    sleep 1
  done
  kill -KILL "$worker_pid" 2>/dev/null || true
fi
wait "$worker_pid" 2>/dev/null || true

[ -f "$results" ] || : >"$results"
if ! "$checker" check -shard "$shard" -backend "$backend" -results "$results" -log "$log" -resources "$resources" -report "$out/coverage.json" "${check_args[@]}"; then
  status=1
fi
if [ "$status" -ne 0 ]; then
  if [ -f "$out/coverage.json" ] && grep -q '"status": "passed"' "$out/coverage.json"; then
    "$checker" check -shard "$shard" -backend "$backend" -results /dev/null -report "$out/coverage.json" >/dev/null 2>&1 || true
  fi
  tail -n 80 "$log" >&2 || true
fi
exit "$status"
