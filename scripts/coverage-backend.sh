#!/usr/bin/env bash
set -euo pipefail

nats_image="nats:2.11.9@sha256:4e97bea2e69ffe4449cdc9b4c7fa707984aa9a4c090bf2faf5441cb6c97c99a4"
nats_box_image="natsio/nats-box:0.18.0@sha256:abdc9f9f0120bb8adfbf674eb037d1551db55356eb198b7bd4ffed377f6950a6"
sftp_image="atmoz/sftp:alpine@sha256:43dd1c023cf1bae487127d4b54660b0aee6e94c188a6599be36fb89ba115f961"
rclone_image="rclone/rclone:1.70.3@sha256:34c729127386abec1c610b2aa024e39b4498dc2b4a72a0798ae21fbdc1b0493b"

action="${1:-}"
shard="${2:-}"
if [ -z "$action" ] || [ -z "$shard" ]; then
  printf 'usage: %s start|stop <shard>\n' "$0" >&2
  exit 2
fi

state="${COVERAGE_BACKEND_DIR:-${RUNNER_TEMP:-/tmp}/coverage-backend}"
mkdir -p "$state"

export_secret() {
  local name="$1" value="$2"
  if [ -n "${GITHUB_ENV:-}" ]; then
    printf '::add-mask::%s\n' "$value"
    printf '%s=%s\n' "$name" "$value" >>"$GITHUB_ENV"
  fi
  export "$name=$value"
}

generated() {
  od -An -N20 -tx1 /dev/urandom | tr -d ' \n'
}

wait_for() {
  local description="$1"
  shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  printf '%s did not become ready\n' "$description" >&2
  return 1
}

port_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

start_minio() {
  local user secret
  user="soak$(generated | cut -c1-12)"
  secret="$(generated)"
  export_secret COVERAGE_MINIO_ACCESS_KEY "$user"
  export_secret COVERAGE_MINIO_SECRET_KEY "$secret"
  mkdir -p "$state/minio"
  MINIO_ROOT_USER="$user" MINIO_ROOT_PASSWORD="$secret" MINIO_BROWSER=off MINIO_UPDATE=off \
    nohup minio server "$state/minio" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 >"$state/minio.log" 2>&1 &
  printf '%s\n' "$!" >"$state/minio.pid"
  wait_for MinIO curl -fsS http://127.0.0.1:9000/minio/health/live
  MC_CONFIG_DIR="$state/mc" mc alias set coverage http://127.0.0.1:9000 "$user" "$secret" >/dev/null
  MC_CONFIG_DIR="$state/mc" mc mb --ignore-existing coverage/litestream-soak
}

start_sftp() {
  local password
  password="$(generated)"
  export_secret COVERAGE_SFTP_PASSWORD "$password"
  docker run -d --name coverage-sftp -p 127.0.0.1:2222:22 "$sftp_image" "soak:$password:1001::upload" >/dev/null
  wait_for SFTP port_open 2222
  sleep 3
}

start_nats() {
  docker run -d --name coverage-nats -p 127.0.0.1:4222:4222 "$nats_image" -js >/dev/null
  wait_for NATS port_open 4222
  docker run --rm --network host "$nats_box_image" nats -s nats://127.0.0.1:4222 object add "coverage-$shard" >/dev/null
}

start_webdav() {
  local password
  password="$(generated)"
  export_secret COVERAGE_WEBDAV_PASSWORD "$password"
  docker run -d --name coverage-webdav -p 127.0.0.1:8080:8080 "$rclone_image" \
    serve webdav /data --addr :8080 --user soak --pass "$password" >/dev/null
  wait_for WebDAV port_open 8080
}

stop_all() {
  if [ -f "$state/minio.pid" ]; then
    kill "$(cat "$state/minio.pid")" 2>/dev/null || true
    rm -f "$state/minio.pid"
  fi
  for container in coverage-sftp coverage-nats coverage-webdav; do
    if docker inspect "$container" >/dev/null 2>&1; then
      docker logs --tail 50 "$container" >"$state/$container.log" 2>&1 || true
      docker rm -f "$container" >/dev/null 2>&1 || true
    fi
  done
}

case "$action" in
  start)
    case "$shard" in
      backend-minio) start_minio ;;
      backend-sftp) start_sftp ;;
      backend-nats) start_nats ;;
      backend-webdav) start_webdav ;;
      *) ;;
    esac
    ;;
  stop) stop_all ;;
  *)
    printf 'unknown action: %s\n' "$action" >&2
    exit 2
    ;;
esac
