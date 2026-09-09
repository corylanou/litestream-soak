#!/usr/bin/env bash
set -euo pipefail

candidate=${1:-4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3}
workload=ae88b164dd6304bcbb654a681df767ee59042eed
if [[ ! "$candidate" =~ ^[0-9a-f]{40}$ ]]; then
  echo 'unsupported: candidate must be a full lowercase commit SHA' >&2
  exit 2
fi
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$root/.local/compatibility"
evidence=$(mktemp -d "$root/.local/compatibility/run.XXXXXX")
exec > >(tee "$evidence/run.log") 2>&1
report_result() {
  compatibility_exit=$?
  if [ "$compatibility_exit" -eq 0 ]; then
    echo "result=passed"
  else
    echo "result=failed-or-inconclusive exit=$compatibility_exit; retain all attempt evidence"
  fi
}
trap report_result EXIT
export GOTOOLCHAIN=go1.25.13
export CGO_ENABLED=1
printf 'candidate=%s\nworkload=%s\nevidence=%s\n' "$candidate" "$workload" "$evidence"
for prerequisite in git go cc; do
  command -v "$prerequisite" >/dev/null || { echo "inconclusive: missing prerequisite $prerequisite"; exit 2; }
done
go version
for role in candidate workload; do
  revision=$candidate
  command_name=litestream
  if [ "$role" = workload ]; then revision=$workload; command_name=litestream-test; fi
  source_dir="$evidence/$role"
  git init -q "$source_dir"
  git -C "$source_dir" fetch --depth=1 https://github.com/benbjohnson/litestream.git "$revision"
  git -C "$source_dir" checkout -q --detach FETCH_HEAD
  test "$(git -C "$source_dir" rev-parse HEAD)" = "$revision"
  CGO_CFLAGS=-DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0 go -C "$source_dir" build \
    -ldflags "-X main.Version=$revision" -o "$evidence/$command_name" "./cmd/$command_name"
  go version -m "$evidence/$command_name" > "$evidence/$command_name.buildinfo"
done
SOAK_LOGICAL_LITESTREAM_BINARY="$evidence/litestream" \
SOAK_LOGICAL_WORKLOAD_BINARY="$evidence/litestream-test" \
SOAK_COMPATIBILITY_SHA="$candidate" SOAK_COMPATIBILITY_EVIDENCE="$evidence" \
  go -C "$root" test ./internal/worker -run '^Test(LogicalOraclePinnedLitestream|VerificationBoundaryPinnedBinary)$' -count=1 -v -timeout=2m
