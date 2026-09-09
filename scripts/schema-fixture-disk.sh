#!/usr/bin/env bash
set -euo pipefail
export GOTOOLCHAIN=go1.25.13
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="${SCHEMA_FIXTURE_IMAGE:?set SCHEMA_FIXTURE_IMAGE to a local image containing the pinned /usr/local/bin/litestream}"
sha="${SCHEMA_FIXTURE_SHA:-4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3}"
mode="${1:-control}"
case "$mode" in
  control) size=256m; maximum=268435456; rows=1024; payload=1024 ;;
  negative) size=16m; maximum=16777216; rows=16384; payload=4096 ;;
  *) echo 'usage: schema-fixture-disk.sh control|negative' >&2; exit 2 ;;
esac
mkdir -p "$root/.local/state"
evidence="$(mktemp -d "$root/.local/state/schema-disk-$mode-XXXXXX")"
arch="$(docker image inspect "$image" --format '{{.Architecture}}')"
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go -C "$root" build -o "$evidence/schemafixture" ./cmd/schemafixture
name="schema-fixture-$mode-$(date +%s)-$$"
printf 'container=%s evidence=%s\n' "$name" "$evidence"
set +e
docker run --name "$name" --network none --memory 1g --cpus 2 \
  --tmpfs "/fixture:rw,size=$size" \
  --mount "type=bind,src=$evidence,dst=/evidence" \
  --entrypoint /bin/sh "$image" -c '
    export TMPDIR=/fixture
    /evidence/schemafixture -dir /fixture/run -litestream /usr/local/bin/litestream \
      -sha "$1" -rows "$2" -payload-bytes "$3" -max-headroom-bytes "$4" \
      -timeout 2m > /evidence/output.json
    result=$?
    if [ -d /fixture/run ]; then cp -R /fixture/run /evidence/artifacts || exit 125; fi
    exit "$result"
  ' schema-fixture "$sha" "$rows" "$payload" "$maximum"
result=$?
set -e
python3 - "$evidence/output.json" "$mode" "$result" <<'PY'
import json, sys
with open(sys.argv[1]) as source:
    result = json.load(source)
mode, status = sys.argv[2], int(sys.argv[3])
print(json.dumps(result, indent=2))
if mode == "control":
    assert status == 0 and result["verdict"] == "scenario_success"
    assert len(result["boundaries"]) == 10
else:
    assert status == 1 and result["verdict"] == "unrelated_failure"
    assert result.get("error"), "external output must retain failure details"
    assert any("full" in boundary.get("error", "").lower() for boundary in result["boundaries"])
    assert result["boundaries"][0]["headroom_bytes"] <= 16 * 1024 * 1024
PY
printf 'Expected %s outcome verified; artifacts and stopped container retained.\n' "$mode"
