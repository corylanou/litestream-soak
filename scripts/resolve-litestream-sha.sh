#!/usr/bin/env bash
set -euo pipefail

ref="${1:-main}"

if echo "$ref" | grep -qE '^[0-9a-fA-F]{7,40}$'; then
  printf '%s\n' "$ref"
  exit 0
fi

pattern="$ref"
if [ "$ref" = "main" ]; then
  pattern="refs/heads/main"
fi

sha="$(git ls-remote https://github.com/benbjohnson/litestream.git "$pattern" | awk 'NR==1{print $1}')"
if [ -z "$sha" ]; then
  echo "failed to resolve Litestream ref $ref" >&2
  exit 1
fi

printf '%s\n' "$sha"
