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

refs="$(git ls-remote https://github.com/benbjohnson/litestream.git "$pattern" "${pattern}^{}")"
# annotated tags list both the tag object and the peeled commit (ref^{});
# prefer the peeled line so builds pin the commit, not the tag object
sha="$(printf '%s\n' "$refs" | awk '$2 ~ /\^\{\}$/ {print $1; exit}')"
if [ -z "$sha" ]; then
  sha="$(printf '%s\n' "$refs" | awk 'NF{print $1; exit}')"
fi
if [ -z "$sha" ]; then
  echo "failed to resolve Litestream ref $ref" >&2
  exit 1
fi

printf '%s\n' "$sha"
