#!/bin/sh
set -eu

DATA_DIR="${DATA_DIR:-/data}"
DB_PATH="${DB_PATH:-${SOAK_DATA_DB:-}}"
EXPECTED_OWNER="10001:10001"

needs_chown() {
	path="$1"
	[ -e "$path" ] || return 1
	[ "$(stat -c "%u:%g" "$path")" != "$EXPECTED_OWNER" ]
}

if needs_chown "$DATA_DIR" || { [ -n "$DB_PATH" ] && needs_chown "$DB_PATH"; }; then
	chown -R soak:soak "$DATA_DIR"
fi

export HOME=/data

exec setpriv --reuid soak --regid soak --init-groups "$@"
