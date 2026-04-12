#!/usr/bin/env bash
set -euo pipefail

app="${APP:-litestream-soak}"
target_image="${TARGET_IMAGE:-}"
run_updates="${RUN:-0}"

machine_json="$(fly machine list -a "$app" --json)"

if [[ -z "$target_image" ]]; then
  target_image="$(printf '%s' "$machine_json" | jq -r '
    (
      map(select((.config.metadata.fly_process_group // "") == "app" and (.config.image // "") != ""))
      | sort_by(.updated_at)
      | last
      | .config.image
    ) // (
      map(select((.config.image // "") != ""))
      | sort_by(.updated_at)
      | last
      | .config.image
    ) // empty
  ')"
fi

if [[ -z "$target_image" ]]; then
  echo "Could not determine target image for $app" >&2
  exit 1
fi

mapfile -t machines < <(printf '%s' "$machine_json" | jq -r --arg img "$target_image" '
  map(
    select(
      (.state == "started") and
      ((.config.metadata.fly_process_group // "") != "app") and
      ((.config.image // "") != $img)
    )
  )
  | sort_by(.name)
  | .[]
  | [.id, .name, .config.image]
  | @tsv
')

echo "App: $app"
echo "Target image: $target_image"

if [[ ${#machines[@]} -eq 0 ]]; then
  echo "All non-app worker machines already match the target image."
  exit 0
fi

echo
echo "Machines to refresh:"
for entry in "${machines[@]}"; do
  IFS=$'\t' read -r machine_id machine_name current_image <<<"$entry"
  printf '  %s\t%s\t%s\n' "$machine_id" "$machine_name" "$current_image"
done

echo
if [[ "$run_updates" != "1" ]]; then
  echo "Dry run only. Re-run with RUN=1 to update these machines."
  echo "Example:"
  echo "  RUN=1 make refresh-worker-fleet"
  exit 0
fi

for entry in "${machines[@]}"; do
  IFS=$'\t' read -r machine_id machine_name current_image <<<"$entry"
  echo "Updating $machine_name ($machine_id)"
  fly machine update "$machine_id" -a "$app" --image "$target_image" --yes
done
