#!/usr/bin/env bash
#
# Imports the Shadowtap data views, saved searches and dashboard into Kibana through the saved
# objects API (overwriting earlier versions), so a fresh Kibana shows captures, diffs, traces and
# logs without clicking through the UI.
#
# Usage: deploy/kibana/import.sh [kibana-url]     (default: http://localhost:5601)
#   KIBANA_USER / KIBANA_PASSWORD   basic auth when Kibana security is enabled
#   KIBANA_API_KEY                  API key instead of a user

set -euo pipefail

kibana=${1:-http://localhost:5601}
dir=$(cd "$(dirname "$0")" && pwd)
objects=$dir/saved-objects.ndjson

auth=()
if [ -n "${KIBANA_API_KEY:-}" ]; then
	auth=(-H "Authorization: ApiKey $KIBANA_API_KEY")
elif [ -n "${KIBANA_USER:-}" ]; then
	auth=(-u "$KIBANA_USER:${KIBANA_PASSWORD:-}")
fi

# Kibana answers the saved objects API only once it is fully up.
for _ in $(seq 1 60); do
	if curl -fsS "${auth[@]}" -o /dev/null --max-time 3 "$kibana/api/status"; then
		break
	fi
	sleep 2
done

curl -fsS "${auth[@]}" -X POST "$kibana/api/saved_objects/_import?overwrite=true" \
	-H "kbn-xsrf: shadowtap" \
	--form "file=@$objects" |
	python3 -c 'import json,sys; d=json.load(sys.stdin); print("imported", d.get("successCount", 0), "objects"); sys.exit(0 if d.get("success") else 1)'
