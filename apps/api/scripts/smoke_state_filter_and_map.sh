#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
WEB_BASE_URL="${WEB_BASE_URL:-http://localhost:3000}"

echo "== State filter and map smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo "WEB_BASE_URL=${WEB_BASE_URL}"
echo

mkdir -p .smoke/state_filter

echo "-- Screener API state=MI includes Detroit metro"
curl -fsS "${API_BASE_URL}/markets/screener?geo_type=metro&state=MI&limit=100" \
  > .smoke/state_filter/screener_mi.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/state_filter/screener_mi.json").read_text())
items = payload["items"]

matches = [
    item for item in items
    if item["market"]["geo_id"] == "metro_19820"
    or "detroit" in (item["market"].get("display_name") or item["market"].get("name") or "").lower()
]

print(f"MI items={len(items)} total={payload['total']}")
print("Detroit matches:", [m["market"]["display_name"] or m["market"]["name"] for m in matches[:5]])

assert matches, "Expected Detroit metro in state=MI screener results"
PY

echo
echo "-- Map page returns 200"
status="$(
  curl -fsS -o .smoke/state_filter/map.html \
    -w "%{http_code}" \
    "${WEB_BASE_URL}/map"
)"

echo "status=${status}"
test "${status}" = "200"

python - <<'PY'
from pathlib import Path

html = Path(".smoke/state_filter/map.html").read_text(encoding="utf-8", errors="ignore")
assert "<html" in html.lower(), "Expected HTML shell"

print("Map page returned HTML.")
PY

echo
echo "State filter and map smoke passed."
