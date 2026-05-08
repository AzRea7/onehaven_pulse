#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
WEB_BASE_URL="${WEB_BASE_URL:-http://localhost:3000}"

echo "== Story 12.3 investor signal frontend smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo "WEB_BASE_URL=${WEB_BASE_URL}"
echo

mkdir -p .smoke/investor_signal_frontend

echo "-- API investor signal contract"
curl -fsS "${API_BASE_URL}/markets/metro_19820/investor-signal" \
  > .smoke/investor_signal_frontend/detroit_signal.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/investor_signal_frontend/detroit_signal.json").read_text())

assert payload["geo_id"] == "metro_19820"
assert payload["rule_version"] == "investor_signal_v2"
assert payload["stance"] == "watchlist"
assert payload["stance_label"] == "Watchlist"
assert payload["stance_reason"]
assert isinstance(payload["stance_score"], (int, float))
assert payload["drivers"]
assert payload["risks"]

print("Investor signal API contract passed.")
PY

echo
echo "-- Frontend market detail page returns 200"
status="$(
  curl -fsS -o .smoke/investor_signal_frontend/detroit_page.html \
    -w "%{http_code}" \
    "${WEB_BASE_URL}/markets/metro_19820"
)"

echo "status=${status}"
test "${status}" = "200"

python - <<'PY'
from pathlib import Path

html = Path(".smoke/investor_signal_frontend/detroit_page.html").read_text(
    encoding="utf-8",
    errors="ignore",
)

assert "<html" in html.lower(), "Expected HTML response"
assert "market" in html.lower(), "Expected market page shell HTML"

print("Frontend market detail page returned HTML.")
PY

echo
echo "Story 12.3 investor signal frontend smoke passed."
