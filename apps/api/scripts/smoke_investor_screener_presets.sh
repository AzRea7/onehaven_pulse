#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
WEB_BASE_URL="${WEB_BASE_URL:-http://localhost:3000}"

echo "== Story 12.5 investor screener v2 alignment smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo "WEB_BASE_URL=${WEB_BASE_URL}"
echo

mkdir -p .smoke/investor_screener

echo "-- Investor signal API still classifies Detroit as Watchlist"
curl -fsS "${API_BASE_URL}/markets/metro_19820/investor-signal" \
  > .smoke/investor_screener/detroit_signal.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/investor_screener/detroit_signal.json").read_text())

assert payload["geo_id"] == "metro_19820"
assert payload["stance"] == "watchlist"
assert payload["rule_version"] == "investor_signal_v2"
assert payload["material_missing_score_inputs"] is True

print("Detroit investor signal contract passed.")
PY

echo
echo "-- Screener API returns investor_signal_v2 fields"
curl -fsS "${API_BASE_URL}/markets/screener?geo_type=metro&min_confidence=0.5&limit=50" \
  > .smoke/investor_screener/screener_api.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/investor_screener/screener_api.json").read_text())

allowed = {
    "attractive",
    "watchlist",
    "mixed",
    "avoid",
    "insufficient_data",
}

assert "items" in payload
assert "total" in payload
assert payload["limit"] == 50
assert isinstance(payload["items"], list)
assert payload["items"], "Expected at least one screener item"

stances = set()

for item in payload["items"]:
    assert "market" in item
    assert "investor_signal" in item
    assert "investor_stance" in item
    assert "investor_stance_label" in item
    assert "investor_stance_score" in item
    assert "investor_signal_rule_version" in item
    assert "material_missing_score_inputs" in item
    assert "confidence_score" in item
    assert "values" in item
    assert "missing_metrics" in item

    assert item["investor_signal_rule_version"] == "investor_signal_v2"
    assert item["investor_stance"] in allowed, item["investor_stance"]
    assert isinstance(item["investor_stance_score"], (int, float))
    assert isinstance(item["material_missing_score_inputs"], bool)

    stances.add(item["investor_stance"])

print(
    "Screener API v2 contract passed: "
    f"items={len(payload['items'])}, total={payload['total']}, stances={sorted(stances)}"
)
PY

echo
echo "-- Screener API can filter watchlist via investor_signal"
curl -fsS "${API_BASE_URL}/markets/screener?geo_type=metro&investor_signal=watchlist&limit=20" \
  > .smoke/investor_screener/screener_watchlist.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/investor_screener/screener_watchlist.json").read_text())

assert "items" in payload
for item in payload["items"]:
    assert item["investor_stance"] == "watchlist", item

print(f"Watchlist filter passed: items={len(payload['items'])}, total={payload['total']}")
PY

echo
echo "-- Screener page returns 200"
status="$(
  curl -fsS -o .smoke/investor_screener/screener.html \
    -w "%{http_code}" \
    "${WEB_BASE_URL}/screener"
)"

echo "status=${status}"
test "${status}" = "200"

python - <<'PY'
from pathlib import Path

html = Path(".smoke/investor_screener/screener.html").read_text(
    encoding="utf-8",
    errors="ignore",
)

assert "<html" in html.lower(), "Expected HTML response"
assert "screener" in html.lower() or "market" in html.lower(), "Expected screener shell HTML"

print("Screener page returned HTML.")
PY

echo
echo "Story 12.5 investor screener v2 alignment smoke passed."
