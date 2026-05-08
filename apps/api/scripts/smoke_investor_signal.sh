#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Story 12.2 investor signal smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke/investor_signal

echo "-- Unit tests"
docker compose exec -T api pytest tests/test_investor_signal_framework.py -q

echo
echo "-- Route exists"
curl -fsS "${API_BASE_URL}/openapi.json" > .smoke/investor_signal/openapi.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/investor_signal/openapi.json").read_text())
paths = payload.get("paths", {})

assert "/markets/{geo_id}/investor-signal" in paths, "investor-signal route missing from OpenAPI"
print("/markets/{geo_id}/investor-signal route exists.")
PY

echo
echo "-- Detroit investor signal"
curl -fsS "${API_BASE_URL}/markets/metro_19820/investor-signal" \
  > .smoke/investor_signal/detroit.json

python -m json.tool .smoke/investor_signal/detroit.json | head -160

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/investor_signal/detroit.json").read_text())

allowed_stances = {
    "attractive",
    "watchlist",
    "mixed",
    "avoid",
    "insufficient_data",
}

assert payload["geo_id"] == "metro_19820"
assert payload["stance"] in allowed_stances
assert payload["stance_label"]
assert payload["deterministic"] is True
assert payload["rule_version"] == "investor_signal_v2"
assert payload["stance_reason"]
assert isinstance(payload["stance_score"], (int, float))
assert isinstance(payload["material_missing_score_inputs"], bool)
assert isinstance(payload["coverage"], dict)
assert isinstance(payload["dimension_statuses"], dict)
assert isinstance(payload["drivers"], list)
assert isinstance(payload["risks"], list)
assert isinstance(payload["rule_trace"], list)
assert "price_momentum" in payload["dimension_statuses"]
assert "rent_momentum" in payload["dimension_statuses"]
assert "affordability" in payload["dimension_statuses"]
assert "labor_stability" in payload["dimension_statuses"]
assert "coverage_quality" in payload["dimension_statuses"]

# Detroit's current context payload exposes these metrics, so the investor signal
# must extract them instead of falsely marking every dimension as missing.
assert payload["dimension_statuses"]["price_momentum"] != "missing", payload["dimension_statuses"]
assert payload["dimension_statuses"]["rent_momentum"] != "missing", payload["dimension_statuses"]
assert payload["dimension_statuses"]["affordability"] != "missing", payload["dimension_statuses"]
assert payload["dimension_statuses"]["labor_stability"] != "missing", payload["dimension_statuses"]

assert payload["drivers"] or payload["risks"], "Expected at least one driver or risk"

# With current Detroit data, this should be promising but imperfect:
# negative price momentum, neutral confidence, missing permits, and material missing score inputs.
assert payload["stance"] == "watchlist", payload
assert payload["material_missing_score_inputs"] is True

print(
    "Detroit investor signal passed: "
    f"stance={payload['stance']}, confidence={payload.get('confidence_score')}, "
    f"dimensions={payload['dimension_statuses']}"
)
PY

echo
echo "-- Representative markets investor signal"
python - <<'PY'
import json
import urllib.request

base = "http://localhost:8000"

geo_ids = [
    "us",
    "metro_19820",
    "metro_16980",
    "metro_19100",
    "metro_12420",
    "metro_45300",
    "metro_38060",
    "metro_12060",
    "metro_42660",
    "metro_14460",
    "metro_31080",
    "metro_37980",
]

allowed_stances = {
    "attractive",
    "watchlist",
    "mixed",
    "avoid",
    "insufficient_data",
}

results = []

for geo_id in geo_ids:
    with urllib.request.urlopen(f"{base}/markets/{geo_id}/investor-signal", timeout=30) as response:
        assert response.status == 200, (geo_id, response.status)
        payload = json.loads(response.read().decode("utf-8"))

    assert payload["geo_id"] == geo_id
    assert payload["stance"] in allowed_stances
    assert payload["deterministic"] is True
    assert payload["rule_trace"]

    results.append((geo_id, payload["stance"], payload.get("confidence_score")))

for geo_id, stance, confidence in results:
    print(f"{geo_id}: stance={stance}, confidence={confidence}")

print("Representative investor signals passed.")
PY

echo
echo "Story 12.2 investor signal smoke passed."
