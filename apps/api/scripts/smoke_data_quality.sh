#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Data quality smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke/data_quality

echo "-- Build market data quality"
python pipelines/quality/build_market_data_quality.py

echo
echo "-- Data quality list endpoint"
curl -fsS "${API_BASE_URL}/data-quality/markets?limit=25" \
  > .smoke/data_quality/list.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/data_quality/list.json").read_text())

assert "items" in payload
assert "total" in payload
assert payload["items"], "Expected data quality rows"

first = payload["items"][0]
for key in [
    "geo_id",
    "coverage_score",
    "freshness_score",
    "validity_score",
    "overall_quality_score",
    "missing_categories",
    "stale_categories",
    "quality_issues",
]:
    assert key in first, key

print(f"Data quality list passed: items={len(payload['items'])}, total={payload['total']}")
PY

echo
echo "-- Detroit data quality endpoint"
curl -fsS "${API_BASE_URL}/data-quality/markets/metro_19820" \
  > .smoke/data_quality/detroit.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/data_quality/detroit.json").read_text())

assert payload["geo_id"] == "metro_19820"
assert 0 <= payload["overall_quality_score"] <= 1
assert "missing_categories" in payload
assert "quality_issues" in payload

print(
    "Detroit quality:",
    payload["overall_quality_score"],
    "coverage:",
    payload["coverage_score"],
    "freshness:",
    payload["freshness_score"],
    "missing:",
    payload["missing_categories"],
)
PY

echo
echo "Data quality smoke passed."
