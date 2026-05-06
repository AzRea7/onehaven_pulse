#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Story 10.3 AI compare summary smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke/ai

echo "-- Unit tests"
PYTHONPATH=. pytest apps/api/tests/test_ai_compare_summary.py -q

echo
echo "-- Route exists"
docker compose exec -T api python - <<'PY'
from app.main import app

routes = {
    (getattr(route, "path", ""), tuple(sorted(getattr(route, "methods", []) or [])))
    for route in app.routes
}

assert any(path == "/ai/compare-summary" and "POST" in methods for path, methods in routes)

print("/ai/compare-summary route exists.")
PY

echo
echo "-- Compare summary endpoint"
curl -fsS \
  -X POST "${API_BASE_URL}/ai/compare-summary" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: smoke-ai-compare-summary" \
  -d '{
    "geo_ids": ["metro_19820", "metro_16980"],
    "metrics": ["zhvi_yoy", "zori_yoy", "payment_to_income_ratio", "unemployment_rate"],
    "start_date": "2024-01-01",
    "audience": "investor",
    "detail_level": "standard"
  }' \
  > .smoke/ai/compare_summary.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/ai/compare_summary.json").read_text())

required = [
    "geo_ids",
    "summary",
    "key_takeaways",
    "confidence_explanation",
    "missing_data_explanation",
    "deterministic_scores_note",
    "not_investment_advice",
    "citations",
    "structured_payloads",
]

for key in required:
    assert key in payload, f"Missing response key: {key}"

assert payload["geo_ids"] == ["metro_19820", "metro_16980"], payload["geo_ids"]
assert isinstance(payload["summary"], str) and len(payload["summary"]) > 100
assert isinstance(payload["key_takeaways"], list) and payload["key_takeaways"]
assert "does not override" in payload["deterministic_scores_note"].lower()
assert "not investment" in payload["not_investment_advice"].lower()
assert isinstance(payload["citations"], list) and len(payload["citations"]) >= 5

structured = payload["structured_payloads"]
assert "compare" in structured
assert "contexts" in structured
assert "coverages" in structured

for geo_id in payload["geo_ids"]:
    assert geo_id in structured["contexts"], geo_id
    assert geo_id in structured["coverages"], geo_id

summary_lower = payload["summary"].lower()
assert "deterministic" in summary_lower
assert "confidence" in summary_lower
assert "missing data" in summary_lower

print("Compare summary payload passed.")
PY

echo
echo "-- Missing data disclosure"
python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/ai/compare_summary.json").read_text())

missing_text = payload["missing_data_explanation"].lower()
assert "missing" in missing_text or "complete score-category coverage" in missing_text

coverage_payloads = payload["structured_payloads"]["coverages"]

has_false_coverage = any(
    value is False
    for coverage in coverage_payloads.values()
    for value in coverage.get("coverage", {}).values()
)

if has_false_coverage:
    assert "incomplete coverage" in missing_text, missing_text

print("Missing-data disclosure passed.")
PY

echo
echo "-- Invalid compare request fails clearly"
status="$(
  curl -sS \
    -o .smoke/ai/invalid_compare_summary.json \
    -w "%{http_code}" \
    -X POST "${API_BASE_URL}/ai/compare-summary" \
    -H "Content-Type: application/json" \
    -d '{
      "geo_ids": ["metro_19820"],
      "metrics": ["zhvi_yoy"]
    }'
)"

echo "invalid_status=${status}"

if [ "${status}" -lt 400 ]; then
  echo "Expected invalid compare-summary request to fail."
  cat .smoke/ai/invalid_compare_summary.json
  exit 1
fi

echo
echo "Story 10.3 AI compare summary smoke passed."
