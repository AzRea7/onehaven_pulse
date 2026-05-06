#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Source freshness smoke =="
echo "API_BASE_URL=${API_BASE_URL}"

response="$(curl -fsS "${API_BASE_URL}/admin/source-freshness")"

echo "${response}" | python -m json.tool

RESPONSE_JSON="${response}" python - <<'PY'
import json
import os

payload = json.loads(os.environ["RESPONSE_JSON"])

assert "summary" in payload, "Missing summary"
assert "items" in payload, "Missing items"
assert isinstance(payload["items"], list), "items must be a list"

required = {
    "source",
    "dataset",
    "latest_source_period",
    "last_loaded_at",
    "record_count",
    "status",
    "error_message",
    "stale_reason",
    "is_stale",
    "expected_frequency",
    "freshness_threshold_days",
}

for item in payload["items"]:
    missing = required - set(item)
    assert not missing, f"Missing fields: {missing}"

print("source freshness smoke passed")
PY
