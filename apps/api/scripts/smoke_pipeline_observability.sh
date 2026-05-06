#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Pipeline observability smoke =="
echo "API_BASE_URL=${API_BASE_URL}"

runs_response="$(curl -fsS "${API_BASE_URL}/admin/pipeline-runs?limit=20")"
summary_response="$(curl -fsS "${API_BASE_URL}/admin/pipeline-runs/summary")"

echo "Pipeline runs response:"
echo "${runs_response}" | python -m json.tool
echo

echo "Pipeline summary response:"
echo "${summary_response}" | python -m json.tool
echo

RUNS_JSON="${runs_response}" SUMMARY_JSON="${summary_response}" python - <<'PY'
import json
import os

runs_payload = json.loads(os.environ["RUNS_JSON"])
summary_payload = json.loads(os.environ["SUMMARY_JSON"])

assert "summary" in runs_payload, "Missing summary in pipeline runs response"
assert "items" in runs_payload, "Missing items in pipeline runs response"
assert isinstance(runs_payload["items"], list), "items must be a list"

required_summary = {
    "total",
    "running",
    "success",
    "failed",
    "other",
    "records_extracted",
    "records_loaded",
    "records_failed",
    "unmatched_count",
    "latest_started_at",
}
assert required_summary <= set(summary_payload), f"Missing summary fields: {required_summary - set(summary_payload)}"

required_item = {
    "id",
    "pipeline_name",
    "source",
    "dataset",
    "status",
    "started_at",
    "finished_at",
    "duration_seconds",
    "records_extracted",
    "records_loaded",
    "records_failed",
    "unmatched_count",
    "error_message",
    "metadata_json",
}

for item in runs_payload["items"]:
    missing = required_item - set(item)
    assert not missing, f"Missing item fields: {missing}"

print("pipeline observability smoke passed")
PY
