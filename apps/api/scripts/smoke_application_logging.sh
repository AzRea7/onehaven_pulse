#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
REQUEST_ID="smoke-log-$(date +%s)"

ARTIFACT_DIR=".smoke"
HEALTH_RESPONSE_FILE="${ARTIFACT_DIR}/onehaven_health_response.json"
REQUEST_LOG_FILE="${ARTIFACT_DIR}/onehaven_request_log_line.txt"

mkdir -p "${ARTIFACT_DIR}"

echo "== Application logging smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo "REQUEST_ID=${REQUEST_ID}"

curl -fsS \
  -H "x-request-id: ${REQUEST_ID}" \
  "${API_BASE_URL}/health" \
  > "${HEALTH_RESPONSE_FILE}"

echo "Health response:"
cat "${HEALTH_RESPONSE_FILE}"
echo

echo "Checking Docker API logs for request_completed event..."
docker compose logs --no-color api \
  | tail -300 \
  | grep "${REQUEST_ID}" \
  | grep "request_completed" \
  > "${REQUEST_LOG_FILE}"

echo "Matched log line:"
cat "${REQUEST_LOG_FILE}"
echo

REQUEST_LOG_FILE="${REQUEST_LOG_FILE}" python - <<'PY'
import json
import os
from pathlib import Path

log_file = Path(os.environ["REQUEST_LOG_FILE"])
line = log_file.read_text(encoding="utf-8").strip().splitlines()[-1]

start = line.find("{")
assert start >= 0, f"No JSON object found in log line: {line}"

payload = json.loads(line[start:])

assert payload["event"] == "request_completed"
assert payload["request_id"].startswith("smoke-log-")
assert payload["path"] == "/health"
assert "duration_ms" in payload
assert payload["status_code"] == 200

payload_text = json.dumps(payload).lower()

for forbidden in [
    "fred_api_key",
    "hud_usps_access_token",
    "authorization",
    "cookie",
    "secret",
]:
    assert forbidden not in payload_text, f"Secret-like value leaked in log: {forbidden}"

print("application logging smoke passed")
PY
