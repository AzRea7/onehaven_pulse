#!/usr/bin/env bash
set -euo pipefail

WEB_BASE_URL="${WEB_BASE_URL:-http://localhost:3000}"
API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== Story 12.6 investor workflow UX smoke =="
echo "WEB_BASE_URL=${WEB_BASE_URL}"
echo "API_BASE_URL=${API_BASE_URL}"
echo

mkdir -p .smoke/investor_workflow

fetch_with_retry() {
  local url="$1"
  local output="$2"
  local attempts="${3:-20}"
  local sleep_seconds="${4:-2}"

  for attempt in $(seq 1 "${attempts}"); do
    if curl -fsS -o "${output}" -w "%{http_code}" "${url}" > "${output}.status"; then
      local status
      status="$(cat "${output}.status")"
      if [ "${status}" = "200" ]; then
        echo "status=${status}"
        return 0
      fi
      echo "attempt ${attempt}/${attempts}: ${url} returned status=${status}"
    else
      echo "attempt ${attempt}/${attempts}: ${url} did not return a complete response"
    fi

    sleep "${sleep_seconds}"
  done

  echo "Failed to fetch ${url} after ${attempts} attempts"
  return 1
}

echo "-- Wait for frontend readiness"
fetch_with_retry "${WEB_BASE_URL}/" ".smoke/investor_workflow/home_ready.html" 30 2

echo
echo "-- Investor signal API"
curl -fsS "${API_BASE_URL}/markets/metro_19820/investor-signal" \
  > .smoke/investor_workflow/detroit_signal.json

python - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path(".smoke/investor_workflow/detroit_signal.json").read_text())

assert payload["geo_id"] == "metro_19820"
assert payload["stance"] == "watchlist"
assert payload["rule_version"] == "investor_signal_v2"
assert payload["drivers"]
assert payload["risks"]

print("Investor signal API passed.")
PY

echo
echo "-- UX routes return 200"

for path in \
  "/" \
  "/workflow" \
  "/dashboard" \
  "/screener" \
  "/markets/metro_19820" \
  "/compare" \
  "/map" \
  "/admin/source-freshness"
do
  echo "GET ${WEB_BASE_URL}${path}"
  safe_name="$(echo "${path}" | sed 's#/#_#g; s#^_$#home#')"
  fetch_with_retry \
    "${WEB_BASE_URL}${path}" \
    ".smoke/investor_workflow/${safe_name}.html" \
    10 \
    1
done

echo
echo "Story 12.6 investor workflow UX smoke passed."
