#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
ARTIFACT_DIR="${ARTIFACT_DIR:-.smoke/performance}"

MARKETS_MAX_MS="${MARKETS_MAX_MS:-300}"
MARKET_DETAIL_MAX_MS="${MARKET_DETAIL_MAX_MS:-500}"
TIMESERIES_MAX_MS="${TIMESERIES_MAX_MS:-500}"
MAP_MAX_MS="${MAP_MAX_MS:-1000}"
COMPARE_MAX_MS="${COMPARE_MAX_MS:-1000}"
TIMESERIES_MAX_POINTS="${TIMESERIES_MAX_POINTS:-120}"

mkdir -p "${ARTIFACT_DIR}"

echo "== Performance guardrails smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo "ARTIFACT_DIR=${ARTIFACT_DIR}"
echo

check_perf() {
  local name="$1"
  local path="$2"
  local max_ms="$3"
  local validator="$4"
  local output_file="${ARTIFACT_DIR}/${name}.json"
  local headers_file="${ARTIFACT_DIR}/${name}.headers"
  local timing_file="${ARTIFACT_DIR}/${name}.timing"
  local url="${API_BASE_URL}${path}"

  echo "-- ${name}"
  echo "GET ${url}"
  echo "Target: <= ${max_ms}ms"

  curl -sS \
    -H "Accept-Encoding: gzip" \
    -D "${headers_file}" \
    -o "${output_file}" \
    -w "%{http_code} %{time_total}" \
    "${url}" \
    > "${timing_file}" || true

  read -r http_status time_total < "${timing_file}"

  if [[ "${http_status}" != "200" ]]; then
    echo "FAILED: ${name}"
    echo "HTTP status: ${http_status}"
    echo "Response:"
    cat "${output_file}" || true
    echo
    exit 1
  fi

  RESPONSE_FILE="${output_file}" HEADERS_FILE="${headers_file}" CHECK_NAME="${name}" VALIDATOR="${validator}" MAX_MS="${max_ms}" TIME_TOTAL="${time_total}" TIMESERIES_MAX_POINTS="${TIMESERIES_MAX_POINTS}" python - <<'PYVALIDATE'
import gzip
import json
import os
from pathlib import Path

response_file = Path(os.environ["RESPONSE_FILE"])
headers_file = Path(os.environ["HEADERS_FILE"])
name = os.environ["CHECK_NAME"]
validator = os.environ["VALIDATOR"]
max_ms = float(os.environ["MAX_MS"])
time_total = float(os.environ["TIME_TOTAL"])
elapsed_ms = round(time_total * 1000, 2)
timeseries_max_points = int(os.environ["TIMESERIES_MAX_POINTS"])

headers = headers_file.read_text(encoding="utf-8", errors="ignore").lower()
body = response_file.read_bytes()

if "content-encoding: gzip" in headers:
    body = gzip.decompress(body)

payload = json.loads(body.decode("utf-8"))

def require(condition, message):
    if not condition:
        raise AssertionError(f"{name}: {message}")

require(elapsed_ms <= max_ms, f"{elapsed_ms}ms exceeded target {max_ms}ms")

if validator == "markets":
    require("items" in payload or isinstance(payload, list), "markets payload missing items/list")
    items = payload.get("items") if isinstance(payload, dict) else payload
    require(isinstance(items, list), "markets items should be list")

elif validator == "market_detail":
    require("market" in payload and isinstance(payload["market"], dict), "market object missing")
    require(payload["market"].get("geo_id"), "market.geo_id missing")

elif validator == "timeseries":
    require("items" in payload and isinstance(payload["items"], list), "timeseries.items missing")
    require(len(payload["items"]) > 0, "timeseries.items empty")
    require(len(payload["items"]) <= timeseries_max_points, f"timeseries returned {len(payload['items'])} points; expected <= {timeseries_max_points}")
    first = payload["items"][0]
    require("period_month" in first, "timeseries period_month missing")
    require("values" in first, "timeseries values missing")

elif validator == "map":
    require(payload.get("type") == "FeatureCollection", "map should be FeatureCollection")
    require("features" in payload and isinstance(payload["features"], list), "map.features missing")
    require(len(payload["features"]) > 0, "map.features empty")
    require("content-encoding: gzip" in headers, "map response was not gzip-compressed")

elif validator == "compare":
    require("markets" in payload and isinstance(payload["markets"], list), "compare.markets missing")
    require("latest" in payload and isinstance(payload["latest"], list), "compare.latest missing")
    require("timeseries" in payload and isinstance(payload["timeseries"], list), "compare.timeseries missing")

else:
    raise AssertionError(f"Unknown validator: {validator}")

print(f"{name}: ok in {elapsed_ms}ms")
PYVALIDATE

  echo
}

check_perf "markets" "/markets?limit=50" "${MARKETS_MAX_MS}" "markets"
check_perf "market-detail-us" "/markets/us" "${MARKET_DETAIL_MAX_MS}" "market_detail"
check_perf "timeseries-us-default-window" "/markets/us/timeseries?metrics=home_price_yoy,rent_yoy,mortgage_rate_30y,unemployment_rate,composite_cycle_score" "${TIMESERIES_MAX_MS}" "timeseries"
check_perf "map-markets-metro" "/map/markets?geo_type=metro&metric=building_permits" "${MAP_MAX_MS}" "map"
check_perf "compare-markets" "/compare/markets?geo_ids=us,metro_19820&metrics=home_price_yoy,rent_yoy,payment_to_income_ratio,unemployment_rate,composite_cycle_score" "${COMPARE_MAX_MS}" "compare"

echo "Performance guardrails smoke passed."
