#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
ARTIFACT_DIR="${ARTIFACT_DIR:-.smoke/api}"
mkdir -p "${ARTIFACT_DIR}"

echo "== OneHaven Epic 5 API smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo "ARTIFACT_DIR=${ARTIFACT_DIR}"
echo

check_json() {
  local name="$1"
  local path="$2"
  local validator="$3"
  local output_file="${ARTIFACT_DIR}/${name}.json"
  local status_file="${ARTIFACT_DIR}/${name}.status"
  local url="${API_BASE_URL}${path}"

  echo "-- ${name}"
  echo "GET ${url}"

  http_status="$(curl -sS -w "%{http_code}" -o "${output_file}" "${url}" || true)"
  echo "${http_status}" > "${status_file}"

  if [[ "${http_status}" != "200" ]]; then
    echo "FAILED: ${name}"
    echo "HTTP status: ${http_status}"
    echo "Response:"
    cat "${output_file}" || true
    echo
    exit 1
  fi

  RESPONSE_FILE="${output_file}" CHECK_NAME="${name}" VALIDATOR="${validator}" python - <<'PY'
import json
import os
from pathlib import Path

response_file = Path(os.environ["RESPONSE_FILE"])
check_name = os.environ["CHECK_NAME"]
validator = os.environ["VALIDATOR"]

try:
    payload = json.loads(response_file.read_text(encoding="utf-8"))
except Exception as exc:
    raise AssertionError(f"{check_name}: response is not valid JSON: {exc}") from exc

def require(condition, message):
    if not condition:
        raise AssertionError(f"{check_name}: {message}")

if validator == "health":
    require(payload.get("status") in {"healthy", "ok"}, "health.status should be healthy/ok")
    require("database" in payload, "health.database missing")

elif validator == "markets_list":
    require("items" in payload or isinstance(payload, list), "markets list should expose items or be a list")
    items = payload.get("items") if isinstance(payload, dict) else payload
    require(isinstance(items, list), "markets items should be a list")

elif validator == "market_detail_us":
    require(payload.get("market", {}).get("geo_id") == "us", "market.geo_id should be us")
    require("confidence_score" in payload, "confidence_score missing")
    require("source_freshness" in payload, "source_freshness missing")

elif validator == "market_detail_detroit":
    require(payload.get("market", {}).get("geo_id") == "metro_19820", "market.geo_id should be metro_19820")
    require("score_breakdown" in payload, "score_breakdown missing")

elif validator == "context":
    require("market" in payload, "context.market missing")
    require("evidence" in payload, "context.evidence missing")
    require("risks" in payload and isinstance(payload["risks"], list), "context.risks should be array")

elif validator == "coverage":
    require(payload.get("geo_id") == "metro_19820", "coverage geo_id should be metro_19820")
    require("coverage" in payload and isinstance(payload["coverage"], dict), "coverage object missing")
    require("available_metrics" in payload and isinstance(payload["available_metrics"], list), "available_metrics missing")

elif validator == "timeseries":
    require("items" in payload and isinstance(payload["items"], list), "timeseries.items should be array")
    require(len(payload["items"]) > 0, "timeseries.items empty")
    require("period_month" in payload["items"][0], "period_month missing")
    require("values" in payload["items"][0], "values missing")

elif validator == "map":
    require(payload.get("type") == "FeatureCollection", "map type should be FeatureCollection")
    require("features" in payload and isinstance(payload["features"], list), "map.features should be array")
    require(len(payload["features"]) > 0, "map.features empty")

elif validator == "compare":
    require("markets" in payload and isinstance(payload["markets"], list), "compare.markets should be array")
    require("latest" in payload and isinstance(payload["latest"], list), "compare.latest should be array")
    require("timeseries" in payload and isinstance(payload["timeseries"], list), "compare.timeseries should be array")

elif validator == "screener":
    require("items" in payload and isinstance(payload["items"], list), "screener.items should be array")
    require("total" in payload and isinstance(payload["total"], int), "screener.total should be integer")

elif validator == "source_freshness":
    require("summary" in payload, "source freshness summary missing")
    require("items" in payload and isinstance(payload["items"], list), "source freshness items should be array")
    require(len(payload["items"]) > 0, "source freshness empty")
    first = payload["items"][0]
    for key in ["source", "dataset", "status", "is_stale", "stale_reason"]:
        require(key in first, f"source freshness item missing {key}")

elif validator == "pipeline_observability":
    require("summary" in payload, "pipeline summary missing")
    require("items" in payload and isinstance(payload["items"], list), "pipeline items should be array")
    require("records_loaded" in payload["summary"], "records_loaded missing from summary")
    require("failed" in payload["summary"], "failed missing from summary")

else:
    raise AssertionError(f"Unknown validator: {validator}")

print(f"{check_name}: ok")
PY

  echo
}

check_json "health" "/health" "health"
check_json "markets" "/markets?limit=10" "markets_list"
check_json "market-us" "/markets/us" "market_detail_us"
check_json "market-detroit" "/markets/metro_19820" "market_detail_detroit"
check_json "context-detroit" "/markets/metro_19820/context" "context"
check_json "coverage-detroit" "/markets/metro_19820/coverage" "coverage"
check_json "timeseries-us" "/markets/us/timeseries?metrics=home_price_yoy,rent_yoy,mortgage_rate_30y,unemployment_rate,composite_cycle_score&start_date=2024-01-01" "timeseries"
check_json "map-markets" "/map/markets?geo_type=metro&metric=building_permits" "map"
check_json "compare-markets" "/compare/markets?geo_ids=us,metro_19820&metrics=home_price_yoy,rent_yoy,payment_to_income_ratio,unemployment_rate,composite_cycle_score&start_date=2024-01-01" "compare"
check_json "screener" "/markets/screener?geo_type=metro&min_confidence=0.5&limit=10" "screener"
check_json "source-freshness" "/admin/source-freshness" "source_freshness"
check_json "pipeline-runs" "/admin/pipeline-runs?limit=10" "pipeline_observability"

echo "Epic 5 API smoke passed."
