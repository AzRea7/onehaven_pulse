#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
ARTIFACT_DIR="${ARTIFACT_DIR:-.smoke/geo}"
mkdir -p "${ARTIFACT_DIR}"

echo "== OneHaven geography smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo "ARTIFACT_DIR=${ARTIFACT_DIR}"
echo

check_json() {
  local name="$1"
  local path="$2"
  local validator="$3"
  local output_file="${ARTIFACT_DIR}/${name}.json"
  local url="${API_BASE_URL}${path}"

  echo "-- ${name}"
  echo "GET ${url}"

  http_status="$(curl -sS -w "%{http_code}" -o "${output_file}" "${url}" || true)"

  if [[ "${http_status}" != "200" ]]; then
    echo "FAILED: ${name}"
    echo "HTTP status: ${http_status}"
    echo "Response:"
    cat "${output_file}" || true
    echo
    exit 1
  fi

  RESPONSE_FILE="${output_file}" CHECK_NAME="${name}" VALIDATOR="${validator}" python - <<'PYVALIDATE'
import json
import os
from pathlib import Path

payload = json.loads(Path(os.environ["RESPONSE_FILE"]).read_text(encoding="utf-8"))
name = os.environ["CHECK_NAME"]
validator = os.environ["VALIDATOR"]

def require(condition, message):
    if not condition:
        raise AssertionError(f"{name}: {message}")

if validator == "markets_search":
    require("items" in payload or isinstance(payload, list), "expected items or list")
    items = payload.get("items") if isinstance(payload, dict) else payload
    require(isinstance(items, list), "items should be list")
    require(len(items) > 0, "market search returned no items")

elif validator == "market_detail":
    require("market" in payload and isinstance(payload["market"], dict), "market object missing")
    require(payload["market"].get("geo_id") == "metro_19820", "market.geo_id should be metro_19820")
    require(payload["market"].get("geo_type") == "metro", "market.geo_type should be metro")
    require(payload["market"].get("name") or payload["market"].get("display_name"), "market name/display_name missing")

elif validator == "market_context":
    require(payload.get("geo_id") == "metro_19820", "context geo_id mismatch")
    require(payload.get("geo_type") == "metro", "context geo_type mismatch")
    require(isinstance(payload.get("market"), str) and payload["market"], "context market name missing")
    require("cycle_phase" in payload, "cycle_phase missing")
    require("investor_signal" in payload, "investor_signal missing")
    require("evidence" in payload and isinstance(payload["evidence"], dict), "evidence missing")
    require("coverage" in payload and isinstance(payload["coverage"], dict), "coverage missing")
    require("risks" in payload and isinstance(payload["risks"], list), "risks missing")

elif validator == "coverage":
    require(payload.get("geo_id") == "metro_19820", "coverage geo_id mismatch")
    require("coverage" in payload and isinstance(payload["coverage"], dict), "coverage missing")
    require("available_metrics" in payload and isinstance(payload["available_metrics"], list), "available_metrics missing")

elif validator == "map":
    require(payload.get("type") == "FeatureCollection", "map type should be FeatureCollection")
    require("features" in payload and isinstance(payload["features"], list), "features missing")
    require(len(payload["features"]) > 0, "features empty")

    geo_ids = set()
    for feature in payload["features"]:
        props = feature.get("properties", {}) if isinstance(feature, dict) else {}
        geo_id = props.get("geo_id")
        if geo_id:
            geo_ids.add(geo_id)

    require(len(geo_ids) > 0, "map features do not include geo_id properties")

else:
    raise AssertionError(f"Unknown validator: {validator}")

print(f"{name}: ok")
PYVALIDATE

  echo
}

check_json "markets-search-detroit" "/markets?query=Detroit&limit=10" "markets_search"
check_json "market-detail-detroit" "/markets/metro_19820" "market_detail"
check_json "market-context-detroit" "/markets/metro_19820/context" "market_context"
check_json "market-coverage-detroit" "/markets/metro_19820/coverage" "coverage"
check_json "map-metro-geographies" "/map/markets?geo_type=metro&metric=building_permits" "map"

echo "Geography smoke passed."
