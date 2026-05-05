#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== OneHaven Epic 6 API smoke =="
echo "API_BASE_URL=${API_BASE_URL}"
echo

require_jq() {
  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required."
    exit 1
  fi
}

check_json() {
  local name="$1"
  local url="$2"
  local jq_expr="$3"

  echo "-- ${name}"
  echo "GET ${url}"

  response="$(curl -fsS "${url}")"
  echo "${response}" | jq "${jq_expr}"
  echo
}

require_jq

check_json \
  "health" \
  "${API_BASE_URL}/health" \
  '.status, .database'

check_json \
  "national detail" \
  "${API_BASE_URL}/markets/us" \
  '.market.geo_id, .latest_period, .cycle_phase, .investor_signal, .confidence_score'

check_json \
  "detroit detail" \
  "${API_BASE_URL}/markets/metro_19820" \
  '.market.geo_id, .latest_period, .cycle_phase, .investor_signal, .confidence_score'

check_json \
  "detroit coverage" \
  "${API_BASE_URL}/markets/metro_19820/coverage" \
  '.geo_id, .coverage, .available_metrics'

check_json \
  "detroit context" \
  "${API_BASE_URL}/markets/metro_19820/context" \
  '.market, .latest_period, .confidence_score, .evidence, .coverage, .risks'

check_json \
  "national timeseries" \
  "${API_BASE_URL}/markets/us/timeseries?metrics=home_price_yoy,rent_yoy,mortgage_rate_30y,unemployment_rate,composite_cycle_score&start_date=2024-01-01" \
  '.market.geo_id, (.items | length), .items[0]'

check_json \
  "metro map" \
  "${API_BASE_URL}/map/markets?geo_type=metro&metric=building_permits" \
  '.type, (.features | length), .features[0].properties.geo_id'

check_json \
  "compare" \
  "${API_BASE_URL}/compare/markets?geo_ids=us,metro_19820&metrics=home_price_yoy,rent_yoy,payment_to_income_ratio,unemployment_rate,composite_cycle_score&start_date=2024-01-01" \
  '(.markets | length), (.latest | length), (.timeseries | length)'

check_json \
  "screener" \
  "${API_BASE_URL}/markets/screener?geo_type=metro&min_confidence=0.5&limit=10" \
  '.total, (.items | length), .items[0].market.geo_id'

check_json \
  "source freshness" \
  "${API_BASE_URL}/audit/source-freshness" \
  '(.items | length), .items[0]'

echo "Epic 6 API smoke passed."
