#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== OneHaven geography search API smoke =="
echo "API_BASE_URL=${API_BASE_URL}"

echo
echo "-- Search Detroit"
detroit_json="$(curl -s "${API_BASE_URL}/geographies/search?q=Detroit&limit=10")"
echo "${detroit_json}" | jq '{count: (.items | length), geo_ids: [.items[].geography.geo_id]}'

if ! echo "${detroit_json}" | jq -e '.items[].geography.geo_id | select(. == "metro_19820")' >/dev/null; then
  echo "Expected metro_19820 in Detroit search results"
  exit 1
fi

if ! echo "${detroit_json}" | jq -e '.items[].geography.geo_id | select(. == "place_2622000")' >/dev/null; then
  echo "Expected place_2622000 in Detroit search results"
  exit 1
fi

echo
echo "-- Search ZCTA 48201"
zcta_json="$(curl -s "${API_BASE_URL}/geographies/search?q=48201&limit=10")"
echo "${zcta_json}" | jq '{count: (.items | length), geo_ids: [.items[].geography.geo_id]}'

if ! echo "${zcta_json}" | jq -e '.items[].geography.geo_id | select(. == "zcta_48201")' >/dev/null; then
  echo "Expected zcta_48201 in 48201 search results"
  exit 1
fi

echo
echo "-- Filter ZCTAs"
zctas_json="$(curl -s "${API_BASE_URL}/geographies/search?geo_type=zcta&limit=10")"
echo "${zctas_json}" | jq '{count: (.items | length), geo_ids: [.items[].geography.geo_id]}'

if ! echo "${zctas_json}" | jq -e '.items[].geography.geo_id | select(. == "zcta_48202")' >/dev/null; then
  echo "Expected zcta_48202 in ZCTA filter results"
  exit 1
fi

echo
echo "-- Search Michigan state"
state_json="$(curl -s "${API_BASE_URL}/geographies/search?q=Michigan&geo_type=state&limit=10")"
echo "${state_json}" | jq '{count: (.items | length), geo_ids: [.items[].geography.geo_id]}'

if ! echo "${state_json}" | jq -e '.items[].geography.geo_id | select(. == "state_26")' >/dev/null; then
  echo "Expected state_26 in Michigan search results"
  exit 1
fi

echo "Geography search API smoke passed."
