#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

echo "== OneHaven geography relationship API smoke =="
echo "API_BASE_URL=${API_BASE_URL}"

echo
echo "-- Detroit place children"
children_json="$(curl -s "${API_BASE_URL}/geographies/place_2622000/children?child_geo_type=zcta")"
echo "${children_json}" | jq '.geo_id, .relationship_type, .child_geo_type, (.items | length), [.items[].child.geo_id]'

children_count="$(echo "${children_json}" | jq '.items | length')"
if [[ "${children_count}" -lt 3 ]]; then
  echo "Expected at least 3 Detroit ZCTA children, got ${children_count}"
  exit 1
fi

if ! echo "${children_json}" | jq -e '.items[].child.geo_id | select(. == "zcta_48201")' >/dev/null; then
  echo "Expected zcta_48201 in Detroit place children"
  exit 1
fi

echo
echo "-- Detroit ZCTA parents"
parents_json="$(curl -s "${API_BASE_URL}/geographies/zcta_48201/parents")"
echo "${parents_json}" | jq '.geo_id, .relationship_type, (.items | length), [.items[].parent.geo_id]'

if ! echo "${parents_json}" | jq -e '.items[].parent.geo_id | select(. == "place_2622000")' >/dev/null; then
  echo "Expected place_2622000 in zcta_48201 parents"
  exit 1
fi

if ! echo "${parents_json}" | jq -e '.items[].parent.geo_id | select(. == "metro_19820")' >/dev/null; then
  echo "Expected metro_19820 in zcta_48201 parents"
  exit 1
fi

echo
echo "-- Detroit place related"
related_json="$(curl -s "${API_BASE_URL}/geographies/place_2622000/related")"
echo "${related_json}" | jq '.geo_id, (.parents | length), (.children | length)'

parent_count="$(echo "${related_json}" | jq '.parents | length')"
child_count="$(echo "${related_json}" | jq '.children | length')"

if [[ "${parent_count}" -lt 2 ]]; then
  echo "Expected at least 2 parents for place_2622000, got ${parent_count}"
  exit 1
fi

if [[ "${child_count}" -lt 3 ]]; then
  echo "Expected at least 3 children for place_2622000, got ${child_count}"
  exit 1
fi

echo
echo "-- missing geography"
missing_json="$(curl -s "${API_BASE_URL}/geographies/not_real/children")"
echo "${missing_json}" | jq '.detail.code // .error.code, .detail.message // .error.message'

if ! echo "${missing_json}" | jq -e '(.detail.code // .error.code) == "geography_not_found"' >/dev/null; then
  echo "Expected geography_not_found for missing geography"
  exit 1
fi

echo "Geography relationship API smoke passed."
