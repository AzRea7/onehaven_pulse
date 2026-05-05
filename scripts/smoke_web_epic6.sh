#!/usr/bin/env bash
set -euo pipefail

WEB_BASE_URL="${WEB_BASE_URL:-http://localhost:3000}"

echo "== OneHaven Epic 6 web route smoke =="
echo "WEB_BASE_URL=${WEB_BASE_URL}"
echo

check_route() {
  local route="$1"
  local url="${WEB_BASE_URL}${route}"

  echo "-- ${route}"
  status="$(curl -sS -o /dev/null -w "%{http_code}" "${url}")"

  if [[ "${status}" != "200" && "${status}" != "307" && "${status}" != "308" ]]; then
    echo "Route failed: ${url}"
    echo "HTTP status: ${status}"
    exit 1
  fi

  echo "status=${status}"
  echo
}

check_route "/dashboard"
check_route "/map"
check_route "/markets"
check_route "/markets/us"
check_route "/markets/metro_19820"
check_route "/compare"
check_route "/screener"
check_route "/admin/source-freshness"

echo "Epic 6 web route smoke passed."
