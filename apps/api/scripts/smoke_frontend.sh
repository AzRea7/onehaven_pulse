#!/usr/bin/env bash
set -euo pipefail

WEB_BASE_URL="${WEB_BASE_URL:-http://localhost:3000}"
ARTIFACT_DIR="${ARTIFACT_DIR:-.smoke/frontend}"
mkdir -p "${ARTIFACT_DIR}"

echo "== OneHaven frontend smoke =="
echo "WEB_BASE_URL=${WEB_BASE_URL}"
echo "ARTIFACT_DIR=${ARTIFACT_DIR}"
echo

check_route() {
  local route="$1"
  local safe_name
  safe_name="$(echo "${route}" | sed 's#/#_#g; s#[^A-Za-z0-9_.-]#_#g')"
  local output_file="${ARTIFACT_DIR}/${safe_name:-root}.html"
  local url="${WEB_BASE_URL}${route}"

  echo "-- ${route}"
  echo "GET ${url}"

  http_status="$(curl -sS -L -w "%{http_code}" -o "${output_file}" "${url}" || true)"

  if [[ "${http_status}" != "200" && "${http_status}" != "307" && "${http_status}" != "308" ]]; then
    echo "FAILED: frontend route ${route}"
    echo "HTTP status: ${http_status}"
    echo "Response preview:"
    head -80 "${output_file}" || true
    echo
    exit 1
  fi

  if [[ ! -s "${output_file}" ]]; then
    echo "FAILED: frontend route ${route} returned empty body"
    exit 1
  fi

  echo "status=${http_status}"
  echo
}

check_route "/"
check_route "/dashboard"
check_route "/map"
check_route "/markets"
check_route "/markets/us"
check_route "/markets/metro_19820"
check_route "/compare"
check_route "/screener"
check_route "/admin/source-freshness"

echo "Frontend smoke passed."
