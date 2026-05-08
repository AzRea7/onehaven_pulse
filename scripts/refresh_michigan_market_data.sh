#!/usr/bin/env bash
set -euo pipefail

RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
LOG_DIR="${LOG_DIR:-logs}"
mkdir -p "${LOG_DIR}" data/diagnostics/michigan .smoke/michigan_data

LOG_FILE="${LOG_DIR}/michigan_data_refresh_$(date -u +"%Y%m%dT%H%M%SZ").log"

exec > >(tee -a "${LOG_FILE}") 2>&1

echo "== Michigan market data refresh =="
echo "started_at=${RUN_STARTED_AT}"
echo "log_file=${LOG_FILE}"
echo

export PYTHONPATH="${PYTHONPATH:-.}"
export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@127.0.0.1:5432/onehaven_market}"

echo "-- Environment"
echo "PYTHONPATH=${PYTHONPATH}"
echo "DATABASE_URL=${DATABASE_URL}"
echo

echo "-- Docker services"
docker compose up -d postgres api web
docker compose ps
echo

echo "-- API readiness"
for i in $(seq 1 30); do
  if curl -fsS "http://localhost:8000/ready" >/tmp/onehaven_ready.json; then
    cat /tmp/onehaven_ready.json
    echo
    break
  fi

  echo "API not ready yet, attempt ${i}/30"
  sleep 2
done

echo
echo "-- Alembic current"
docker compose exec -T api alembic current || {
  echo "Alembic current failed. Stop here."
  exit 1
}

echo
echo "-- Available extractors"
python -m pipelines.scripts.run_raw_extractors --list | tee .smoke/michigan_data/available_extractors.txt

FAILED_PIPELINES=()

echo
echo "-- Available transforms"
python -m pipelines.scripts.run_transforms --list | tee .smoke/michigan_data/available_transforms.txt

raw_pipeline_exists() {
  local name="$1"
  grep -E "^- ${name}([[:space:]]|$)" .smoke/michigan_data/available_extractors.txt >/dev/null 2>&1
}

transform_pipeline_exists() {
  local name="$1"
  grep -E "^${name}([[:space:]]|$)" .smoke/michigan_data/available_transforms.txt >/dev/null 2>&1
}

run_extractor_if_available() {
  local name="$1"

  if raw_pipeline_exists "${name}"; then
    echo
    echo "-- Running extractor: ${name}"
    if ! python -m pipelines.scripts.run_raw_extractors "${name}"; then
      echo "FAILED extractor: ${name}"
      FAILED_PIPELINES+=("extractor:${name}")
    fi
  else
    echo
    echo "-- Skipping extractor not registered: ${name}"
  fi
}

run_transform_if_available() {
  local name="$1"

  if transform_pipeline_exists "${name}"; then
    echo
    echo "-- Running transform: ${name}"
    if ! python -m pipelines.scripts.run_transforms "${name}"; then
      echo "FAILED transform: ${name}"
      FAILED_PIPELINES+=("transform:${name}")
    fi
  else
    echo
    echo "-- Skipping transform not registered: ${name}"
  fi
}

echo
echo "== Source refresh =="

# Core Michigan investor workflow sources.
run_extractor_if_available "zillow"
run_extractor_if_available "redfin"
run_extractor_if_available "fred"
run_extractor_if_available "fhfa"
run_extractor_if_available "census_bps"
run_extractor_if_available "census_acs"
run_extractor_if_available "bls_laus"

# Optional/heavier sources. Enabled only if registered and you want them.
if [ "${RUN_HEAVY_SOURCES:-0}" = "1" ]; then
  run_extractor_if_available "hmda"
  run_extractor_if_available "fema_nri"
  run_extractor_if_available "overture_maps"
fi

echo
echo "== Transform refresh =="

run_transform_if_available "zillow_value_rent"
run_transform_if_available "redfin_market_tracker"
run_transform_if_available "fred_macro_monthly"
run_transform_if_available "fhfa_hpi"
run_transform_if_available "census_building_permits"
run_transform_if_available "census_acs_profile"
run_transform_if_available "bls_laus_labor_market"
run_transform_if_available "derived_market_ratios"

if [ "${RUN_HEAVY_SOURCES:-0}" = "1" ]; then
  run_transform_if_available "hmda_mortgage_credit"
  run_transform_if_available "fema_nri_hazard_risk"
  run_transform_if_available "overture_places_amenities"
fi

echo
echo "== Derived layer refresh =="

if [ -f "pipelines/features/build_market_features_monthly.py" ]; then
  echo "-- Building market features"
  python pipelines/features/build_market_features_monthly.py
else
  echo "-- Skipping market features: script not found"
fi

if [ -f "pipelines/quality/build_market_data_quality.py" ]; then
  echo "-- Building market data quality"
  python pipelines/quality/build_market_data_quality.py
else
  echo "-- Skipping market data quality: script not found"
fi

echo
echo "== Michigan validation =="

./apps/api/scripts/smoke_michigan_data_freshness.sh

echo
echo "== Michigan jurisdiction status report =="

python scripts/report_michigan_jurisdiction_status.py

echo
echo "== Pipeline failure summary =="

if [ "${#FAILED_PIPELINES[@]}" -gt 0 ]; then
  printf '%s\n' "${FAILED_PIPELINES[@]}" | tee data/diagnostics/michigan/failed_pipelines_latest.txt
  echo
  echo "Refresh completed with pipeline failures."
  echo "started_at=${RUN_STARTED_AT}"
  echo "finished_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "log_file=${LOG_FILE}"

  # Do not hide failures from Task Scheduler / CI.
  exit 1
else
  rm -f data/diagnostics/michigan/failed_pipelines_latest.txt
fi

echo
echo "== Michigan data refresh complete =="
echo "started_at=${RUN_STARTED_AT}"
echo "finished_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "log_file=${LOG_FILE}"
