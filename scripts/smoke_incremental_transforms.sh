#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"
export PYTHONPATH="${PYTHONPATH:-.}"

echo "== OneHaven incremental transform smoke =="

python -m py_compile pipelines/common/transform_options.py
python -m py_compile pipelines/common/periods.py
python -m py_compile pipelines/scripts/run_transforms.py
python -m py_compile pipelines/transforms/zillow/value_rent_transform.py
python -m py_compile pipelines/transforms/bls_laus/labor_market_transform.py
python -m py_compile pipelines/transforms/derived/market_ratios_transform.py

pytest pipelines/tests/test_transform_options.py -q

echo
echo "-- Zillow recent 3 months"
time python -m pipelines.scripts.run_transforms --mode recent --recent-months 3 zillow_value_rent

echo
echo "-- BLS recent 3 months"
time python -m pipelines.scripts.run_transforms --mode recent --recent-months 3 bls_laus_labor_market

echo
echo "-- Derived ratios recent 3 months"
time python -m pipelines.scripts.run_transforms --mode recent --recent-months 3 derived_market_ratios

echo
echo "-- Detroit context"
curl -s "http://localhost:8000/markets/metro_19820/context" \
  | jq '.latest_period, .confidence_score, .coverage'

echo "Incremental transform smoke passed."
