#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"
export PYTHONPATH="${PYTHONPATH:-.}"

echo "== OneHaven metric loader performance smoke =="
echo "DATABASE_URL=${DATABASE_URL}"
echo

python -m py_compile pipelines/transforms/common/market_metric_loader.py

pytest pipelines/tests/test_market_metric_loader.py -q
pytest pipelines/tests/test_market_metric_loader_bulk_integration.py -q

echo
echo "-- Running derived_market_ratios as a medium-size loader smoke"
time python -m pipelines.scripts.run_transforms derived_market_ratios

echo
echo "-- Checking loader integrity"
./scripts/smoke_metric_loader_integrity.sh

echo "Metric loader performance smoke passed."
