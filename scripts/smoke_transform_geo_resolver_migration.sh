#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"

echo "== OneHaven transform geography resolver migration smoke =="
echo "DATABASE_URL=${DATABASE_URL}"
echo

python -m py_compile pipelines/common/geography/resolver.py
python -m py_compile pipelines/transforms/zillow/value_rent_transform.py
python -m py_compile pipelines/transforms/bls_laus/labor_market_transform.py

PYTHONPATH=. pytest pipelines/tests/test_geography_resolver.py -q

./scripts/smoke_geo_crosswalk_seed.sh
./scripts/smoke_geo_resolver.sh

PYTHONPATH=. python -m pipelines.scripts.run_transforms zillow_value_rent
PYTHONPATH=. python -m pipelines.scripts.run_transforms bls_laus_labor_market
PYTHONPATH=. python -m pipelines.scripts.run_transforms derived_market_ratios

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    geo_id,
    metric_name,
    source,
    dataset,
    COUNT(*) AS rows,
    MIN(period_month) AS first_period,
    MAX(period_month) AS latest_period
FROM analytics.market_metric_sources
WHERE geo_id = 'metro_19820'
  AND source IN ('zillow', 'bls_laus')
GROUP BY geo_id, metric_name, source, dataset
ORDER BY source, metric_name;
"

curl -s "http://localhost:8000/markets/metro_19820/context" \
  | jq '.latest_period, .confidence_score, .coverage'

echo "Transform geography resolver migration smoke passed."
