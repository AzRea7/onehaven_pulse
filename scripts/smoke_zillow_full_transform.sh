#!/usr/bin/env bash
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market}"
export PYTHONPATH="${PYTHONPATH:-.}"

echo "== OneHaven full Zillow transform smoke =="
echo "DATABASE_URL=${DATABASE_URL}"
echo

time python -m pipelines.scripts.run_transforms zillow_value_rent

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
  AND source = 'zillow'
GROUP BY geo_id, metric_name, source, dataset
ORDER BY metric_name;
"

./scripts/smoke_metric_loader_integrity.sh

echo "Full Zillow transform smoke passed."
