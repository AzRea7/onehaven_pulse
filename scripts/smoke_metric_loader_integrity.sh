#!/usr/bin/env bash
set -euo pipefail

echo "== OneHaven metric loader integrity smoke =="

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    geo_id,
    period_month,
    COUNT(*) AS rows
FROM analytics.market_monthly_metrics
GROUP BY geo_id, period_month
HAVING COUNT(*) > 1
LIMIT 20;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    geo_id,
    period_month,
    metric_name,
    source,
    dataset,
    COUNT(*) AS rows
FROM analytics.market_metric_sources
GROUP BY geo_id, period_month, metric_name, source, dataset
HAVING COUNT(*) > 1
LIMIT 20;
"

docker compose exec -T postgres psql -U onehaven -d onehaven_market -c "
SELECT
    source,
    dataset,
    COUNT(*) AS rows,
    COUNT(DISTINCT geo_id) AS geos,
    MIN(period_month) AS first_period,
    MAX(period_month) AS latest_period
FROM analytics.market_metric_sources
GROUP BY source, dataset
ORDER BY source, dataset;
"

echo "Metric loader integrity smoke passed if duplicate queries returned zero rows."
