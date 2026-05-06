\echo '== Latest market metrics by geo =='
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM analytics.market_monthly_metrics
WHERE geo_id = 'us'
ORDER BY period_month DESC
LIMIT 1;

\echo '== Default time-series window by geo =='
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM analytics.market_monthly_metrics
WHERE geo_id = 'us'
ORDER BY period_month DESC
LIMIT 120;

\echo '== Metro map latest metric candidate rows =='
EXPLAIN (ANALYZE, BUFFERS)
WITH latest_period AS (
    SELECT max(period_month) AS period_month
    FROM analytics.market_monthly_metrics
)
SELECT m.geo_id, m.period_month, m.building_permits
FROM analytics.market_monthly_metrics m
JOIN geo.dim_geo g
    ON g.geo_id = m.geo_id
JOIN latest_period lp
    ON lp.period_month = m.period_month
WHERE g.geo_type = 'metro'
  AND g.is_active = TRUE
LIMIT 1000;

\echo '== Compare two markets default window =='
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM analytics.market_monthly_metrics
WHERE geo_id IN ('us', 'metro_19820')
ORDER BY geo_id, period_month DESC
LIMIT 240;
