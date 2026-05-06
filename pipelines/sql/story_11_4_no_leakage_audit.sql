\echo '== Story 11.4 no-leakage: feature table summary =='

SELECT
  feature_version,
  COUNT(*) AS rows,
  COUNT(DISTINCT geo_id) AS geos,
  MIN(period_month) AS min_period,
  MAX(period_month) AS max_period,
  COUNT(*) FILTER (WHERE is_trainable) AS trainable_rows,
  COUNT(*) FILTER (WHERE target_available) AS target_available_rows
FROM analytics.market_features_monthly
GROUP BY feature_version
ORDER BY feature_version;

\echo '== Story 11.4 no-leakage: safety flags =='

SELECT
  COUNT(*) AS rows,
  COUNT(*) FILTER (
    WHERE quality_flags ->> 'point_in_time_safe' = 'true'
  ) AS point_in_time_safe_rows,
  COUNT(*) FILTER (
    WHERE quality_flags ->> 'target_columns_separated' = 'true'
  ) AS target_separated_rows
FROM analytics.market_features_monthly;

\echo '== Story 11.4 no-leakage: trainable rows require target_price_growth_12m =='

SELECT
  COUNT(*) AS bad_rows
FROM analytics.market_features_monthly
WHERE is_trainable = true
  AND target_price_growth_12m IS NULL;

\echo '== Story 11.4 no-leakage: target availability flag is consistent =='

SELECT
  COUNT(*) AS bad_rows
FROM analytics.market_features_monthly
WHERE (
    target_price_growth_12m IS NOT NULL
    OR target_rent_growth_12m IS NOT NULL
    OR target_drawdown_12m IS NOT NULL
    OR target_cycle_phase_12m IS NOT NULL
)
AND target_available = false;

\echo '== Story 11.4 no-leakage: missing_feature_names does not include target columns =='

SELECT
  COUNT(*) AS bad_rows
FROM analytics.market_features_monthly
WHERE missing_feature_names::text ILIKE '%target_%';

\echo '== Story 11.4 no-leakage: quality flags do not list target as missing feature =='

SELECT
  COUNT(*) AS bad_rows
FROM analytics.market_features_monthly
WHERE quality_flags::text ILIKE '%target_%'
  AND quality_flags::text NOT ILIKE '%target_columns_separated%';

\echo '== Story 11.4 no-leakage: duplicate key check =='

SELECT
  COUNT(*) AS duplicate_key_groups
FROM (
    SELECT geo_id, period_month, feature_version, COUNT(*) AS rows
    FROM analytics.market_features_monthly
    GROUP BY geo_id, period_month, feature_version
    HAVING COUNT(*) > 1
) duplicates;

\echo '== Story 11.4 no-leakage: source period must not exceed feature period =='

SELECT
  COUNT(*) AS bad_rows
FROM analytics.market_features_monthly
WHERE source_period_max > period_month;

\echo '== Story 11.4 no-leakage: latest periods are not trainable without future target =='

SELECT
  COUNT(*) AS bad_rows
FROM analytics.market_features_monthly
WHERE period_month >= (
    SELECT MAX(period_month) - INTERVAL '11 months'
    FROM analytics.market_features_monthly
)
AND is_trainable = true
AND target_price_growth_12m IS NULL;
