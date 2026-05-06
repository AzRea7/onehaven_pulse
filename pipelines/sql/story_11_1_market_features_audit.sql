\echo '== Story 11.1 feature table row counts =='

SELECT
  feature_version,
  COUNT(*) AS rows,
  COUNT(DISTINCT geo_id) AS geo_count,
  MIN(period_month) AS min_period,
  MAX(period_month) AS max_period,
  COUNT(*) FILTER (WHERE is_trainable) AS trainable_rows,
  COUNT(*) FILTER (WHERE target_available) AS target_available_rows
FROM analytics.market_features_monthly
GROUP BY feature_version
ORDER BY feature_version;

\echo '== Story 11.1 representative market feature coverage =='

WITH representative_markets AS (
    SELECT *
    FROM (
        VALUES
            ('us'),
            ('metro_19820'),
            ('metro_16980'),
            ('metro_19100'),
            ('metro_12420'),
            ('metro_45300'),
            ('metro_38060'),
            ('metro_12060'),
            ('metro_42660'),
            ('metro_14460'),
            ('metro_31080'),
            ('metro_37980')
    ) AS t(geo_id)
),
coverage AS (
    SELECT
        geo_id,
        COUNT(*) AS rows,
        COUNT(*) FILTER (WHERE feature_completeness_score >= 0.8) AS high_completeness_rows,
        COUNT(*) FILTER (WHERE is_trainable) AS trainable_rows,
        COUNT(*) FILTER (WHERE target_available) AS target_available_rows,
        MIN(period_month) AS min_period,
        MAX(period_month) AS max_period
    FROM analytics.market_features_monthly
    WHERE geo_id IN (SELECT geo_id FROM representative_markets)
    GROUP BY geo_id
)
SELECT
    r.geo_id,
    COALESCE(c.rows, 0) AS rows,
    COALESCE(c.high_completeness_rows, 0) AS high_completeness_rows,
    COALESCE(c.trainable_rows, 0) AS trainable_rows,
    COALESCE(c.target_available_rows, 0) AS target_available_rows,
    c.min_period,
    c.max_period
FROM representative_markets r
LEFT JOIN coverage c
  ON c.geo_id = r.geo_id
ORDER BY r.geo_id;

\echo '== Story 11.1 leakage guard: target columns separated =='

SELECT
  COUNT(*) AS rows,
  COUNT(*) FILTER (
    WHERE quality_flags ->> 'point_in_time_safe' = 'true'
      AND quality_flags ->> 'target_columns_separated' = 'true'
  ) AS rows_with_safety_flags
FROM analytics.market_features_monthly;

\echo '== Story 11.1 duplicate key check =='

SELECT
  geo_id,
  period_month,
  feature_version,
  COUNT(*) AS duplicate_count
FROM analytics.market_features_monthly
GROUP BY geo_id, period_month, feature_version
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

\echo '== Story 11.1 sample rows =='

SELECT
  geo_id,
  period_month,
  price_growth_12m,
  rent_growth_12m,
  payment_to_income_ratio,
  unemployment_rate,
  feature_completeness_score,
  is_trainable,
  target_price_growth_12m,
  target_available,
  missing_feature_names
FROM analytics.market_features_monthly
WHERE geo_id IN ('metro_19820', 'metro_16980', 'metro_19100', 'metro_12420')
ORDER BY geo_id, period_month DESC
LIMIT 40;
