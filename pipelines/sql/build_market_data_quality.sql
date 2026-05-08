BEGIN;

DELETE FROM analytics.market_data_quality
WHERE quality_version = 'v1'
  AND as_of_date = CURRENT_DATE;

WITH latest AS (
    SELECT
        geo_id,
        MAX(period_month) AS latest_period
    FROM analytics.market_monthly_metrics
    GROUP BY geo_id
),
latest_rows AS (
    SELECT m.*
    FROM analytics.market_monthly_metrics m
    JOIN latest l
      ON l.geo_id = m.geo_id
     AND l.latest_period = m.period_month
),
base AS (
    SELECT
        g.geo_id,
        m.period_month AS latest_period,

        (m.zhvi_yoy IS NOT NULL OR m.home_price_index_yoy IS NOT NULL OR m.median_sale_price_yoy IS NOT NULL) AS has_price,
        (m.zori_yoy IS NOT NULL OR m.median_rent_yoy IS NOT NULL) AS has_rent,
        (m.active_listings IS NOT NULL OR m.months_supply IS NOT NULL OR m.median_days_on_market IS NOT NULL) AS has_inventory,
        (m.payment_to_income_ratio IS NOT NULL OR m.price_to_income_ratio IS NOT NULL OR m.estimated_monthly_payment IS NOT NULL) AS has_affordability,
        (m.unemployment_rate IS NOT NULL) AS has_labor,
        (m.building_permits IS NOT NULL OR m.permits_per_1000_people IS NOT NULL) AS has_permits,

        CASE
            WHEN m.payment_to_income_ratio IS NOT NULL
             AND (m.payment_to_income_ratio < 0 OR m.payment_to_income_ratio > 1.5)
            THEN true ELSE false
        END AS bad_payment_to_income,

        CASE
            WHEN m.unemployment_rate IS NOT NULL
             AND (m.unemployment_rate < 0 OR m.unemployment_rate > 40)
            THEN true ELSE false
        END AS bad_unemployment_rate,

        CASE WHEN m.zhvi IS NOT NULL AND m.zhvi <= 0 THEN true ELSE false END AS bad_zhvi,
        CASE WHEN m.zori IS NOT NULL AND m.zori <= 0 THEN true ELSE false END AS bad_zori,

        CASE
            WHEN m.median_days_on_market IS NOT NULL
             AND (m.median_days_on_market < 0 OR m.median_days_on_market > 730)
            THEN true ELSE false
        END AS bad_dom

    FROM geo.dim_geo g
    LEFT JOIN latest_rows m
      ON m.geo_id = g.geo_id
    WHERE g.is_active = true
),
scored AS (
    SELECT
        *,
        ARRAY_REMOVE(ARRAY[
            CASE WHEN NOT has_price THEN 'price' END,
            CASE WHEN NOT has_rent THEN 'rent' END,
            CASE WHEN NOT has_inventory THEN 'inventory' END,
            CASE WHEN NOT has_affordability THEN 'affordability' END,
            CASE WHEN NOT has_labor THEN 'labor' END,
            CASE WHEN NOT has_permits THEN 'permits' END
        ], NULL) AS missing_categories,

        ARRAY_REMOVE(ARRAY[
            CASE
                WHEN latest_period IS NULL THEN 'all'
                WHEN CURRENT_DATE - latest_period > 90 THEN 'latest_period'
            END
        ], NULL) AS stale_categories,

        ARRAY_REMOVE(ARRAY[
            CASE WHEN bad_payment_to_income THEN 'bad_payment_to_income' END,
            CASE WHEN bad_unemployment_rate THEN 'bad_unemployment_rate' END,
            CASE WHEN bad_zhvi THEN 'bad_zhvi' END,
            CASE WHEN bad_zori THEN 'bad_zori' END,
            CASE WHEN bad_dom THEN 'bad_dom' END
        ], NULL) AS quality_issues
    FROM base
),
final AS (
    SELECT
        *,
        (
            CASE WHEN has_price THEN 1 ELSE 0 END +
            CASE WHEN has_rent THEN 1 ELSE 0 END +
            CASE WHEN has_inventory THEN 1 ELSE 0 END +
            CASE WHEN has_affordability THEN 1 ELSE 0 END +
            CASE WHEN has_labor THEN 1 ELSE 0 END +
            CASE WHEN has_permits THEN 1 ELSE 0 END
        )::numeric / 6.0 AS coverage_score,

        CASE
            WHEN latest_period IS NULL THEN 0::numeric
            WHEN CURRENT_DATE - latest_period <= 75 THEN 1::numeric
            WHEN CURRENT_DATE - latest_period <= 120 THEN 0.75::numeric
            WHEN CURRENT_DATE - latest_period <= 180 THEN 0.5::numeric
            ELSE 0::numeric
        END AS freshness_score,

        CASE
            WHEN array_length(quality_issues, 1) IS NULL THEN 1::numeric
            ELSE GREATEST(0::numeric, 1::numeric - array_length(quality_issues, 1)::numeric * 0.25)
        END AS validity_score
    FROM scored
)
INSERT INTO analytics.market_data_quality (
    geo_id,
    quality_version,
    as_of_date,
    latest_period,

    coverage_score,
    freshness_score,
    validity_score,
    overall_quality_score,

    has_price,
    has_rent,
    has_inventory,
    has_affordability,
    has_labor,
    has_permits,

    is_fresh,
    has_bad_values,

    missing_categories,
    stale_categories,
    quality_issues
)
SELECT
    geo_id,
    'v1',
    CURRENT_DATE,
    latest_period,

    ROUND(coverage_score, 4),
    ROUND(freshness_score, 4),
    ROUND(validity_score, 4),
    ROUND((coverage_score * 0.50 + freshness_score * 0.30 + validity_score * 0.20), 4),

    has_price,
    has_rent,
    has_inventory,
    has_affordability,
    has_labor,
    has_permits,

    freshness_score >= 0.75,
    array_length(quality_issues, 1) IS NOT NULL,

    missing_categories,
    stale_categories,
    quality_issues
FROM final;

COMMIT;

SELECT
    COUNT(*) AS rows,
    ROUND(AVG(overall_quality_score), 4) AS avg_quality,
    COUNT(*) FILTER (WHERE overall_quality_score = 1.0) AS perfect_rows,
    COUNT(*) FILTER (WHERE is_fresh) AS fresh_rows,
    COUNT(*) FILTER (WHERE has_bad_values) AS bad_value_rows
FROM analytics.market_data_quality
WHERE quality_version = 'v1'
  AND as_of_date = CURRENT_DATE;
