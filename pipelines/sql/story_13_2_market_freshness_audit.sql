\echo '== Story 13.2 market freshness audit =='

WITH metric_families AS (
    SELECT * FROM (
        VALUES
            ('price', 'zhvi_yoy', 75),
            ('price', 'home_price_index_yoy', 75),
            ('price', 'median_sale_price_yoy', 75),

            ('rent', 'zori_yoy', 75),
            ('rent', 'median_rent_yoy', 75),

            ('inventory', 'active_listings_yoy', 75),
            ('inventory', 'months_supply', 75),
            ('inventory', 'median_days_on_market', 75),

            ('affordability', 'payment_to_income_ratio', 75),
            ('affordability', 'price_to_income_ratio', 75),
            ('affordability', 'estimated_monthly_payment', 75),

            ('labor', 'unemployment_rate', 75),

            ('permits', 'building_permits', 120),
            ('permits', 'permits_per_1000_people', 120),

            ('rates', 'mortgage_rate_30y', 45),
            ('rates', 'fed_funds_rate', 45),

            ('demographics', 'population', 730),
            ('demographics', 'median_household_income', 730),
            ('demographics', 'households', 730)
    ) AS t(metric_family, metric_name, max_age_days)
),
long_metrics AS (
    SELECT geo_id, period_month, 'zhvi_yoy' AS metric_name, zhvi_yoy AS value FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'home_price_index_yoy', home_price_index_yoy FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'median_sale_price_yoy', median_sale_price_yoy FROM analytics.market_monthly_metrics

    UNION ALL SELECT geo_id, period_month, 'zori_yoy', zori_yoy FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'median_rent_yoy', median_rent_yoy FROM analytics.market_monthly_metrics

    UNION ALL SELECT geo_id, period_month, 'active_listings_yoy', active_listings_yoy FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'months_supply', months_supply FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'median_days_on_market', median_days_on_market FROM analytics.market_monthly_metrics

    UNION ALL SELECT geo_id, period_month, 'payment_to_income_ratio', payment_to_income_ratio FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'price_to_income_ratio', price_to_income_ratio FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'estimated_monthly_payment', estimated_monthly_payment FROM analytics.market_monthly_metrics

    UNION ALL SELECT geo_id, period_month, 'unemployment_rate', unemployment_rate FROM analytics.market_monthly_metrics

    UNION ALL SELECT geo_id, period_month, 'building_permits', building_permits FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'permits_per_1000_people', permits_per_1000_people FROM analytics.market_monthly_metrics

    UNION ALL SELECT geo_id, period_month, 'mortgage_rate_30y', mortgage_rate_30y FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'fed_funds_rate', fed_funds_rate FROM analytics.market_monthly_metrics

    UNION ALL SELECT geo_id, period_month, 'population', population FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'median_household_income', median_household_income FROM analytics.market_monthly_metrics
    UNION ALL SELECT geo_id, period_month, 'households', households FROM analytics.market_monthly_metrics
),
latest_metric AS (
    SELECT
        lm.geo_id,
        mf.metric_family,
        lm.metric_name,
        MAX(lm.period_month) FILTER (WHERE lm.value IS NOT NULL) AS latest_period,
        mf.max_age_days
    FROM long_metrics lm
    JOIN metric_families mf
      ON mf.metric_name = lm.metric_name
    GROUP BY lm.geo_id, mf.metric_family, lm.metric_name, mf.max_age_days
),
freshness AS (
    SELECT
        g.geo_id,
        g.geo_type,
        COALESCE(g.display_name, g.name) AS market_name,
        g.state_code,
        lm.metric_family,
        lm.metric_name,
        lm.latest_period,
        lm.max_age_days,
        CASE
            WHEN lm.latest_period IS NULL THEN NULL
            ELSE CURRENT_DATE - lm.latest_period
        END AS age_days,
        CASE
            WHEN lm.latest_period IS NULL THEN 'missing'
            WHEN CURRENT_DATE - lm.latest_period <= lm.max_age_days THEN 'fresh'
            ELSE 'stale'
        END AS freshness_status
    FROM geo.dim_geo g
    LEFT JOIN latest_metric lm
      ON lm.geo_id = g.geo_id
    WHERE g.is_active = true
)
SELECT
    geo_id,
    geo_type,
    market_name,
    state_code,
    metric_family,
    metric_name,
    latest_period,
    age_days,
    max_age_days,
    freshness_status
FROM freshness
ORDER BY
    freshness_status DESC,
    age_days DESC NULLS LAST,
    geo_type,
    market_name,
    metric_family,
    metric_name;
