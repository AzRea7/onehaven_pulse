\echo '== Story 13.1 market data quality audit =='

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
quality AS (
    SELECT
        g.geo_id,
        g.geo_type,
        g.display_name,
        g.name,
        g.state_code,

        m.period_month AS latest_period,

        -- Price
        (m.zhvi IS NOT NULL OR m.home_price_index IS NOT NULL OR m.median_sale_price IS NOT NULL) AS has_price_level,
        (m.zhvi_yoy IS NOT NULL OR m.home_price_index_yoy IS NOT NULL OR m.median_sale_price_yoy IS NOT NULL) AS has_price_growth,

        -- Rent
        (m.zori IS NOT NULL OR m.median_rent IS NOT NULL) AS has_rent_level,
        (m.zori_yoy IS NOT NULL OR m.median_rent_yoy IS NOT NULL) AS has_rent_growth,

        -- Inventory
        (m.active_listings IS NOT NULL OR m.months_supply IS NOT NULL OR m.median_days_on_market IS NOT NULL) AS has_inventory,

        -- Affordability
        (m.payment_to_income_ratio IS NOT NULL OR m.price_to_income_ratio IS NOT NULL OR m.estimated_monthly_payment IS NOT NULL) AS has_affordability,

        -- Labor
        (m.unemployment_rate IS NOT NULL) AS has_labor,

        -- Permits / supply
        (m.building_permits IS NOT NULL OR m.permits_per_1000_people IS NOT NULL) AS has_permits,

        -- Rates / macro
        (m.mortgage_rate_30y IS NOT NULL OR m.fed_funds_rate IS NOT NULL) AS has_rates,

        -- Population / income
        (m.population IS NOT NULL OR m.median_household_income IS NOT NULL OR m.households IS NOT NULL) AS has_demographics,

        -- Sanity checks
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

        CASE
            WHEN m.zhvi IS NOT NULL
             AND m.zhvi <= 0
            THEN true ELSE false
        END AS bad_zhvi,

        CASE
            WHEN m.zori IS NOT NULL
             AND m.zori <= 0
            THEN true ELSE false
        END AS bad_zori,

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

        (
            CASE WHEN has_price_growth THEN 1 ELSE 0 END +
            CASE WHEN has_rent_growth THEN 1 ELSE 0 END +
            CASE WHEN has_inventory THEN 1 ELSE 0 END +
            CASE WHEN has_affordability THEN 1 ELSE 0 END +
            CASE WHEN has_labor THEN 1 ELSE 0 END +
            CASE WHEN has_permits THEN 1 ELSE 0 END
        ) AS available_core_categories,

        (
            CASE WHEN has_price_growth THEN 1 ELSE 0 END +
            CASE WHEN has_rent_growth THEN 1 ELSE 0 END +
            CASE WHEN has_inventory THEN 1 ELSE 0 END +
            CASE WHEN has_affordability THEN 1 ELSE 0 END +
            CASE WHEN has_labor THEN 1 ELSE 0 END +
            CASE WHEN has_permits THEN 1 ELSE 0 END
        )::numeric / 6.0 AS raw_coverage_score,

        (
            bad_payment_to_income
            OR bad_unemployment_rate
            OR bad_zhvi
            OR bad_zori
            OR bad_dom
        ) AS has_bad_values

    FROM quality
)
SELECT
    geo_id,
    geo_type,
    COALESCE(display_name, name) AS market_name,
    state_code,
    latest_period,

    has_price_growth,
    has_rent_growth,
    has_inventory,
    has_affordability,
    has_labor,
    has_permits,

    available_core_categories,
    ROUND(raw_coverage_score, 4) AS raw_coverage_score,
    has_bad_values,

    ARRAY_REMOVE(ARRAY[
        CASE WHEN NOT has_price_growth THEN 'price_growth' END,
        CASE WHEN NOT has_rent_growth THEN 'rent_growth' END,
        CASE WHEN NOT has_inventory THEN 'inventory' END,
        CASE WHEN NOT has_affordability THEN 'affordability' END,
        CASE WHEN NOT has_labor THEN 'labor' END,
        CASE WHEN NOT has_permits THEN 'permits' END,
        CASE WHEN bad_payment_to_income THEN 'bad_payment_to_income' END,
        CASE WHEN bad_unemployment_rate THEN 'bad_unemployment_rate' END,
        CASE WHEN bad_zhvi THEN 'bad_zhvi' END,
        CASE WHEN bad_zori THEN 'bad_zori' END,
        CASE WHEN bad_dom THEN 'bad_dom' END
    ], NULL) AS data_quality_issues

FROM scored
ORDER BY
    raw_coverage_score ASC,
    has_bad_values DESC,
    geo_type,
    market_name;
