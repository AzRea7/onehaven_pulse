BEGIN;

DELETE FROM analytics.market_features_monthly
WHERE feature_version = 'v1';

WITH base AS (
    SELECT
        m.geo_id,
        g.geo_type,
        m.period_month,

        m.zhvi,
        m.zori,
        m.active_listings,
        m.median_days_on_market,
        m.mortgage_rate_30y,
        m.unemployment_rate,
        m.price_to_income_ratio,
        m.payment_to_income_ratio,
        m.building_permits,

        LAG(m.zhvi, 1) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS zhvi_lag_1,
        LAG(m.zhvi, 3) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS zhvi_lag_3,
        LAG(m.zhvi, 12) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS zhvi_lag_12,

        LAG(m.zori, 1) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS zori_lag_1,
        LAG(m.zori, 3) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS zori_lag_3,
        LAG(m.zori, 12) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS zori_lag_12,

        LAG(m.active_listings, 3) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS active_listings_lag_3,
        LAG(m.active_listings, 12) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS active_listings_lag_12,
        LAG(m.median_days_on_market, 3) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS dom_lag_3,

        LAG(m.mortgage_rate_30y, 3) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS mortgage_rate_lag_3,
        LAG(m.unemployment_rate, 3) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS unemployment_lag_3,

        LEAD(m.zhvi, 12) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS zhvi_lead_12,
        LEAD(m.zori, 12) OVER (PARTITION BY m.geo_id ORDER BY m.period_month) AS zori_lead_12,

        MIN(m.zhvi) OVER (
            PARTITION BY m.geo_id
            ORDER BY m.period_month
            ROWS BETWEEN 1 FOLLOWING AND 12 FOLLOWING
        ) AS zhvi_forward_12m_min

    FROM analytics.market_monthly_metrics m
    LEFT JOIN geo.dim_geo g
      ON g.geo_id = m.geo_id
    WHERE m.period_month IS NOT NULL
),
feature_values AS (
    SELECT
        geo_id,
        geo_type,
        period_month,

        CASE WHEN zhvi_lag_1 IS NOT NULL AND zhvi_lag_1 <> 0 THEN ((zhvi / zhvi_lag_1) - 1)::numeric(14, 6) END AS price_growth_1m,
        CASE WHEN zhvi_lag_3 IS NOT NULL AND zhvi_lag_3 <> 0 THEN ((zhvi / zhvi_lag_3) - 1)::numeric(14, 6) END AS price_growth_3m,
        CASE WHEN zhvi_lag_12 IS NOT NULL AND zhvi_lag_12 <> 0 THEN ((zhvi / zhvi_lag_12) - 1)::numeric(14, 6) END AS price_growth_12m,

        CASE WHEN zori_lag_1 IS NOT NULL AND zori_lag_1 <> 0 THEN ((zori / zori_lag_1) - 1)::numeric(14, 6) END AS rent_growth_1m,
        CASE WHEN zori_lag_3 IS NOT NULL AND zori_lag_3 <> 0 THEN ((zori / zori_lag_3) - 1)::numeric(14, 6) END AS rent_growth_3m,
        CASE WHEN zori_lag_12 IS NOT NULL AND zori_lag_12 <> 0 THEN ((zori / zori_lag_12) - 1)::numeric(14, 6) END AS rent_growth_12m,

        CASE WHEN active_listings_lag_3 IS NOT NULL AND active_listings_lag_3 <> 0 THEN ((active_listings / active_listings_lag_3) - 1)::numeric(14, 6) END AS inventory_change_3m,
        CASE WHEN active_listings_lag_12 IS NOT NULL AND active_listings_lag_12 <> 0 THEN ((active_listings / active_listings_lag_12) - 1)::numeric(14, 6) END AS inventory_change_12m,
        CASE WHEN dom_lag_3 IS NOT NULL THEN (median_days_on_market - dom_lag_3)::numeric(14, 6) END AS days_on_market_change_3m,

        mortgage_rate_30y,
        CASE WHEN mortgage_rate_lag_3 IS NOT NULL THEN (mortgage_rate_30y - mortgage_rate_lag_3)::numeric(14, 6) END AS rate_change_3m,

        unemployment_rate,
        CASE WHEN unemployment_lag_3 IS NOT NULL THEN (unemployment_rate - unemployment_lag_3)::numeric(14, 6) END AS unemployment_change_3m,

        price_to_income_ratio,
        payment_to_income_ratio,

        CASE
            WHEN payment_to_income_ratio IS NULL THEN NULL
            WHEN payment_to_income_ratio <= 0.20 THEN 1.0
            WHEN payment_to_income_ratio >= 0.45 THEN 0.0
            ELSE ((0.45 - payment_to_income_ratio) / 0.25)::numeric(14, 6)
        END AS affordability_score,

        NULL::numeric AS cycle_score,
        NULL::integer AS cycle_phase_encoded,
        NULL::numeric AS confidence_score,

        zhvi,
        zori,
        active_listings,
        median_days_on_market,
        building_permits,

        period_month AS source_period_max,

        CASE WHEN zhvi_lead_12 IS NOT NULL AND zhvi IS NOT NULL AND zhvi <> 0
            THEN ((zhvi_lead_12 / zhvi) - 1)::numeric(14, 6)
        END AS target_price_growth_12m,

        CASE WHEN zori_lead_12 IS NOT NULL AND zori IS NOT NULL AND zori <> 0
            THEN ((zori_lead_12 / zori) - 1)::numeric(14, 6)
        END AS target_rent_growth_12m,

        CASE WHEN zhvi_forward_12m_min IS NOT NULL AND zhvi IS NOT NULL AND zhvi <> 0
            THEN ((zhvi_forward_12m_min / zhvi) - 1)::numeric(14, 6)
        END AS target_drawdown_12m,

        NULL::text AS target_cycle_phase_12m

    FROM base
),
features AS (
    SELECT
        *,
        (zhvi IS NOT NULL OR price_growth_12m IS NOT NULL) AS has_price,
        (zori IS NOT NULL OR rent_growth_12m IS NOT NULL) AS has_rent,
        (active_listings IS NOT NULL OR median_days_on_market IS NOT NULL) AS has_inventory,
        (price_to_income_ratio IS NOT NULL OR payment_to_income_ratio IS NOT NULL) AS has_affordability,
        (unemployment_rate IS NOT NULL) AS has_labor,
        (building_permits IS NOT NULL) AS has_permits
    FROM feature_values
),
quality AS (
    SELECT
        f.*,

        ARRAY_REMOVE(ARRAY[
            CASE WHEN price_growth_12m IS NULL THEN 'price_growth_12m' END,
            CASE WHEN rent_growth_12m IS NULL THEN 'rent_growth_12m' END,
            CASE WHEN payment_to_income_ratio IS NULL THEN 'payment_to_income_ratio' END,
            CASE WHEN unemployment_rate IS NULL THEN 'unemployment_rate' END
        ], NULL) AS missing_core_features,

        (
            (
                CASE WHEN price_growth_12m IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN rent_growth_12m IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN payment_to_income_ratio IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN unemployment_rate IS NOT NULL THEN 1 ELSE 0 END
            )::numeric / 4.0
        )::numeric(8, 6) AS feature_completeness_score

    FROM features f
)
INSERT INTO analytics.market_features_monthly (
    geo_id,
    period_month,
    geo_type,
    price_growth_1m,
    price_growth_3m,
    price_growth_12m,
    rent_growth_1m,
    rent_growth_3m,
    rent_growth_12m,
    inventory_change_3m,
    inventory_change_12m,
    days_on_market_change_3m,
    mortgage_rate_30y,
    rate_change_3m,
    unemployment_rate,
    unemployment_change_3m,
    price_to_income_ratio,
    payment_to_income_ratio,
    affordability_score,
    cycle_score,
    cycle_phase_encoded,
    confidence_score,
    has_price,
    has_rent,
    has_inventory,
    has_affordability,
    has_labor,
    has_permits,
    feature_completeness_score,
    missing_feature_names,
    source_period_max,
    is_trainable,
    quality_flags,
    target_price_growth_12m,
    target_rent_growth_12m,
    target_drawdown_12m,
    target_cycle_phase_12m,
    target_available,
    feature_version,
    created_at,
    updated_at
)
SELECT
    geo_id,
    period_month,
    geo_type,
    price_growth_1m,
    price_growth_3m,
    price_growth_12m,
    rent_growth_1m,
    rent_growth_3m,
    rent_growth_12m,
    inventory_change_3m,
    inventory_change_12m,
    days_on_market_change_3m,
    mortgage_rate_30y,
    rate_change_3m,
    unemployment_rate,
    unemployment_change_3m,
    price_to_income_ratio,
    payment_to_income_ratio,
    affordability_score,
    cycle_score,
    cycle_phase_encoded,
    confidence_score,
    has_price,
    has_rent,
    has_inventory,
    has_affordability,
    has_labor,
    has_permits,
    feature_completeness_score,
    to_jsonb(missing_core_features),
    source_period_max,
    (
        feature_completeness_score >= 0.75
        AND target_price_growth_12m IS NOT NULL
    ) AS is_trainable,
    jsonb_build_object(
        'feature_generation', 'story_11_1_market_features_monthly',
        'point_in_time_safe', true,
        'target_columns_separated', true,
        'missing_core_features', missing_core_features
    ) AS quality_flags,
    target_price_growth_12m,
    target_rent_growth_12m,
    target_drawdown_12m,
    target_cycle_phase_12m,
    (
        target_price_growth_12m IS NOT NULL
        OR target_rent_growth_12m IS NOT NULL
        OR target_drawdown_12m IS NOT NULL
        OR target_cycle_phase_12m IS NOT NULL
    ) AS target_available,
    'v1',
    now(),
    now()
FROM quality;

COMMIT;
