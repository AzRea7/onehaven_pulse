-- Epic 4 metric coverage and data quality report

\echo '== mart completeness =='

SELECT
    COUNT(*) AS mart_rows,
    COUNT(DISTINCT geo_id) AS geo_count,
    MIN(period_month) AS min_period,
    MAX(period_month) AS max_period,
    COUNT(*) FILTER (WHERE median_sale_price IS NOT NULL OR zhvi IS NOT NULL) AS price_rows,
    COUNT(*) FILTER (WHERE median_rent IS NOT NULL OR zori IS NOT NULL) AS rent_rows,
    COUNT(*) FILTER (WHERE unemployment_rate IS NOT NULL) AS unemployment_rows,
    COUNT(*) FILTER (WHERE population IS NOT NULL) AS population_rows,
    COUNT(*) FILTER (WHERE building_permits IS NOT NULL OR permit_units IS NOT NULL) AS permit_rows,
    COUNT(*) FILTER (WHERE hazard_risk_score IS NOT NULL) AS hazard_rows,
    COUNT(*) FILTER (WHERE hmda_applications IS NOT NULL) AS hmda_rows,
    COUNT(*) FILTER (WHERE amenity_place_count IS NOT NULL) AS amenity_rows,
    COUNT(*) FILTER (WHERE estimated_monthly_payment IS NOT NULL) AS payment_rows
FROM analytics.market_monthly_metrics;

\echo '== metric coverage =='

SELECT
    metric_name,
    source,
    dataset,
    COUNT(*) AS source_rows,
    COUNT(DISTINCT geo_id) AS geo_count,
    MIN(period_month) AS min_period,
    MAX(period_month) AS max_period,
    COUNT(DISTINCT period_month) AS period_count
FROM analytics.market_metric_sources
GROUP BY metric_name, source, dataset
ORDER BY source, dataset, metric_name;

\echo '== latest transform runs =='

WITH ranked_runs AS (
    SELECT
        pipeline_name,
        source,
        dataset,
        status,
        records_extracted,
        records_loaded,
        records_failed,
        error_message,
        started_at,
        finished_at,
        ROW_NUMBER() OVER (
            PARTITION BY pipeline_name
            ORDER BY started_at DESC
        ) AS row_number
    FROM audit.pipeline_runs
    WHERE pipeline_name LIKE '%transform%'
)
SELECT
    pipeline_name,
    source,
    dataset,
    status,
    records_extracted,
    records_loaded,
    records_failed,
    error_message,
    started_at,
    finished_at
FROM ranked_runs
WHERE row_number = 1
ORDER BY pipeline_name;

\echo '== source trace duplicates =='

SELECT
    geo_id,
    period_month,
    metric_name,
    source,
    dataset,
    COUNT(*) AS duplicate_count
FROM analytics.market_metric_sources
GROUP BY geo_id, period_month, metric_name, source, dataset
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, source, dataset, metric_name;

\echo '== overture category health =='

SELECT
    area_slug,
    area_name,
    COUNT(*) AS raw_rows,
    COUNT(primary_category) AS categorized_rows,
    ROUND(
        COUNT(primary_category)::numeric / NULLIF(COUNT(*)::numeric, 0),
        4
    ) AS categorized_ratio
FROM raw.overture_places
GROUP BY area_slug, area_name
ORDER BY raw_rows DESC;

\echo '== overture category distribution =='

SELECT
    primary_category,
    COUNT(*) AS rows
FROM raw.overture_places
GROUP BY primary_category
ORDER BY rows DESC
LIMIT 50;

\echo '== hmda raw health =='

SELECT
    activity_year,
    COUNT(*) AS raw_rows,
    COUNT(loan_amount) AS loan_amount_rows,
    COUNT(*) FILTER (WHERE action_taken = '1') AS originations,
    COUNT(*) FILTER (WHERE action_taken = '3') AS denials,
    ROUND(
        COUNT(*) FILTER (WHERE action_taken = '3')::numeric
        / NULLIF(COUNT(*)::numeric, 0),
        4
    ) AS raw_denial_ratio
FROM raw.hmda_modified_lar
GROUP BY activity_year
ORDER BY activity_year DESC;

\echo '== geography counts =='

SELECT
    geo_type,
    COUNT(*) AS rows
FROM geo.dim_geo
GROUP BY geo_type
ORDER BY geo_type;
